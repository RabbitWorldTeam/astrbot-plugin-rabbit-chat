"""RabbitCore Bridge v1 endpoint; independent from AstrBot for protocol tests."""
import asyncio
import hmac
import json
import re
import time
import uuid
from collections import OrderedDict

from aiohttp import web, WSMsgType


def clean(value, limit=2000):
    value = re.sub(r"§.", "", str(value))
    return "".join(c for c in value if c.isprintable() or c == " ")[:limit].strip()


def command(text):
    parts = text.strip().split()
    if parts and parts[0].lower() in ("白名单", "/白名单", "whitelist", "/whitelist"):
        parts.insert(0, "rabbit")
    if not parts or parts[0].lower() not in ("rabbit", "/rabbit"):
        return None
    if parts[1:] in (["状态"], ["status"]):
        return {"action": "connection"}
    if len(parts) < 3 or parts[1].lower() not in ("白名单", "whitelist"):
        raise ValueError("用法：/rabbit 白名单 添加|移除|暂停|恢复|查看 玩家名；列表 [页码]；状态")
    aliases = {"添加": "add", "移除": "remove", "暂停": "pause", "恢复": "resume",
               "启用": "resume", "查看": "info", "查询": "info", "列表": "list", "状态": "status"}
    action = aliases.get(parts[2], parts[2].lower())
    args = parts[3:]
    if action in ("add", "remove", "pause", "resume", "info"):
        if len(args) != 1 or not re.fullmatch(r"[A-Za-z0-9_]{3,16}", args[0]):
            raise ValueError("玩家名必须为 3–16 位英文字母、数字或下划线，且只能填写一个。")
        result = {"action": action, "name": args[0]}
        if action in ("pause", "resume"):
            result.update(action="set", enabled=action == "resume")
        return result
    if action == "status" and not args:
        return {"action": action}
    if action == "list" and (not args or len(args) == 1 and args[0].isascii() and args[0].isdigit() and 0 < int(args[0]) <= 1000000):
        return {"action": action, "page": int(args[0]) if args else 1}
    raise ValueError("不支持的操作或参数；不允许通过群聊关闭整个白名单。")


def result_text(data):
    if not data.get("ok"):
        return f"白名单操作失败 [{clean(data.get('code', 'ERROR'), 64)}]：{clean(data.get('message', ''), 500)}"
    lines = ["白名单：" + clean(data.get("message", "操作成功"), 500)]
    if isinstance(data.get("enabled"), bool):
        lines.append("全局白名单：" + ("开启" if data["enabled"] else "关闭"))
    entries = data.get("entries", [])
    if isinstance(data.get("entry"), dict):
        entries = [data["entry"]]
    for entry in entries[:20]:
        if isinstance(entry, dict):
            lines.append(f"{clean(entry.get('name', ''), 16)}：{'启用' if entry.get('enabled') else '暂停'}")
    if "page" in data:
        lines.append(f"第 {data.get('page')} / {data.get('pages')} 页，共 {data.get('total')} 人")
    return "\n".join(lines)


class Bridge:
    def __init__(self, config, deliver):
        self.config, self.deliver = config, deliver
        self.ws = None
        self.runner = None
        self.pending = {}
        self.seen = OrderedDict()
        self.capabilities = set()
        self.tokens, self.token_time = 10.0, time.monotonic()
        self.send_lock = asyncio.Lock()
        self.expiry_task = None

    @property
    def ready(self):
        return self.ws is not None and not self.ws.closed

    def envelope(self, kind, data):
        return dict(version=1, id=str(uuid.uuid4()), server_id=self.config["server_id"],
                    timestamp=int(time.time() * 1000), type=kind, data=data)

    async def start(self):
        if not self.config.get("enabled"):
            return
        if len(self.config.get("token", "")) < 24:
            raise ValueError("RabbitChat token 至少需要 24 个字符")
        if not re.fullmatch(r"[A-Za-z0-9_-]{1,64}", self.config["server_id"]):
            raise ValueError("无效 server_id")
        app = web.Application(client_max_size=32768)
        app.router.add_get("/rabbit/ws", self.handle)
        self.runner = web.AppRunner(app, access_log=None)
        try:
            await self.runner.setup()
            await web.TCPSite(self.runner, self.config["host"], int(self.config["port"])).start()
            self.expiry_task = asyncio.create_task(self.expire())
        except BaseException:
            await self.stop()
            raise

    async def stop(self):
        if self.expiry_task:
            self.expiry_task.cancel()
            await asyncio.gather(self.expiry_task, return_exceptions=True)
            self.expiry_task = None
        if self.ws:
            await self.ws.close(code=1001)
        if self.runner:
            await self.runner.cleanup()
            self.runner = None

    async def emit(self, kind, data):
        async with self.send_lock:
            if not self.ready:
                raise ConnectionError("游戏服务器未连接")
            message = self.envelope(kind, data)
            await asyncio.wait_for(self.ws.send_json(message), 5)
            return message["id"]

    def consume(self):
        now = time.monotonic()
        self.tokens = min(10, self.tokens + (now - self.token_time) * 2)
        self.token_time = now
        if self.tokens < 1:
            return False
        self.tokens -= 1
        return True

    def validate(self, raw):
        if len(raw.encode()) > 32768:
            raise ValueError("message too large")
        msg = json.loads(raw)
        if not isinstance(msg, dict) or type(msg.get("version")) is not int or msg["version"] != 1:
            raise ValueError("version")
        if not isinstance(msg.get("id"), str) or msg.get("server_id") != self.config["server_id"] or str(uuid.UUID(msg["id"])) != msg["id"]:
            raise ValueError("identity")
        if type(msg.get("timestamp")) is not int or abs(time.time()*1000 - msg["timestamp"]) > 30000:
            raise ValueError("expired")
        if not isinstance(msg.get("data"), dict) or not isinstance(msg.get("type"), str):
            raise ValueError("data")
        return msg

    async def handle(self, request):
        supplied = request.headers.get("Authorization", "")
        if not hmac.compare_digest(supplied.encode(), ("Bearer " + self.config["token"]).encode()):
            raise web.HTTPUnauthorized()
        # Reserve before the first await so concurrent handshakes cannot replace one another.
        if getattr(self, "occupied", False):
            raise web.HTTPConflict()
        self.occupied = True
        ws = web.WebSocketResponse(max_msg_size=32768, heartbeat=30)
        try:
            await ws.prepare(request)
            frame = await asyncio.wait_for(ws.receive(), 10)
            if frame.type != WSMsgType.TEXT:
                raise ValueError("hello required")
            hello = self.validate(frame.data)
            caps = hello["data"].get("capabilities", [])
            if hello["type"] != "hello" or not isinstance(caps, list) or not all(isinstance(x, str) for x in caps):
                raise ValueError("hello required")
            self.capabilities = set(caps)
            await ws.send_json(self.envelope("ready", {"reply_to": hello["id"]}))
            self.ws = ws
            async for frame in ws:
                if frame.type == WSMsgType.TEXT:
                    msg = self.validate(frame.data)
                    now = time.monotonic()
                    while self.seen and next(iter(self.seen.values())) < now - 300:
                        self.seen.popitem(last=False)
                    if msg["id"] in self.seen:
                        continue
                    self.seen[msg["id"]] = now
                    if len(self.seen) > 4096:
                        self.seen.popitem(last=False)
                    await self.incoming(msg)
                elif frame.type in (WSMsgType.ERROR, WSMsgType.CLOSE, WSMsgType.CLOSED):
                    break
        except (ValueError, KeyError, TypeError, RecursionError, asyncio.TimeoutError):
            await ws.close(code=1008, message=b"Invalid bridge protocol")
        finally:
            if self.ws is ws:
                self.ws = None
                self.capabilities.clear()
                waiting, self.pending = self.pending, {}
                for item in waiting.values():
                    await self.deliver("连接已断开，操作可能已生效，请重连后查询。", item[1])
            self.occupied = False
        return ws

    async def incoming(self, msg):
        data, kind = msg["data"], msg["type"]
        if kind in ("admin.whitelist.result", "error"):
            key = data.get("reply_to")
            pending = self.pending.get(key)
            if not pending:
                return
            _, sender, action = pending
            if kind != "error" and (data.get("source_id") != self.config["source_id"] or data.get("sender_id") != sender or data.get("action") != action):
                return
            self.pending.pop(key)
            await self.deliver(result_text(data), sender)
        elif kind in ("chat.game", "player.join", "player.leave", "player.death"):
            if not self.config.get("game_to_group", True) or not self.consume():
                return
            if not isinstance(data.get("text"), str) or len(data["text"]) > 2000:
                raise ValueError("invalid text")
            text = clean(data["text"])
            if kind == "chat.game":
                sender = data.get("sender")
                if not isinstance(sender, dict) or not isinstance(sender.get("name"), str):
                    raise ValueError("invalid sender")
                text = f"[MC] {clean(sender['name'], 64)}：{text}"
            if text:
                await self.deliver(text, None)

    async def admin(self, sender, payload):
        if sender not in self.config["admin_ids"]:
            raise PermissionError("该账号未配置白名单管理权限")
        if not self.ready or "admin.whitelist" not in self.capabilities:
            raise ConnectionError("服务器未连接，或 RabbitCore 未开启白名单管理")
        if len(self.pending) >= 32 or not self.consume():
            raise ValueError("操作过于频繁，请稍后重试")
        data = dict(payload, source={"id": self.config["source_id"]}, sender={"id": sender})
        # Insert before awaiting send: a fast response must find its route.
        async with self.send_lock:
            if not self.ready:
                raise ConnectionError("服务器未连接")
            msg = self.envelope("admin.whitelist", data)
            self.pending[msg["id"]] = (time.monotonic(), sender, payload["action"])
            try:
                await asyncio.wait_for(self.ws.send_json(msg), 5)
            except Exception:
                self.pending.pop(msg["id"], None)
                raise ConnectionError("发送中断，操作可能已生效，请查询状态") from None

    async def expire(self):
        while True:
            await asyncio.sleep(1)
            for key, item in list(self.pending.items()):
                if time.monotonic() - item[0] > 20:
                    self.pending.pop(key, None)
                    await self.deliver("操作回执超时，可能已生效；请先查询状态，不要重复添加或移除。", item[1])
