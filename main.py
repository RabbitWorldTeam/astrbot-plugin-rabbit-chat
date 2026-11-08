"""Single AstrBot group to single RabbitCore server bridge."""
import asyncio
from astrbot.api import AstrBotConfig, logger
from astrbot.api.event import AstrMessageEvent, MessageChain, filter
from astrbot.api.star import Context, Star, register
from astrbot.api.message_components import Plain
from astrbot.core.star.filter.command import GreedyStr

from .bridge import Bridge, clean, command


@register("astrbot_plugin_rabbit_chat", "Tsurumaki-Kokoro", "RabbitCore 单群聊天与白名单桥接", "0.2.2")
class RabbitChatPlugin(Star):
    def __init__(self, context: Context, config: AstrBotConfig):
        super().__init__(context)
        self.config = config
        self.bridge = None

    async def initialize(self):
        cfg = dict(self.config)
        if cfg.get("enabled"):
            if not cfg.get("platform_id") or not cfg.get("group_id"):
                raise ValueError("请配置唯一机器人 platform_id 和 group_id")
            if cfg.get("id_namespace") not in ("qq", "discord", "telegram"):
                raise ValueError("请配置稳定的平台 ID 命名空间")
        namespace = cfg.get("id_namespace", "qq")
        cfg["source_id"] = f"{namespace}:group:{cfg.get('group_id', '')}"
        cfg["admin_ids"] = [f"{namespace}:{x}" for x in cfg.get("admin_ids", [])]
        self.bridge = Bridge(cfg, self.deliver)
        await self.bridge.start()
        logger.info("RabbitChat initialized (enabled=%s)", cfg.get("enabled", False))

    async def deliver(self, text, sender=None):
        # No user-controlled destination. Always send to the configured unique session.
        cfg = self.bridge.config
        session = f"{cfg['platform_id']}:GroupMessage:{cfg['group_id']}"
        if sender:
            text = f"[{clean(sender, 128)}] {text}"
        for start in range(0, len(text), 1500):
            try:
                chain = MessageChain().message(text[start:start+1500])
                chain.use_t2i_ = False
                chain.use_markdown_ = False
                await asyncio.wait_for(self.context.send_message(session, chain), 10)
            except Exception:
                logger.warning("RabbitChat group delivery failed; message was not retried")

    def matches(self, event):
        cfg = self.bridge.config if self.bridge else {}
        return (cfg.get("enabled") and not event.is_private_chat()
                and str(event.get_platform_id()) == cfg.get("platform_id")
                and str(event.get_group_id()) == cfg.get("group_id")
                and str(event.get_sender_id()) != str(event.get_self_id()))

    @filter.command("rabbit")
    async def rabbit(self, event: AstrMessageEvent, arguments: GreedyStr):
        if not self.matches(event):
            return
        logger.info("RabbitChat command received (platform=%s, group=%s)", event.get_platform_id(), event.get_group_id())
        try:
            payload = command("/rabbit " + arguments)
            sender = f"{self.bridge.config['id_namespace']}:{event.get_sender_id()}"
            if payload["action"] == "connection":
                text = "RabbitCore：" + ("已连接" if self.bridge.ready else "未连接")
            else:
                await self.bridge.admin(sender, payload)
                event.stop_event()
                return
        except (ValueError, PermissionError, ConnectionError) as exc:
            text = str(exc)
        yield event.plain_result(text)
        # AstrBot must run its send stages before propagation is stopped.
        event.stop_event()

    @filter.regex(r"^/?rabbit(?:\s|$)")
    async def rabbit_without_wake_prefix(self, event: AstrMessageEvent):
        # Profiles may use only name-based wake prefixes. Keep /rabbit usable
        # without changing those profiles or invoking both command handlers.
        if event.is_at_or_wake_command:
            return
        parts = event.get_message_str().strip().split(maxsplit=1)
        arguments = parts[1] if len(parts) == 2 else ""
        async for result in self.rabbit(event, arguments):
            yield result

    @filter.event_message_type(filter.EventMessageType.GROUP_MESSAGE)
    async def group_chat(self, event: AstrMessageEvent):
        if not self.matches(event):
            return
        raw = event.get_message_str().strip()
        # All commands are consumed elsewhere or ignored, never relayed as game chat.
        if not raw or raw.startswith(("/", "!", "！")) or event.is_at_or_wake_command:
            return
        if raw.split()[0].lower() in ("rabbit", "白名单", "whitelist"):
            return
        if not self.bridge.config.get("group_to_game", True) or not self.bridge.ready:
            return
        # Text only; media and mentions do not become executable content.
        if any(not isinstance(part, Plain) for part in event.get_messages()):
            return
        if len(raw) > 2000 or not self.bridge.consume():
            return
        cfg = self.bridge.config
        try:
            await self.bridge.emit("chat.external", {
                "source": {"id": cfg["source_id"], "name": clean(cfg.get("group_name", "群聊"), 64)},
                "sender": {"id": f"{cfg['id_namespace']}:{event.get_sender_id()}", "name": clean(event.get_sender_name(), 64) or "群成员"},
                "text": clean(raw), "mentions": []})
        except (ConnectionError, OSError, TimeoutError):
            logger.warning("RabbitChat chat dropped while disconnected")

    async def terminate(self):
        if self.bridge:
            await self.bridge.stop()
