# RabbitChat

AstrBot 4.29.4 / Python 3.14 verified. One configured bot instance and one group connect to one RabbitCore Bridge v1 server. RabbitAuth is needed only for whitelist operations.

## Setup

Install into `data/plugins/astrbot_plugin_rabbit_chat`, install `requirements.txt` in AstrBot's Python environment, then reload the plugin. Configure in AstrBot:

- `enabled`: default false.
- `host`: default `127.0.0.1`; same-machine deployments require no public listener.
- `port`: default 26670; endpoint `/rabbit/ws`.
- `token`: independent random secret, at least 24 characters; never put it into group messages.
- `server_id`: must equal RabbitCore's value.
- `platform_id`: AstrBot's unique bot instance ID, not adapter type. For llbot/aiocqhttp the namespace is `qq`.
- `group_id`: one group; private messages and other groups are ignored.
- `admin_ids`: explicit raw account IDs. Group owner/admin roles do not grant authority.

On RabbitCore, configure bridge `url`, `token`, `server_id`, `enabled`; configure `[bridge.admin.whitelist]` with `enabled=true`, `source_ids=["qq:group:GROUP_ID"]`, `sender_ids=["qq:ADMIN_ID"]`, and `actions=["add","remove","set","info","list","status"]`. Reload with `/rabbit_bridge reload`. Across hosts use a restricted listener and TLS reverse proxy (`wss://`).

## Commands

Use AstrBot's command prefix (normally `/`):

```text
/rabbit 状态
/rabbit 白名单 添加 Steve
/rabbit 白名单 移除 Steve
/rabbit 白名单 暂停 Steve
/rabbit 白名单 恢复 Steve
/rabbit 白名单 查看 Steve
/rabbit 白名单 列表 1
/rabbit 白名单 状态
```

The connection status is visible to the configured group. Whitelist commands require the explicit account allowlist in both plugins. Group members cannot change global whitelist enablement, import entries, edit administrator IDs, or execute console commands. RabbitAuth performs persistence and immediate kicks on remove/pause.

Only plain text is forwarded; media, mentions and commands are ignored. Game chat, join/leave and death notifications go only to the configured session. The bot's own messages are ignored to avoid loops. Messages are bounded to 2000 characters with a shared 2/s, burst 10 token bucket; output is split at 1500 characters. No offline replay.

Management requests use UUIDs, 20-second receipt timeout and a maximum of 32 pending operations. Results must match the request, server, group, sender and action. Disconnect/timeouts can mean an operation already took effect: query state before retrying. Tokens and message contents are not logged. This release uses AstrBot's `GreedyStr` parser from its core API, verified on 4.29.4.

## Verification

```sh
python -m unittest -v test_bridge.py test_plugin.py
```

14 test cases cover actual aiohttp WebSocket authentication, handshake, one-connection exclusion, protocol validation, deduplication, request/result routing, permission denial, timeout, disconnect, installed AstrBot command argument parsing, bot/group binding, command exclusion and fixed output routing, and response delivery before stopping event propagation. Adapter tests mock external message sending; they do not send group messages. Tests do not establish full live Minecraft/AstrBot end-to-end coverage.

After reloading both sides, run `/rabbit 状态`, check `/rabbit_bridge status`, then test text in both directions and whitelist info/list with an authorized account. Use a disposable player to verify pause/remove kicks and persistence across restart.

Version 0.2.2: an exact /rabbit fallback works in the bound group even when the profile uses only bot-name wake prefixes. Standard command handling is retained; the fallback does not duplicate it. No profile wake settings are changed.
