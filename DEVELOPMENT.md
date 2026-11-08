# RabbitChat development

Branch: `codex/rabbit-chat-bridge`.
Remote checkout: `/home/yang/services/astrbot/data/plugins/astrbot_plugin_rabbit_chat`.
Environment: AstrBot 4.29.4, Python 3.14.4, aiohttp available in AstrBot's uv environment.

Version 0.2.2 implements RabbitCore Bridge v1, single-group text relay and RabbitAuth whitelist commands. Run the tests described in README.md using `/home/yang/.local/share/uv/tools/astrbot/bin/python`.

Production configuration and activation require explicit approval. Repository defaults are disabled, with no token, group or administrator credentials. No live group messages are sent by the tests.

2026-11-08: the user confirmed live chat integration completion after the wake-prefix fallback fix. The bridge reached READY with the actual Minecraft server. External whitelist administration remains disabled; unit/protocol tests cover its adapter logic, not live group authorization. Runtime configuration and tokens are outside version control.
