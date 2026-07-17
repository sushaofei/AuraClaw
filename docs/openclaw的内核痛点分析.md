# 内核痛点
## 1.内核与 Pi/channels 强耦合
Agent 主循环半死在 `@mariozechner/pi-*`（SessionManager、StreamFn、compaction）。
改内核就要动`Pi`的约定。
## 2.配置/会话贯穿全局
`loadconfig()`、`SessionEntry`、`updateSessionStore`到处被直接调用，没有清晰的“配置/会话接口”层。
## 3.工具与 Gateway 双向依赖
工具表有`createOpenClawTools(options)`根据大量 session/channel 参数拼出；
Gateway 的 Agent 处理又依赖这些工具和 commands。
## 4.渠道与内核同仓库混在一起
每个渠道（Telegram/Discord/...）直接依赖`src/`的很多模块，plugin-sdk尚未完全收敛，导致改内核容易牵动所有渠道。
## 5.外部依赖又多又重
各个渠道一个库（grammy、bolt、baileys、...）、Pi 全家桶、playwright、多种 LLM SDK 等，都进了一个单体。