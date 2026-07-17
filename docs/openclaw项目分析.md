# OpenClaw 项目分析笔记

> 本文档为 AuraClaw 实现过程中的参考，基于对 openclaw 源码与文档的梳理。
> openclaw项目地址：/Users/soo/workspace/sourcecode/github/openclaw

---

## 一句话总结

OpenClaw 是一个**本地优先的多通道 AI 网关**：用 TypeScript/Node 编写，以 Gateway 为控制面，通过插件化通道（WhatsApp、Telegram、Slack、Discord 等）接收消息，由 Pi Agent 运行时处理并回写。架构清晰、插件与技能可扩展，适合作为 AuraClaw 的参考实现。

---

## 项目类型与主要技术栈

| 类别 | 技术 |
|------|------|
| 语言 | TypeScript (ESM)，Node ≥22.12 |
| 包管理 | pnpm，monorepo（pnpm-workspace.yaml） |
| 构建 | tsdown（打包 dist/），plugin-sdk 单独生成 .d.ts |
| 测试 | Vitest（unit / e2e / gateway / live / extensions） |
| 格式化/静态检查 | oxfmt、oxlint |
| 运行时形态 | CLI（openclaw.mjs → dist/entry） + Gateway 常驻进程 + 可选 TUI/Web UI |

**核心依赖（节选）：**

- **Agent 运行时**：`@mariozechner/pi-agent-core`、`pi-coding-agent`、`pi-tui` 等
- **协议/前端**：`@agentclientprotocol/sdk`
- **通道**：`@whiskeysockets/baileys`(WhatsApp)、`grammy`(Telegram)、`@slack/bolt`、Discord 等
- **Web/HTTP**：Express、undici
- **工具/工具链**：commander、chalk、dotenv、zod、playwright-core、sharp 等

---

## 总体架构与分层

- **控制面**：单机 Gateway（WebSocket + HTTP），负责会话、通道、工具、事件、Cron、Webhook、Control UI、Canvas 宿主。
- **入口**：`openclaw.mjs` 校验 Node 版本后动态加载 `dist/entry.js` 或 `dist/entry.mjs`；`src/entry.ts` 里根据 argv 派发到 CLI（gateway / agent / onboard / doctor 等）。
- **分层倾向**：
  - 通道层（channels/plugins + extensions/*）
  - Gateway 服务层（gateway/server*.ts）
  - Agent/会话层（agents, sessions, config）
  - 配置/密钥/插件层（config, secrets, plugins）
  - 基础设施（infra, process, media, logging）

---

## 目录与模块结构（与 AuraClaw 对照用）

```
openclaw/
├── openclaw.mjs              # CLI 入口（Node 版本检查 → dist/entry）
├── package.json              # 根包、bin、exports（含大量 plugin-sdk 子路径）
├── pnpm-workspace.yaml       # 根包、ui、packages/*、extensions/*
├── tsconfig.json / tsdown.config.ts
├── src/
│   ├── entry.ts              # 主入口：CLI 派发、respawn、子进程桥接
│   ├── index.ts
│   ├── extensionAPI.ts       # 对 agents/plugin 等的能力导出（给插件用）
│   ├── runtime.ts
│   ├── cli/                  # 命令行解析、profile、respawn 策略
│   ├── commands/             # 具体命令实现（onboard, gateway, agent, doctor…）
│   ├── config/               # 配置加载、校验、迁移、路径、runtime overrides
│   ├── agents/               # Pi 嵌入式 Agent、workspace、identity、skills 刷新
│   ├── gateway/              # 核心：server.impl.ts 启动、server-channels 管理通道
│   │   ├── server*.ts        # WS/HTTP、认证、Control UI、健康、Cron、插件加载
│   │   ├── server-channels.ts # createChannelManager，按配置启停通道
│   │   └── server-methods*   # RPC 方法、exec 审批、secrets 等
│   ├── channels/             # 通道抽象：plugins/index（list/get）、registry、类型
│   │   └── plugins/          # ChannelPlugin 类型、directory、allowlist 等
│   ├── plugin-sdk/           # 插件 SDK 实现（core、各 channel 的 adapter 封装）
│   ├── plugins/              # 插件加载、registry、runtime、hook、services
│   ├── web/                  # WebChat、inbound 消息、auto-reply 监控
│   ├── sessions/             # 会话模型、存储
│   ├── routing/              # 会话键、路由
│   ├── telegram/, slack/, discord/, signal/, whatsapp/, imessage/  # 内置通道
│   ├── browser/, media/, tts/, hooks/, cron/, daemon/, pairing/
│   ├── infra/, logging/, process/, secrets/, security/
│   └── wizard/               # 引导流程
├── extensions/               # 独立包：telegram, msteams, matrix, voice-call, memory-* 等
├── packages/                 # 如 moltbot, clawdbot
├── skills/                   # 内置技能（1password, github, canvas, …）
├── ui/                       # 前端（Vite 等）
├── scripts/                  # 构建、协议生成、Docker、文档等
├── test/                     # 全局 setup、e2e、fixtures、helpers
├── docs/                     # Mintlify 文档站
└── apps/                     # macOS / iOS / Android 等原生应用
```

**要点：**

- **通道**：内置在 `src/` 的 telegram、slack 等与 `extensions/*` 的插件都通过 `channels/plugins` 的 registry 注册；Gateway 启动时通过 `createChannelManager` 从配置里启停各 channel。
- **插件**：`extensions/*` 的 package.json 里用 `openclaw.extensions: ["./index.ts"]` 声明入口；插件可注册 Gateway RPC、HTTP 路由、Agent 工具、CLI 子命令、技能等；Plugin SDK 通过 `openclaw/plugin-sdk/*` 子路径导出，供扩展使用。
- **配置**：集中从 `config/` 读（含 JSON5、环境变量、runtime overrides）；通道配置在 `channels.<channelId>`，插件在 `plugins.entries.<id>.config` 等。

---

## 框架与技术选型要点

- **CLI**：commander + 自定义 profile/env（如 `OPENCLAW_*`），入口在 `entry.ts` 中根据子命令 spawn 或直接调用。
- **Gateway**：Express 提供 HTTP，WebSocket 提供控制面；认证、限流、Cron、Webhook、Control UI、Canvas 均由 gateway 包内模块完成。
- **通道抽象**：`ChannelPlugin`（含 status、gateway、messaging、outbound、pairing 等 adapter）；各通道实现 `ChannelGatewayAdapter` 等，由 `server-channels` 在启动时拉取插件列表并调用 lifecycle。
- **Agent**：Pi 系列包跑在进程内（RPC 模式），支持工具流、块流；会话与路由由 `sessions/`、`routing/` 与 config 绑定。
- **构建与产物**：tsdown 生成 `dist/`；plugin-sdk 单独用 `tsconfig.plugin-sdk.dts.json` 生成类型；`openclaw.mjs` 仅做引导，不打包进 dist。

---

## 关键流程（便于在 AuraClaw 中复刻）

### 1. 进程与 CLI 入口

1. 用户执行 `openclaw <cmd> ...` → `openclaw.mjs`。
2. 检查 Node 版本、可选 enableCompileCache、warning filter。
3. 动态 import `./dist/entry.js`（或 .mjs）→ `src/entry.ts` 的 runCli/派发逻辑。
4. 子命令如 `gateway` 会 spawn 或直接调起 Gateway 启动逻辑。

### 2. Gateway 启动与通道加载

1. `startGatewayServer`（server.impl.ts）：加载配置、合并认证、初始化插件 registry、加载插件、创建 ChannelManager、启动 HTTP/WS、挂载 Control UI、Cron、Discovery 等。
2. `createChannelManager`（server-channels.ts）：根据当前 config 和 `listChannelPlugins()` 的结果，对每个启用的 channel（及 account）调用 plugin 的 gateway lifecycle（start）；支持按 channel/account 启停、重试、backoff。
3. 通道插件通过 `plugins/runtime` 的 registry 注册；内置通道在 core 里注册，扩展通道在 `extensions/*` 的入口里注册。

### 3. 消息入站到 Agent 回写

1. 各通道实现接收消息（长轮询/Webhook/WS 等），归一化为统一入站结构（如 `WebInboundMessage` 风格）。
2. 入站消息进入 web/inbound、auto-reply 等层，做访问控制、去重、会话解析。
3. 路由到会话/Agent 后，由 Pi Agent 执行（可带 thinking、工具调用等）。
4. 回复通过 channel 的 outbound adapter 发回（reply/sendMedia 等）。

---

## 配置与扩展点（实现 AuraClaw 时可复用思路）

- **主配置**：单文件（如 JSON5），路径由环境变量或默认路径决定；含 `gateway`、`channels`、`plugins`、`models`、`messages`、`tools` 等。
- **通道**：`channels.<id>.enabled`、accounts、token/凭证、dmPolicy（pairing/open）、allowFrom 等；不同通道有各自字段（如 Telegram 的 groups、Slack 的 token）。
- **插件**：`plugins.entries.<id>.config`、slots（如 memory）；安装通过 `openclaw plugins install @openclaw/xxx`，由 Gateway 加载并注入 `api`（含 config、runtime、registerHttpRoute 等）。
- **技能**：skills 目录 + 配置中的技能列表；新技能推荐发到 ClawHub，核心只保留少量内置技能。

---

## 安全与运维（可直接借鉴）

- **DM 策略**：默认 pairing（未知发件人先配对），可选 open + allowFrom；避免未授权 DM 直接进 Agent。
- **密钥**：不落盘明文；通过 secrets 模块与 runtime snapshot 解析；命令行 audit 等只读模式。
- **插件**：同进程信任模型；不执行未审核代码；HTTP 路由需显式注册，避免任意暴露。
- **文档**：SECURITY.md、VISION.md、docs 中安全与加固说明；适合作为 AuraClaw 的安全基线。

---

## 工程实践与代码质量

- **测试**：Vitest 多配置（unit、e2e、gateway、live、extensions）；覆盖率阈值；大量 `*.test.ts` 与 e2e harness。
- **规范**：AGENTS.md 规定 PR/issue、提交信息、文档链接、安全分析流程；单 PR 单主题、大 PR 需例外审批。
- **类型**：严格 TypeScript；plugin-sdk 独立生成 d.ts 便于扩展开发。
- **日志**：子系统 logger（createSubsystemLogger）、结构化日志与诊断事件。

---

## 对 AuraClaw 的实现建议（按优先级）

1. **先做最小闭环**：CLI 入口（如 AuraClaw.mjs + dist/entry）→ 单通道（如先 Telegram 或 WebChat）→ 单一 Agent 调用 → 回写该通道；不急于复刻全部通道。
2. **配置与 Gateway 边界**：沿用「单配置文件 + 环境变量覆盖」；Gateway 只做控制面（会话、通道启停、WebSocket/HTTP），不把业务逻辑塞进 Gateway。
3. **通道抽象**：定义类似 `ChannelPlugin` 的接口（id、status、gateway、messaging、outbound）；AuraClaw 可先实现 1～2 个通道，再通过插件或扩展目录挂更多。
4. **插件与技能**：若不需要完整插件生态，可先做「技能目录 + 配置启用」；后续再引入 plugin registry 与 SDK 子路径导出。
5. **安全**：从第一天起采用 pairing 或等价访问控制，避免开放 DM；密钥与配置分离，不写死敏感信息。
6. **构建与发布**：用 pnpm workspace 管理 monorepo；CLI 入口保持轻量，主逻辑放在 TypeScript 构建产物中；若提供 SDK，可单独生成 d.ts。

---

## 可深入阅读的文档与代码

- **设计/愿景**：`VISION.md`、`docs.acp.md`、`docs/design/kilo-gateway-integration.md`（了解 Gateway 与 provider 集成方式）。
- **插件**：`docs/tools/plugin.md`、`src/plugins/`、`src/plugin-sdk/`、任意 `extensions/*/index.ts`。
- **通道**：`src/channels/plugins/types*.ts`、`src/gateway/server-channels.ts`、`src/web/inbound/`。
- **配置与启动**：`src/config/`、`src/gateway/server.impl.ts`、`src/entry.ts`。
- **协作与规范**：`AGENTS.md`、`CONTRIBUTING.md`、`SECURITY.md`。

---

## 小结

OpenClaw 的架构可概括为：**一个 Node/TS 的 Gateway 控制面 + 多通道插件 + 内嵌 Pi Agent + 统一配置与密钥管理**。实现 AuraClaw 时建议先对齐「入口 → 配置 → Gateway → 单通道 → Agent → 回写」这条主链路，再按需增加通道、插件和技能，并沿用其安全默认与工程实践。本笔记中的目录与模块划分可直接作为 AuraClaw 的对照表和实现清单使用。
