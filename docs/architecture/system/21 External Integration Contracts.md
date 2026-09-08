# External Integration Contracts

## 定位

本文统一定义架构图中的 Timer / Scheduler、Web / Human、API Client、Result Sink 和 External Systems / APIs 的边界。它们是外部参与者或集成端点，不是 Managed Agent 内部事实源。

## Timer / Scheduler

职责：

- 根据计划生成任务触发。
- 引用版本化 Task Template。
- 为每次触发生成稳定幂等键。
- 配置 Result Delivery 策略。

流程：

```text
Timer -> Task Gateway -> 创建 Root Session -> accepted
```

Timer 在任务被接纳后结束本次触发，不保持连接和轮询结果。结果由 Result Delivery 或 Query Service 提供。

## Web / Human

使用两条独立链路：

```text
命令：Web -> Task Gateway
实时：Web <- Streaming Gateway
```

创建、继续、取消、审批和反馈必须走命令链路。关闭页面或 Stream 不等于取消任务。

## API Client

支持：

- `POST /tasks` 提交并获得 `202 Accepted`。
- 通过 Query Service 轮询状态和结果。
- 通过 Streaming Gateway 订阅实时事件。
- 配置 Callback / Result Sink 接收主动交付。

客户端应使用 idempotency key、ETag、Retry-After 和稳定 Cursor。

## Result Sink

Sink 类型：

```text
Webhook
Kafka / Message Topic
Email / Notification Channel
Parent Session
Artifact-only Destination
```

契约要求：

- 使用 `delivery_id` 去重。
- 校验签名、时间戳和防重放信息。
- 返回明确 ACK/NACK。
- 不在响应中回显 Secret。
- 长耗时处理先 ACK，再异步消费。

## External Systems / APIs

包括 SaaS、企业系统、数据库、代码托管、浏览器目标、消息和通知系统。

访问规则：

- 必须经 Tool Gateway / Hands / Credential Proxy。
- 能力按读、写、删除、管理区分。
- 外部 Resource Version/ETag 写入结果证据。
- 不确定副作用返回 `unknown`，禁止盲目重试。
- 外部回调重新进入系统时通过 Task Gateway 或专用受控入口。

## 通用可靠性

- 所有写请求带 correlation/idempotency key。
- Timeout 与失败分开建模。
- 重试使用指数退避和上限。
- 签名、证书、IP/域名策略和数据分类由 Policy/Credential Plane 统一治理。
- 外部内容视为不可信输入，需要 Schema、大小和 Prompt Injection 防护。

## 验收条件

- Timer 不承担结果轮询。
- Human 审批不能通过 SSE 上行修改状态。
- Result Sink 重复收到相同 delivery_id 时不会重复处理。
- Agent 和 Sandbox 不能绕过 Gateway 直接访问受控外部系统。

## 当前实现对照

- 已落地的外部契约包括 Task HTTP/SSE、Webhook Result Sink、Parent Session Sink、OpenAI-compatible Model、
  Managed MCP、Java HTTP API、S3/OBS 与 Vault。
- 外部调用统一经过内部身份、Policy decision、Credential Reference、allowlist、timeout、幂等/调用账本和脱敏边界。
- Connector 与 Result Sink 的具体选择在 `composition/builders/`，业务包不直接绑定第三方 SDK。

## 现有缺陷与待完善

- Timer/Scheduler、Email/Notification、Kafka Result Sink、浏览器和数据库 Connector 仍是接口目标，尚无完整生产适配器。
- 各外部系统对 ETag、幂等键、异步 operation 与 unknown side effect 的支持差异较大，缺少统一 conformance 分级。
- 外部内容的 prompt injection/恶意内容治理仍以 schema、大小、目录边界为主，缺少集中内容安全平面。
- 待补：Connector 认证矩阵、版本/弃用策略、契约测试套件、沙箱网络证明和第三方故障演练。
