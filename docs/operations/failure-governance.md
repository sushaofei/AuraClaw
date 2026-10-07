# 错误、失败队列与生命周期治理

AuraClaw 的公共错误和异步失败面使用同一份机器可读契约：
`GET /v1/operations/contract`。该接口要求正常租户身份，返回 `schema_version`、错误分类、
调用方处置动作、统一失败状态以及各队列的 owner、事实源和恢复动作。契约只描述运维语义，
不会绕过各服务的数据所有权。

## 公共错误信封

Task API 与 Streaming Gateway 的 `AuraClawError` 响应固定包含：

- `code`：稳定程序码；客户端不得依赖 message 文本分支。
- `category`：request、authentication、authorization、conflict、capacity、dependency、runtime 或 internal。
- `retryable`：本次失败是否可由调用方安全重试；并不意味着写请求可忽略幂等键。
- `operator_action`：correct_request、refresh_identity、resolve_conflict、retry_with_backoff、
  inspect_dependency、inspect_runtime 或 escalate。
- `trace_id`：与响应 `traceparent` 中 trace id 一致，用于日志、Trace 与告警关联。
- `message`、`detail`：供人阅读的脱敏说明；`Retry-After` 存在时优先遵循。

内部 HTTP 契约沿用受限 `InternalErrorCode`，但 `retryable` 由同一错误分类器产生，避免把认证、
协议或永久策略拒绝错误误判为可重试。

## 失败队列所有权

| 队列 | Owner | 失败状态 | 恢复入口 | 权威事实/恢复来源 |
| --- | --- | --- | --- | --- |
| Projection | Projection Worker | quarantined | redrive 或 tenant rebuild | Canonical Session Events |
| Delivery | Delivery Worker | dead_lettered / reconciling | redrive 或 reconcile | delivery_job + Canonical delivery events |
| Skill lifecycle | Action Hands | retry_wait | retry 或 snapshot reconcile | lifecycle broadcast outbox + PostgreSQL snapshot |
| Runtime Event | Streaming Gateway | 无 DLQ | 游标重连，随后读取 Result API | Canonical Session Events |

`pending`、`claimed`、`retry_wait`、`quarantined`、`dead_lettered`、`reconciling` 和
`completed` 是统一的运维状态词汇。适配器内部状态可更细，但对告警、Dashboard 和 Runbook
必须映射到这些值。Runtime Event 是短期体验流，不伪造 DLQ 或结果交付保证。

## 处置顺序

1. 用 `trace_id`、tenant、queue 和 item id 定位失败；不得把 Secret 或完整不可信 payload 写入工单。
2. 先检查 owner 的 status 与依赖健康，再决定 retry/redrive/reconcile/rebuild。
3. redrive 必须携带 tenant 且由 owner 执行；不得直接修改其他服务的表。
4. `side_effect_status=unknown` 或 reconciling 时先对账，禁止盲目重放外部写副作用。
5. Projection rebuild 只重建可丢弃视图；Delivery 终态和 Canonical Session Events 不可因重建而改写。
6. 恢复后核对队列水位、Projection version、最终 Result 与审计记录，再解除告警。

错误分类或队列契约发生不兼容变化时必须提升 `schema_version`，同步 SDK/Dashboard，并通过发布门禁。
