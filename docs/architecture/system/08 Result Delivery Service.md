# Result Delivery Service

## 定位

Result Delivery Service 将完成、失败、等待审批等持久事件主动投递到 Webhook、Topic、Email、Parent Session 或其他 Result Sink。它负责如何可靠交付，不负责判断任务何时完成。

## 触发条件

主要触发源是 Session Service 的 Transactional Outbox：

```text
Root Session 达到可交付状态
  -> 原子写 run.completed/result_ref
  -> 原子写 result.delivery.requested Outbox
  -> Result Delivery Service 消费
```

首次投递必须满足：

- Root 或显式配置的 Child Session 达到目标事件。
- `result_ref`、`artifact_refs` 已提交。
- 配置了 Result Sink 和允许的事件类型。
- Payload 所需 Projection 已达到要求版本。

## 核心模块

```text
Delivery Event Consumer
Delivery Job Store
Sink Configuration Resolver
Payload Assembler
Result Projection Reader
Artifact Resolver
Signer / Encoder
Delivery Adapter Registry
Idempotency / ACK Tracker
Retry Scheduler
Circuit Breaker
Dead Letter Queue
Delivery Event Writer
```

## Delivery Job

```text
delivery_id
event_id
tenant_id
session_id / run_id
sink_type / sink_target_ref
payload_ref
status
attempt_count
next_attempt_at
last_response_summary
created_at / completed_at
```

目标地址和认证信息使用受控引用，Secret 不保存在 Job 中。

## 投递流程

```text
Outbox Event
 -> 创建幂等 Delivery Job
 -> 读取 Result Projection / Artifact 元数据
 -> 构造并脱敏 Payload
 -> Credential Proxy 获取受控调用能力
 -> 签名投递
 -> ACK: 写 delivery.succeeded
 -> 暂时失败: retry_wait
 -> 重试耗尽: dead_lettered
```

所有状态必须回写 Session，使 Query Service 和审计能够看到真实交付状态。

## Result Sink Adapter

```text
Webhook Adapter
Kafka / Event Topic Adapter
Email / Notification Adapter
Parent Session Adapter
Artifact-only Adapter
```

Webhook Payload 至少包含 `delivery_id`、`event_id`、`session_id`、事件类型、结果摘要、受控 Artifact 链接和签名时间。

## 幂等、重试与 ACK

- `delivery_id` 对一个 Sink 唯一且稳定。
- 超时不等于失败；重试使用同一 `delivery_id`。
- 仅对可重试错误指数退避。
- 4xx 权限或参数错误通常直接失败；429/5xx/timeout 可重试。
- Sink ACK/NACK 和响应摘要进入 Attempt History。
- Manual Redelivery 创建新的 attempt，不修改历史证据。

## 跨副本熔断

熔断范围是 `(tenant_id, sink_id)`，属于 Delivery Job Store 的共享业务状态，不属于某个
Worker 进程。连续可重试失败、`open_until`、generation、半开探针 owner/token/TTL 均持久化；
所有副本先原子申请调用许可，再访问 Sink。熔断打开后其他副本停止外呼，到期时仅一个副本取得
half-open 探针；探针 owner 失联后可按 TTL 接管。服务重启不得清空失败阈值或提前关闭熔断。

熔断只限制外部调用，不代替 Delivery Job 状态机。被熔断阻止的 Job 仍记录可重试 attempt，继续遵守
原 delivery ID、退避、最大次数与 DLQ 规则。阈值、打开时长和探针 TTL 分别由
`AURACLAW_DELIVERY_CIRCUIT_FAILURE_THRESHOLD`、`AURACLAW_DELIVERY_CIRCUIT_RESET_SECONDS`、
`AURACLAW_DELIVERY_CIRCUIT_PROBE_TTL_SECONDS` 配置。Admin `status` 传 tenant/sink 可查询当前状态。

## 与 Outbox 的边界

Session Outbox 保证“需要投递”不会丢；Delivery Job Store 保证“投递过程”可恢复。Result Delivery 自身不能扫描 Session 状态推测哪些任务应该投递。

## 安全与观测

- Webhook 签名、时间戳和防重放。
- Artifact 使用短期、最小权限下载链接。
- 通过 Credential Proxy 调用外部 Sink。
- 指标：delivery latency、success rate、retry count、DLQ size、ACK latency、sink circuit state。

## 验收条件

- Session 提交完成事件后，即使 Delivery 服务停机也不会丢通知。
- 相同 Outbox Event 不会产生重复业务交付。
- 投递成功、失败和 DLQ 均能通过 Query API 查询。
- Runtime Event Bus 丢失不影响结果主动交付。
