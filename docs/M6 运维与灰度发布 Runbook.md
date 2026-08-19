# M6 运维与灰度发布 Runbook

## 观测关联与排障入口

所有 Trace、Audit、Metric 和 Alert 使用统一关联字段：`trace_id`、`span_id`、`tenant_id`、
`root_session_id`、`session_id`、`run_id`、`event_id`、`command_id`、
`tool_invocation_id`、`runtime_id`、`delivery_id`、`approval_id`。普通日志不保存 Prompt、
Response、凭证或 Secret；敏感 Payload 只允许保存受控 `payload_ref`。

租户内 Session 时间线：

```text
GET /v1/operations/sessions/{session_id}/timeline
GET /v1/operations/metrics
```

排障顺序：先用 Root/Session ID 打开 Timeline，确认最后一个 Canonical Event；再按 Run、Tool、
Delivery 或 Approval ID 定位 Span/Audit；最后检查 Projection、Lease、Unknown Side Effect 和
Delivery DLQ 告警。Runtime Event 只能辅助实时诊断，不能用作完成或交付事实。

## SLO 与主动告警

| SLO | 目标 | 告警指标与阈值 |
|---|---:|---|
| Canonical append | 可用性 ≥ 99.9%，P95 < 100 ms | `session.append.*` |
| Projection freshness | P95 < 2 s | `projection.lag.seconds > 2` |
| Task start | P95 < 5 s | `task.start.latency.seconds` |
| Runtime recovery | P95 < 30 s | `runtime.lease_lost.count > 0` 立即告警 |
| SSE event latency | P95 < 1 s | `streaming.event.latency.seconds` |
| Webhook delivery | Sink 可用时 99%/60 s | `delivery.dlq.count > 0` 立即告警 |
| Tool side effect | Unknown 为 0 | `tool.side_effect_unknown.count > 0` 立即告警 |
| Secret leakage | 0 | 发布 Secret 扫描任一命中即失败 |
| Task API 身份 | 写命令认证可用 | Task API span `http_status` 401/403 突增立即告警 |
| Assertion 重放 | 0 副作用 | 同一 `jti` 绑定不同 command 必须拒绝 |

`projection.lag.seconds` 为 warning；lease lost、unknown side effect、delivery DLQ 和身份
fail-open 为 critical。告警可带 tenant/user/`identity_kid`/`identity_jti` 摘要，但不复制
Assertion、workload token 或 OAuth secret。

## 故障处置

### 数据库短断或 Outbox 积压

Canonical append 失败时客户端以相同 tenant、operation 和幂等键重试。数据库恢复后检查：

```bash
uv run auraclaw operations status --tenant TENANT
uv run auraclaw projection relay --watch
```

禁止跳过 Event Store 直接写 Projection。Outbox lag 回落且 Projection version 追平后解除门禁。

### Projection 落后或 Poison Event

先停止该 Aggregate 的错误投影扩散，保留 Canonical Event。修复兼容性后执行：

```bash
uv run auraclaw operations redrive --tenant TENANT --queue projection --item-id EVENT_ID
uv run auraclaw projection rebuild --tenant TENANT
```

重建前后抽样比对 Task/Approval/Collaboration View；Canonical Event 不删除、不改写。

### Runtime 崩溃或 Lease 丢失

Orchestrator `reconcile` 回收过期 Lease 并使用更高 fencing token 接管。旧 Runtime 的 Session 写入、
checkpoint 和工具调用必须持续被拒绝。若 30 秒内未恢复，暂停新调度并保留现有 Session 事实。

### Tool Side Effect Unknown

立即停止自动重试并触发 `tool.side_effect_unknown.count`。通过稳定 invocation/idempotency key 向外部
系统对账；确认未执行才允许人工重投，确认已执行则追加结果事实。不得猜测或覆盖原调用记录。

### Delivery 5xx 或 DLQ

5xx/429/timeout 由持久 Job 退避重试。耗尽后检查 Attempt History、Sink 健康和签名配置，再执行：

```bash
uv run auraclaw operations redrive --tenant TENANT --queue delivery --item-id DELIVERY_ID
```

重投增加 attempt，不覆盖历史；稳定 `delivery_id` 和接收方 Idempotency-Key 防止重复业务效果。

### chaintower Assertion 验签失败或密钥不可用

Task API 写与敏感读必须 fail closed（401）。先确认 `kid` 仍在 N/N-1 集合、clock skew
未超出配置，再检查 chaintower 签发服务。禁止为恢复流量改回裸 `X-Tenant-ID` /
`X-Actor-ID`。轮换时先加载新 `kid`，chaintower 切签发后再撤旧密钥。

## 保留、GC 与安全

默认 Metric 30 天、Trace 14 天、Audit 365 天、Alert 90 天：

```bash
uv run auraclaw operations retention
```

Artifact 依据 `retention_until` GC；同租户相同 Hash 的对象仍被其他元数据引用时不删除对象。
Audit 保留时间独立于普通 Log/Trace。租户删除或合规保留必须同时覆盖 Event、Telemetry、Artifact
和 Delivery 数据。每次发布运行 `uv run python scripts/release_gate.py`；任何 Secret 命中均阻断。

## 灰度与回滚

按以下顺序放量，每级观察错误率、恢复率、Projection lag、unknown side effect、Delivery DLQ、
Task API 401/403 和 Secret 扫描；任一门禁失败立即停止放量：

1. PostgreSQL 影子对照与只读 Timeline。
2. 内部 managed 单 Agent，无外部写工具。
3. 只读工具和 Artifact。
4. 审批后的写工具。
5. Child DAG 与 Reviewer。
6. 外部 Webhook Delivery。
7. chaintower signed Agent Context（保留 development Header adapter，生产不得开启）。
8. Hands → chaintower MCP `workload_trusted_context`。

应用回滚不得回滚或删除 Canonical Event。Schema 回滚仅在确认新表没有继续写入且已导出审计记录后，
执行 `0007_m6_observability_reliability.down.sql`；通常优先回滚应用并保留向前兼容的观测 Schema。
