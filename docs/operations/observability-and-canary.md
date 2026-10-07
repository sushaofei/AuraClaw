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
GET /v1/operations/metrics/summary?window_hours=24
```

`metrics/summary` 在 tenant 边界内按指标返回 count、sum、average、min、max、p50、p95、p99，窗口限制
1～720 小时。

## 外部观测出口

生产部署必须显式配置 `AURACLAW_OBSERVABILITY_OTLP_HTTP_ENDPOINT` 和
`AURACLAW_ALERT_RECEIVER_URL`。Trace 与 Metric 使用 OTLP/HTTP JSON，分别投递到 Collector 的
`/v1/traces`、`/v1/metrics`；规则告警使用 Alertmanager v2 `POST /api/v2/alerts`。两个地址在
production profile 下必须使用 HTTPS。若平台需要应用层令牌，分别通过
`AURACLAW_OBSERVABILITY_EXPORTER_TOKEN_FILE`、`AURACLAW_ALERT_RECEIVER_TOKEN_FILE` 注入，令牌不写入
环境模板、日志或告警标签。

投递顺序固定为先写 PostgreSQL 观测表，再进入有界异步队列执行外部投递。超时、重试和队列容量由
`AURACLAW_OBSERVABILITY_EXPORT_TIMEOUT_SECONDS`、
`AURACLAW_OBSERVABILITY_EXPORT_RETRY_ATTEMPTS`、
`AURACLAW_OBSERVABILITY_EXPORT_QUEUE_CAPACITY` 控制。Collector 或 Alertmanager 不可用时记录
`observability_export_failed` 结构化错误，但不得回滚 Canonical Event、改变 Session 状态或触发工具重放；
PostgreSQL 中的 Trace、Metric、Alert 仍是补采和事故审计依据。告警接收端按 `alertname`、`tenant_id`、
`session_id` 和稳定 `alert_id` 聚合，禁止将 Prompt、Response、凭据或 Artifact 正文复制进标签。

发布前至少验证：Collector 对两类 OTLP 请求返回 2xx、Alertmanager 对合成告警返回 2xx、应用日志中没有
Authorization 值、关闭外部端点后业务请求仍只产生可观测性降级而非业务失败。连续投递失败必须由平台侧
日志告警捕获；恢复后根据 PostgreSQL 保留窗口补采缺口，补采不得写回业务事件。

Skill 加载灰度至少观察以下门禁：

- 同一 run 第二轮后的 `skill.runtime.content_cache.miss.count` 应为 0；持续非零停止放量。
- `skill.runtime.prompt.rejected.count` 应为 0；出现时按 prompt budget 处理，不复制正文到工单。
- `model.prompt_cache.hit_ratio` 只在 Provider 返回 cached tokens 后评估；开关开启 24 小时且合格长前缀
  样本不少于 100 时，p50 仍为 0 需要检查前缀稳定性或关闭无收益的显式 key。
- `model.ttft.seconds` 使用业务既有 Task-start SLO；相对开关前 24 小时 p95 退化超过 20% 时回滚 cache key
  开关。该相对阈值用于 canary，不替代 Provider 自身可用性 SLO。
- Hands 滚动替换每次只启动一个冷副本；readiness 前的 Artifact 下载量不得超过
  `Publication 数 × 平均包大小` 的单副本容量模型，热 reconciliation 下载必须为 0。

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

### upstream Assertion 验签失败或密钥不可用

Task API 写与敏感读必须 fail closed（401）。先确认 `kid` 仍在 N/N-1 集合、clock skew
未超出配置，再检查 upstream 签发服务。禁止为恢复流量改回裸 `X-Tenant-ID` /
`X-Actor-ID`。轮换时先加载新 `kid`，upstream 切签发后再撤旧密钥。

## 保留、GC 与安全

默认 Metric 30 天、Trace 14 天、Audit 365 天、Alert 90 天：

```bash
uv run auraclaw operations retention
```

Artifact 依据 `retention_until` GC；同租户相同 Hash 的对象仍被其他元数据引用时不删除对象。
Audit 保留时间独立于普通 Log/Trace。租户删除或合规保留必须同时覆盖 Event、Telemetry、Artifact
和 Delivery 数据。每次发布运行 `uv run python scripts/release_gate.py`；任何 Secret 命中均阻断。

Skill admission 安全账本默认保留 365 天，由 Action Hands 启动和周期任务按批次清理。相关配置为
`AURACLAW_SKILL_ADMISSION_RETENTION_DAYS`、`AURACLAW_SKILL_ADMISSION_CLEANUP_INTERVAL_SECONDS`
和 `AURACLAW_SKILL_ADMISSION_CLEANUP_BATCH_SIZE`。清理只删除严格早于 cutoff 的记录，不暴露人工删除
API；缩短保留期前必须先完成合规审批和外部归档。

安全运维使用 `/v1/admin/skill-admissions/metrics?window_hours=24` 检查
`skill.admission.quarantine_ratio`。阈值和最小样本由
`AURACLAW_SKILL_ADMISSION_QUARANTINE_ALERT_RATIO` 与
`AURACLAW_SKILL_ADMISSION_QUARANTINE_ALERT_MIN_SAMPLES` 控制；`insufficient_data` 不应升级为事故，
`firing` 时按 policy version、Source 和 stable finding code 下钻，禁止复制包正文或匹配片段到工单。

## 灰度与回滚

按以下顺序放量，每级观察错误率、恢复率、Projection lag、unknown side effect、Delivery DLQ、
Task API 401/403 和 Secret 扫描；任一门禁失败立即停止放量：

1. PostgreSQL 影子对照与只读 Timeline。
2. 内部 managed 单 Agent，无外部写工具。
3. 只读工具和 Artifact。
4. 审批后的写工具。
5. Child DAG 与 Reviewer。
6. 外部 Webhook Delivery。
7. upstream signed Agent Context（保留 development Header adapter，生产不得开启）。
8. Hands → upstream MCP `workload_trusted_context`。

应用回滚不得回滚或删除 Canonical Event。Schema 回滚仅在确认新表没有继续写入且已导出审计记录后，
执行 `0007_m6_observability_reliability.down.sql`；通常优先回滚应用并保留向前兼容的观测 Schema。
