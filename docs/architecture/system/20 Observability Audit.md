# Observability / Audit

## 定位

Observability / Audit 是横切所有组件的可观测与审计平面，用于定位问题发生在任务事实、模型、工具、运行环境、调度、投影、流式交付还是安全策略。

## 核心模块

```text
Trace Collector
Metrics Pipeline
Structured Log Pipeline
Audit Event Store
Session Timeline View
Cost / Usage Accounting
SLO / Alerting
Sensitive Data Redaction
Debug Artifact Store
Replay / Investigation Tools
```

## Trace 分层

```text
Session Trace
Agent / Harness Trace
Model Trace
Tool Trace
Hands / Sandbox Trace
Orchestration Trace
Projection Trace
Streaming Trace
Result Delivery Trace
Policy / Credential Trace
```

统一 Trace Context 至少包含：

```text
trace_id, span_id, tenant_id
root_session_id, session_id, run_id
event_id, command_id, tool_invocation_id
runtime_id, delivery_id, approval_id
```

## Metrics

### 任务体验

- 接纳到 runnable、runnable 到 started、Time to First Token、总完成时间。
- 成功、失败、取消、等待审批和恢复次数。

### 可靠性

- Session append、Outbox lag、Projection lag、Lease conflict。
- Runtime crash、Tool unknown side effect、Delivery DLQ。

### 资源与成本

- Model Token/Cost、Sandbox CPU/Memory、Artifact Storage、外部 API 使用量。

### Streaming

- 活跃连接、Event-to-client 延迟、Replay 命中、慢消费者和序列缺口。

## Audit Event

必须审计：

- 权限和策略决策。
- Human Approval 请求与响应。
- 凭证代调用。
- 有副作用工具调用。
- Artifact 下载和分享。
- Session Handoff、取消和人工重投。
- Result Delivery 的签名、Attempt 和 ACK。

Audit 记录不可包含真实 Secret；敏感 Payload 使用受控 Artifact Reference。

## 调试视图

理想时间线能够回答：

```text
模型看到了什么 Context？
为什么调用工具？
实际执行了什么？
结果是否进入 Session？
Projection 是否追上？
Streaming 是否交付？
失败发生在哪个层？
能否从哪个事件恢复？
```

## SLO 示例

```text
Session append availability / latency
Projection freshness
Task start latency
Streaming event latency
Result query latency
Delivery success within target time
Approval notification latency
```

## 数据治理

- Log、Trace、Audit 使用不同保留和访问策略。
- Prompt/Response 默认不全文进入普通日志。
- Debug Artifact 需要审批、期限和访问审计。
- Tenant 删除与合规保留需要同时覆盖各条 Telemetry 管道。

## 验收条件

- 任一失败可以关联到 Root/Child Session 和具体组件 Span。
- 审计员不需要访问生产数据库即可还原高风险动作。
- 任何日志和 Trace 中都检测不到真实 Secret。
- Projection Lag、Lease 丢失和 Delivery DLQ 有主动告警。
