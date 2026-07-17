# Runtime Event Bus

## 定位

Runtime Event Bus 承载 Token Delta、进度、工具状态、协作状态和运行时生命周期等短期实时事件。它面向 Streaming 和实时观测，不是 Canonical Event Log，也不能作为可靠结果交付的唯一来源。

## 技术实现

可以使用 Kafka、Pulsar 或 NATS JetStream。采用 Kafka 时：

```text
Topic: managed-agent.runtime-events
Message Key: root_session_id 或 session_id
Consumer Group: streaming-ingestor
Retention: 小时到数天
Delivery: at-least-once
```

同一 Stream 的事件使用同一分区键，业务顺序仍由 `sequence` 检查。

## 核心模块

```text
Topic / Partition Management
Producer SDK
Event Schema Validation
Partition Key Strategy
Retention / Replay Buffer
Consumer Group Management
ACL / Tenant Isolation
Lag Monitoring
Quota / Size Limit
Dead Producer Detection
```

Kafka 是基础设施实现，不是业务模块；事件过滤、连接路由和 SSE 写入属于 Streaming Gateway。

## 生产者

| 生产者 | 事件 |
|---|---|
| Agent Runtime | model.output.delta、plan.updated、agent.progress |
| Tool Gateway | tool.started、tool.progress、tool.completed |
| Orchestrator | runtime.started、paused、migrated、failed |
| Session/Collaboration | child.created、approval.requested、run.completed |
| Result Delivery | delivery.attempting、retrying、succeeded |

## 事件分类

短期事件：

```text
model.output.delta
runtime.progress
typing
heartbeat
token.usage.partial
tool.progress
```

完成输出、工具结果、审批结果和终态必须同时进入 Canonical Session Log。

## Producer SDK

SDK 负责：

- 自动填充 tenant、root/session/run、event_id 和 timestamp。
- 分配或校验 sequence。
- Schema 和消息大小校验。
- visibility 分类和敏感字段拦截。
- 批量、压缩和 Token Delta 合并。
- 重试、指标和 Trace Context 传播。

## 消费和重放

- Consumer 使用 Offset 恢复内部消费位置。
- 浏览器游标使用公开 `session_seq` 或不透明 Cursor，不暴露 Kafka Offset。
- Replay 只承诺保留期内可用；超出保留期从 Session Snapshot/完整消息恢复。
- Consumer 以 `event_id` 去重并检测 sequence gap。

## 背压

- Producer 侧合并过细 Token Delta。
- 限制单事件和单 Session 每秒事件量。
- Streaming Gateway 每连接使用有界队列。
- 非关键进度允许采样或覆盖；终态和审批通知不可静默丢弃。

## 安全与观测

- Topic 和 Consumer Group 按服务账户授权。
- 消息不得包含 Secret、完整凭证或未脱敏 Tool Result。
- 指标：produce latency、consumer lag、partition skew、replay hit、dropped/coalesced events、schema reject。

## 验收条件

- Runtime Event Bus 不可用不会破坏 Canonical Session 事实。
- 同一 Root Stream 可保持可解释顺序。
- 慢网页不会阻塞 Agent Runtime。
- 删除短期事件后仍能查询任务最终结果。
