# Projection Service

## 定位

Projection Service 将 Canonical Session Events 转换为面向调度、协作、查询、结果交付和审批的只读模型。它是派生状态计算层，不接收业务命令，也不是第二事实源。

## 核心 Projector

| Projector | 输出 | 消费者 |
|---|---|---|
| Control Projector | pending、runnable、running、恢复引用、资源声明 | Orchestrator |
| Collaboration Projector | Root/Child、DAG、依赖、角色、所有权 | Coordinator、协作查询 |
| Result Projector | 状态、进度、结果、错误、Artifact | Query、Result Delivery |
| Approval Projector | 待审批动作、响应、期限和风险 | Web、Task Gateway、Tool Gateway |
| Audit/Search Projector | 可检索时间线、主体、动作和敏感级别 | 运维、审计 |

## 核心模块

```text
Event Consumer
Ordering / Gap Detector
Idempotency Guard
Projection Handler Registry
Atomic Projection Writer
Checkpoint Manager
Schema Version Manager
Replay / Rebuild Coordinator
Poison Event Queue
Lag Monitor
```

## 消费语义

- 使用 `event_id` 幂等。
- 单个 Session 按 `aggregate_version` 顺序处理。
- 发现版本缺口时暂停该 Aggregate 并回补，不能跳过。
- Projection 数据与 Checkpoint 尽量在同一事务提交。
- 默认使用 at-least-once 消费；重复投递不得改变结果。

## Projection 记录元数据

```text
tenant_id
session_id
root_session_id
projection_name
projection_schema_version
source_aggregate_version
source_event_id
projected_at
```

## Read-your-writes

Projection 最终一致。需要读取刚写入状态时，调用方携带 `min_version`：

```text
Query(min_version=42)
  -> projection version >= 42: 返回
  -> projection version < 42: 等待、202 或 Retry-After
```

Orchestrator 不应调度落后于触发事件版本的 Control Projection。

## 重放与迁移

支持：

- 全量重建某个 Projection。
- 按租户、Root Session 或时间范围局部重建。
- 新旧 Schema 双写。
- Shadow Projection 验证后原子切换。
- 从指定 Event / Checkpoint 继续。
- Poison Event 隔离、人工修复和重放。

Projection 中不得保存无法从 Canonical Events 或 Artifact 重建的唯一事实。

## 接口

```text
consume(event)
project(event, projectionName)
getCheckpoint(projectorId, partition)
waitForVersion(sessionId, version)
rebuild(projectionName, scope)
pausePartition(partition)
resumePartition(partition)
```

## 失败处理

- 暂时性数据库错误按分区退避重试。
- Schema 不兼容进入 Poison Queue 并告警。
- 单个 Aggregate 错误不应阻塞所有租户。
- 重建期间旧 Projection 继续服务，避免读请求中断。

## 观测指标

```text
projection_lag_events
projection_lag_seconds
checkpoint_version
duplicate_events_total
event_gap_total
poison_events_total
rebuild_progress
projection_write_latency
```

## 验收条件

- 重复消费同一事件不产生重复数据。
- 删除任一 Projection 后能够完整重建。
- 不同 Session 可以并行，同一 Session 保持顺序。
- Projector 失败不会修改 Canonical Event。
