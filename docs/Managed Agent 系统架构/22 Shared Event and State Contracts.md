# Shared Event and State Contracts

本文定义各组件共享的标识、事件信封、状态机和幂等语义，避免不同服务自行解释 Session、Run、Delivery 和 Approval。

## 统一标识

| 字段 | 含义 |
|---|---|
| `tenant_id` | 租户隔离边界 |
| `session_id` | 一个 Root 或 Child Session |
| `root_session_id` | 整个任务树的根标识 |
| `parent_session_id` | 直接父 Session |
| `run_id` | Session 的一次运行尝试 |
| `event_id` | 全局唯一事件标识，用于去重 |
| `command_id` | 命令幂等标识 |
| `correlation_id` | 跨服务调用关联标识 |
| `causation_id` | 直接导致当前事件的命令或事件 |
| `artifact_id` | 不可变 Artifact 版本标识 |

## Canonical Event 信封

```json
{
  "event_id": "evt_01...",
  "tenant_id": "tenant_1",
  "root_session_id": "ses_root",
  "session_id": "ses_child",
  "run_id": "run_1",
  "aggregate_version": 42,
  "type": "tool.call.completed",
  "occurred_at": "2026-07-16T10:00:00Z",
  "actor": {"type": "agent", "id": "worker_1"},
  "correlation_id": "corr_1",
  "causation_id": "cmd_1",
  "visibility": "internal",
  "schema_version": 1,
  "payload": {}
}
```

约束：

- `aggregate_version` 在单个 Session 内严格单调递增。
- 同一个 `command_id` 只能产生一次业务效果。
- 事件写入成功后不可修改，只能追加补偿事件。
- 大对象只保存 `artifact_ref`，不直接写入 Event Log。
- 敏感字段必须在写入前分类，不能依赖展示端补救。

## Runtime Event 信封

```json
{
  "event_id": "rte_01...",
  "root_session_id": "ses_root",
  "session_id": "ses_child",
  "run_id": "run_1",
  "sequence": 128,
  "type": "model.output.delta",
  "timestamp": "2026-07-16T10:00:01Z",
  "durable": false,
  "visibility": "user",
  "payload": {"delta": "..."}
}
```

Runtime Event 使用 `root_session_id` 或 `session_id` 作为分区键。`sequence` 只保证一个 Stream 范围内的显示顺序，不能代替 Canonical `aggregate_version`。

## Session 状态

```text
created
  -> pending
  -> runnable
  -> running
  -> waiting_for_human | paused | retry_wait
  -> runnable
  -> completed | failed | cancelled
```

状态转换只能由 Canonical Events 推导。Control State Store 的实例状态不能直接修改业务 Session 状态。

## Child Session 状态与依赖

```text
blocked     尚有未完成依赖
runnable    所有依赖满足并且未被取消
running     已获得执行所有权
completed   已发布符合输出契约的结果
failed      不可继续或重试耗尽
cancelled   被父任务、用户或策略取消
```

Task DAG 必须无环。依赖完成和所有权变更通过事件表达，由 Collaboration Projection 计算 `runnable`。

## Delivery 状态

```text
requested -> attempting -> succeeded
                       -> retry_wait -> attempting
                       -> dead_lettered
                       -> cancelled
```

`delivery_id` 在同一个 Sink 中幂等。成功、失败、重试和 Dead Letter 必须回写 Session。

## Approval 状态

```text
requested -> waiting -> approved | rejected | expired | cancelled
```

批准必须绑定：

```text
approval_id + session_id + action_digest + policy_version
```

执行前由 Tool Gateway 再次校验，参数发生变化必须重新审批。

## 错误模型

```json
{
  "code": "sandbox_unavailable",
  "category": "infrastructure",
  "retryable": true,
  "scope": "tool_call",
  "user_message": "执行环境暂时不可用",
  "internal_detail_ref": "artifact://log/..."
}
```

错误类别至少包括：`model`、`tool`、`sandbox`、`orchestration`、`external`、`policy`、`delivery` 和 `data_consistency`。

## 版本与兼容性

- API 使用显式版本或兼容字段演进。
- Event Schema 只能向后兼容增加可选字段；破坏性修改升级 `schema_version`。
- Projector 必须能识别未知事件并进入隔离队列，不能静默跳过关键事件。
- Artifact、Approval 和 Tool Schema 都需要独立版本。
