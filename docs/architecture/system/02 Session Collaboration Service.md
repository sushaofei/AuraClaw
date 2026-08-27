# Session / Collaboration Service

## 定位

Session / Collaboration Service 是 Managed Agent 的稳定事实核心。它保存 Root/Child Session、Task DAG、所有权、结果引用和全部重要生命周期事件。Brain、Hands、Orchestrator 和 Projector 都可以替换，Canonical Event Log 不能丢失。

## 逻辑边界

```text
Session Service
  Command Handler
  Canonical Event Log
  Snapshot / Checkpoint
  Transactional Outbox

Collaboration Service
  Root / Child Session
  Task DAG Validation
  Dependency / Ownership
  Join / Handoff / Publish Result
```

早期可以合并部署和存储，但所有协作变化仍必须追加为 Canonical Events。

## 核心模块

| 模块 | 功能 |
|---|---|
| Command Handler | 校验命令、前置版本、权限和状态转换 |
| Event Store | 按 Session 追加不可变事件并分配版本 |
| Aggregate Loader | 从 Snapshot + Events 恢复当前聚合状态 |
| Session Relationship | 管理 root、parent、role 和 lineage |
| Task DAG Validator | 防止环、非法依赖和跨租户引用 |
| Ownership Manager | 单写者、执行租约引用和 handoff 事实 |
| Result Publisher | 保存 result summary、result_ref、artifact_refs |
| Snapshot Manager | 控制事件重放成本，但不替代原始历史 |
| Transactional Outbox | 与 Event Log 同事务写入待发布事件 |
| Outbox Relay | 发布到 Projector、Delivery 和通知通道 |

## 领域模型

```text
Session
  session_id
  root_session_id
  parent_session_id
  role
  goal
  input_refs
  output_contract
  dependency_ids
  owner
  status
  result_summary
  result_ref
  artifact_refs
  aggregate_version
```

Root 和 Child 使用同一模型。串行、并行、树形和混合协作由 `dependency_ids` 形成的 DAG 表达，不定义不同 Root Session 类型。

## 核心接口

```text
createSession(command)
appendEvent(sessionId, expectedVersion, event)
getEvents(sessionId, fromVersion)
getSnapshot(sessionId)
requestRun(sessionId)
closeSession(sessionId, reason)
createChild(parentId, role, goal, outputContract)
setDependencies(childId, dependencyIds)
delegate(childId, agentProfile)
publishResult(sessionId, resultRef)
handoff(sessionId, targetRole)
cancel(sessionId, reason)
```

## Canonical Events

```text
session.created
user.message.appended
run.requested / scheduled / started
child.created / dependency.changed / delegated
model.output.completed
tool.call.requested / completed / failed
artifact.attached
approval.requested / human.response.recorded
session.paused / resumed / handed_off / closed
run.completed / failed / cancelled
delivery.succeeded / retrying / dead_lettered
```

Token Delta、Typing、Heartbeat 等高频事件不进入 Canonical Log。

Root Session 与 Run 使用独立生命周期。一条 Root Session 可按顺序承载多个 Run；
`run.completed / failed / cancelled` 只结束对应 Run，并使 Root Session 回到 `ready`。
只有显式 `session.closed` 才终结 Root Session。Child Session 仍以发布合同结果作为终态，
以保持 DAG、Review 与 Join 语义。

## Transactional Outbox

同一数据库事务必须同时完成：

```text
1. 写入 Canonical Event
2. 增加 aggregate_version
3. 写入 Outbox Record
```

Outbox 至少包含：`event_id`、目标通道、payload_ref、created_at、publish_attempt、published_at`。Relay 使用 `event_id` 幂等发布。Projection 和 Result Delivery 都不能依赖提交后的普通 HTTP 回调。

## 并发与所有权

- 每次写入携带 `expected_version`，拒绝过期更新。
- 同一 Session 同一时刻只有执行租约持有者可以推进运行状态。
- 多 Worker 写入隔离的 Child Session，不并发修改 Root Session。
- Artifact 使用独立所有权或 Patch Merge。
- 外部副作用使用 `command_id` / `tool_invocation_id` 幂等。

## 恢复与保留

- 使用 Snapshot 降低长 Session 重放成本。
- 大对象、日志和文件进入 Artifact Store，Session 只保存引用。
- 热事件、冷事件和审计归档使用分层保留。
- Snapshot 损坏时必须能够从 Event Log 重建。
- Session 删除遵循租户、合规和 Artifact 引用策略。

## 安全与观测

- 每条事件记录 actor、权限上下文和 schema version。
- 对敏感字段分类、加密和访问审计。
- 核心指标：append latency、版本冲突、Outbox lag、Aggregate 重放长度、Snapshot 命中率。

## 验收条件

- Session 和 Outbox 不会出现一方成功、一方失败。
- Task DAG 无环，跨租户依赖被拒绝。
- Runtime 全部死亡后仍能恢复 Root/Child 状态。
- Projection 全部删除后可以从 Event Log 重建。
