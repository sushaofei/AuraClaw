# Control State Store

## 定位

Control State Store 是 Orchestrator 的运行控制数据库，保存实例、租约、调度队列、Assignment 和短期恢复状态。它可以使用 PostgreSQL 或强一致 KV 实现，但不保存任务语义事实。

## 核心模块与表

### Runtime Instance Registry

```text
runtime_id, runtime_type, role, node_id
capabilities, status, last_heartbeat_at, started_at
```

### Lease Manager

```text
resource_id, lease_owner, expires_at
fencing_token, lease_version
```

支持 acquire、renew、release、expire 和 Compare-and-Set。

### Runnable Queue

```text
task_id, session_id, priority, available_at
required_capability, attempt, queue_partition, status
```

支持 enqueue、claim、ack、release 和 reschedule。

### Assignment Manager

```text
task_id, runtime_id, assignment_status
assigned_at, started_at, deadline, fencing_token
```

### Retry / Timer Manager

保存 `retry_count`、`next_retry_at`、`backoff_policy`、`timeout_at` 和最后错误分类。

### Capacity / Concurrency

保存租户、模型、工具、Sandbox、Agent Role 的容量和预留计数。

### Recovery / Reconciliation

扫描过期 Lease、僵尸 Assignment、丢失心跳和已终结 Session 的残留控制数据。

## 原子操作

```text
claimRunnable(queue, worker, limit)
acquireLease(resource, owner, ttl)
renewLease(resource, owner, fencingToken)
assign(task, runtime, fencingToken)
reserveCapacity(scope, amount)
releaseAssignment(task, outcome)
```

所有竞争操作必须依赖原子条件更新、唯一约束或行锁，不能使用“先查后写”。

## 与 Read Model Store 的区别

| Read Model Store | Control State Store |
|---|---|
| 业务事实的派生视图 | 调度器的运行态工作存储 |
| Projection Service 写 | Orchestrator 写 |
| 最终一致、可重建 | 强一致竞争更新 |
| 面向查询 | 面向租约、Claim 和恢复 |
| 接近 Session 生命周期 | 通常短于任务生命周期 |

可以共用 PostgreSQL 集群，但应使用独立 Schema、独立写入者、无跨域外键和无跨 Store 事务。

## 清理策略

- Lease 到期后保留短期审计摘要。
- 已完成 Assignment 延迟清理，便于 Reconciliation。
- Runnable Item 在 Canonical 终态后强制失效。
- 高密度 Heartbeat 按时间分区或覆盖更新。

## 观测指标

```text
runnable_queue_depth
claim_conflict
lease_expired / lease_renew_failed
heartbeat_age
orphan_assignment
capacity_utilization
reconciliation_repairs
```

## 验收条件

- 并发 Claim 同一任务只有一个成功。
- Lease 重新分配后 Fencing Token 单调递增。
- 显式关闭或 Child 终态 Session 不会再次被 Runnable Queue 调度；Root Session 的 Run 终态会释放执行租约，后续 Run 可再次入队。
- 数据库恢复后 Reconciler 能消除僵尸 Assignment。

## 当前实现对照

- 归属：`control/ports.py` 与 `infrastructure/persistence/postgres_control_store.py`；表包括 runtime lease、
  runnable、assignment、instance、capacity reservation、checkpoint、cancellation 和 fencing watermark。
- Claim/renew/release/complete 使用数据库条件更新、租约期限和 token 校验，支持多副本 worker 接管。
- 内存实现只用于开发与测试，生产装配要求 SQL 后端。

## 现有缺陷与待完善

- Retry/Timer Manager 不是独立完整子系统；延迟重试与 deadline 的索引、批量扫描和时钟语义需要统一。
- Runtime Instance Registry 缺少跨区域拓扑、维护/drain 状态和容量预测字段。
- Store 可从 Canonical/Runnable Feed 部分恢复，但尚无一键全量重建、差异校验和恢复期间的调度隔离流程。
- 待补：表膨胀清理、热点租户分区、锁等待监控、故障注入与恢复工具。
