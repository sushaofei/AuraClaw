# Python 后端结构说明

本项目已移除旧 TypeScript CLI/Agent 实现，重构为纯 Python Managed Agent 后端。

## 当前完成范围

当前实现覆盖事实/查询主链与 M2 Managed Runtime 控制链：

```text
HTTP Command
  -> Task Application Service
  -> Session Aggregate
  -> Canonical Event Store + Transactional Outbox
  -> Task Projection
  -> Query API

Control Projection
  -> Runnable Queue / Atomic Claim
  -> Lease + Monotonic Fencing Token
  -> Orchestrator / Runtime Assignment
  -> Recoverable Agent Harness
  -> Session / Model / Tool / Runtime Event Ports
```

内存适配器用于快速测试。PostgreSQL 适配器负责 Canonical Event、Aggregate Version、
Command Dedup 与 Outbox 的单事务提交，并保存可选 Snapshot。Outbox Relay 以
at-least-once 方式驱动幂等投影；投影记录 source version、processed event 和 checkpoint，
检测 gap，并将未知关键事件放入 Poison Event Queue。

## 依赖方向

```text
api -> application -> domain -> contracts
                |       ^
                v       |
          infrastructure / projections
```

- `contracts`：跨边界稳定类型，不依赖框架。
- `domain`：Session 聚合、状态机和端口，不依赖 FastAPI/数据库。
- `application`：编排命令，不直接保存状态。
- `infrastructure`：Event Store、Outbox 和未来 PostgreSQL 适配器。
- `projections`：可删除、可重建的查询模型。
- `api`：鉴权上下文、幂等键、版本前置条件和 HTTP 表示。
- `runtime`：Runtime 端口、fenced client、可恢复 Harness 和 Model Gateway。

## 与目标架构的对应关系

| 目标组件 | 当前模块 | 状态 |
|---|---|---|
| Task Gateway / Admission | `api/routes/tasks.py` | 最小命令边界已实现 |
| Session / Collaboration | `domain/session.py` | Root Session 事实核心已实现 |
| Canonical Event / Outbox | `infrastructure/postgres.py` | PostgreSQL 与内存适配器已实现 |
| Projection / Read Model | `projections/tasks.py`、`infrastructure/postgres.py` | 幂等、gap、checkpoint、重建已实现 |
| Task Query / Result | `api/routes/tasks.py` | 状态与结果查询已实现 |
| Control State | `infrastructure/control_memory.py`、`control_postgres.py` | Queue、Lease、Fencing、Assignment、Heartbeat、Capacity、Checkpoint |
| Orchestrator | `application/orchestration.py` | watch、claim、schedule、provision、cancel、heartbeat、reconcile |
| Agent Runtime | `runtime/harness.py`、`runtime/clients.py` | Budget、Deadline、Cancel、Checkpoint 与四类端口 |
| Model Gateway | `runtime/model_gateway.py` | Provider Adapter、路由和 Gateway 内 Credential 解析 |
| 其他组件 | 对应设计文档 | 按实施计划继续建设 |

## M2 运维边界

- API 请求会尝试即时 relay，以提供开发和单进程部署下的快速可见性；可靠恢复仍以
  Transactional Outbox 为准。
- 独立 Worker 使用 `auraclaw projection relay --watch` 持续消费。
- `auraclaw projection rebuild [--tenant ...]` 从 Canonical Event Log 重建 Task Read Model。
- Snapshot 仅优化聚合加载；损坏或缺失时仍可从完整事件历史恢复。
- Control State Store 与 Canonical Event Store 使用独立 Schema/写入边界，不做跨 Store 事务。
- Orchestrator 只调度资源，不解析目标、不拆分 Task DAG。
- Runtime Event Bus 故障不阻止完整模型输出与最终结果写入 Canonical Event Log。
- Runtime checkpoint、模型调用 ID 和工具调用 ID 都稳定，接管后不会重复产生业务事实；
  Tool Gateway 的真实外部副作用幂等、审批和 Sandbox 在 M3 完成。
