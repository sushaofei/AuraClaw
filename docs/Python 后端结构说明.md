# Python 后端结构说明

本项目已移除旧 TypeScript CLI/Agent 实现，重构为纯 Python Managed Agent 后端。

## 当前完成范围

第一阶段实现架构中的事实与查询主链：

```text
HTTP Command
  -> Task Application Service
  -> Session Aggregate
  -> Canonical Event Store + Transactional Outbox
  -> Task Projection
  -> Query API
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
- `runtime`：为后续 Orchestrator、Coordinator、Worker、Reviewer 保留端口。

## 与目标架构的对应关系

| 目标组件 | 当前模块 | 状态 |
|---|---|---|
| Task Gateway / Admission | `api/routes/tasks.py` | 最小命令边界已实现 |
| Session / Collaboration | `domain/session.py` | Root Session 事实核心已实现 |
| Canonical Event / Outbox | `infrastructure/postgres.py` | PostgreSQL 与内存适配器已实现 |
| Projection / Read Model | `projections/tasks.py`、`infrastructure/postgres.py` | 幂等、gap、checkpoint、重建已实现 |
| Task Query / Result | `api/routes/tasks.py` | 状态与结果查询已实现 |
| Control State / Orchestrator | `runtime/ports.py`、SQL Schema | 仅端口/表结构 |
| 其他组件 | 对应设计文档 | 按实施计划继续建设 |

## M1 运维边界

- API 请求会尝试即时 relay，以提供开发和单进程部署下的快速可见性；可靠恢复仍以
  Transactional Outbox 为准。
- 独立 Worker 使用 `auraclaw projection relay --watch` 持续消费。
- `auraclaw projection rebuild [--tenant ...]` 从 Canonical Event Log 重建 Task Read Model。
- Snapshot 仅优化聚合加载；损坏或缺失时仍可从完整事件历史恢复。
- 下一阶段实现 Control Store、Lease、Fencing Token、Orchestrator 与 Agent Runtime。
