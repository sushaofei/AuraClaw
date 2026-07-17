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

当前内存适配器让该链路可直接运行和测试。它不是生产持久化方案；PostgreSQL 初始
Schema 已定义在 `migrations/0001_initial.sql`。

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
| Canonical Event / Outbox | `infrastructure/memory.py` | 开发适配器已实现 |
| Projection / Read Model | `projections/tasks.py` | Task 视图已实现 |
| Task Query / Result | `api/routes/tasks.py` | 状态与结果查询已实现 |
| Control State / Orchestrator | `runtime/ports.py`、SQL Schema | 仅端口/表结构 |
| 其他组件 | 对应设计文档 | 按实施计划继续建设 |

## 下一阶段

1. 实现 PostgreSQL Event Store，确保 Event、Version、Command Dedup、Outbox 同事务。
2. 把同步投影替换为 Outbox Relay + Projection Worker。
3. 实现 Control Store、Lease、Fencing Token 与 Orchestrator。
4. 将 Model Gateway 和 Agent Runtime 接入 `run.requested` 主链。
5. 随后建设 Tool Gateway、Hands、Artifact、Policy/Approval、Streaming 与 Delivery。
