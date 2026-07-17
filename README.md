# AuraClaw

AuraClaw 是一个遵循 Managed Agent 架构的纯 Python 后端服务。系统以 Canonical
Session Event Log 为事实源，以可重建 Projection 提供查询，并把运行时控制、Agent
Runtime、工具执行和结果交付保持为清晰的逻辑边界。

当前版本完成第一条后端竖切链路：

```text
Task API -> Session Aggregate -> Canonical Events + Outbox
         -> Projection -> Task Query API
```

## 本地启动

```bash
uv sync --extra dev
uv run uvicorn auraclaw.main:app --reload
```

服务启动后可访问：

- `GET /health/live`
- `GET /health/ready`
- `POST /v1/tasks`
- `GET /v1/tasks/{session_id}`
- `POST /v1/sessions/{session_id}/cancel`
- `POST /v1/sessions/{session_id}/resume`

开发检查：

```bash
uv run ruff check .
uv run mypy src/auraclaw
uv run pytest
```

## 代码结构

```text
src/auraclaw/
  api/             HTTP 接入与查询边界
  application/     用例编排，不保存事实
  contracts/       跨模块事件、状态和错误契约
  domain/          Session 聚合与状态机
  infrastructure/  Event Store、Outbox 等适配器
  projections/     可重建 Read Model
  runtime/         Orchestrator/Agent Runtime 端口
```

设计依据见 [Managed Agent 系统架构](docs/Managed%20Agent%20系统架构/00%20Managed%20Agent%20系统架构总览.md)，实施顺序见 [开发方案与实施计划](docs/Managed%20Agent%20开发方案与实施计划.md)。

## 当前边界

默认存储适配器为内存实现，用于验证领域契约与 API。它保留 Event Store、Outbox、
Projection 的独立接口，但不具备进程重启持久性。PostgreSQL 表结构已放在
`migrations/0001_initial.sql`，下一阶段将实现生产适配器、异步 Outbox Relay 和
Projection Worker。
