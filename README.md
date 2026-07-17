# AuraClaw

AuraClaw 是一个遵循 Managed Agent 架构的纯 Python 后端服务。系统以 Canonical
Session Event Log 为事实源，以可重建 Projection 提供查询，并把运行时控制、Agent
Runtime、工具执行和结果交付保持为清晰的逻辑边界。

当前版本已实现 M2 Managed Runtime 与控制平面：

```text
Task API -> Session Aggregate -> PostgreSQL Canonical Events + Transactional Outbox
         -> Outbox Relay -> Disposable Projection -> Task Query API
Control Projection -> Runnable Queue -> Orchestrator -> Lease/Fencing/Assignment
                   -> Recoverable Agent Harness -> Model/Tool/Session Gateway Ports
```

Agent Runtime 不读取模型 Provider Secret，也不直接修改 Session 状态。完整模型输出进入
Canonical Event Log；Token Delta 只发布到可丢弃的 Runtime Event Bus。Runtime 在模型调用前、
模型完成后、工具执行前、工具执行后均有持久 checkpoint，可由新 fencing token 的 Runtime
接管。

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
- `GET /v1/tasks/{session_id}/result`
- `POST /v1/sessions/{session_id}/messages`
- `POST /v1/sessions/{session_id}/runs`
- `POST /v1/sessions/{session_id}/cancel`
- `POST /v1/sessions/{session_id}/resume`

写接口要求 `Idempotency-Key`；修改既有 Session 时还要求 `X-Expected-Version`。
查询支持 `ETag`、`If-None-Match` 和 `min_version`，投影未追上时返回 `202` 与
`Retry-After`。

## PostgreSQL

存储配置支持两种形式：

- `AURACLAW_DATABASE_URL=postgresql+asyncpg://...`
- 现有环境的 `DB_HOST`、`DB_PORT`、`DB_USER`、`DB_PWD`、`DB_NAME_DEV`、
  `DB_NAME_PRO`（旧 `DB_NAME` 仍兼容）

当存在完整 `DB_*` 配置时默认启用 PostgreSQL；可设置
`AURACLAW_STORAGE_BACKEND=memory` 强制使用开发内存适配器。首次启动前，按顺序应用：

`AURACLAW_ENV=development/test` 选择 `DB_NAME_DEV`，`production/prod` 选择
`DB_NAME_PRO`。

```text
migrations/0001_initial.sql
migrations/0002_m1_fact_query.sql
migrations/0003_m2_managed_runtime.sql
```

对应的 `.down.sql` 文件提供 M1/M2 Schema 回滚。生产部署应由迁移系统执行这些 SQL，
不应由 API 进程在启动时自动修改 Schema。

Outbox Worker 与投影重建：

```bash
uv run auraclaw projection relay --watch
uv run auraclaw projection rebuild
uv run auraclaw projection rebuild --tenant tenant_1
```

重建只读取 Canonical Event Log；Read Model 和 checkpoint 可以删除后恢复。真实
PostgreSQL 集成测试使用独立测试库：

```bash
# 默认使用 .env 的 DB_NAME_DEV
uv run pytest tests/integration/test_postgres_m1.py
```

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
  runtime/         Runtime 端口、Fenced Clients、Harness、Model Gateway
```

设计依据见 [Managed Agent 系统架构](docs/Managed%20Agent%20系统架构/00%20Managed%20Agent%20系统架构总览.md)，实施顺序见 [开发方案与实施计划](docs/Managed%20Agent%20开发方案与实施计划.md)。

## 当前边界

项目保留内存适配器用于快速测试；配置 PostgreSQL 后使用持久 Event Store、Control
State Store、Snapshot、Command Dedup、Transactional Outbox、Task Read Model、Projector
Checkpoint 和 Poison Event Queue。M2 的 Tool Client 是受 fencing 保护的执行边界与幂等
开发适配器；真实 Tool Registry、审批、Sandbox、Credential Proxy 和 Artifact 闭环属于 M3。
