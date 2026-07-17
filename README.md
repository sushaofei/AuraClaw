# AuraClaw

AuraClaw 是一个遵循 Managed Agent 架构的纯 Python 后端服务。系统以 Canonical
Session Event Log 为事实源，以可重建 Projection 提供查询，并把运行时控制、Agent
Runtime、工具执行和结果交付保持为清晰的逻辑边界。

当前版本已实现 M4 多 Agent 协作与独立评审闭环：

```text
Task API -> Session Aggregate -> PostgreSQL Canonical Events + Transactional Outbox
         -> Outbox Relay -> Disposable Projection -> Task Query API
Control Projection -> Runnable Queue -> Orchestrator -> Lease/Fencing/Assignment
                   -> Recoverable Agent Harness -> Model/Tool/Session Gateway Ports
Tool Call -> Registry/Schema/Policy -> Approval Digest -> Hands/Credential Proxy
          -> Result Redaction/Normalization -> Inline Result or Artifact Reference
Root Session -> Coordinator -> Child DAG/Contracts -> Runnable Projection
             -> Worker Result -> Reviewer Evidence/Decision -> Join + Lineage
```

Agent Runtime 不读取模型 Provider Secret，也不直接修改 Session 状态。完整模型输出进入
Canonical Event Log；Token Delta 只发布到可丢弃的 Runtime Event Bus。Runtime 在模型调用前、
模型完成后、工具执行前、工具执行后均有持久 checkpoint，可由新 fencing token 的 Runtime
接管。

写入与破坏性工具在没有匹配 `Session + action digest + policy version` 的有效审批时
fail closed。Human Response 只能通过 Task Gateway 写回 Canonical Event；审批视图可从事件
重建。Hands 不继承宿主 Secret，Credential Proxy 代 Runtime 使用 `credential_ref`，并在结果
进入 Session 前执行脱敏。超出内联大小上限的 Tool Result 直接保存为不可变 Artifact，
Session 只接收 `artifact_ref`。

复杂任务由 Coordinator 通过 Collaboration Service 创建 Root/Child DAG；稳定 `task_key` 保证
重启不会重复创建相同 Child。服务强制同 tenant/Root、DAG 无环以及深度、宽度、Child 总数和
预算限制。Worker 只能发布自己拥有的 Child Result，Reviewer 使用独立 Review Session，只能
发布带证据的三态决策，不能覆盖 Worker Artifact。Join 生成的 Root Result 保存 Child Result、
Review Evidence 和 Artifact lineage。

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
- `GET /v1/tasks/{session_id}/children`
- `POST /v1/sessions/{session_id}/messages`
- `POST /v1/sessions/{session_id}/runs`
- `POST /v1/sessions/{session_id}/cancel`
- `POST /v1/sessions/{session_id}/resume`
- `POST /v1/sessions/{session_id}/approvals/{approval_id}/responses`

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
migrations/0004_m3_tool_artifact_approval.sql
migrations/0005_m4_collaboration_review.sql
```

对应的 `.down.sql` 文件提供 M1～M4 Schema 回滚。生产部署应由迁移系统执行这些 SQL，
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

M3 关键实现位于 `application/tooling.py`、`domain/approval.py`、
`infrastructure/hands.py`、`infrastructure/artifacts.py`、
`infrastructure/credentials.py` 和 `projections/approvals.py`。M4 关键实现位于
`application/collaboration.py`、`domain/collaboration.py`、
`contracts/collaboration.py` 和 `projections/collaboration.py`。

设计依据见 [Managed Agent 系统架构](docs/Managed%20Agent%20系统架构/00%20Managed%20Agent%20系统架构总览.md)，实施顺序见 [开发方案与实施计划](docs/Managed%20Agent%20开发方案与实施计划.md)。

## 当前边界

项目保留内存适配器用于快速测试；配置 PostgreSQL 后使用持久 Event Store、Control
State Store、Snapshot、Command Dedup、Transactional Outbox、Task/Approval/Collaboration
Read Model、
Projector Checkpoint 和 Poison Event Queue。M3 提供本地 Hands Sandbox、内存对象适配器与
Vault 测试适配器；生产对象存储、企业 Vault 和外部 Connector 通过现有端口接入。M4 提供
确定性的 Coordinator/Worker/Reviewer 命令与权限边界；具体模型驱动的语义拆分和动态修复
策略通过现有 Model Gateway 与 Collaboration 端口扩展。
