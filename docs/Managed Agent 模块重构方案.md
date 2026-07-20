# Managed Agent 模块重构方案（RFC）

> **状态**：Draft — 待 Review  
> **日期**：2026-07-20  
> **依据**：[Managed Agent 系统架构图](./Managed%20Agent%20系统架构图.png)、[架构代码梳理](./Managed%20Agent%20架构代码梳理.md)、[系统架构总览](./Managed%20Agent%20系统架构/00%20Managed%20Agent%20系统架构总览.md)  
> **范围**：`src/auraclaw/` 包内模块划分与装配边界；不涉及微服务拆分、不改 Canonical Event 语义。

---

## 1. 背景

AuraClaw 已完成 M1–M7 功能竖切，逻辑组件与架构图基本对齐。当前主要问题不是「缺能力」，而是 **代码目录与架构部署单元未 1:1 对应**，导致：

- 新人难以从目录结构直接映射到架构图；
- 进程装配集中在 `api/dependencies.py`，开发与生产 Runtime 边界靠读代码才能理解；
- 部分模块过大（`infrastructure/postgres.py` 780 行），读写与投影逻辑分散在两个包；
- `application/` 平铺 8 个服务，Service Gateway / Intelligence / Action 三层不可见；
- 缺少 import 规则 enforcement，重构后容易再次耦合。

本 RFC 提出 **模块化单体内的包结构重构**，为后续 Worker 独立进程、生产 Runtime 装配和团队协作奠定边界。

---

## 2. 目标与非目标

### 2.1 目标

| # | 目标 | 验收标准 |
|---|---|---|
| G1 | 目录结构与架构图部署单元 1:1 可映射 | Review 者无需读 DI 代码即可指出组件所在包 |
| G2 | 每个模块有明确「唯一写入方」 | Session / Control / Delivery / Runtime Event 写入边界文档化且可 lint |
| G3 | Projection 内聚 | 投影规则与 Store 适配器同域；`postgres.py` 拆分为 ≤300 行/文件 |
| G4 | 装配与 HTTP 分离 | `api/` 仅 routes + DTO；Worker / Dev Runtime 在 `composition/` |
| G5 | 补全生产 Worker 入口 | CLI 提供 `runtime run`、`delivery run`，与架构部署单元对应 |
| G6 | 零行为变更 | M1–M7 全量测试通过；仅移动/重命名，不改业务语义 |

### 2.2 非目标

- **不**在本阶段拆微服务或引入 gRPC/消息总线新中间件。
- **不**重写 Session 聚合、Canonical Event 类型或 Projection 语义。
- **不**在本阶段实现 OpenAI/Anthropic Adapter、S3 Artifact、企业 Vault。
- **不**引入额外抽象层（如全量 Repository / UseCase 框架）。
- **不**修改 frontend 协议与公开 HTTP API 路径（`/v1/*` 保持不变）。

---

## 3. 现状问题（证据）

### 3.1 模块体量

| 文件 | 行数 | 问题 |
|---|---:|---|
| `infrastructure/postgres.py` | 780 | EventStore + 3 个 Projection 同文件 |
| `infrastructure/delivery.py` | 536 | Job Store + Sink 实现混合 |
| `application/tooling.py` | 417 | Tool Gateway + Policy + 直连 infra |
| `api/dependencies.py` | 236 | DI + Relay + Dev Worker 组装 |
| `runtime/ports.py` | 235 | Control + Model + Tool + RuntimeEvent 端口混合 |

### 3.2 依赖违规

| 违规 | 位置 | 说明 |
|---|---|---|
| application → infrastructure | `application/tooling.py` | 直接 import `ArtifactStore`、`CredentialProxy` |
| application → infrastructure | `application/observability.py` | 直接 import `redact_sensitive` |
| infrastructure → projections | `infrastructure/postgres.py` | PG 投影实现远离投影规则 |
| infrastructure → domain | `infrastructure/postgres.py` | EventStore 适配器引用 `CollaborationAggregate` |

### 3.3 架构图 vs 代码映射缺口

| 架构部署单元 | 当前代码位置 | 缺口 |
|---|---|---|
| Task API Deployment | `api/routes/tasks.py` + `application/tasks.py` | Gateway / Query 未分包 |
| Session Deployment | `domain/` + `projections/` + `infrastructure/postgres.py` | 投影分裂 |
| Runtime Control Deployment | `application/orchestration.py` + `runtime/` | 无 Worker 入口；dev 与 prod 混在 `dependencies.py` |
| Delivery Deployment | `application/delivery.py` + `infrastructure/delivery.py` | 无 CLI Worker；未挂 lifespan |

---

## 4. 目标包结构

### 4.1 总体分层

```text
auraclaw/
├── contracts/                 # 不变：跨边界稳定类型
├── domain/                    # 不变：聚合与领域规则（session/collaboration/approval）
│
├── gateways/                  # 接入平面（Service Gateway）
│   ├── task/                  # Task Gateway + Admission
│   ├── query/                 # Task Query / Result（只读）
│   └── streaming/             # Streaming Gateway（只推送）
│
├── session/                   # Session / Collaboration 写侧应用服务
│   ├── task_service.py        # 原 application/tasks.py
│   └── collaboration_service.py
│
├── projection/                # Projection Service + Read Model
│   ├── relay.py
│   ├── task/
│   ├── approval/
│   ├── collaboration/
│   └── observability/
│
├── control/                   # Orchestrator + Control State 语义
│   ├── orchestrator.py
│   └── ports.py               # 自 runtime/ports.py 拆出 Control 相关
│
├── runtime/                   # Agent Runtime Pool
│   ├── harness.py
│   ├── clients.py
│   ├── model_gateway.py
│   └── ports.py               # Model / Tool / RuntimeEvent / Assignment
│
├── action/                    # Tool Gateway / Policy / Hands / Credential 编排
│   ├── tool_gateway.py
│   ├── policy.py
│   └── ports.py               # HandsExecutor / CredentialInvoker / ArtifactWriter
│
├── delivery/                  # Result Delivery Worker + Sink 编排
│   ├── worker.py
│   └── ports.py
│
├── observability/             # Trace / Audit / Ops 应用服务
│   └── service.py
│
├── infrastructure/            # 纯适配器（按技术栈分子目录）
│   ├── persistence/           # event_store, control_store, delivery_job_store
│   ├── projection/            # postgres/memory projection stores
│   ├── kafka/                 # runtime_events producer/ingestor
│   ├── memory/                # 内存适配器集合
│   ├── hands/
│   ├── artifacts/
│   ├── credentials/
│   └── observability/
│
├── composition/               # 进程装配（不含业务逻辑）
│   ├── api.py                 # FastAPI DI
│   ├── worker_projection.py
│   ├── worker_runtime.py
│   ├── worker_delivery.py
│   └── development.py         # DevelopmentRuntimeWorker 组装
│
├── api/                       # 薄 HTTP 层
│   ├── routes/
│   └── models.py
│
├── config.py                  # 或移入 composition/settings.py
├── main.py
└── __main__.py
```

### 4.2 与架构图部署单元对应

```text
┌─────────────────────────────────────────────────────────────┐
│ Task API Deployment                                         │
│   api/routes + gateways/{task,query,streaming}              │
└─────────────────────────────────────────────────────────────┘
         │                              │
         ▼                              ▼
┌─────────────────────┐    ┌──────────────────────────────────┐
│ Session Deployment  │    │ Delivery Deployment              │
│ session/            │    │ gateways/streaming + delivery/   │
│ projection/         │    │ composition/worker_delivery.py   │
│ composition/        │    └──────────────────────────────────┘
│   worker_projection │
└─────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────────┐
│ Runtime Control Deployment                                  │
│ control/ + runtime/ + composition/worker_runtime.py         │
│ composition/development.py（仅 dev）                        │
└─────────────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────────┐
│ Action Plane（横切，由 Runtime 调用）                        │
│ action/ + infrastructure/{hands,artifacts,credentials}      │
└─────────────────────────────────────────────────────────────┘
```

### 4.3 依赖方向（重构后）

```text
api → gateways → session | projection(read) | observability
                    ↓              ↑
                 domain ← contracts
                    ↑
session / projection / control / runtime / action / delivery
                    ↓
              infrastructure（仅经 composition 或 ports 注入）
```

**硬规则**：

1. `domain`、`contracts` 不依赖任何其他顶层包。
2. `gateways` 不直接依赖 `infrastructure`。
3. `infrastructure` 不依赖 `api`、`gateways`。
4. `composition` 是唯一允许「跨平面组装」的包。
5. Gateway 层（query/streaming）**不得** import Session 写侧服务。

---

## 5. 文件迁移表

### 5.1 Phase 1 — 拆分大文件与 Projection 内聚

| 当前路径 | 目标路径 | 说明 |
|---|---|---|
| `infrastructure/postgres.py`（EventStore 部分） | `infrastructure/persistence/postgres_event_store.py` | ~250 行 |
| `infrastructure/postgres.py`（TaskProjection 部分） | `infrastructure/projection/postgres_task_store.py` | 存储适配 |
| `infrastructure/postgres.py`（CollaborationProjection） | `infrastructure/projection/postgres_collaboration_store.py` | 存储适配 |
| `infrastructure/postgres.py`（ApprovalProjection） | `infrastructure/projection/postgres_approval_store.py` | 存储适配 |
| `projections/tasks.py`（投影规则） | `projection/task/projector.py` | 纯逻辑 |
| `projections/tasks.py`（InMemory store） | `projection/task/memory_store.py` | 内存适配 |
| `projections/approvals.py` | `projection/approval/projector.py` + `memory_store.py` | 同上 |
| `projections/collaboration.py` | `projection/collaboration/projector.py` + `memory_store.py` | 同上 |
| `projections/relay.py` | `projection/relay.py` | 位置调整 |
| `infrastructure/memory.py` | `infrastructure/persistence/memory_event_store.py` | 命名对齐 |
| `infrastructure/control_postgres.py` | `infrastructure/persistence/postgres_control_store.py` | |
| `infrastructure/control_memory.py` | `infrastructure/persistence/memory_control_store.py` | |
| `infrastructure/delivery.py`（JobStore） | `infrastructure/persistence/postgres_delivery_job_store.py` 等 | 按职责拆 |
| `infrastructure/delivery.py`（Sink） | `infrastructure/delivery/webhook_sink.py` 等 | |
| `infrastructure/runtime_events.py` | `infrastructure/kafka/runtime_events.py` | |

**兼容策略**：Phase 1 在旧路径保留 re-export shim（deprecated），Phase 3 删除。

```python
# infrastructure/postgres.py（过渡期 shim）
from auraclaw.infrastructure.persistence.postgres_event_store import PostgresEventStore
from auraclaw.infrastructure.projection.postgres_task_store import PostgresTaskProjection
# ...
```

### 5.2 Phase 2 — application 按 bounded context 分包

| 当前路径 | 目标路径 |
|---|---|
| `application/tasks.py` | `session/task_service.py` |
| `application/collaboration.py` | `session/collaboration_service.py` |
| `application/orchestration.py` | `control/orchestrator.py` |
| `application/streaming.py` | `gateways/streaming/gateway.py` |
| `application/tooling.py`（ToolGateway） | `action/tool_gateway.py` |
| `application/tooling.py`（PolicyEngine） | `action/policy.py` |
| `application/delivery.py` | `delivery/worker.py` |
| `application/observability.py` | `observability/service.py` |
| `application/maintenance.py` | `projection/maintenance.py` |

**新增 gateways 薄封装**（从 `api/routes/tasks.py` 逻辑下沉）：

| 新文件 | 职责 |
|---|---|
| `gateways/task/admission.py` | `AdmissionController` 实现与扩展点 |
| `gateways/task/commands.py` | 命令 DTO → `session/task_service` 转发 |
| `gateways/query/reader.py` | 只读 Task / Result / Children；封装 ETag / min_version |
| `gateways/streaming/gateway.py` | 自 `application/streaming.py` 迁入 |

### 5.3 Phase 3 — 装配剥离与 Worker 入口

| 当前路径 | 目标路径 |
|---|---|
| `api/dependencies.py`（HTTP DI 部分） | `composition/api.py` |
| `api/dependencies.py`（Dev Worker 部分） | `composition/development.py` |
| `runtime/development.py`（Worker 本体） | `composition/adapters/development_worker.py` |
| `runtime/development.py`（ModelClient） | `composition/adapters/development_model.py` |
| `__main__.py`（projection 命令） | `composition/worker_projection.py` + 薄 wrapper |
| —（新增） | `composition/worker_runtime.py` |
| —（新增） | `composition/worker_delivery.py` |

**CLI 目标**：

```text
auraclaw serve                    # composition/api.py → uvicorn
auraclaw projection relay       # composition/worker_projection.py
auraclaw projection rebuild
auraclaw runtime run              # 新增：生产 Orchestrator + Harness 循环
auraclaw delivery run             # 新增：ResultDeliveryWorker 循环
auraclaw operations status|retention|redrive
```

### 5.4 Phase 4 — 端口分组

| 当前 | 目标 |
|---|---|
| `domain/ports.py`（EventStore 等） | `session/ports.py` |
| `domain/ports.py`（TaskReader 等） | `projection/ports.py` |
| `runtime/ports.py`（Control 相关） | `control/ports.py` |
| `runtime/ports.py`（Model/Tool/RuntimeEvent） | `runtime/ports.py`（瘦身） |
| `application/tooling.py` 内 Protocol | `action/ports.py` |
| `application/delivery.py` 内 Protocol | `delivery/ports.py` |

---

## 6. Import 规则（import-linter 草案）

在 `pyproject.toml` 增加 `[tool.importlinter]`：

```toml
[[tool.importlinter.contracts]]
name = "Layered architecture"
type = "layers"
layers = [
  "auraclaw.api",
  "auraclaw.gateways",
  "auraclaw.composition",
  "auraclaw.session | auraclaw.projection | auraclaw.control | auraclaw.runtime | auraclaw.action | auraclaw.delivery | auraclaw.observability",
  "auraclaw.domain",
  "auraclaw.contracts",
]
```

**额外 forbidden 规则**：

| 规则 | 说明 |
|---|---|
| `gateways` → `infrastructure` | Gateway 只通过 ports + composition 注入 |
| `session` → `gateways` | 写侧不依赖接入层 |
| `infrastructure` → `api` | 适配器不感知 HTTP |
| `projection/projector` → `infrastructure` | 投影规则纯函数，不绑存储 |
| `query gateway` → `session/task_service` | Query 只读 Projection |

CI 步骤：`uv run lint-imports`（或等价工具），与 ruff/mypy 并列。

---

## 7. 分阶段实施计划

### Phase 1：Projection 内聚 + postgres 拆分（1 PR）

**范围**：§5.1  
**风险**：低（mostly move + re-export）  
**验证**：

- [ ] `pytest` 全绿
- [ ] `mypy src/auraclaw` 通过
- [ ] 无单文件 > 400 行（postgres 拆分后）

### Phase 2：application → bounded context 分包（1–2 PR）

**范围**：§5.2  
**风险**：中（import 路径大量变更）  
**策略**：先移文件 + shim re-export，再改 import，最后删 shim  
**验证**：

- [ ] 同上
- [ ] `grep -r "from auraclaw.application" src tests` 为零（shim 除外）

### Phase 3：composition 剥离 + Worker CLI（1 PR）

**范围**：§5.3  
**风险**：中（lifespan / CLI 行为）  
**验证**：

- [ ] `test_m7_development_runtime.py` 通过
- [ ] `auraclaw serve` + 前端 Streaming 冒烟
- [ ] `auraclaw runtime run` 在 memory 后端可调度任务（新增集成测试）
- [ ] `auraclaw delivery run --once` 可 ingest 一条 delivery outbox（新增测试）

### Phase 4：端口分组 + import-linter（1 PR）

**范围**：§5.4 + §6  
**风险**：低  
**验证**：

- [ ] import-linter 全绿
- [ ] 更新 [架构代码梳理](./Managed%20Agent%20架构代码梳理.md) 与 [Python 后端结构说明](./Python%20后端结构说明.md)

---

## 8. PR 切分建议

| PR | 标题 | 文件数估计 | 可独立合并 |
|---|---|---:|---|
| PR-1 | `refactor: split postgres and colocate projections` | ~20 | ✅ |
| PR-2 | `refactor: extract session and projection packages` | ~25 | ✅（依赖 PR-1） |
| PR-3 | `refactor: extract gateways, control, action, delivery` | ~30 | ✅（依赖 PR-2） |
| PR-4 | `refactor: add composition layer and worker CLI` | ~15 | ✅（依赖 PR-3） |
| PR-5 | `refactor: split ports and add import-linter` | ~20 | ✅（依赖 PR-4） |

每个 PR：

1. 保持测试全绿；
2. 不混合行为变更；
3. 在 PR 描述中附「旧路径 → 新路径」对照；
4. 更新 AGENTS.md 中的目录说明（若 import 路径变化）。

---

## 9. 测试策略

| 层级 | 要求 |
|---|---|
| 单元测试 | 现有 M1–M7 全量回归；import 路径批量替换 |
| 集成测试 | 新增 `test_worker_runtime.py`：memory 后端 create_task → runtime run → completed |
| 集成测试 | 新增 `test_worker_delivery.py`：terminal event → delivery run → job delivered |
| 冒烟 | M7 前端 Streaming + Result 一致性（development runtime） |
| 静态 | ruff + mypy + import-linter |

**不做**：本阶段不引入覆盖率门槛变更；不删现有 postgres/kafka skip 测试。

---

## 10. 风险与回滚

| 风险 | 影响 | 缓解 |
|---|---|---|
| 大规模 import 变更导致 merge 冲突 | 高 | 5 个小 PR；shim 过渡期 1 周 |
| Worker CLI 引入新 bug | 中 | 默认 `serve` 行为不变；Worker  opt-in |
| import-linter 误报 | 低 | 先 warn 后 error；composition 包白名单 |
| Review 周期过长 | 中 | Phase 1 可独立合并，立即获得 postgres 拆分收益 |

**回滚**：每个 PR 可独立 revert；shim 层保证 revert 后旧 import 仍可用。

---

## 11. 开放问题（Review 请标注意见）

| # | 问题 | 选项 | 建议 |
|---|---|---|---|
| Q1 | 新顶层包名用 `gateways/` 还是保留 `application/gateways/`？ | A. 顶层 `gateways/` B. 嵌套 | **A**：与架构图一致 |
| Q2 | `config.py` 是否移入 `composition/settings.py`？ | A. 移 B. 保留根目录 | **B**：减少 churn；Phase 5 可选 |
| Q3 | Phase 3 是否同时实现生产 `runtime run`？ | A. 是 B. 仅 composition 剥离 | **A**：补齐架构缺口 |
| Q4 | import-linter 工具选型 | A. import-linter B. 自定义 ruff rule | **A**：成熟方案 |
| Q5 | shim 过渡期时长 | A. 1 PR B. 1 Release | **B**：一个完整 release 周期 |
| Q6 | `domain/` 是否 rename 为 `domain/aggregates/`？ | A. 是 B. 否 | **B**：非必要 |

---

## 12. Review 检查清单

Review 者请逐项确认：

- [ ] 目标包结构是否与 [系统架构总览](./Managed%20Agent%20系统架构/00%20Managed%20Agent%20系统架构总览.md) 部署边界一致？
- [ ] 文件迁移表是否有遗漏组件（Admission、Policy、Credential、Hands）？
- [ ] Phase 切分是否足够小、可独立合并？
- [ ] import 规则是否过严/过松？
- [ ] 非目标是否清晰，避免 scope creep？
- [ ] 开放问题 Q1–Q6 是否同意建议选项？

---

## 13. 参考

- [Managed Agent 架构代码梳理](./Managed%20Agent%20架构代码梳理.md)
- [Python 后端结构说明](./Python%20后端结构说明.md)
- [开发阶段校验清单](./开发阶段校验清单.md)
- [M6 运维与灰度发布 Runbook](./M6%20运维与灰度发布%20Runbook.md)
- [AGENTS.md](../AGENTS.md)
