# Managed Agent 模块重构方案（RFC）

> **状态**：Implemented — Issue #8 已完成实现与门禁验证
> **日期**：2026-07-20  
> **依据**：[Managed Agent 系统架构图](./Managed%20Agent%20系统架构图.png)、[架构代码梳理](./Managed%20Agent%20架构代码梳理.md)、[系统架构总览](./Managed%20Agent%20系统架构/00%20Managed%20Agent%20系统架构总览.md)  
> **范围**：`src/auraclaw/` 包内模块划分、装配边界与依赖门禁；不涉及微服务拆分、不新增生产 Worker 行为、不改 Canonical Event 语义。

---

## 1. 背景

AuraClaw 已完成 M1–M7 功能竖切，逻辑组件与架构图基本对齐。代码包与部署单元本来就不是 1:1 关系；当前问题是 **架构组件、Python 包与进程入口之间缺少唯一、可审查的映射**，导致：

- 新人难以从目录结构直接映射到架构图；
- 进程装配集中在 `api/dependencies.py`，开发与生产 Runtime 边界靠读代码才能理解；
- 部分模块过大（`infrastructure/postgres.py` 780 行），读写与投影逻辑分散在两个包；
- `application/` 平铺 8 个服务，Service Gateway / Intelligence / Action 三层不可见；
- 缺少 import 规则 enforcement，重构后容易再次耦合。

本 RFC 提出 **模块化单体内的包结构重构**，为后续独立实现生产 Worker、拆分部署和团队协作奠定边界。生产 Worker 入口属于新增行为，将由独立 feature issue 跟踪，不混入本次重构。

---

## 2. 目标与非目标

### 2.1 目标

| # | 目标 | 验收标准 |
|---|---|---|
| G1 | 建立组件可追踪映射 | 每个架构组件都有唯一主归属包，并记录「组件 → 包 → 进程入口」 |
| G2 | 明确唯一写入方和依赖护栏 | 写入边界文档化；import-linter 约束可静态验证的依赖，行为测试验证写入语义 |
| G3 | Projection 内聚 | 规则按 projection bounded context 聚合，Store 适配器按同一上下文命名；`postgres.py` 拆分为 ≤300 行/文件 |
| G4 | 装配与 HTTP 分离 | `api/` 仅 transport、DTO 与依赖接口；Dev Runtime 和实现选择在 `composition/` |
| G5 | 装配方向单向可审查 | entrypoint → composition → api/gateways/adapters；`api`、`gateways` 不反向依赖 composition |
| G6 | 零业务行为变更 | M1–M7 全量测试通过；公开 API、CLI、Canonical Event、Projection 和运行语义不变 |

### 2.2 非目标

- **不**在本阶段拆微服务或引入 gRPC/消息总线新中间件。
- **不**重写 Session 聚合、Canonical Event 类型或 Projection 语义。
- **不**在本阶段实现 OpenAI/Anthropic Adapter、S3 Artifact、企业 Vault。
- **不**新增 `auraclaw runtime run`、`auraclaw delivery run` 或生产 Worker 生命周期；另开 feature issue 实现。
- **不**引入额外抽象层（如全量 Repository / UseCase 框架）。
- **不**修改公开 HTTP API 路径（`/v1/*` 保持不变）。

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

基础设施适配器依赖 domain/application 定义的端口或领域类型并非天然违规；禁止的是核心层反向依赖具体基础设施。`PostgresEventStore` 中重建 `CollaborationAggregate` 的代码应在拆分时检查职责归属，但不以 import 方向本身判错。

### 3.3 架构图 vs 代码映射缺口

| 架构部署单元 | 当前代码位置 | 缺口 |
|---|---|---|
| Task API Deployment | `api/routes/tasks.py` + `application/tasks.py` | Gateway / Query 未分包 |
| Session Deployment | `domain/` + `projections/` + `infrastructure/postgres.py` | 投影分裂 |
| Runtime Control Deployment | `application/orchestration.py` + `runtime/` | dev 装配与 HTTP DI 混在 `dependencies.py`；生产 Worker 是后续功能 |
| Delivery Deployment | `application/delivery.py` + `infrastructure/delivery.py` | 应用逻辑与适配器边界不直观；生产 Worker 是后续功能 |

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
│   └── development.py         # DevelopmentRuntimeWorker 组装
│
├── api/                       # 薄 HTTP 层
│   ├── dependencies.py        # RequestIdentity、Header 解析、抽象 dependency tokens
│   ├── routes/
│   └── models.py
│
├── config.py                  # 本阶段保留根目录，减少无收益 churn
├── main.py
└── __main__.py
```

### 4.2 与架构图部署单元对应

```text
┌─────────────────────────────────────────────────────────────┐
│ Task API Deployment                                         │
│   api/routes + gateways/{task,query}                        │
└─────────────────────────────────────────────────────────────┘
         │                              │
         ▼                              ▼
┌─────────────────────┐    ┌──────────────────────────────────┐
│ Session Deployment  │    │ Delivery Deployment              │
│ session/            │    │ gateways/streaming + delivery/   │
│ projection/         │    │ 生产入口由后续 feature 提供       │
│ composition/        │    └──────────────────────────────────┘
│   worker_projection │
└─────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────────┐
│ Runtime Control Deployment                                  │
│ control/ + runtime/                                         │
│ composition/development.py（仅 dev；生产入口后续实现）      │
└─────────────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────────┐
│ Action Plane（横切，由 Runtime 调用）                        │
│ action/ + infrastructure/{hands,artifacts,credentials}      │
└─────────────────────────────────────────────────────────────┘
```

组件、包与进程入口采用可追踪映射，而不是声称目录和部署单元 1:1：

| 架构组件 | 主归属包 | 当前/本阶段进程入口 |
|---|---|---|
| Task Gateway / Query | `gateways/task`、`gateways/query` | `auraclaw serve` → `composition/api.py` |
| Streaming Gateway | `gateways/streaming` | `auraclaw serve` → `composition/api.py` |
| Session / Collaboration | `session` | 由 API、Runtime 客户端经端口调用 |
| Projection / Read Model | `projection` | `auraclaw projection ...` → `composition/worker_projection.py` |
| Orchestrator / Runtime | `control`、`runtime` | `composition/development.py`（仅 dev）；生产入口后续实现 |
| Tool / Policy | `action` | 由 Runtime 经端口调用 |
| Result Delivery | `delivery` | 库级 Worker；生产入口后续实现 |
| 具体技术适配器 | `infrastructure` | 仅由 composition 选择和注入 |

### 4.3 依赖方向（重构后）

```text
main / __main__
       ↓
composition ─────→ api ─────→ gateways
     │                         │
     ├────→ infrastructure     ├────→ session / projection(read) / observability
     │          │              │
     │          └──── implements ports
     └────→ session / projection / control / runtime / action / delivery
                                      │
                                      └────→ domain ─────→ contracts
```

**硬规则**：

1. `contracts` 不依赖其他 AuraClaw 顶层包；`domain` 只可依赖 `contracts`。
2. `api` 不依赖 `composition` 或 `infrastructure`；具体实现由入口经 composition 注册或注入。
3. `gateways` 不直接依赖 `composition` 或 `infrastructure`。
4. `infrastructure` 可依赖其实现的 ports 和稳定类型，但不依赖 `api`、`gateways`、`composition`。
5. `composition` 是唯一允许跨平面选择具体实现和组装对象图的包；业务包不得导入它。
6. Query / Streaming Gateway 不 import Session 写侧服务，只读 Projection 或 Runtime Event 端口。
7. import-linter 只验证静态依赖；唯一写入方、事实源和状态转换仍须由契约/集成测试验证。

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

**兼容策略**：旧路径 shim 仅用于堆叠 PR 之间保持可合并；仓库没有承诺公开内部 Python import API，因此最终 Phase 删除全部 shim，不跨 release 保留。若实施前确认存在外部 Python 使用者，另行制定版本化弃用策略。

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

### 5.3 Phase 3 — 装配剥离（不新增 Worker 行为）

| 当前路径 | 目标路径 |
|---|---|
| `api/dependencies.py`（身份/Header/命令上下文/抽象 dependency tokens） | 保留 `api/dependencies.py`，不得选择具体适配器 |
| `api/dependencies.py`（存储选择、具体 provider、对象图） | `composition/api.py`，通过 app factory / dependency override 注入 |
| `api/dependencies.py`（Dev Worker 部分） | `composition/development.py` |
| `runtime/development.py`（Worker 本体） | `composition/adapters/development_worker.py` |
| `runtime/development.py`（ModelClient） | `composition/adapters/development_model.py` |
| `main.py`（app factory / lifespan 具体装配） | `composition/api.py`；`main.py` 仅保留稳定 ASGI 导出 |
| `__main__.py`（projection 命令） | `composition/worker_projection.py` + 薄 wrapper |

**CLI 保持不变**：

```text
auraclaw serve                    # composition/api.py → uvicorn
auraclaw projection relay       # composition/worker_projection.py
auraclaw projection rebuild
auraclaw operations status|retention|redrive
```

`runtime run`、`delivery run` 需要独立定义信号处理、租约恢复、重试、健康检查和配置失败语义，作为后续 feature issue 实施。

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

不使用一个全局线性 layers contract：composition 是最外层装配根，而 bounded contexts 之间也存在经过端口允许的调用，强行排成单一层级会把正确依赖判错。改用多个可解释的 forbidden contract，并在必要时对真正独立的兄弟包增加 independence contract。

在 `pyproject.toml` 增加：

```toml
[tool.importlinter]
root_package = "auraclaw"

[[tool.importlinter.contracts]]
name = "API is transport-only"
type = "forbidden"
source_modules = ["auraclaw.api"]
forbidden_modules = ["auraclaw.composition", "auraclaw.infrastructure"]

[[tool.importlinter.contracts]]
name = "Gateways do not select adapters"
type = "forbidden"
source_modules = ["auraclaw.gateways"]
forbidden_modules = ["auraclaw.composition", "auraclaw.infrastructure"]

[[tool.importlinter.contracts]]
name = "Adapters do not know delivery surfaces"
type = "forbidden"
source_modules = ["auraclaw.infrastructure"]
forbidden_modules = ["auraclaw.api", "auraclaw.gateways", "auraclaw.composition"]

[[tool.importlinter.contracts]]
name = "Business packages do not import composition"
type = "forbidden"
source_modules = [
  "auraclaw.session", "auraclaw.projection", "auraclaw.control",
  "auraclaw.runtime", "auraclaw.action", "auraclaw.delivery",
  "auraclaw.observability",
]
forbidden_modules = ["auraclaw.composition"]
```

**另行配置的细粒度规则**：

| 规则 | 说明 |
|---|---|
| `gateways` → `infrastructure` | Gateway 只通过 ports + composition 注入 |
| `session` → `gateways` | 写侧不依赖接入层 |
| `api` → `composition` | transport 不反向选择实现，避免装配循环 |
| `infrastructure` → `api/gateways/composition` | 适配器可实现 ports，但不感知交付面或装配根 |
| `projection/projector` → `infrastructure` | 投影规则纯函数，不绑存储 |
| `query gateway` → `session/task_service` | Query 只读 Projection |

CI 步骤：`uv run lint-imports`，与 ruff/mypy 并列。正式落地前先用目标包骨架运行配置，确保 contract 名称和模块粒度与所安装的 import-linter 版本匹配；门禁直接以 error 生效，不设置长期 warn 模式。

---

## 7. 分阶段实施计划

Phase 1–4 各自视为一个开发阶段。每个阶段开工时在 [开发阶段校验清单](./开发阶段校验清单.md) 的 R1 小节更新范围，完成时逐项记录功能、架构、测试、安全、文档、迁移/不适用、提交与 push 证据。仅通过 pytest/ruff/mypy 不能宣告阶段完成。

### Phase 1：Projection 内聚 + postgres 拆分（1 PR）

**范围**：§5.1  
**风险**：低（mostly move + re-export）  
**验证**：

- [x] `pytest` 全绿
- [x] `ruff check .` 通过
- [x] `mypy src/auraclaw` 通过
- [x] 原 `infrastructure/postgres.py` 已按职责拆分，相关文件均 ≤300 行
- [x] R1.1 阶段门禁与旧路径 shim 范围已记录

### Phase 2：application → bounded context 分包（1–2 PR）

**范围**：§5.2  
**风险**：中（import 路径大量变更）  
**策略**：先移文件 + 临时 shim re-export，再改 import；shim 最迟在 Phase 4 删除
**验证**：

- [x] 同上
- [x] 仓库内 `auraclaw.application` import 为零，最终版本不保留 shim
- [x] R1.2 阶段门禁与组件映射已更新

### Phase 3：composition 剥离（1 PR）

**范围**：§5.3  
**风险**：中（lifespan 与 FastAPI dependency override 装配）
**验证**：

- [x] `test_m7_development_runtime.py` 通过
- [x] `auraclaw serve` 装配与 Streaming/Result 自动化回归通过
- [x] 现有 projection / operations CLI 可加载，命令契约不变
- [x] `api`、`gateways` 不 import `composition`，不存在装配循环
- [x] R1.3 阶段门禁与真实 API/Dev Runtime 冒烟证据已记录

### Phase 4：端口分组 + import-linter（1 PR）

**范围**：§5.4 + §6  
**风险**：低  
**验证**：

- [x] import-linter 8 条 contract 全绿
- [x] 更新 [架构代码梳理](./Managed%20Agent%20架构代码梳理.md) 与 [Python 后端结构说明](./Python%20后端结构说明.md)
- [x] 删除仅服务于堆叠 PR 的旧路径 shim
- [x] R1.4 全部门禁完成，并以 intentional commit push 到 `origin`

---

## 8. PR 切分建议

| PR | 标题 | 文件数估计 | 可独立合并 |
|---|---|---:|---|
| PR-1 | `refactor: split postgres and colocate projections` | ~20 | ✅ |
| PR-2 | `refactor: extract session and projection packages` | ~25 | ✅（依赖 PR-1） |
| PR-3 | `refactor: extract gateways, control, action, delivery` | ~30 | ✅（依赖 PR-2） |
| PR-4 | `refactor: extract composition layer` | ~12 | ✅（依赖 PR-3） |
| PR-5 | `refactor: split ports and add import-linter` | ~20 | ✅（依赖 PR-4） |

每个 PR：

1. 保持测试全绿；
2. 不混合行为变更；
3. 在 PR 描述中附「旧路径 → 新路径」对照；
4. 更新 AGENTS.md 中的目录说明（若 import 路径变化）；
5. 更新 `docs/开发阶段校验清单.md` 对应 R1.x，并在完成后按阶段提交、push。

---

## 9. 测试策略

| 层级 | 要求 |
|---|---|
| 单元测试 | 现有 M1–M7 全量回归；import 路径批量替换 |
| 集成测试 | 保持现有 API、Projection Relay、Development Runtime、Delivery 库级行为回归 |
| 结构测试 | 关键旧/新 import、FastAPI dependency override、CLI 子命令与 import-linter contract |
| 冒烟 | M7 前端 Streaming + Result 一致性（development runtime） |
| 静态 | ruff + mypy + import-linter |

**不做**：本阶段不引入覆盖率门槛变更；不删现有 postgres/kafka skip 测试。

---

## 10. 风险与回滚

| 风险 | 影响 | 缓解 |
|---|---|---|
| 大规模 import 变更导致 merge 冲突 | 高 | 5 个小 PR；shim 仅跨堆叠 PR 保留 |
| composition 剥离破坏 lifespan/override | 中 | 保留入口行为测试与真实 Streaming 冒烟 |
| import-linter 误报 | 低 | 使用多个小 contract；先在目标骨架验证配置再纳入门禁 |
| Review 周期过长 | 中 | Phase 1 可独立合并，立即获得 postgres 拆分收益 |

**回滚**：每个 PR 可独立 revert；shim 层保证 revert 后旧 import 仍可用。

---

## 11. Review 决策记录

| # | 问题 | 决策 | 理由 |
|---|---|---|---|
| Q1 | 新顶层包名 | 使用顶层 `gateways/` | 保持架构词汇可见；用映射矩阵表达跨部署关系，不宣称 1:1 |
| Q2 | `config.py` 位置 | 保留根目录 | 减少无收益 churn |
| Q3 | 生产 Worker CLI | 从本 RFC 拆出 | 新入口是功能变化，需独立定义生命周期和恢复语义 |
| Q4 | 依赖门禁工具 | 使用 import-linter 的多个小 contract | 比自定义 Ruff 规则更直接；避免错误的全局线性层级 |
| Q5 | shim 时长 | 仅保留到最终重构 Phase | 当前没有公开内部 Python import 兼容承诺 |
| Q6 | `domain/` 重命名 | 不重命名 | 非必要变更 |

---

## 12. 实施检查清单

实施者与 Review 者逐项确认：

- [x] 「组件 → 包 → 进程入口」映射与系统架构总览一致。
- [x] 文件迁移覆盖 Admission、Policy、Credential 与 Hands。
- [x] 实施按 R1.1～R1.4 顺序完成并以完整回归收口。
- [x] import 规则拆为 8 条小型 contract，未替代行为测试。
- [x] 非目标清晰，未引入业务语义或公开协议变更。
- [x] R1.x 阶段门禁与验证证据已更新。
- [x] 生产 Worker CLI 保持在本 issue 范围之外。

---

## 13. 参考

- [Managed Agent 架构代码梳理](./Managed%20Agent%20架构代码梳理.md)
- [Python 后端结构说明](./Python%20后端结构说明.md)
- [开发阶段校验清单](./开发阶段校验清单.md)
- [M6 运维与灰度发布 Runbook](./M6%20运维与灰度发布%20Runbook.md)
- [S5 Docker Compose 生产部署与故障演练 Runbook](./S5%20Docker%20Compose%20生产部署与故障演练%20Runbook.md)
- [AGENTS.md](../AGENTS.md)
