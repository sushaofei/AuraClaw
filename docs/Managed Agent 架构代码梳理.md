# Managed Agent 架构代码梳理

> 依据 [Managed Agent 系统架构图](./Managed%20Agent%20系统架构图.png) 与 `docs/Managed Agent 系统架构/`，对当前 `src/auraclaw/` 代码做组件级映射。  
> 分析范围：纯 Python Managed Agent 后端（FastAPI）。  
> 梳理日期：2026-07-22（Issue #8 模块边界重构已完成；[Issue #12](https://github.com/sushaofei/AuraClaw/issues/12)
> S0 已冻结 12 服务生产目标，当前代码仍是模块化单体）。

## 一句话总结

AuraClaw 当前是以 **Canonical Session Event Log** 为唯一任务事实源的模块化单体：HTTP 经 `gateways/` 接入，
`composition/` 统一装配；API lifespan 内启动同一个 `RuntimeWorker`。Issue #12 将在不改变 Canonical/Projection
语义的前提下演进为 12 个生产入口；S0 只建立契约、所有权与迁移门禁，尚未声称进程拆分已经完成。

---

## 1. 分析边界与技术栈

| 项 | 说明 |
|---|---|
| **系统类型** | 纯 Python Managed Agent 后端（模块化单体，可合并部署） |
| **入口** | `auraclaw.main:app`（FastAPI） / CLI `auraclaw` |
| **语言与运行时** | Python ≥ 3.11 |
| **核心依赖** | FastAPI、Pydantic Settings、asyncpg、aiokafka、httpx、uvicorn |
| **存储** | 当前：内存适配器或 PostgreSQL；目标：PostgreSQL 独立角色 + SeaweedFS S3 Artifact Object |
| **实时总线** | 内存 Replay Buffer 或 Kafka `managed-agent.runtime-events` |
| **Runtime 联调** | 统一 `RuntimeWorker`；模型、Store、Event Bus 与 CORS 均由当前 `.env` 提供 |
| **质量门禁** | `ruff` + `mypy` + `pytest` + `lint-imports`（`pyproject.toml` `[tool.importlinter]`） |
| **不包含** | 生产云厂商 Model SDK、企业 Vault/KMS、S3 Artifact、远程容器 Hands |

当前依赖方向（由 import-linter 持续执行）：

```text
entrypoint (main / __main__)
    → composition (providers / api / cli / adapters)
        → api → gateways → session | projection | control | runtime | action | delivery | observability
                                    └→ domain → contracts
        → infrastructure（仅由 composition 选择具体适配器）
```

**硬规则**（与 `AGENTS.md` 一致）：

- `api`、`gateways` 不 import `composition` 或 `infrastructure`
- `gateways.query` / `gateways.streaming` 不 import `session.task_service` / `session.collaboration_service`
- `projection.{task,approval,collaboration}` 不 import `infrastructure`
- 业务包不 import `composition`；`infrastructure` 不 import `api` / `gateways` / `composition`

Issue #8 已按 [Managed Agent 模块重构方案](./Managed%20Agent%20模块重构方案.md) 落地：包与部署单元通过「组件 → 主归属包 → 进程入口」矩阵保持可追踪，不强制目录与进程 1:1。

### 2.1 重构前后路径对照（主要迁移）

| 重构前 | 重构后 |
|---|---|
| `application/tasks.py` | `session/task_service.py` |
| `application/collaboration.py` | `session/collaboration_service.py` |
| `application/orchestration.py` | `control/orchestrator.py` |
| `application/streaming.py` | `gateways/streaming/gateway.py` |
| `application/tooling.py` | `action/tool_gateway.py` + `action/policy.py` |
| `application/delivery.py` | `delivery/worker.py` |
| `application/observability.py` | `observability/service.py` |
| `application/maintenance.py` | `projection/maintenance.py` |
| `projections/{tasks,approvals,collaboration}.py` | `projection/{task,approval,collaboration}/projector.py` |
| `projections/relay.py` | `projection/relay.py` |
| `domain/ports.py`（EventStore） | `session/ports.py` |
| `domain/ports.py`（TaskReader 等） | `projection/ports.py` |
| `runtime/ports.py`（Control） | `control/ports.py` |
| 原 `runtime/development.py` | `composition/adapters/runtime_worker.py`（已统一环境逻辑） |
| `infrastructure/postgres.py` | `infrastructure/persistence/postgres_event_store.py` + `infrastructure/projection/postgres_*_store.py` |
| `infrastructure/{hands,credentials,delivery,observability,runtime_events}.py` | `infrastructure/{hands,credentials,delivery,observability,kafka}/` 子包 |
| `api/dependencies.py`（DI 装配） | `composition/providers.py` + `composition/api.py`（`dependency_overrides`） |
| `main.py`（FastAPI 工厂） | `composition/api.py`；`main.py` 仅 re-export |
| `__main__.py`（CLI） | `composition/cli.py`；`__main__.py` 仅 wrapper |

---

## 2. 架构图 ↔ 代码目录总览

```text
┌─────────────────────────────────────────────────────────────────────────┐
│ API Client                                                              │
│   ↓ HTTP                                                                │
│ api/routes/{tasks,streams,operations,health}.py                         │
│   Task Gateway / Query / Streaming / Ops                                │
└─────────────────────────────────────────────────────────────────────────┘
          │                         │                      │
          ▼                         ▼                      ▼
 gateways/{task,query,streaming}    observability/
 session/       control/       action/       delivery/
          │
          ▼
 domain/{session,collaboration,approval}.py
 contracts/{events,commands,state,tools,delivery,...}.py
          │
    ┌─────┴──────────────────────────────┐
    ▼                                    ▼
 projection/                       infrastructure/
  relay / task / approval /         persistence / projection / kafka /
  collaboration / maintenance       hands / artifacts / credentials /
                                    delivery / observability
          │
          ▼
 runtime/{harness,clients,model_gateway,ports}.py
 composition/{api,cli,providers,adapters/}.py
   Agent Runtime Pool + Model Gateway + 进程装配
```

| 架构图图层 | 代码包 | 职责 |
|---|---|---|
| Service Gateway | `api/` + `gateways/` | 命令准入、查询、SSE、运维只读 |
| Intelligence | `domain/` + `session/` + `control/` + `runtime/` + `projection/` | Session 事实、协作、调度、Runtime、投影 |
| Data / System / Tool | `infrastructure/` | Event Store、Control、Artifact、Hands、Kafka、Delivery、Vault |
| Shared Contracts | `contracts/` | 跨边界稳定类型，无框架依赖 |

| 架构组件 | 主归属包 | 当前进程入口 |
|---|---|---|
| Task Command / Query | `gateways/task`、`gateways/query` | `auraclaw serve` → `composition/api.py` |
| Streaming Gateway | `gateways/streaming` | `auraclaw serve` → `composition/api.py` |
| Session / Collaboration | `session` | API 或 Runtime client 经端口调用 |
| Projection / Read Model | `projection` | `auraclaw projection ...` → `composition/cli.py` |
| Orchestrator / Runtime | `control`、`runtime` | `composition/adapters/runtime_worker.py`（所有资源配置共用） |
| Tool / Policy | `action` | Runtime 经 Action ports 调用 |
| Result Delivery | `delivery` | 库级 Worker；生产入口仍属后续 feature |
| 技术适配器 | `infrastructure` | 仅由 `composition/providers.py` 选择与注入 |

### 2.2 Issue #12 目标组件 → 包 → 生产入口

包与服务不是 1:1。下表记录业务组件的主归属包、当前装配点和目标入口；目标入口在 S2 之前均为“未实现”。

| 生产服务 | 主归属包 | 当前装配/实现 | 目标入口 |
|---|---|---|---|
| task-api | `api`、`gateways/task`、`gateways/query` | Projection 只读 role + Session/Policy HTTP Client | `auraclaw api run` |
| session | `session`、`domain` | Canonical/Outbox 唯一写入服务 | `auraclaw session run` |
| projection-worker | `projection` | Session claim/ack Client + Projection owner role | `auraclaw projection relay --watch` |
| orchestrator | `control` | Control Store owner + Control HTTP API | `auraclaw orchestrator run` |
| agent-runtime | `runtime` | 仅 Control/Session/Model HTTP Client 与 Hands MCP Client | `auraclaw runtime run` |
| model-gateway | `model_gateway` + Provider adapters | Provider Secret owner + Policy Client | `auraclaw model-gateway run` |
| action-hands | `action` + `infrastructure/hands` | MCP Server、Tool Registry、Invocation Store、远程 Policy/Credential/Artifact Client | `auraclaw hands run`（`/mcp`） |
| policy | `policy` + `action/policy.py` | Decision/Approval owner 与持久 evidence | `auraclaw policy run` |
| credential-proxy | `credential_proxy` + `infrastructure/credentials` | Vault KV、egress allowlist、Reference/Usage owner | `auraclaw credential-proxy run` |
| artifact-service | `artifact` + `infrastructure/artifacts` | PostgreSQL Metadata + SeaweedFS SigV4 presigned objects | `auraclaw artifact run` |
| streaming-gateway | `gateways/streaming` + Kafka adapter | 独立公开 SSE 入口 | `auraclaw streaming run` |
| delivery-worker | `delivery` + `infrastructure/delivery` | Session claim/ack + Delivery owner DB + Policy/Credential Client | `auraclaw delivery run` |

详细通信、数据库角色、MCP、Policy/Credential、SeaweedFS 与迁移决策见
[ADR-001](./ADR-001%20生产服务部署边界与通信契约.md)。

---

## 3. 组件映射明细

实现状态约定：

- **已实现**：逻辑与端口完整，有测试覆盖
- **部分实现**：核心逻辑有，但缺生产适配器或未挂入主进程
- **桩/缺口**：仅占位或尚未接线

### 3.1 Task Gateway / Admission

| 项 | 内容 |
|---|---|
| **架构角色** | Web/API/Timer 请求 → Session Command；幂等、租户、版本前置条件 |
| **主路径** | `api/routes/tasks.py`、`gateways/task/commands.py`、`gateways/task/admission.py`、`gateways/query/reader.py` |
| **关键类型** | `TaskCommandGateway`（命令）、`TaskQueryService`（查询）、`AllowAllAdmissionController` |
| **写侧委托** | `TaskCommandGateway` → `session/task_service.TaskService` |
| **状态** | **部分实现**（命令边界完整；Admission 为放行桩） |
| **HTTP** | 见 §5 |
| **缺口** | 无真实身份认证/配额；`AllowAllAdmissionController.admit` 为空实现 |

### 3.2 Session / Collaboration Service

| 项 | 内容 |
|---|---|
| **架构角色** | Canonical Event Log 唯一业务写入方；Root/Child 协作事实 |
| **主路径** | `domain/session.py`、`domain/collaboration.py`、`session/task_service.py`、`session/collaboration_service.py` |
| **存储** | `infrastructure/persistence/memory_event_store.py`、`infrastructure/persistence/postgres_event_store.py` |
| **端口** | `session/ports.py`：`EventStore`、`AppendResult`、`SessionSnapshot` |
| **关键类型** | `SessionAggregate`、`CollaborationAggregate`、`CollaborationService`、`CoordinatorRole` / `WorkerRole` / `ReviewerRole` |
| **状态** | **已实现**（库级）；Collaboration 命令无独立 REST |
| **缺口** | 协作写路径靠应用服务/测试驱动，非独立微服务 |

### 3.3 Projection Service

| 项 | 内容 |
|---|---|
| **架构角色** | Transactional Outbox → 幂等投影；可删除重建 |
| **主路径** | 投影规则：`projection/{task,approval,collaboration}/projector.py`；PG 存储：`infrastructure/projection/postgres_*_store.py` |
| **关键类型** | `OutboxRelay`、`InMemoryTaskProjection` / `PostgresTaskProjection`、`CompositeProjection`、`ProjectionMaintenanceService` |
| **状态** | **已实现** |
| **运维** | `auraclaw projection relay [--watch]`、`auraclaw projection rebuild` |
| **说明** | API 路径在命令后即时 `relay_once`；可靠消费仍以 Outbox + Worker 为准 |

### 3.4 Read Model Store

| 项 | 内容 |
|---|---|
| **架构角色** | 可丢弃查询副本（Task / Approval / Collaboration / Observability 视图） |
| **主路径** | `projection/{task,approval,collaboration}/projector.py` + `infrastructure/projection/postgres_*_store.py` |
| **端口** | `projection/ports.py`：`TaskReader`、`CollaborationReader`、`ApprovalViewReader`、`ProjectionWriter` |
| **状态** | **已实现**（memory + postgres） |
| **缺口** | 无独立 ES/CDB 读副本；与写库同 PG 时靠 schema 隔离 |

### 3.5 Task Query / Result Service

| 项 | 内容 |
|---|---|
| **架构角色** | 只读任务状态与终态结果；支持 ETag / `min_version` |
| **主路径** | `gateways/query/reader.py`（`TaskQueryService`）、`api/routes/tasks.py` |
| **状态** | **已实现** |
| **说明** | Query 只读 `projection/ports`；经 import-linter 禁止直连 `session/task_service` |

### 3.6 Runtime Event Bus

| 项 | 内容 |
|---|---|
| **架构角色** | Token Delta、进度、工具状态等短期实时事件；不替代 Canonical Log |
| **主路径** | `infrastructure/kafka/runtime_events.py`、`runtime/ports.py`（`RuntimeEvent` / `RuntimeEventPublisher`） |
| **关键类型** | `RuntimeEventProducerSDK`、`ReplayRuntimeEventBus`、`KafkaRuntimeEventProducer`、`KafkaStreamingIngestor` |
| **状态** | **已实现**（memory + Kafka） |
| **配置** | `AURACLAW_RUNTIME_EVENT_BACKEND`、`KAFKA_HOST` / `KAFKA_PORT` |
| **统一路径** | `RuntimeWorker` 经 `RuntimeEventProducerSDK` 写入配置的 Kafka 或内存 Replay Bus |
| **缺口** | 生产 Runtime Producer 仍未挂入主服务 DI；Kafka Producer 与开发 Worker 分路径 |

### 3.7 Streaming Gateway

| 项 | 内容 |
|---|---|
| **架构角色** | 租户授权后把 Runtime Event 转为 SSE；只推送，不收改变状态的命令 |
| **主路径** | `gateways/streaming/gateway.py`（`StreamingGateway`）、`api/routes/streams.py` |
| **HTTP** | `GET /v1/streams/{session_id}`（支持 `Last-Event-ID`） |
| **状态** | **已实现**（dev 环境可端到端验证多 delta SSE + Result 一致性） |
| **缺口** | 无 WebSocket；仅 `visibility=="user"` 下发 |

### 3.8 Result Delivery Service

| 项 | 内容 |
|---|---|
| **架构角色** | Canonical Delivery Outbox 驱动的可靠外部通知（重试、熔断、DLQ） |
| **主路径** | `delivery/worker.py`、`delivery/ports.py`、`infrastructure/delivery/`、`contracts/delivery.py` |
| **关键类型** | `ResultDeliveryWorker`、`CircuitBreaker`、`WebhookResultSink`、`ParentSessionResultSink`、`PostgresDeliveryJobStore` |
| **状态** | **已实现**（库级）；**未挂 FastAPI lifespan / CLI 常驻 Worker** |
| **缺口** | 无 `auraclaw delivery` 子命令；需外部进程或测试驱动 |

### 3.9 Orchestrator

| 项 | 内容 |
|---|---|
| **架构角色** | 资源调度（queue / claim / lease / provision / assign）；不做语义拆分 |
| **主路径** | `control/orchestrator.py`、`control/ports.py`（`Orchestrator` Protocol） |
| **关键类型** | `ManagedOrchestrator`、`LocalRuntimeProvisioner` |
| **状态** | **已实现**；统一 `RuntimeWorker` 接入 HTTP 进程 |
| **端口** | `watch`、`schedule_once`、`cancel`、`heartbeat`、`reconcile` |
| **进程装配** | `composition/providers.py` → `build_runtime_worker()` → `ManagedOrchestrator` + `LocalRuntimeProvisioner` |
| **缺口** | 生产无独立 orchestrator worker 入口；Provisioner 为本地计数式 |

### 3.10 Control State Store

| 项 | 内容 |
|---|---|
| **架构角色** | 租约、fencing token、队列、assignment、heartbeat、capacity、checkpoint |
| **主路径** | `infrastructure/persistence/memory_control_store.py`、`infrastructure/persistence/postgres_control_store.py` |
| **关键类型** | `InMemoryControlStateStore`、`PostgresControlStateStore` |
| **状态** | **已实现** |
| **说明** | 与 Canonical Event Store 独立 schema/写入边界，不做跨 Store 事务 |
| **缺口** | 无 Redis 适配器 |

### 3.11 Agent Runtime Pool

| 项 | 内容 |
|---|---|
| **架构角色** | `Role + Harness + Model Client + Context Policy + Tool Client`；可恢复执行 |
| **主路径** | `runtime/harness.py`、`runtime/clients.py`、`runtime/ports.py`、`session/collaboration_service.py`（角色门面） |
| **关键类型** | `AgentHarness`、`FencedSessionClient`、`FencedToolClient`、`IdempotentToolClient`、`CoordinatorRole` / `WorkerRole` / `ReviewerRole` |
| **Worker** | `composition/adapters/runtime_worker.py`：所有资源配置共用 `RuntimeWorker` |
| **状态** | **部分实现**（Harness + 角色及同进程 Worker 完整；非多进程 Pool） |
| **说明** | 架构图中的 Planner 对应代码中的 **Reviewer** 角色（校验生产结果），无独立 `PlannerRuntime` 类 |
| **缺口** | 尚未拆分独立多进程 Runtime Pool；可用 `AURACLAW_RUNTIME_ENABLED` 关闭同进程 Worker |

### 3.12 Model Gateway

| 项 | 内容 |
|---|---|
| **架构角色** | Runtime 与 LLM Provider 之间的唯一凭证解析与调用边界 |
| **主路径** | `runtime/model_gateway.py`、`infrastructure/model/openai_compatible.py`、`runtime/ports.py` |
| **关键类型** | `ModelGateway`、`StaticCredentialResolver` |
| **Provider** | `OpenAICompatibleProvider`：所有部署经相同 Gateway 调用各自配置的模型资源 |
| **状态** | **已实现**（generate 流式调用、usage、tool calls、错误映射与 Credential 隔离） |
| **缺口** | 多模型路由、fallback、Vault/KMS 与独立 Gateway 服务未实现 |

### 3.13 Tool Gateway / Dispatcher

| 项 | 内容 |
|---|---|
| **架构角色** | 工具发现、Schema、权限、审批、幂等、取消、结果规范化；可视为 MCP 风格边界 |
| **主路径** | `action/tool_gateway.py`、`action/policy.py`、`action/ports.py`、`contracts/tools.py` |
| **关键类型** | `ToolRegistry`、`JsonSchemaValidator`、`PolicyEngine`、`ToolGateway`；Runtime 侧经 `GatewayToolClient`（`runtime/clients.py`） |
| **Action 端口** | `action/ports.py`：`HandsExecutor`、`CredentialInvoker`、`ArtifactWriter`（由 composition 注入 infra 实现） |
| **状态** | **已实现**（库级）；未挂 HTTP |
| **分发** | Hands（本地行动）或 Credential Proxy（代持凭证调用） |

### 3.14 Hands Service

| 项 | 内容 |
|---|---|
| **架构角色** | 工具实际执行环境（Hands）；无 Shell、最小环境、文件根隔离 |
| **主路径** | `infrastructure/hands/local.py` |
| **关键类型** | `LocalHandsService` |
| **状态** | **已实现**（本地适配器） |
| **缺口** | 无远程/容器 Hands |

### 3.15 Artifact Store

| 项 | 内容 |
|---|---|
| **架构角色** | 大结果/文件不可变对象 + lineage + ACL + 短期下载令牌 |
| **主路径** | `infrastructure/artifacts/store.py`（`ArtifactStore` 实现 `ArtifactWriter`）、`contracts/tools.py` |
| **关键类型** | `ArtifactStore`、`InMemoryObjectStorage` |
| **状态** | **部分实现** |
| **缺口** | 无 S3/文件系统适配器；`Settings.artifact_root` 已声明但未接到 `ArtifactStore` |

### 3.16 Policy / Approval Service

| 项 | 内容 |
|---|---|
| **架构角色** | 工具权限策略 + action digest 绑定审批；Human 经 Task Gateway 回写 Canonical Event |
| **主路径** | `action/policy.py`（`PolicyEngine`）、`domain/approval.py`、`projection/approval/projector.py`、`api/routes/tasks.py` |
| **状态** | **已实现**（嵌入 Tool Gateway + Session，非独立微服务） |
| **缺口** | 策略版本硬编码风格（如 `m3-v1`）；无独立 `policy/` 包 |

### 3.17 Credential Proxy / Vault

| 项 | 内容 |
|---|---|
| **架构角色** | Runtime/Hands 不可见真实密钥；代理代调并递归脱敏 |
| **主路径** | `infrastructure/credentials/proxy.py` |
| **关键类型** | `CredentialProxy`、`InMemoryVault`、`SecretRedactor` |
| **状态** | **部分实现** |
| **缺口** | 无企业 Vault/KMS；模型凭证（`StaticCredentialResolver`）与工具凭证两套解析器 |

### 3.18 Observability / Audit

| 项 | 内容 |
|---|---|
| **架构角色** | Trace / Metric / Audit / Alert / Session Timeline；不参与业务决策 |
| **主路径** | `observability/service.py`、`observability/redaction.py`、`infrastructure/observability/stores.py`、`infrastructure/persistence/postgres_operations_store.py`、`api/routes/operations.py` |
| **关键类型** | `ObservabilityService`、`ObservabilityProjector`、`PostgresOperationsStore`、`StructuredLogger` |
| **HTTP** | `GET /v1/operations/sessions/{id}/timeline`、`GET /v1/operations/metrics` |
| **状态** | **已实现** |
| **缺口** | 无 OTel 导出器 |

### 3.19 Shared Contracts

| 项 | 内容 |
|---|---|
| **架构角色** | 跨组件稳定信封与枚举（架构文档 22） |
| **主路径** | `contracts/events.py`、`commands.py`、`state.py`、`tools.py`、`delivery.py`、`collaboration.py`、`observability.py`、`errors.py` |
| **关键类型** | `CanonicalEvent`、`NewEvent`、`CommandContext`、`SessionStatus`、`ToolInvocation`、`DeliveryJob`、`CollaborationRole`、`AuraClawError` |
| **状态** | **已实现** |

### 3.20 Result Sink / 外部系统

| 项 | 内容 |
|---|---|
| **架构角色** | Webhook / Parent Session 等最终结果投递目标 |
| **主路径** | `infrastructure/delivery/sinks.py`（`WebhookResultSink`、`ParentSessionResultSink`） |
| **状态** | **已实现**（两类 Sink） |
| **缺口** | 无 Sink 管理 API；Email/MQ 等未做 |

### 3.21 Runtime Worker

| 项 | 内容 |
|---|---|
| **架构角色** | 进程内 Worker，保留 Queue → Orchestrator → Harness 边界 |
| **主路径** | `composition/adapters/runtime_worker.py`、`composition/providers.py`、`composition/api.py` |
| **关键类型** | `RuntimeWorker`、`ModelGateway`、`RuntimeEventProducerSDK` |
| **触发条件** | `AURACLAW_RUNTIME_ENABLED=true` 且 Model Gateway 资源配置完整 |
| **状态** | **已实现**（所有部署使用同一逻辑） |
| **说明** | 轮询 runnable 任务并经统一 Harness 执行；Store、Model Provider 与 Runtime Event backend 只由当前 `.env` 选择 |
| **缺口** | 尚未拆为独立 Runtime 进程 |

---

## 4. 关键调用链

### 4.1 创建任务（Command）

```text
POST /v1/tasks
  → TaskCommandGateway.create_task
  → AllowAllAdmissionController.admit
  → TaskService → SessionAggregate.create
  → EventStore.append（+ Transactional Outbox）
  → save_snapshot
  → OutboxRelay.relay_once
      → CompositeProjection
         (Task + Approval + Collaboration + ObservabilityProjector)
  → 202 TaskAcceptedResponse（含 status / result / stream URL）
```

HTTP 依赖注入：`api/routes` → abstract tokens（`api/dependencies.py`）→ `composition/api.py` 的 `dependency_overrides` → `composition/providers.py`。

### 4.2 查询

```text
GET /v1/tasks/{id} | /result | /children
  → TaskQueryService.get_task | get_result | list_children
  → projection/ports（TaskReader / CollaborationReader）
```

### 4.3 实时推送

```text
GET /v1/streams/{session_id}
  → StreamingGateway.authorize（TaskReader 租户校验）
  → ReplayRuntimeEventBus.subscribe
      （可选 KafkaStreamingIngestor 灌入）
  → SSE（user visibility；cursor 过期 → stream.reset）
```

**统一 Runtime 端到端**：

```text
POST /v1/tasks（goal 含用户问题）
  → Canonical Event + Projection（status=pending）
  → RuntimeWorker.run（lifespan 后台轮询）
      → orchestrator.watch → schedule_once → AgentHarness.execute
      → ModelGateway.generate（多 model.output.delta）
      → RuntimeEventProducerSDK → Kafka/ReplayRuntimeEventBus
      → relay_once → Projection（status=completed）
  → GET /v1/streams/{session_id} 收到多个 delta
  → GET /v1/tasks/{session_id}/result 与 delta 拼接一致
```

Runtime Event 发布链：

```text
AgentHarness → RuntimeEventPublisher.publish
  → KafkaRuntimeEventProducer | ReplayRuntimeEventBus
```

### 4.4 调度与执行（库路径）

```text
Read Model runnable
  → ManagedOrchestrator.watch → ControlStateStore.enqueue
  → schedule_once → claim → lease → LocalRuntimeProvisioner.provision
  → assign → SessionClient.append(run.scheduled | runtime.reprovisioned)
  → AgentHarness.execute(assignment)
       → ModelGateway.generate
       → GatewayToolClient → ToolGateway
            → Hands 或 CredentialProxy
            → ArtifactStore（超限结果）
       → FencedSessionClient.append（Canonical tool/model 事实）
```

### 4.5 可靠交付（库路径）

```text
Canonical 终态 / delivery Outbox
  → ResultDeliveryWorker.ingest_once → create_job
  → claim_due → WebhookResultSink | ParentSessionResultSink
  → record_attempt / retry / Circuit / DLQ
  → EventStore.append(delivery.*) → Projection 更新 delivery_* 字段
```

### 4.6 协作（Coordinator 语义拆分）

```text
AgentHarness（按 Assignment.role 暴露工具）
  → RuntimeCollaborationController
  → RemoteCollaborationClient + 签名 Lease Assertion
  → CollaborationInternalService（派生 actor、校验角色/Lease/版本）
  → CollaborationService → Canonical Events
  → RunnableFeedConsumer 按 Root Feed 重算 runnable / 唤醒等待中的 Root

Coordinator → create_child / set_dependencies / request_review / await_children / join
Worker / Repair → publish_result
Reviewer → publish_review（独立 Review Session，不覆盖 Worker Artifact）
```

Runtime 实例统一注册到 `agent` Pool；`root`、`worker`、`reviewer`、`repair` 是 Assignment 的语义
角色。V1 模型工具不暴露 `delegate` / `handoff`，Service 能力与 owner 事件语义继续保留。

---

## 5. HTTP API 一览

| 方法 | 路径 | 架构组件 |
|---|---|---|
| `GET` | `/health/live` | 存活探针 |
| `GET` | `/health/ready` | 就绪探针（返回资源 backend、Model Gateway、Runtime Event 与 `runtime_worker` 状态） |
| `POST` | `/v1/tasks` | Task Gateway |
| `GET` | `/v1/tasks/{session_id}` | Task Query |
| `GET` | `/v1/tasks/{session_id}/result` | Task Query / Result |
| `GET` | `/v1/tasks/{session_id}/children` | Collaboration Read Model |
| `POST` | `/v1/sessions/{session_id}/messages` | Task Gateway |
| `POST` | `/v1/sessions/{session_id}/runs` | Task Gateway |
| `POST` | `/v1/sessions/{session_id}/cancel` | Task Gateway |
| `POST` | `/v1/sessions/{session_id}/resume` | Task Gateway |
| `POST` | `/v1/sessions/{session_id}/approvals/{approval_id}/responses` | Policy + Task Gateway |
| `GET` | `/v1/streams/{session_id}` | Streaming Gateway |
| `GET` | `/v1/operations/sessions/{session_id}/timeline` | Observability |
| `GET` | `/v1/operations/metrics` | Observability |

鉴权/幂等 Header：`X-Tenant-ID`、`X-Actor-ID`、`Idempotency-Key`、`X-Expected-Version`。

**CORS（开发）**：`env=development/dev` 时默认允许 `http://127.0.0.1:3000` 与 `http://localhost:3000`；其他来源通过 `AURACLAW_CORS_ALLOW_ORIGINS`（逗号分隔）配置。`composition/api.py` 中 `CORSMiddleware` 暴露 `ETag`、`Retry-After`、`traceparent`。

---

## 6. 存储与配置后端

`src/auraclaw/config.py` — `Settings`（前缀 `AURACLAW_`，可读 `.env`）。

| 数据面 | Memory | Postgres | Kafka |
|---|---|---|---|
| Canonical Event / Outbox / Snapshot | `infrastructure/persistence/memory_event_store` | `infrastructure/persistence/postgres_event_store` | — |
| Task / Approval / Collaboration RM | `projection/*/projector`（内存实现） | `infrastructure/projection/postgres_*_store` | — |
| Control State | `infrastructure/persistence/memory_control_store` | `infrastructure/persistence/postgres_control_store` | — |
| Delivery Jobs | `infrastructure/delivery/memory_job_store` | `infrastructure/delivery/postgres_job_store` | — |
| Observability / Ops | `infrastructure/observability/stores`（内存） | 同上 + `infrastructure/persistence/postgres_operations_store` | — |
| Runtime Events | `infrastructure/kafka/runtime_events.ReplayRuntimeEventBus` | — | Producer + StreamingIngestor |
| Artifact / Vault | `infrastructure/artifacts/store`（内存对象） | — | — |

| 配置键 | 行为 |
|---|---|
| `storage_backend=auto\|memory\|postgres` | `auto`：有 `DB_HOST`+用户+库名则 PG |
| `runtime_event_backend=auto\|memory\|kafka` | `auto`：有 `KAFKA_HOST` 则 Kafka |
| `artifact_root` | 已声明；当前 Artifact 仍用内存对象存储 |
| `runtime_enabled` | 默认 `true`；关闭同进程 Worker 时设为 `false` |
| `runtime_poll_interval` | 统一 Worker 轮询间隔（默认 `0.05`s） |
| `cors_allow_origins` | 当前部署显式提供的逗号分隔 Origin |

装配点：`composition/providers.py`（`lru_cache` 单例按开关选择实现）；`composition/api.py` 通过 `app.dependency_overrides` 绑定 `api/dependencies.py` 中的抽象 token。

### 6.1 import-linter 契约（节选）

| 契约名 | 约束 |
|---|---|
| Contracts are framework independent | `contracts` 不依赖任何业务/infra 包 |
| Domain only depends on contracts | `domain` 仅依赖 `contracts` |
| API is transport only | `api` 不 import `composition` / `infrastructure` |
| Gateways do not select adapters | `gateways` 不 import `composition` / `infrastructure` |
| Read gateways do not call Session write services | `gateways.query` / `gateways.streaming` 不 import `session.*_service` |
| Projection rules are adapter independent | `projection.{task,approval,collaboration}` 不 import `infrastructure` |
| Business packages do not import composition | session / projection / control / runtime / action / delivery / observability 不 import `composition` |

本地执行：`uv run lint-imports`。

---

## 7. CLI 与进程边界

### 当前入口

```text
auraclaw serve                    # composition/cli.py → uvicorn auraclaw.main:app
auraclaw projection relay [--tenant] [--watch] [--interval]
auraclaw projection rebuild [--tenant]
auraclaw operations status [--tenant]
auraclaw operations retention
auraclaw operations redrive --tenant --queue {projection|delivery} --item-id ...
```

| 架构组件 | 是否在 `composition/api.py` lifespan 常驻 |
|---|---|
| Task / Query / Streaming / Observability HTTP | ✅ |
| Kafka Streaming Ingestor（若启用） | ✅（best-effort；失败不阻断 Canonical API） |
| **RuntimeWorker**（`runtime_enabled` 且模型资源完整） | ✅ |
| Projection Relay Worker | ❌ CLI `projection relay --watch` → `composition/cli.py` |
| Orchestrator / AgentHarness 循环 | ✅（同进程统一装配） |
| Result Delivery Worker | ❌ 库 + 测试 |
| Model Gateway | ✅（由统一 Harness 组装） |

架构文档允许 MVP 合并部署，但要求逻辑边界不合并。当前所有部署使用同一个进程内
`RuntimeWorker` 验证完整 Streaming 链路；独立多进程 Worker 仍作为后续部署演进。

### Issue #12 目标入口（S2）

```text
auraclaw api run
auraclaw session run
auraclaw projection relay --watch
auraclaw orchestrator run
auraclaw runtime run
auraclaw model-gateway run
auraclaw hands run
auraclaw policy run
auraclaw credential-proxy run
auraclaw artifact run
auraclaw streaming run
auraclaw delivery run
```

目标入口使用同一应用镜像但不同对象图、service identity、数据库角色与 readiness。`auraclaw serve` 只保留为
development combined profile。S1 先建立稳定 Port/DTO 与 in-process/HTTP/MCP 同构 contract tests，S2 才增加入口，
S3 再移除跨边界 direct Store 装配。

---

## 8. 里程碑与测试对应

| 里程碑 | 主题 | 主要测试 |
|---|---|---|
| **M1** | 事实 / Outbox / Projection / 查询 | `test_m1_fact_query.py`、`test_event_store.py`、`test_session_aggregate.py`、`test_task_api.py`、`test_postgres_m1.py` |
| **M2** | Control / Orchestrator / Harness / Model | `test_m2_managed_runtime.py`、`test_postgres_m2.py` |
| **M3** | Tool / Hands / Artifact / Approval / Credential | `test_m3_tool_security.py`、`test_postgres_m3.py` |
| **M4** | Collaboration DAG / 角色 | `test_m4_collaboration.py`、`test_postgres_m4.py` |
| **M5** | Streaming / Kafka / Delivery | `test_m5_streaming_delivery.py`、`test_postgres_m5.py`、`test_kafka_m5.py` |
| **M6** | Observability / Ops / 可靠性 | `test_m6_reliability_observability.py`、`test_postgres_m6.py` |
| **M7** | 开发 Runtime 联调 | `test_m7_development_runtime.py`（CORS、端到端 Streaming+Result）；后端 49+ 项回归 |

阶段门禁见 `docs/开发阶段校验清单.md`（M1–M7 均已勾选完成）。M7.1 协议测试页、M7.2 本地 CORS + 开发 Runtime、M7.3 真实 Streaming 修复详见 `docs/M7 测试报告.md`。

---

## 9. 源码文件索引（按包）

```text
src/auraclaw/
├── main.py                 # 稳定 ASGI 导出（→ composition/api.py）
├── __main__.py             # 稳定 CLI wrapper（→ composition/cli.py）
├── config.py
├── api/
│   ├── dependencies.py     # RequestIdentity / command_context / 抽象 dependency tokens
│   ├── models.py
│   └── routes/             # tasks / streams / operations / health
├── gateways/
│   ├── task/               # TaskCommandGateway + AllowAllAdmissionController
│   ├── query/              # TaskQueryService（只读）
│   └── streaming/          # StreamingGateway
├── session/
│   ├── task_service.py
│   ├── collaboration_service.py
│   └── ports.py            # EventStore / OutboxRelayPort / SessionSnapshot
├── projection/
│   ├── relay.py
│   ├── maintenance.py
│   ├── ports.py            # TaskReader / CollaborationReader / ApprovalViewReader
│   └── {task,approval,collaboration}/projector.py
├── control/
│   ├── orchestrator.py
│   └── ports.py            # ControlStateStore / Orchestrator / RunnableItem …
├── action/
│   ├── tool_gateway.py
│   ├── policy.py
│   └── ports.py            # HandsExecutor / CredentialInvoker / ArtifactWriter
├── delivery/
│   ├── worker.py
│   └── ports.py
├── observability/
│   ├── service.py
│   └── redaction.py
├── domain/                 # Session / Collaboration / Approval 聚合
├── contracts/
├── infrastructure/
│   ├── persistence/        # event / control / operations stores
│   ├── projection/         # postgres read-model stores
│   ├── kafka/              # runtime_events
│   ├── delivery/           # job stores + sinks
│   ├── hands/ credentials/ artifacts/ observability/
├── runtime/                # harness / clients / model_gateway / ports（Model·Tool·RuntimeEvent）
└── composition/
    ├── api.py              # FastAPI factory / lifespan / dependency_overrides / CORS
    ├── providers.py        # 唯一对象图与适配器选择
    ├── cli.py              # serve / projection / operations
    └── adapters/           # unified runtime_worker
```

---

## 10. 相对架构图的主要缺口

**Issue #8 已关闭的结构债**（重构后不再列为缺口）：

- `application/` 平铺、`infrastructure/postgres.py` 巨型文件、Projection 规则与 PG 存储分裂
- `api/dependencies.py` 承担全部 DI / Worker 装配
- Query 与 Command 未分包、缺少 import-linter 门禁

**Issue #12 S5 生产收口**：

S4 已完成 Runnable Feed、Orchestrator 竞争恢复、Runtime 无黏性 checkpoint 接管、Projection 与
Delivery 顺序消费、共享 Streaming cursor、Artifact multipart/scan/GC、Hands Invocation、Model
幂等/配额、Policy bundle 和 Credential 撤销/usage 状态。

1. **生产部署**：`compose.prod.yml` 已提供副本、资源限额、服务身份、DB role、内部网络、
   Secret mount 与 migration job；普通 Compose 不虚构 HPA/PDB/NetworkPolicy，零停机使用两套
   Compose project 蓝绿切换。
2. **Artifact 治理**：Retention/GC 已落地；Legal Hold 与外部 DLP/AV 平台集成作为后续增强。
3. **模型治理**：跨 Provider fallback、供应商级限流和成本路由仍属于后续生产增强。
4. **外部资源门禁**：部署环境必须实际应用 `deploy/postgres/roles.sql`，并为 Artifact Service
   credential 授予目标 SeaweedFS bucket 的最小 PUT/GET/HEAD/DELETE 权限。

---

## 11. 参考文档

- [Managed Agent 系统架构图](./Managed%20Agent%20系统架构图.png)
- [Managed Agent 系统架构总览](./Managed%20Agent%20系统架构/00%20Managed%20Agent%20系统架构总览.md)
- [Python 后端结构说明](./Python%20后端结构说明.md)
- [Managed Agent 模块重构方案](./Managed%20Agent%20模块重构方案.md)
- [ADR-001：生产服务部署边界与通信契约](./ADR-001%20生产服务部署边界与通信契约.md)
- [Issue #8 — 模块重构跟踪](https://github.com/sushaofei/AuraClaw/issues/8)
- [Issue #12 — 12 服务生产边界](https://github.com/sushaofei/AuraClaw/issues/12)
- [开发阶段校验清单](./开发阶段校验清单.md)
- [AGENTS.md](../AGENTS.md)
- [M7 测试报告](./M7%20测试报告.md)
- [S5 Docker Compose 生产部署与故障演练 Runbook](./S5%20Docker%20Compose%20生产部署与故障演练%20Runbook.md)
