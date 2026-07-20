# Managed Agent 架构代码梳理

> 依据 [Managed Agent 系统架构图](./Managed%20Agent%20系统架构图.png) 与 `docs/Managed Agent 系统架构/`，对当前 `src/auraclaw/` 代码做组件级映射。  
> 分析范围：纯 Python Managed Agent 后端（FastAPI）；前端工作台仅作 API Client 引用。  
> 梳理日期：2026-07-20（M7.3 开发 Runtime 联调更新）。

## 一句话总结

AuraClaw 是以 **Canonical Session Event Log** 为唯一任务事实源的模块化单体：HTTP 进程已装配 Task/Query/Streaming/Observability；**开发环境**额外在进程内启动 `DevelopmentRuntimeWorker`，经 Orchestrator + AgentHarness 产出真实 SSE 增量与终态 Result。生产路径下 Orchestrator、Agent Runtime、Tool Gateway、Result Delivery 仍以应用库和测试为主，仅 Projection 已有常驻 CLI；逻辑边界与架构图对齐，部署上尚未拆成独立 Worker 进程。

---

## 1. 分析边界与技术栈

| 项 | 说明 |
|---|---|
| **系统类型** | 纯 Python Managed Agent 后端（模块化单体，可合并部署） |
| **入口** | `auraclaw.main:app`（FastAPI） / CLI `auraclaw` |
| **语言与运行时** | Python ≥ 3.11 |
| **核心依赖** | FastAPI、Pydantic Settings、asyncpg、aiokafka、httpx、uvicorn |
| **存储** | 内存适配器（测试/本地）或 PostgreSQL（生产） |
| **实时总线** | 内存 Replay Buffer 或 Kafka `managed-agent.runtime-events` |
| **开发联调** | `DevelopmentRuntimeWorker`（dev 环境进程内）、CORS 默认放行 `localhost:3000` |
| **不包含** | 生产云厂商 Model SDK、企业 Vault/KMS、S3 Artifact、远程容器 Hands |

当前依赖方向（现状描述，不作为重构后的门禁配置）：

```text
api → application → domain → contracts
              ↓           ↑
     infrastructure / projections / runtime
```

Issue #8 的目标方向见 [Managed Agent 模块重构方案](./Managed%20Agent%20模块重构方案.md)：entrypoint 经 `composition` 组装 `api`、gateways、业务包和 infrastructure adapters；`api`/gateways 不反向导入 composition 或 infrastructure。包与部署单元不追求 1:1，而通过「组件 → 主归属包 → 进程入口」矩阵保持可追踪。

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
 application/tasks.py      application/streaming.py   application/observability.py
 application/collaboration.py  application/delivery.py  application/orchestration.py
 application/tooling.py
          │
          ▼
 domain/{session,collaboration,approval,ports}.py
 contracts/{events,commands,state,tools,delivery,...}.py
          │
    ┌─────┴──────────────────────────────┐
    ▼                                    ▼
 projections/                      infrastructure/
  relay / tasks /                  postgres / memory / control_* /
  approvals / collaboration        runtime_events / hands / artifacts /
                                   credentials / delivery / observability
          │
          ▼
 runtime/{harness,clients,model_gateway,development,ports}.py
   Agent Runtime Pool + Model Gateway + 开发 Runtime Worker
```

| 架构图图层 | 代码包 | 职责 |
|---|---|---|
| Service Gateway | `api/` + 部分 `application/` | 命令准入、查询、SSE、运维只读 |
| Intelligence | `domain/` + `application/` + `runtime/` + `projections/` | Session 事实、协作、调度、Runtime、投影 |
| Data / System / Tool | `infrastructure/` | Event Store、Control、Artifact、Hands、Kafka、Delivery、Vault |
| Shared Contracts | `contracts/` | 跨边界稳定类型，无框架依赖 |

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
| **主路径** | `api/routes/tasks.py`、`api/dependencies.py`、`api/models.py`、`application/tasks.py` |
| **关键类型** | `TaskService`、`AllowAllAdmissionController` |
| **状态** | **部分实现**（命令边界完整；Admission 为放行桩） |
| **HTTP** | 见 §5 |
| **缺口** | 无真实身份认证/配额；`AllowAllAdmissionController.admit` 为空实现 |

### 3.2 Session / Collaboration Service

| 项 | 内容 |
|---|---|
| **架构角色** | Canonical Event Log 唯一业务写入方；Root/Child 协作事实 |
| **主路径** | `domain/session.py`、`domain/collaboration.py`、`application/tasks.py`、`application/collaboration.py` |
| **存储** | `infrastructure/memory.py`（`InMemoryEventStore`）、`infrastructure/postgres.py`（`PostgresEventStore`） |
| **端口** | `domain/ports.py`：`EventStore`、`AppendResult`、`SessionSnapshot` |
| **关键类型** | `SessionAggregate`、`CollaborationAggregate`、`CollaborationService`、`CoordinatorRole` / `WorkerRole` / `ReviewerRole` |
| **状态** | **已实现**（库级）；Collaboration 命令无独立 REST |
| **缺口** | 协作写路径靠应用服务/测试驱动，非独立微服务 |

### 3.3 Projection Service

| 项 | 内容 |
|---|---|
| **架构角色** | Transactional Outbox → 幂等投影；可删除重建 |
| **主路径** | `projections/relay.py`、`projections/tasks.py`、`projections/approvals.py`、`projections/collaboration.py`、`application/maintenance.py` |
| **关键类型** | `OutboxRelay`、`InMemoryTaskProjection` / `PostgresTaskProjection`、`CompositeProjection`、`ProjectionMaintenanceService` |
| **状态** | **已实现** |
| **运维** | `auraclaw projection relay [--watch]`、`auraclaw projection rebuild` |
| **说明** | API 路径在命令后即时 `relay_once`；可靠消费仍以 Outbox + Worker 为准 |

### 3.4 Read Model Store

| 项 | 内容 |
|---|---|
| **架构角色** | 可丢弃查询副本（Task / Approval / Collaboration / Observability 视图） |
| **主路径** | 同上 Projection + `Postgres*Projection` |
| **端口** | `TaskReader`、`CollaborationReader`、`ApprovalViewReader` |
| **状态** | **已实现**（memory + postgres） |
| **缺口** | 无独立 ES/CDB 读副本；与写库同 PG 时靠 schema 隔离 |

### 3.5 Task Query / Result Service

| 项 | 内容 |
|---|---|
| **架构角色** | 只读任务状态与终态结果；支持 ETag / `min_version` |
| **主路径** | `api/routes/tasks.py`（`get_task` / `get_result` / `list_children`） |
| **状态** | **已实现** |
| **说明** | 只读 Projection，不直读 Event Log |

### 3.6 Runtime Event Bus

| 项 | 内容 |
|---|---|
| **架构角色** | Token Delta、进度、工具状态等短期实时事件；不替代 Canonical Log |
| **主路径** | `infrastructure/runtime_events.py`、`runtime/ports.py`（`RuntimeEvent` / `RuntimeEventPublisher`） |
| **关键类型** | `RuntimeEventProducerSDK`、`ReplayRuntimeEventBus`、`KafkaRuntimeEventProducer`、`KafkaStreamingIngestor` |
| **状态** | **已实现**（memory + Kafka） |
| **配置** | `AURACLAW_RUNTIME_EVENT_BACKEND`、`KAFKA_HOST` / `KAFKA_PORT` |
| **开发路径** | `DevelopmentRuntimeWorker` 经 `DelayedRuntimeEventPublisher` 直接写入 `ReplayRuntimeEventBus`，即使 `.env` 选了 Kafka 也不依赖 Kafka 可用 |
| **缺口** | 生产 Runtime Producer 仍未挂入主服务 DI；Kafka Producer 与开发 Worker 分路径 |

### 3.7 Streaming Gateway

| 项 | 内容 |
|---|---|
| **架构角色** | 租户授权后把 Runtime Event 转为 SSE；只推送，不收改变状态的命令 |
| **主路径** | `application/streaming.py`（`StreamingGateway`）、`api/routes/streams.py` |
| **HTTP** | `GET /v1/streams/{session_id}`（支持 `Last-Event-ID`） |
| **状态** | **已实现**（dev 环境可端到端验证多 delta SSE + Result 一致性） |
| **缺口** | 无 WebSocket；仅 `visibility=="user"` 下发 |

### 3.8 Result Delivery Service

| 项 | 内容 |
|---|---|
| **架构角色** | Canonical Delivery Outbox 驱动的可靠外部通知（重试、熔断、DLQ） |
| **主路径** | `application/delivery.py`、`infrastructure/delivery.py`、`contracts/delivery.py` |
| **关键类型** | `ResultDeliveryWorker`、`CircuitBreaker`、`WebhookResultSink`、`ParentSessionResultSink`、`PostgresDeliveryJobStore` |
| **状态** | **已实现**（库级）；**未挂 FastAPI lifespan / CLI 常驻 Worker** |
| **缺口** | 无 `auraclaw delivery` 子命令；需外部进程或测试驱动 |

### 3.9 Orchestrator

| 项 | 内容 |
|---|---|
| **架构角色** | 资源调度（queue / claim / lease / provision / assign）；不做语义拆分 |
| **主路径** | `application/orchestration.py`、`runtime/ports.py`（`Orchestrator` Protocol） |
| **关键类型** | `ManagedOrchestrator`、`LocalRuntimeProvisioner` |
| **状态** | **已实现**（库级）；**dev 环境经 `DevelopmentRuntimeWorker` 接入 HTTP 进程** |
| **端口** | `watch`、`schedule_once`、`cancel`、`heartbeat`、`reconcile` |
| **开发装配** | `api/dependencies.py` → `build_development_runtime_worker()` → `ManagedOrchestrator` + `LocalRuntimeProvisioner("development")` |
| **缺口** | 生产无独立 orchestrator worker 入口；Provisioner 为本地计数式 |

### 3.10 Control State Store

| 项 | 内容 |
|---|---|
| **架构角色** | 租约、fencing token、队列、assignment、heartbeat、capacity、checkpoint |
| **主路径** | `infrastructure/control_memory.py`、`infrastructure/control_postgres.py` |
| **关键类型** | `InMemoryControlStateStore`、`PostgresControlStateStore` |
| **状态** | **已实现** |
| **说明** | 与 Canonical Event Store 独立 schema/写入边界，不做跨 Store 事务 |
| **缺口** | 无 Redis 适配器 |

### 3.11 Agent Runtime Pool

| 项 | 内容 |
|---|---|
| **架构角色** | `Role + Harness + Model Client + Context Policy + Tool Client`；可恢复执行 |
| **主路径** | `runtime/harness.py`、`runtime/clients.py`、`runtime/ports.py`、`application/collaboration.py`（角色门面） |
| **关键类型** | `AgentHarness`、`FencedSessionClient`、`FencedToolClient`、`IdempotentToolClient`、`CoordinatorRole` / `WorkerRole` / `ReviewerRole` |
| **开发 Worker** | `runtime/development.py`：`DevelopmentRuntimeWorker`、`DevelopmentModelClient`、`DelayedRuntimeEventPublisher` |
| **状态** | **部分实现**（Harness + 角色完整；dev 有进程内 Worker；非多进程 Pool） |
| **说明** | 架构图中的 Planner 对应代码中的 **Reviewer** 角色（校验生产结果），无独立 `PlannerRuntime` 类 |
| **缺口** | 生产无常驻 Runtime 进程池；`development_runtime_active` 仅在 `env=development/dev` 且 `AURACLAW_DEVELOPMENT_RUNTIME_ENABLED=true` 时启用 |

### 3.12 Model Gateway

| 项 | 内容 |
|---|---|
| **架构角色** | Runtime 与 LLM Provider 之间的唯一凭证解析与调用边界 |
| **主路径** | `runtime/model_gateway.py`、`runtime/ports.py`（`ModelClient` / `ProviderAdapter`） |
| **关键类型** | `ModelGateway`、`StaticCredentialResolver` |
| **开发模型** | `DevelopmentModelClient`（`runtime/development.py`）：确定性本地回答 + 多 delta 流式输出，不读 Provider Secret |
| **状态** | **部分实现**（网关框架完整；dev 有确定性 Model Client；无生产 OpenAI/Anthropic Adapter） |
| **缺口** | 生产 `ModelGateway` 未接入主服务 DI；开发路径绕过真实 Provider |

### 3.13 Tool Gateway / Dispatcher

| 项 | 内容 |
|---|---|
| **架构角色** | 工具发现、Schema、权限、审批、幂等、取消、结果规范化；可视为 MCP 风格边界 |
| **主路径** | `application/tooling.py`、`contracts/tools.py` |
| **关键类型** | `ToolRegistry`、`JsonSchemaValidator`、`PolicyEngine`、`ToolGateway`、`GatewayToolClient` |
| **状态** | **已实现**（库级）；未挂 HTTP |
| **分发** | Hands（本地行动）或 Credential Proxy（代持凭证调用） |

### 3.14 Hands Service

| 项 | 内容 |
|---|---|
| **架构角色** | 工具实际执行环境（Hands）；无 Shell、最小环境、文件根隔离 |
| **主路径** | `infrastructure/hands.py` |
| **关键类型** | `LocalHandsService` |
| **状态** | **已实现**（本地适配器） |
| **缺口** | 无远程/容器 Hands |

### 3.15 Artifact Store

| 项 | 内容 |
|---|---|
| **架构角色** | 大结果/文件不可变对象 + lineage + ACL + 短期下载令牌 |
| **主路径** | `infrastructure/artifacts.py`、`contracts/tools.py`（`ArtifactRef`） |
| **关键类型** | `ArtifactStore`、`InMemoryObjectStorage` |
| **状态** | **部分实现** |
| **缺口** | 无 S3/文件系统适配器；`Settings.artifact_root` 已声明但未接到 `ArtifactStore` |

### 3.16 Policy / Approval Service

| 项 | 内容 |
|---|---|
| **架构角色** | 工具权限策略 + action digest 绑定审批；Human 经 Task Gateway 回写 Canonical Event |
| **主路径** | `application/tooling.py`（`PolicyEngine`）、`domain/approval.py`、`projections/approvals.py`、`api/routes/tasks.py`（`record_approval_response`） |
| **状态** | **已实现**（嵌入 Tool Gateway + Session，非独立微服务） |
| **缺口** | 策略版本硬编码风格（如 `m3-v1`）；无独立 `policy/` 包 |

### 3.17 Credential Proxy / Vault

| 项 | 内容 |
|---|---|
| **架构角色** | Runtime/Hands 不可见真实密钥；代理代调并递归脱敏 |
| **主路径** | `infrastructure/credentials.py` |
| **关键类型** | `CredentialProxy`、`InMemoryVault`、`SecretRedactor` |
| **状态** | **部分实现** |
| **缺口** | 无企业 Vault/KMS；模型凭证（`StaticCredentialResolver`）与工具凭证两套解析器 |

### 3.18 Observability / Audit

| 项 | 内容 |
|---|---|
| **架构角色** | Trace / Metric / Audit / Alert / Session Timeline；不参与业务决策 |
| **主路径** | `application/observability.py`、`infrastructure/observability.py`、`infrastructure/operations.py`、`api/routes/operations.py` |
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
| **主路径** | `infrastructure/delivery.py`（`WebhookResultSink`、`ParentSessionResultSink`） |
| **状态** | **已实现**（两类 Sink） |
| **缺口** | 无 Sink 管理 API；Email/MQ 等未做 |

### 3.21 Development Runtime Worker（开发联调）

| 项 | 内容 |
|---|---|
| **架构角色** | 开发环境进程内 Worker，保留 Queue → Orchestrator → Harness 边界，用于真实 Streaming + Result 联调 |
| **主路径** | `runtime/development.py`、`api/dependencies.py`（`build_development_runtime_worker`）、`main.py`（lifespan 启动） |
| **关键类型** | `DevelopmentRuntimeWorker`、`DevelopmentModelClient`、`DelayedRuntimeEventPublisher` |
| **触发条件** | `Settings.development_runtime_active`：`env=development/dev` 且 `AURACLAW_DEVELOPMENT_RUNTIME_ENABLED=true`（默认 true） |
| **状态** | **已实现**（仅开发环境；生产不启用） |
| **说明** | 轮询 Read Model 中 `pending`/`runnable` 任务 → `orchestrator.watch` → `schedule_once` → `harness.execute` → `relay_once`；Runtime Event 直接写入进程内 `ReplayRuntimeEventBus` |
| **缺口** | 非生产 Runtime；使用 `InMemoryControlStateStore`；关闭开关：`AURACLAW_DEVELOPMENT_RUNTIME_ENABLED=false` |

---

## 4. 关键调用链

### 4.1 创建任务（Command）

```text
POST /v1/tasks
  → TaskService.create_task
  → AllowAllAdmissionController.admit
  → SessionAggregate.create
  → EventStore.append（+ Transactional Outbox）
  → save_snapshot
  → OutboxRelay.relay_once
      → CompositeProjection
         (Task + Approval + Collaboration + ObservabilityProjector)
  → 202 TaskAcceptedResponse（含 status / result / stream URL）
```

### 4.2 查询

```text
GET /v1/tasks/{id} | /result | /children
  → TaskService.get_task | CollaborationReader.list_children
  → Task / Collaboration Read Model
```

### 4.3 实时推送

```text
GET /v1/streams/{session_id}
  → StreamingGateway.authorize（TaskReader 租户校验）
  → ReplayRuntimeEventBus.subscribe
      （可选 KafkaStreamingIngestor 灌入）
  → SSE（user visibility；cursor 过期 → stream.reset）
```

**开发环境端到端（M7.2/M7.3）**：

```text
POST /v1/tasks（goal 含用户问题）
  → Canonical Event + Projection（status=pending）
  → DevelopmentRuntimeWorker.run（lifespan 后台轮询）
      → orchestrator.watch → schedule_once → AgentHarness.execute
      → DevelopmentModelClient.generate（多 model.output.delta）
      → DelayedRuntimeEventPublisher → ReplayRuntimeEventBus.publish
      → relay_once → Projection（status=completed）
  → GET /v1/streams/{session_id} 收到多个 delta
  → GET /v1/tasks/{session_id}/result 与 delta 拼接一致
```

Runtime 侧生产（库/生产路径）：

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
CoordinatorRole / CollaborationService
  → create_child / 合同 / 委派 / Join
  → Canonical Events → Collaboration Projection（runnable children）
  → Orchestrator 只消费 runnable，不拆 DAG
WorkerRole → publish_child_result
ReviewerRole → publish_review（独立 Review Session，不覆盖 Worker Artifact）
```

---

## 5. HTTP API 一览

| 方法 | 路径 | 架构组件 |
|---|---|---|
| `GET` | `/health/live` | 存活探针 |
| `GET` | `/health/ready` | 就绪探针（返回 `storage`、`runtime_events`、`runtime_event_bus_ready`、`development_runtime`） |
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

**CORS（开发）**：`env=development/dev` 时默认允许 `http://127.0.0.1:3000` 与 `http://localhost:3000`；其他来源通过 `AURACLAW_CORS_ALLOW_ORIGINS`（逗号分隔）配置。`main.py` 中 `CORSMiddleware` 暴露 `ETag`、`Retry-After`、`traceparent`。

---

## 6. 存储与配置后端

`src/auraclaw/config.py` — `Settings`（前缀 `AURACLAW_`，可读 `.env`）。

| 数据面 | Memory | Postgres | Kafka |
|---|---|---|---|
| Canonical Event / Outbox / Snapshot | `InMemoryEventStore` | `PostgresEventStore` | — |
| Task / Approval / Collaboration RM | `InMemory*Projection` | `Postgres*Projection` | — |
| Control State | `InMemoryControlStateStore` | `PostgresControlStateStore` | — |
| Delivery Jobs | `InMemoryDeliveryJobStore` | `PostgresDeliveryJobStore` | — |
| Observability / Ops | `InMemoryObservabilityStore` | `PostgresObservabilityStore` / `PostgresOperationsStore` | — |
| Runtime Events | `ReplayRuntimeEventBus` | — | Producer + StreamingIngestor |
| Artifact / Vault | 内存 | — | — |

| 配置键 | 行为 |
|---|---|
| `storage_backend=auto\|memory\|postgres` | `auto`：有 `DB_HOST`+用户+库名则 PG |
| `runtime_event_backend=auto\|memory\|kafka` | `auto`：有 `KAFKA_HOST` 则 Kafka |
| `artifact_root` | 已声明；当前 Artifact 仍用内存对象存储 |
| `development_runtime_enabled` | 默认 `true`；与 `env=development/dev` 共同决定 `development_runtime_active` |
| `development_runtime_poll_interval` | 开发 Worker 轮询间隔（默认 `0.05`s） |
| `development_stream_delay` | 开发 delta 发布间隔（默认 `0.08`s），便于浏览器观察多段 SSE |
| `cors_allow_origins` | 逗号分隔 Origin；dev 未配置时默认放行本地前端 `3000` 端口 |

装配点：`api/dependencies.py`（`lru_cache` 单例按开关选择实现）。

---

## 7. CLI 与进程边界

```text
auraclaw serve                                          # uvicorn auraclaw.main:app
auraclaw projection relay [--tenant] [--watch] [--interval]
auraclaw projection rebuild [--tenant]
auraclaw operations status [--tenant]
auraclaw operations retention
auraclaw operations redrive --tenant --queue {projection|delivery} --item-id ...
```

| 架构组件 | 是否在 `create_app` lifespan 常驻 |
|---|---|
| Task / Query / Streaming / Observability HTTP | ✅ |
| Kafka Streaming Ingestor（若启用） | ✅（best-effort；失败不阻断 Canonical API） |
| **DevelopmentRuntimeWorker**（dev 且 `development_runtime_active`） | ✅ |
| Projection Relay Worker | ❌ CLI `projection relay --watch` |
| Orchestrator / AgentHarness 循环（**生产**） | ❌ 库 + 测试 |
| Result Delivery Worker | ❌ 库 + 测试 |
| Tool Gateway / Model Gateway（**生产**） | ❌ 库（由 Harness 组装） |

架构文档允许 MVP 合并部署，但要求逻辑边界不合并——当前代码满足逻辑边界；**开发环境**已通过 `DevelopmentRuntimeWorker` 在单进程内验证完整 Streaming 链路，**生产 Worker 进程装配**仍是主要缺口。该缺口属于新增运行行为，不纳入 Issue #8 的纯结构重构，后续以独立 feature issue 实施。

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
| **M7** | 前端工作台 + 开发 Runtime 联调 | `test_m7_development_runtime.py`（CORS、端到端 Streaming+Result）、`frontend/` 协议与冒烟测试；后端 49+ 项回归 |

阶段门禁见 `docs/开发阶段校验清单.md`（M1–M7 均已勾选完成）。M7.1 协议测试页、M7.2 本地 CORS + 开发 Runtime、M7.3 真实 Streaming 修复详见 `docs/M7 测试报告.md`。

---

## 9. 源码文件索引（按包）

```text
src/auraclaw/
├── main.py                 # FastAPI 应用、CORS、观测中间件、开发 Runtime lifespan
├── __main__.py             # CLI
├── config.py               # Settings
├── api/
│   ├── dependencies.py     # DI / 存储后端选择 / build_development_runtime_worker
│   ├── models.py           # HTTP DTO
│   └── routes/
│       ├── tasks.py        # Task Gateway + Query
│       ├── streams.py      # Streaming Gateway
│       ├── operations.py   # Observability 只读
│       └── health.py
├── application/
│   ├── tasks.py            # Session 命令用例
│   ├── collaboration.py    # 多 Agent 协作用例 + 角色门面
│   ├── orchestration.py    # Orchestrator
│   ├── tooling.py          # Tool Gateway / Policy
│   ├── streaming.py        # Streaming Gateway
│   ├── delivery.py         # Result Delivery Worker
│   ├── observability.py    # 观测服务与投影
│   └── maintenance.py      # Projection rebuild
├── domain/
│   ├── session.py          # SessionAggregate
│   ├── collaboration.py    # CollaborationAggregate
│   ├── approval.py         # ApprovalAggregate
│   └── ports.py            # 领域端口
├── contracts/              # 稳定跨边界类型
├── projections/
│   ├── relay.py            # OutboxRelay
│   ├── tasks.py
│   ├── approvals.py
│   └── collaboration.py
├── infrastructure/
│   ├── postgres.py / memory.py
│   ├── control_postgres.py / control_memory.py
│   ├── runtime_events.py
│   ├── hands.py / artifacts.py / credentials.py
│   ├── delivery.py / observability.py / operations.py
└── runtime/
    ├── harness.py          # AgentHarness
    ├── clients.py          # Fenced / Idempotent 客户端
    ├── model_gateway.py
    ├── development.py      # DevelopmentRuntimeWorker / DevelopmentModelClient
    └── ports.py            # Runtime / Control / Model 端口
```

**API Client（前端）**：`frontend/` 为独立 SPA，经公开 HTTP/SSE 调用后端；提供「智能问答」Streaming 页与「创建任务」Query/Result 页，默认 API `http://127.0.0.1:8000`。详见 `frontend/README.md`。

---

## 10. 相对架构图的主要缺口

1. **生产进程装配**：Orchestrator、Runtime Pool、Tool、Delivery 未进入生产 `create_app` lifespan；开发环境已有 `DevelopmentRuntimeWorker` 单进程验证。
2. **Admission / Auth**：放行桩；无 OAuth/配额硬门禁。
3. **Model Provider**：生产仅 Protocol + 测试 Adapter；开发用 `DevelopmentModelClient`。
4. **Artifact / Vault**：无 S3、无企业 Vault；`artifact_root` 未接线。
5. **Collaboration 写面**：无 REST；仅 `GET .../children` 读投影。
6. **部署拓扑**：模块化单体；文档中的 Gateway / Workers / Runtime 三分进程未拆。
7. **架构图 Planner**：代码侧为 Reviewer 角色，命名不完全一致。

---

## 11. 参考文档

- [Managed Agent 系统架构图](./Managed%20Agent%20系统架构图.png)
- [Managed Agent 系统架构总览](./Managed%20Agent%20系统架构/00%20Managed%20Agent%20系统架构总览.md)
- [Python 后端结构说明](./Python%20后端结构说明.md)
- [Managed Agent 开发方案与实施计划](./Managed%20Agent%20开发方案与实施计划.md)
- [开发阶段校验清单](./开发阶段校验清单.md)
- [AGENTS.md](../AGENTS.md)
- [M7 测试报告](./M7%20测试报告.md)
- [frontend/README.md](../frontend/README.md)
