# AuraClaw

AuraClaw 是一个遵循 Managed Agent 架构的纯 Python 后端服务。系统以 Canonical
Session Event Log 为事实源，以可重建 Projection 提供查询，并把运行时控制、Agent
Runtime、工具执行和结果交付保持为清晰的逻辑边界。

## 设计哲学

AuraClaw 不是把 CLI Agent 主循环包装成 HTTP 服务，而是把**长期任务**建模为由持久事实、
派生视图、运行时控制、Agent Runtime、工具执行和交付平面共同组成的**受管分布式系统**。
「Managed Agent」中的 Managed 指：Agent 的推理能力是可替换的计算资源；任务的 lifecycle、
状态、协作、安全与交付由系统基础设施负责。

### 核心命题

> Agent 是可替换的计算单元；任务是不可丢失的业务实体。

OpenClaw 类系统的典型痛点——Agent 主循环与特定运行时强耦合、配置/会话状态全局散落、
工具与 Gateway 双向依赖、渠道与内核混在同一仓库——根因都是**把常驻 Agent 进程当作系统本身**。
AuraClaw 的回应是：让 LLM 专注语义推理与判断，把记忆、协议治理和流程编排外化为可管基础设施。

### 三重外化

| 外化维度 | 外化对象 | 工程映射 |
|---|---|---|
| 状态外化 → 记忆 | 跨越时间的执行状态 | Canonical Session Event Log + 可重建 Projection |
| 交互外化 → 协议 | 跨边界的调用、权限、生命周期 | `contracts/` 命令/事件信封 + Gateway 边界 |
| 经验外化 → 技能 | 流程化 know-how（怎么做、怎么选、不能做什么） | Coordinator/Worker/Reviewer 角色合同 + Tool Gateway + Policy |

上下文是 LLM 的「内存」，记忆是系统的「硬盘」。优质记忆系统的目标，是在正确的时间把
正确的历史切片呈现给模型——让算力用在推理上，而不是浪费在回忆上。

### 事实与视图的分离

| 数据 | 角色 | 是否可重建 |
|---|---|---|
| Canonical Session Event Log | 唯一业务事实源 | 否 |
| Read Model Store（Control、Collaboration、Result、Approval） | 查询副本 | 是，可随时全量重建 |
| Control State Store（租约、心跳、队列） | 短期运行控制 | 部分可恢复 |
| Runtime Event Bus（Token Delta、进度通知） | 实时观测 | 不保证长期存在 |

关键边界：

- **Runtime 状态不直接修改 Session 业务事实**；生命周期变化通过 Canonical Event 回写。
- **Runtime Event 流不是结果交付保证**；完成通知由持久 Outbox 驱动的 Result Delivery 负责。
- **Streaming Gateway 只推送通知，不接收改变任务状态的命令**。

网页断线不会取消任务；重连可恢复可见事件；Delivery Worker 重启后不丢失完成通知。

### 职责边界

| 组件 | 回答的问题 | 明确不做 |
|---|---|---|
| Coordinator | 任务如何语义拆分？子任务依赖如何组织？ | 不管理租约、不参与基础设施调度 |
| Orchestrator | 哪个 Session 由哪个 Runtime 在什么资源上运行？ | 不判断任务语义、不拆分 DAG |
| Agent Runtime | 在给定 Role Profile 下执行推理循环 | 不直接写 Session 事实、不读 Secret |
| Tool Gateway | 工具调用的权限、审批、幂等、路由 | 不拥有业务状态 |

语义决策与资源调度分离，使 Prompt 策略演进与调度/恢复机制演进可以独立进行。

### 安全与治理

安全治理是架构前提，而非事后补丁：

- Model、Tool、Credential 均通过 Gateway 访问；Runtime 永远不读 Secret。
- 高风险工具审批绑定 `Session + action digest + policy version`，不可跨动作复用。
- 所有写入携带 tenant、command id、expected version、actor、correlation/causation。
- 概率性 LLM 推理须用确定性系统机制约束其外部副作用。

### 工程原则

MVP 采用「模块化单体 + 独立 Worker + PostgreSQL」，**逻辑边界不能因合并部署而消失**：

```text
api → application → domain → contracts
              ↓           ↑
     infrastructure / projections / runtime
```

- `domain` 与 `contracts` 不含 FastAPI 或基础设施依赖。
- 单一写入者、独立 Schema、无跨域外键、无跨边界事务。
- 适配器通过同一契约测试；业务代码不绑定特定中间件。

架构完成标准以**可恢复性**定义：任一 Brain/Hands/Orchestrator 实例死亡后任务可从 Session
恢复；相同幂等键不重复创建 Root Session 或外部副作用；Projection 可从 Event Log 全量重建。

深入设计见 [Managed Agent 系统架构总览](docs/Managed%20Agent%20系统架构/00%20Managed%20Agent%20系统架构总览.md)、
[开发方案与实施计划](docs/Managed%20Agent%20开发方案与实施计划.md) 与
[架构代码梳理](docs/Managed%20Agent%20架构代码梳理.md)。

## 架构概览

当前版本已实现 M7 前端测试与监控工作台及 M7.1 协议测试页面：

```text
Task API -> Session Aggregate -> PostgreSQL Canonical Events + Transactional Outbox
         -> Outbox Relay -> Disposable Projection -> Task Query API
Control Projection -> Runnable Queue -> Orchestrator -> Lease/Fencing/Assignment
                   -> Recoverable Agent Harness -> Model/Tool/Session Gateway Ports
Tool Call -> Registry/Schema/Policy -> Approval Digest -> Hands/Credential Proxy
          -> Result Redaction/Normalization -> Inline Result or Artifact Reference
Root Session -> Coordinator -> Child DAG/Contracts -> Runnable Projection
             -> Worker Result -> Reviewer Evidence/Decision -> Join + Lineage
Agent/Tool/Orchestrator -> Runtime Event Producer SDK -> Kafka
                        -> Streaming Ingestor/Replay -> Authorized SSE Client
Canonical terminal event -> Transactional Outbox -> Durable Delivery Job
                         -> Webhook/Parent Session -> Retry/Circuit/DLQ -> Query View
All components -> Trace/Metrics/Structured Logs/Audit -> Session Timeline + Alerts
Operations -> Retention/Artifact GC/Poison + Delivery DLQ Redrive -> Release Gate
Browser SPA -> Streaming Chat + Query/Result Lab + Authorized SSE + Timeline + Metrics
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

开发环境默认在 API 进程内启动确定性的本地 Runtime worker，经 Runnable Queue、
Orchestrator 和 Agent Harness 生成多个 `model.output.delta`，用于真实验证 Streaming 与最终
Result 一致性；它兼容内存或 PostgreSQL 存储，测试增量直接进入进程内 SSE 回放总线，不依赖
Kafka 是否可用。它不读取 Provider Secret，生产环境不会自动启用；开发环境若已部署外部 Runtime，
可设置 `AURACLAW_DEVELOPMENT_RUNTIME_ENABLED=false` 关闭。
本地前端来源 `http://127.0.0.1:3000` 与 `http://localhost:3000` 默认允许跨域访问；其他来源通过
逗号分隔的 `AURACLAW_CORS_ALLOW_ORIGINS` 显式配置。

另开终端启动独立前端工作台：

```bash
cd frontend
npm ci
npm run dev
```

前端只调用公开 HTTP/SSE API，提供“智能问答”Streaming 测试页和“创建任务”Query / Result
测试页，并支持 Session 控制、父子 Session、审批响应、断线回放、Timeline、Metrics 和脱敏请求历史。默认 API 地址为 `http://127.0.0.1:8000`，也可在页面运行时
配置。跨域部署需要后端或反向代理明确允许所需 Origin、Methods 和 Headers；详细说明见
[frontend/README.md](frontend/README.md)。

服务启动后可访问：

- `GET /health/live`
- `GET /health/ready`
- `POST /v1/tasks`
- `GET /v1/tasks/{session_id}`
- `GET /v1/tasks/{session_id}/result`
- `GET /v1/tasks/{session_id}/children`
- `GET /v1/streams/{session_id}`（SSE，支持 `Last-Event-ID`）
- `POST /v1/sessions/{session_id}/messages`
- `POST /v1/sessions/{session_id}/runs`
- `POST /v1/sessions/{session_id}/cancel`
- `POST /v1/sessions/{session_id}/resume`
- `POST /v1/sessions/{session_id}/approvals/{approval_id}/responses`
- `GET /v1/operations/sessions/{session_id}/timeline`
- `GET /v1/operations/metrics`

写接口要求 `Idempotency-Key`；修改既有 Session 时还要求 `X-Expected-Version`。
查询支持 `ETag`、`If-None-Match` 和 `min_version`，投影未追上时返回 `202` 与
`Retry-After`。

SSE 连接关闭或 Streaming Gateway 重启不会取消任务。客户端重连时使用公开
`session_id:sequence` 游标；游标仍在保留窗口内时补齐事件，过期时收到 `stream.reset` 并回退
Task Query API。Kafka Offset 只在 Gateway 内部使用，不暴露给客户端。每个连接使用有界队列，
慢连接不会阻塞 Runtime 或 Kafka 分区；Runtime Event Bus 不可用时 Canonical 结果仍正常提交。

## Kafka 与可靠结果交付

配置 `KAFKA_HOST`、`KAFKA_PORT` 后，`AURACLAW_RUNTIME_EVENT_BACKEND=auto` 自动使用 Kafka；
强制本地内存模式可设为 `memory`。Runtime Event Producer SDK 负责 sequence、visibility、消息
大小、敏感字段拦截和 Token Delta 合并。服务级 `streaming-ingestor` Consumer Group 将消息送入
Gateway 的短期 Replay Buffer，不为每个浏览器创建 Kafka Consumer。

最终通知不依赖 Kafka。`run.completed`、`run.failed`、`run.cancelled`、`approval.requested` 和
`child.result_published` 在 Canonical Event 事务内额外写入 `delivery` Outbox。Delivery Worker
按 Sink 创建稳定 `delivery_id` 的持久 Job，支持 Webhook HMAC 签名、Parent Session Sink、
指数退避、Circuit Breaker、DLQ 和 Manual Redelivery；投递状态作为 `delivery.*` Canonical
Event 回写，并通过 Task/Result Query 的 `delivery_status`、`delivery_id`、attempt count 与响应
摘要查询。Sink 只保存 `credential_ref`，Job 不保存 Secret。

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
migrations/0006_m5_streaming_delivery.sql
migrations/0007_m6_observability_reliability.sql
```

对应的 `.down.sql` 文件提供 M1～M6 Schema 回滚。生产部署应由迁移系统执行这些 SQL，
不应由 API 进程在启动时自动修改 Schema。

Outbox Worker 与投影重建：

```bash
uv run auraclaw projection relay --watch
uv run auraclaw projection rebuild
uv run auraclaw projection rebuild --tenant tenant_1
uv run auraclaw operations status --tenant tenant_1
uv run auraclaw operations retention
uv run auraclaw operations redrive --tenant tenant_1 --queue projection --item-id EVENT_ID
uv run auraclaw operations redrive --tenant tenant_1 --queue delivery --item-id DELIVERY_ID
uv run python scripts/release_gate.py
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
  api/             薄 HTTP transport、DTO 与 dependency tokens
  gateways/        Task Command、Query 与 Streaming 接入边界
  session/         Session / Collaboration 写侧应用服务
  projection/      可重建 Read Model、Relay 与维护服务
  control/         Orchestrator 与 Control ports
  runtime/         Runtime ports、Fenced Clients、Harness、Model Gateway
  action/          Tool Gateway、Policy 与 Action ports
  delivery/        Delivery Worker 与 ports
  observability/   Trace / Audit / Ops 应用服务
  infrastructure/  持久化、投影、Kafka、Hands、Credential 等适配器
  composition/     API、CLI 与开发 Runtime 的唯一装配根
  contracts/       跨模块稳定契约
  domain/          Session、Collaboration、Approval 聚合与规则
```

M3 关键实现位于 `action/`、`domain/approval.py`、`infrastructure/hands/`、
`infrastructure/artifacts/`、`infrastructure/credentials/` 和 `projection/approval/`。
M4 关键实现位于 `session/collaboration_service.py`、`domain/collaboration.py`、
`contracts/collaboration.py` 和 `projection/collaboration/`。
M5 关键实现位于 `infrastructure/kafka/runtime_events.py`、
`gateways/streaming/gateway.py`、`api/routes/streams.py`、`delivery/worker.py` 与
`infrastructure/delivery/`。M6 关键实现位于 `contracts/observability.py`、
`observability/`、`infrastructure/observability/`、
`infrastructure/persistence/postgres_operations_store.py` 和
`api/routes/operations.py`。SLO、故障处置、数据保留和灰度回滚见
[M6 运维与灰度发布 Runbook](docs/M6%20运维与灰度发布%20Runbook.md)。

## 当前边界

项目保留内存适配器用于快速测试；配置 PostgreSQL 后使用持久 Event Store、Control
State Store、Snapshot、Command Dedup、Transactional Outbox、Task/Approval/Collaboration
Read Model、
Projector Checkpoint 和 Poison Event Queue。M3 提供本地 Hands Sandbox、内存对象适配器与
Vault 测试适配器；生产对象存储、企业 Vault 和外部 Connector 通过现有端口接入。M4 提供
确定性的 Coordinator/Worker/Reviewer 命令与权限边界。M5 提供 Kafka 和 PostgreSQL 生产
适配器、内存测试适配器以及 Webhook/Parent Session Sink；生产 Credential Proxy/Vault、内部
跨实例 PubSub 和通知渠道 Adapter 继续通过现有端口扩展。M6 提供持久 Observability/Audit
Schema、主动告警、租户隔离 Timeline、Telemetry 保留和失败队列运维；外部 Trace Collector、
Metrics Pipeline 和 Alert Receiver 通过同一观测端口接入。
