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
[架构代码梳理](docs/Managed%20Agent%20架构代码梳理.md)。MCP 数据、工具和 Skill 接入见
[M9 MCP Runtime 实施与运维](docs/M9%20MCP%20Runtime%20实施与运维.md)。扩展能力登记 AuraMCP 见
[AuraMCP 接入](docs/AuraMCP%20接入.md)。

## 架构概览

当前版本已实现 M9 MCP Runtime 能力平面：

```text
Task API -> Session Aggregate -> PostgreSQL Canonical Events + Transactional Outbox
         -> Outbox Relay -> Disposable Projection -> Task Query API
Control Projection -> Runnable Queue -> Orchestrator -> Lease/Fencing/Assignment
                   -> Recoverable Agent Harness -> Model/Tool/Session Gateway Ports
Tool Call -> Registry/Schema/Policy -> Approval Digest -> Hands/Credential Proxy
          -> Result Redaction/Normalization -> Inline Result or Artifact Reference
Runtime -> Internal MCP Gateway -> Capability Catalog / Resource / Prompt / Tool
       -> Signed Skill Package -> Fixed Binding -> Recoverable Skill Runner
       -> Credential Proxy MCP Egress -> OAuth/OIDC + Pinned Remote Server
Root Session -> Coordinator -> Child DAG/Contracts -> Runnable Projection
             -> Worker Result -> Reviewer Evidence/Decision -> Join + Lineage
Agent/Tool/Orchestrator -> Runtime Event Producer SDK -> Kafka
                        -> Streaming Ingestor/Replay -> Authorized SSE Client
Canonical terminal event -> Transactional Outbox -> Durable Delivery Job
                         -> Webhook/Parent Session -> Retry/Circuit/DLQ -> Query View
All components -> Trace/Metrics/Structured Logs/Audit -> Session Timeline + Alerts
Operations -> Retention/Artifact GC/Poison + Delivery DLQ Redrive -> Release Gate
External clients -> Streaming Chat / Query API / Authorized SSE / Timeline / Metrics
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

## 部署模式

AuraClaw 只有两种运行拓扑，本地与生产使用同一套 12 服务边界：

| 模式 | 启动方式 | 适用场景 |
|------|----------|----------|
| **本地开发** | `uv run auraclaw serve` | 本机 12 进程 + Ingress `:8080` |
| **预生产 / 生产** | `docker compose`（各容器 `auraclaw <service> run`） | 服务器测试与生产 |

已移除 **Combined 单进程**（`uvicorn auraclaw.main:app` 内嵌 Task API + Runtime）和
**本地单入口调试**（VS Code 单独 attach 某一个服务）。`auraclaw api run` 等命令仅作为
Compose 容器入口保留；在 `deployment_profile=development` 下 CLI 会拒绝单独 `run`，请始终用
`auraclaw serve` 做本地联调。

## 配置文件

仓库提交三份环境模板，真实密钥文件被 gitignore：

| 模板 | 复制为 | 用途 |
|------|--------|------|
| `.env.dev.example` | `.env.dev` | 本地开发：`uv run auraclaw serve` / VS Code，非 Docker。 |
| `.env.test.example` | `.env.test` | 服务器测试：`docker compose` 部署（如 DEV_SERVICE `10.244.16.131`）。 |
| `.env.prod.example` | `.env.prod` | 服务器生产：Compose 发布（当前与 test 一致）。 |

`.env.dev` 与 `.env.test` 除大模型 `AURACLAW_MODEL_API_KEY` / `AURACLAW_MODEL_BASE_URL` 及本地运行拓扑（`HOST`、12 入口 URL、`NO_PROXY` 等）外保持一致。开发机共用 Vault `http://10.244.16.132:8200`（KV v2 mount `secret`），`credential_ref` 只填引用。`AURACLAW_HOST=0.0.0.0` 让局域网 AuraX 访问 Ingress `:8080`；12 个内部入口 URL 仍指向 `127.0.0.1`。`NO_PROXY` 仅用于本机开发绕过 HTTP 代理，测试 / 生产 Compose 不要配置。

### 服务身份（Workload Token）

各内部服务通过 Bearer Token 互相识别。本地 `auraclaw serve` 与 Compose 应配置**同一组**
固定随机串（不要用运行时随机值），至少包括：

```text
AURACLAW_TASK_API_WORKLOAD_TOKEN
AURACLAW_PROJECTION_WORKLOAD_TOKEN
AURACLAW_ORCHESTRATOR_WORKLOAD_TOKEN
AURACLAW_RUNTIME_WORKLOAD_TOKEN      # agent-runtime ↔ action-hands 必须相同
AURACLAW_MODEL_GATEWAY_WORKLOAD_TOKEN
AURACLAW_ACTION_HANDS_WORKLOAD_TOKEN
AURACLAW_POLICY_WORKLOAD_TOKEN
AURACLAW_CREDENTIAL_PROXY_WORKLOAD_TOKEN
AURACLAW_ARTIFACT_SERVICE_WORKLOAD_TOKEN
AURACLAW_DELIVERY_WORKLOAD_TOKEN
AURACLAW_STREAMING_GATEWAY_WORKLOAD_TOKEN
AURACLAW_LEASE_SIGNING_KEY             # ≥32 字节；Session/Orchestrator/Hands 租约签名
AURACLAW_CHAINTOWER_WORKLOAD_TOKEN     # Task API 对外身份（智问等客户端）
```

生产 Compose 可通过 `scripts/materialize_compose_secrets.py` 从 `.env.prod` 生成
`.runtime/compose-secrets/` 下的 secret 文件。`agent-runtime` 调用 `action-hands` 时携带
`AURACLAW_RUNTIME_WORKLOAD_TOKEN`；Hands 用同值校验并在 Lease Assertion 通过后才执行 Tool。

## 本地启动

本地开发只走与生产同构的 12 进程拓扑：`auraclaw serve` 拉起全部独立入口（端口 8000–8011），
并在 `:8080` 提供 Ingress（`/v1/streams/*` → Streaming Gateway `:8010`，其余 → Task API `:8000`）。
Java 智问代理应指向 `http://127.0.0.1:8080`。

不要使用单进程 Combined 应用或单独 `auraclaw <service> run` 做本地调试。后者仅作为 Compose
容器的进程入口。VS Code 使用 **AuraClaw serve** 调试配置（读取 `.env.dev`）。

多进程 SSE 必须使用共享 SQL 或 Kafka；纯内存事件后端会在启动前明确拒绝：

```bash
uv sync --extra dev
cp .env.dev.example .env.dev
uv run auraclaw serve
```

每个入口都有 `/health/live` 和 `/health/ready`。Task API 不暴露 `/v1/streams/*`，Streaming
Gateway 不暴露 Task Command。

服务器测试 / 生产使用 Compose 拉起同一组 12 服务，但现在拆成各自独立模板：

```bash
# 服务器测试
docker compose --env-file .env.test -f compose.test.yml up -d --wait

# 生产
docker compose --env-file .env.prod -f compose.prod.yml up -d --wait
```

Compose 不启动 SeaweedFS，而是使用 `.env.test` / `.env.prod` 中已部署的外部 S3 endpoint。若 SeaweedFS、
PostgreSQL 或 Kafka 运行在 Docker Host，请把相应 host 配置为 `host.docker.internal`；生产环境
使用服务 DNS、Secret/Workload 注入，不在 Compose 文件中写入密钥。Ingress 将
`/v1/streams/*` 路由到 `streaming-gateway:8010`，其余路径路由到 `task-api:8000`。

生产部署使用独立模板，先运行 checksum/advisory-lock 保护的 migration job，再启动拓扑：

```bash
docker network create auraclaw-platform # 已存在时跳过
uv run python scripts/materialize_compose_secrets.py \
  --env-file .env.prod --output-dir .runtime/compose-secrets
uv run python scripts/compose_preflight.py --env-file .env.prod
docker compose --env-file .env.prod -f compose.prod.yml \
  --profile migrate run --rm migrate migrate up \
  --target 0016 --directory /app/migrations
docker compose --env-file .env.prod -f compose.prod.yml up -d --wait
```

副本、资源、Secret 文件挂载、蓝绿发布、回滚、扩缩容与 kill test 见
[S5 Docker Compose 生产部署与故障演练 Runbook](docs/S5%20Docker%20Compose%20生产部署与故障演练%20Runbook.md)。

S3 生产装配已切换为 owner HTTP/MCP Client：Session、Control、Model、Policy、Credential、
Artifact 和 Admin 写路径不再共享跨域 Store。Action Hands 以 MCP Server 暴露工具，并通过持久
Invocation Store、Policy、Credential Proxy 和 Artifact Service 执行；Runtime 只持有 MCP Client。
SeaweedFS 管理密钥只注入 Artifact Service，Vault Token 只注入 Credential Proxy，Runtime 位于
无 platform egress 的内部 Docker network。本地 `auraclaw serve` 与生产 Compose 使用同一组
12 个入口；环境差异只来自 `.env.dev` / `.env.test` / `.env.prod` 提供的存储、事件总线、模型端点和 CORS
等资源。CLI 与 VS Code debug 都读取 `.env.dev`（可用 `AURACLAW_ENV_FILE` 覆盖）。服务器测试 / 生产通过
Docker Compose 读取 `.env.test` / `.env.prod`。

生产发布复制 `.env.prod.example` 为 gitignored 的 `.env.prod`，填入不可变镜像和密钥后再跑
`scripts/materialize_compose_secrets.py` 与 `scripts/compose_preflight.py`。

Harness 的 delta 经 Runtime Event Producer SDK 排序、校验和脱敏后进入 Kafka，再由
Streaming Ingestor 写入 Replay Bus 供 SSE 消费；Kafka 不可用只会令 Streaming Gateway
`/health/ready` 降级，不会阻止完整模型输出与 `run.completed` 写入 Canonical Event。

远程容器运维：

```bash
./scripts/remote_compose.sh ps
./scripts/remote_compose.sh logs -f task-api
```

AuraClaw 是纯 Python 后端。外部客户端（如智问 UI）只调用公开 HTTP/SSE API；跨域部署通过
`AURACLAW_CORS_ALLOW_ORIGINS` 或反向代理允许所需 Origin、Methods 和 Headers。

服务启动后可访问：

- `GET /health/live`
- `GET /health/ready`
- `POST /v1/tasks`
- `POST /v1/tasks/sync`（Java/脚本一次性等待结果；AuraX / Timer 不要用）
- `GET /v1/tasks/{session_id}`
- `GET /v1/tasks/{session_id}/result`
- `GET /v1/tasks/{session_id}/children`
- `GET /v1/streams/{session_id}`（SSE，支持 `Last-Event-ID`）
- `POST /v1/sessions/{session_id}/messages`
- `POST /v1/sessions/{session_id}/runs`
- `POST /v1/sessions/{session_id}/cancel`
- `POST /v1/sessions/{session_id}/close`
- `POST /v1/sessions/{session_id}/resume`
- `POST /v1/sessions/{session_id}/approvals/{approval_id}/responses`
- `GET /v1/operations/sessions/{session_id}/timeline`
- `GET /v1/operations/metrics`

写接口要求 `Idempotency-Key`；修改既有 Session 时还要求 `X-Expected-Version`。
查询支持 `ETag`、`If-None-Match` 和 `min_version`，投影未追上时返回 `202` 与
`Retry-After`。

Root Session 可以承载多个顺序执行的 Run：一轮 Run 进入 `completed`、`failed` 或
`cancelled` 后，Session 回到 `ready`，后续消息继续使用同一个 `session_id` 并生成新的
`run_id`。Task View 分别返回 `status`（Session）和 `run_status`（最新 Run）；Result 响应的
`status` 表示最新 Run 状态，并通过 `session_status` 返回 Session 状态。只有显式调用
`/close` 产生 `session.closed` 后，Root Session 才拒绝新消息和 Run。

SSE 连接关闭或 Streaming Gateway 重启不会取消任务。客户端重连时使用公开
`session_id:sequence` 游标；`sequence` 在同一 Session 的多个 Run 间保持单调递增。游标仍在保留窗口内时补齐事件，过期时收到 `stream.reset` 并回退
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

## 主存储（MySQL / PostgreSQL）

存储配置支持两种形式：

- `AURACLAW_DATABASE_URL=mysql+aiomysql://...` 或 `postgresql+asyncpg://...`
- `DB_HOST`、`DB_PORT`、`DB_USER`、`DB_PWD`、`DB_NAME`

方言选择：

- `AURACLAW_DB_DIALECT=mysql|postgres`（默认 **mysql**）
- `AURACLAW_STORAGE_BACKEND=auto|memory|mysql|postgres`

当存在完整 `DB_*` 且 `storage_backend=auto` 时启用 SQL 存储，方言默认 MySQL。
继续使用 PostgreSQL 时请显式设置 `AURACLAW_STORAGE_BACKEND=postgres`（或
`AURACLAW_DB_DIALECT=postgres` 且 URL scheme 为 postgresql）。密码中的 `#`、`,` 由
`Settings.resolved_database_url` 自动 URL 编码。

迁移：

```bash
# MySQL（默认目录 migrations/mysql）
uv run auraclaw migrate up

# PostgreSQL
AURACLAW_STORAGE_BACKEND=postgres uv run auraclaw migrate up --directory migrations
```

首次启动前按版本顺序应用 migrations。开发和生产使用各自配置文件中的 `DB_NAME`。

PostgreSQL 脚本在 `migrations/`；MySQL 对照脚本在 `migrations/mysql/`（表名使用
`schema_table` 前缀，例如 `session_core_canonical_event`）。生产角色授权：

- MySQL：`deploy/mysql/roles.sql`（意图文档）；托管实例若拒绝通配 GRANT，使用
  `uv run python scripts/apply_mysql_roles.py` 按前缀展开到具体表
- PostgreSQL：`deploy/postgres/roles.sql`

```text
migrations/0001_initial.sql
migrations/mysql/0001_initial.sql
migrations/0002_m1_fact_query.sql
migrations/0003_m2_managed_runtime.sql
migrations/0004_m3_tool_artifact_approval.sql
migrations/0005_m4_collaboration_review.sql
migrations/0006_m5_streaming_delivery.sql
migrations/0007_m6_observability_reliability.sql
migrations/0008_multi_run_sessions.sql
migrations/0009_s3_owner_boundaries.sql
migrations/0010_s4_claim_recovery.sql
migrations/0011_s4_streaming_state.sql
migrations/0012_s4_model_state.sql
migrations/0013_s4_artifact_lifecycle.sql
migrations/0014_s4_policy_version.sql
```

对应的 `.down.sql` 文件提供逐阶段 Schema 回滚。生产部署应由迁移系统执行这些 SQL，
不应由 API 进程在启动时自动修改 Schema。

多副本生产边界、角色配置、恢复与回滚顺序见
[S4 横向扩展与恢复运行说明](docs/S4%20横向扩展与恢复运行说明.md)，验证证据见
[S4 测试报告](docs/S4%20测试报告.md)。

Outbox Worker 与投影重建（需已运行的 12 服务拓扑；`projection relay --watch` 由 Compose
在 `deployment_profile=production` 下启动，本地请用 `auraclaw serve`）：

```bash
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
# 使用当前 .env.dev 的 DB_NAME
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
  composition/     API factory、CLI、`auraclaw serve` 与 Compose 装配根
  contracts/       跨模块稳定契约
  domain/          Session、Collaboration、Approval 聚合与规则
```

M3 关键实现位于 `action/`、`domain/approval.py`、`infrastructure/hands/`、
`infrastructure/artifacts/`、`infrastructure/credentials/` 和 `projection/approval/`。
M4 关键实现位于 `session/collaboration_service.py`、`domain/collaboration.py`、
`contracts/collaboration.py` 和 `projection/collaboration/`。
M5/M8 关键实现位于 `infrastructure/kafka/runtime_events.py`、
`infrastructure/model/openai_compatible.py`、`composition/providers.py`、
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

## 能力与业务数据

AuraClaw 不再直接读取业务数据库或内置业务 Tool。Tool、Resource 与 Skill 包均通过
`action-hands` 的 MCP / Java API egress 对账发现；业务数据由远端 MCP Server 提供。
Skill 包经 `skill://` Resource 下载并由 `SkillPackageReconciler` 发布。开发与联调见
[MCP 开发手册](docs/MCP%20开发手册.md)；历史价格洞察实施说明见
[M12 价格洞察业务 Skill 实施与运维](docs/M12%20价格洞察业务%20Skill%20实施与运维.md)（**已废弃本地 DWD 直连路径**）。
