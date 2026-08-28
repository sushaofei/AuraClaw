# 开发阶段校验清单

本清单是每个开发阶段的完成门禁。阶段只有在功能、架构、质量和交付项全部通过后，
才允许标记完成、提交 Git 并推送远端。

## 通用阶段门禁

每个阶段复制以下清单，并记录在本文件中：

- [ ] 阶段范围与不包含范围已明确。
- [ ] 功能列表逐项完成，并有对应测试或可复现验证证据。
- [ ] 事实源、唯一写入方、状态转换和失败恢复边界符合架构设计。
- [ ] tenant、权限、幂等、并发、超时、取消和敏感信息处理已检查。
- [ ] 数据库 Schema、迁移和回滚策略已同步；不涉及则标记不适用并说明。
- [ ] Ruff、Mypy、Pytest 全部通过。
- [ ] 关键 API 或 Worker 已完成真实运行冒烟验证。
- [ ] README、架构说明、API 与运维说明已同步。
- [ ] Git 暂存范围已审查，不包含 `.env`、`.history`、缓存、虚拟环境或 Secret。
- [ ] 使用清晰的阶段提交信息完成 commit。
- [ ] 当前分支已 push 到 `origin`，远端提交与本地一致。

## 阶段 P0：纯 Python 后端重构与事实—查询竖切

状态：实现与 intentional commit 已完成，待网络恢复后推送。

交付记录：

- 实现提交：`d1fd6e7 feat: rebuild backend as Python managed agent service`
- 远端分支：`origin/chore/init-workspace`

### 范围

- 删除旧 TypeScript、Node、CLI 与 Agent 实现。
- 建立 Python 3.11+、FastAPI、Pydantic、uv 工程。
- 实现 Task API、Session Aggregate、Canonical Event、Command Dedup、Outbox 和 Task Projection。
- 提供 PostgreSQL 初始 Schema、测试与开发说明。
- 本阶段不包含 PostgreSQL 运行适配器、异步 Worker、Agent Runtime 和工具执行。

### 功能校验

- [x] 仓库中不存在 TypeScript、pnpm、package.json 和 tsconfig 残留。
- [x] `GET /health/live` 与 `GET /health/ready` 可用。
- [x] `POST /v1/tasks` 返回 `202`、Session ID、Run ID 和查询地址。
- [x] 相同 tenant 与幂等键重复创建返回完全相同的响应。
- [x] `GET /v1/tasks/{session_id}` 返回 Task Projection 和 Projection Version。
- [x] `GET /v1/tasks/{session_id}/result` 在未完成时返回 `202`。
- [x] 取消命令校验 `expected_version` 并推进为 `cancelled`。
- [x] 不同 tenant 无法读取其他租户 Session。
- [x] Session Event 的 aggregate version 严格递增。
- [x] Event、Command Result 与 Outbox 在同一个内存临界区原子提交。
- [x] Projection 使用 event ID 去重并检测 version gap。

### 架构与数据校验

- [x] `contracts` 和 `domain` 不依赖 FastAPI 或数据库实现。
- [x] Canonical Event 是任务事实源，Projection 可删除重建。
- [x] API、Application、Domain、Infrastructure、Projection 边界清晰。
- [x] PostgreSQL Schema 分离 session、projection、control、delivery。
- [x] PostgreSQL Schema 包含 Event、Command Dedup、Outbox、Snapshot 和 Projection 元数据。
- [x] `.env` 保持忽略，示例配置不包含真实凭证。
- [x] PostgreSQL 运行适配器尚未实现，已在 README 和结构说明中明确。

### 质量与运行校验

- [x] `ruff check .` 通过。
- [x] `mypy src/auraclaw` 严格检查通过，共检查 26 个源码文件。
- [x] `pytest` 通过，共 6 项单元和 API 集成测试。
- [x] `python -m compileall src` 通过。
- [x] 真实 Uvicorn 服务启动成功。
- [x] 真实 HTTP 冒烟验证：健康检查 `200`、创建任务 `202`、查询任务 `200`。
- [x] README、AGENTS、系统架构与代码组织说明已同步。

### Git 交付校验

- [x] `.env`、`.history`、`.venv`、缓存和本地配置未进入暂存范围。
- [x] 阶段 commit 已完成。
- [x] 当前分支已 push 到 `origin`。

## 后续阶段

## 阶段 M1：事实核心与查询闭环

状态：已完成并推送。

### 范围

- 完成 Root Session 的 create、append message、request run、cancel、resume 命令。
- 实现 PostgreSQL Canonical Event Store、Snapshot、Command Dedup 与 Transactional Outbox。
- 实现 Outbox Relay、Task Projector、Checkpoint、event_id 去重、gap 检测和 Poison Queue。
- 实现 Task Read Model、ETag、`min_version`、`Retry-After` 与按租户重建命令。
- 本阶段不包含 Agent Runtime、Orchestrator、Child Session DAG、模型与工具执行。

### 功能与架构校验

- [x] `POST task → Canonical Event → Outbox Relay → Projection → GET task` 闭环已实现。
- [x] 相同 tenant、operation 与 idempotency key 返回同一 command result。
- [x] 内存契约测试中 100 个并发相同幂等请求只产生一个 Root Session。
- [x] Event、Aggregate Version、Command Result 与 Outbox 在同一 PostgreSQL 事务写入。
- [x] Snapshot 不替代 Event Log；缺失 Snapshot 时仍可从完整事件恢复。
- [x] 投影使用 event_id 去重，按 aggregate version 检测 gap，并原子更新 checkpoint。
- [x] 未知关键事件进入 Poison Queue；单条失败事件不会阻塞其他 Outbox 记录。
- [x] Read Model 可全量或按 tenant 删除重建，查询不扫描 Canonical Event Log。
- [x] 查询支持 ETag、条件请求、projection version、`min_version` 和 `Retry-After`。
- [x] `contracts` 与 `domain` 不导入 FastAPI、asyncpg 或其他基础设施实现。
- [x] 所有命令携带 tenant、command id、expected version、actor、correlation 与 causation。

### 数据、质量与运行校验

- [x] `0002_m1_fact_query.sql` 与对应 down migration 已提供。
- [x] PostgreSQL 配置兼容 `AURACLAW_DATABASE_URL` 与现有 `DB_*` 环境变量。
- [x] Ruff 通过。
- [x] Mypy strict 通过。
- [x] Pytest 12 项通过，包含 PostgreSQL 100 并发、Snapshot、Outbox 与重建测试。
- [x] `DB_NAME_DEV` 开发测试库迁移与 PostgreSQL 集成测试通过。
- [x] 真实 Uvicorn + PostgreSQL API 冒烟通过：create 202、query 200、ETag 304、幂等重试返回同一 Session。
- [x] `projection rebuild --tenant` 真实运行通过并处理 2 个 Canonical Events。
- [x] README、Python 后端结构、迁移与重建 Runbook 已同步。

### Git 交付校验

- [x] `.env`、`.history`、虚拟环境、缓存和 Secret 未进入暂存范围。
- [x] 阶段 commit 已完成。
- [x] 当前分支已 push 到 `origin`，远端提交与本地一致。

后续阶段按 [Managed Agent 系统架构总览](../architecture/system/00%20Managed%20Agent%20系统架构总览.md)
中的 M1～M6 建立独立小节。每一阶段应列出可观察功能，而不是只列内部类或文件；
Git 提交与推送是阶段完成条件，不是可选收尾动作。

## 阶段 M2：Managed Runtime 与控制平面

状态：已完成并推送。

### 范围

- 实现 Control State Store、Orchestrator、Agent Runtime Client/Harness 和 Model Gateway。
- 完整模型输出写 Canonical Event，Token Delta 只写 Runtime Event Bus。
- 本阶段不包含 M3 的真实工具审批、Sandbox、Artifact 与 Credential Proxy。

### 功能与架构校验

- [x] Runnable queue 的 enqueue/claim/reschedule 和 assignment 使用原子竞争操作。
- [x] Lease 支持 acquire、renew、release、expire，fencing token 在接管后单调递增。
- [x] 两个 Orchestrator 竞争同一任务只有一个取得执行权。
- [x] 旧 Runtime 的 Session 写入、checkpoint、heartbeat 和工具执行被 fencing 拒绝。
- [x] Orchestrator 实现 watch、schedule、provision、cancel、heartbeat 和 reconcile。
- [x] Runtime 提供 Session、Model、Tool、Runtime Event 四类端口。
- [x] Harness 校验 run_id、lease、budget、deadline、cancel 并外置 checkpoint。
- [x] 模型前、模型后、工具前、工具后四个故障注入点均可恢复到完成状态。
- [x] Model Gateway 是 Provider/Credential 边界，Runtime 不读取 Provider Secret。
- [x] 更换 Provider 不修改 Session、Orchestrator 或 Harness。
- [x] 完整模型输出进入 Canonical Event；Runtime Bus 故障不丢失最终结果。
- [x] Control State 不直接修改业务 Session 状态，Orchestrator 不做语义拆分。

### 数据、质量与运行校验

- [x] `0003_m2_managed_runtime.sql` 和 down migration 已提供。
- [x] 内存与 PostgreSQL Control State Store 均有自动化测试。
- [x] Ruff 通过。
- [x] Mypy strict 通过，共检查 35 个源码文件。
- [x] Pytest 21 项通过，包含 PostgreSQL 与四个 Runtime 故障注入点。
- [x] M2 验收证据已记录在本阶段清单与对应 Git 提交中。
- [x] README、Python 后端结构和运维边界已同步。

### Git 交付校验

- [x] `.env`、`.history`、虚拟环境、缓存和 Secret 未进入暂存范围。
- [x] 阶段 commit 已完成。
- [x] 当前分支已 push 到 `origin`，远端提交与本地一致。

## 阶段 M3：工具、Artifact 与审批安全闭环

状态：已完成并推送。

### 范围

- 实现 Tool Registry、JSON Schema 校验、权限/风险策略、Dispatcher、取消与 Result Normalizer。
- 实现本地 Hands Process/File Sandbox、Artifact Store、Approval Aggregate/Projection 和
  Credential Proxy/Vault 端口。
- Human Response 通过 Task Gateway 写入 Canonical Session Event，Runtime 在审批后恢复同一
  稳定工具调用。
- 本阶段不包含 M4 Child DAG、Coordinator/Worker/Reviewer 协作与评审。

### 功能与架构校验

- [x] `read-only`、`suggest-only`、`write-with-approval`、`write-autonomous`、
  `destructive/admin` 权限模型与风险等级已实现。
- [x] Tool 输入/输出在不可信边界执行严格 Schema 校验，未注册工具和非法参数不能到达 Hands。
- [x] write-with-approval 与 destructive/admin 工具没有有效审批时 fail closed。
- [x] Approval 绑定 tenant、Session、action digest 和 policy version；修改任一参数后旧审批失效。
- [x] Human Response 经 Task Gateway 鉴权、幂等和 expected version 校验写入 Canonical Event。
- [x] 审批写路径以 Canonical Session Event 重建 ApprovalRecord；投影滞后或缺行时不得 `Approval not found`。
- [x] 多进程 Projection Worker 同时投影 Task / Approval / Collaboration，不再只消费 Task 视图。
- [x] Approval View 仅由 Canonical Event 投影，可删除并从事件重建。
- [x] 相同 tenant/idempotency key 的并发工具调用只产生一次副作用；不同摘要复用 key 被拒绝。
- [x] Runtime 收到 approval_required 后写入审批事件并停止完成流程，批准后恢复同一工具调用。
- [x] Hands Process 不使用 Shell、不继承宿主环境，File Runtime 拒绝路径逃逸并支持取消。
- [x] Credential Proxy 只接受 credential_ref，校验 tenant/operation/expiry/revocation，真实 Secret
  不进入 Runtime、Sandbox、Session Event 或 Tool Result。
- [x] Tool 与外部响应在返回前递归脱敏，适配器错误被标准化且不泄漏内部细节。
- [x] 大型 Tool Result 在进入 Session 前写为 Artifact，Canonical Event 只保存 artifact_ref。
- [x] Artifact 支持 SHA-256、tenant 内对象去重、不可变版本、lineage、ACL 和短期受控下载。
- [x] domain/contracts 不依赖 FastAPI 或基础设施；Runtime 不直接写业务 Session 状态。

### 数据、质量与运行校验

- [x] `0004_m3_tool_artifact_approval.sql` 与 down migration 已提供。
- [x] PostgreSQL 增加 Tool Capability、Approval View、调用幂等、Credential Reference/Audit、
  Artifact Metadata/Access Audit，并保持 Canonical Event 为审批事实源。
- [x] Ruff 通过。
- [x] Mypy strict 通过，共检查 42 个源码文件。
- [x] Pytest 31 项通过，覆盖内存、API、Runtime 恢复与 PostgreSQL Approval Projection。
- [x] 真实 Uvicorn API 冒烟通过，审批响应路由已在测试客户端验证。
- [x] M3 验收证据已记录在本阶段清单与对应 Git 提交中。
- [x] README、Python 后端结构、迁移与安全边界说明已同步。

### Git 交付校验

- [x] `.env`、`.history`、虚拟环境、缓存和 Secret 未进入暂存范围。
- [x] 阶段 commit 已完成。
- [x] 当前分支已 push 到 `origin`，远端提交与本地一致。

## 阶段 M4：多 Agent 协作与评审

状态：已完成并推送。

### 范围

- 实现 Root/Child Collaboration Aggregate、DAG 校验、委派、交接、结果发布与 Join。
- 实现 Collaboration Projection、runnable 计算和 Child 查询。
- 实现 Coordinator、Worker、Reviewer 角色边界、Output Contract 和独立证据评审。
- 本阶段不包含 M5 Streaming 重放、Result Delivery Worker 与外部通知可靠投递。

### 功能与架构校验

- [x] Root 与 Child 使用同一 Canonical Event Store，Projection 不是业务事实源。
- [x] Child、dependency、delegate、handoff、publish result、review 与 join 均追加 Canonical Events。
- [x] DAG 拒绝环、自依赖、跨 Root/tenant 引用，并限制深度、单父宽度、Child 总数和预算。
- [x] 稳定 `task_key` 保证 Coordinator 重启或重试不重复创建相同 Child。
- [x] Collaboration Projection 可删除重建，并根据依赖完成状态计算 runnable Child。
- [x] Orchestrator 只消费 runnable 资源需求；Coordinator 保持语义分解和 Join 所有权。
- [x] Worker Result 必须通过版本化 Output Contract 后才进入 completed。
- [x] Worker 只能写自己的 Child，不能写 Root；Owner 不匹配时 fail closed。
- [x] Reviewer 使用独立 Review Session 和 Context，决策必须包含证据，不能覆盖 Worker Artifact。
- [x] Root Result 可追溯 Child Result、Review Evidence 和 Artifact lineage。
- [x] 串行、并行、树形和混合四类 DAG 端到端场景通过。
- [x] domain/contracts 不依赖 FastAPI 或基础设施；所有写入携带 tenant、command、expected
  version、actor、correlation 与 causation context。

### 数据、质量与运行校验

- [x] `0005_m4_collaboration_review.sql` 与 down migration 已提供。
- [x] PostgreSQL Collaboration View、Root/runnable 索引和 task_key 唯一约束已实现。
- [x] Ruff 通过。
- [x] Mypy strict 通过，共检查 46 个源码文件。
- [x] Pytest 38 项通过，覆盖 7 项 M4 单元/集成场景与 M1～M3 完整回归。
- [x] 真实 PostgreSQL Collaboration Projection 写入、查询和重建通过。
- [x] 真实 Uvicorn API 冒烟通过，Child 查询路由可用。
- [x] M4 验收证据已记录在本阶段清单与对应 Git 提交中。
- [x] README、Python 后端结构、迁移与角色安全边界已同步。

### Git 交付校验

- [x] `.env`、`.history`、虚拟环境、缓存和 Secret 未进入暂存范围。
- [x] 阶段 commit 已完成。
- [x] 当前分支已 push 到 `origin`，远端提交与本地一致。

## 阶段 M5：实时体验与可靠交付

状态：已完成并推送。

### 范围

- 实现 Runtime Event Producer SDK、Kafka Producer/Consumer、sequence、visibility、大小限制与
  Token Delta 合并。
- 实现 Streaming Gateway SSE、tenant 订阅授权、Last-Event-ID、短期重放、过期 Query 回退、
  有界背压和慢连接隔离。
- 实现 Transactional Delivery Outbox、持久 Delivery Job、Webhook/Parent Session Sink、签名、
  重试、Circuit Breaker、DLQ、Attempt History 与 Manual Redelivery。
- Delivery 状态回写 Canonical Session Event，并进入 Task/Result Query Projection。
- 本阶段不包含 M6 的完整 Trace/Metrics/Audit Store、容量压测、SLO/告警与灰度 Runbook。

### 功能与架构校验

- [x] Runtime Event SDK 自动分配/校验 Stream sequence，拦截 secret visibility、敏感字段和超限
  Payload，并按阈值合并 Token Delta。
- [x] Kafka 使用 Root Session 分区键和服务级 Consumer Group；Kafka Offset 不暴露给客户端。
- [x] Streaming Gateway 只桥接实时展示，不创建、取消、审批或调度 Session。
- [x] 未授权 tenant 无法通过猜测 Session ID 订阅；公开游标使用 `session_id:sequence`。
- [x] 保留期内断线重连补齐事件；游标过期返回 `stream.reset` 与 Task Query URL。
- [x] 每连接队列有界，非关键进度允许采样，关键事件不会静默丢弃，慢网页不阻塞 Runtime。
- [x] Gateway/Kafka 不可用时 API 和 Canonical Session 写入继续工作；关闭 SSE 不等于取消任务。
- [x] 可交付终态在 Canonical append 事务内写入独立 delivery Outbox；Delivery Worker 不扫描 Session
  状态推测完成。
- [x] 稳定 `delivery_id` 与 `(event_id, sink_id)` 唯一约束保证重复 Outbox 不产生重复业务 Job。
- [x] PostgreSQL Job 保存 retry_wait、next_attempt_at 与 Attempt History，Worker/Store 重启后继续投递。
- [x] Webhook 使用 timestamp、HMAC-SHA256、稳定 Idempotency-Key；timeout/429/5xx 重试，耗尽进入
  DLQ，永久 4xx 直接失败。
- [x] Parent Session Sink 通过 Canonical Event 交付；Manual Redelivery 创建新 attempt，不覆盖历史。
- [x] Job/Sink 只保存 credential_ref 和受控目标引用，不保存 Secret；Runtime Event/响应摘要已脱敏。
- [x] `delivery.*` 状态回写 Session 并投影到 Task/Result Query，Kafka 事件丢失不影响最终交付。
- [x] `domain`/`contracts` 不导入 FastAPI 或基础设施；Runtime 不直接修改业务 Session 状态。

### 数据、质量与运行校验

- [x] `0006_m5_streaming_delivery.sql` 与 down migration 已提供，包含 Sink、Job、Attempt、恢复索引、
  唯一约束和 Task Delivery 查询字段。
- [x] Ruff 通过。
- [x] Mypy strict 通过，共检查 52 个源码文件。
- [x] 隔离完整 Pytest 回归 39 项通过、6 项外部集成按配置跳过；M5 PostgreSQL/Kafka 真实集成各
  1 项随后使用 `.env` 配置通过。
- [x] 真实 Kafka Producer → Consumer Group → Replay Buffer 往返通过。
- [x] 真实 PostgreSQL Delivery Job 去重、retry_wait、Store 重启恢复、成功 ACK 与 Attempt History
  通过。
- [x] 真实 Task API 回归通过；Streaming 启动失败有 10 秒超时并降级，不阻塞 Canonical API。
- [x] M5 验收证据已记录在本阶段清单与对应 Git 提交中。
- [x] README、Python 后端结构、Kafka 配置、迁移与实时/投递边界已同步。

### Git 交付校验

- [x] `.env`、`.history`、虚拟环境、缓存和 Secret 未进入暂存范围。
- [x] 阶段 commit 已完成。
- [x] 当前分支已 push 到 `origin`，远端提交与本地一致。

## 阶段 M6：可靠性、观测与发布门禁

状态：已完成并推送。

### 范围

- 实现统一 Trace Context、结构化日志、核心 Metrics、Audit Event Store、主动告警和 Session
  Timeline。
- 实现 Telemetry 保留、Artifact GC、Projection Poison/Delivery DLQ 查看与人工重投工具。
- 将故障注入、架构完成标准、Secret 扫描、SLO、Runbook 和灰度回滚纳入发布门禁。
- 本阶段不实现外部商业 Trace/Metrics/Alert 产品适配器；它们通过观测端口接入。

### 功能与架构校验

- [x] Trace/Audit/Metrics/Alert 统一携带 trace、tenant、root/session、run、event、command、tool、
  runtime、delivery 和 approval 关联字段。
- [x] Task Gateway 回传 W3C `traceparent`；结构化日志和观测写入失败不阻断 Canonical API。
- [x] Audit Event Store 持久记录动作、主体、结果、资源引用和受控 Payload 引用，不保存真实 Secret。
- [x] Session Timeline 合并 Canonical Event、Trace、Audit 和 Alert，且 tenant 猜测 Session 返回 404。
- [x] Projection lag、lease lost、unknown side effect 和 delivery DLQ 均有主动告警规则。
- [x] Metric、Trace、Audit、Alert 使用独立保留策略；Artifact GC 不删除仍被引用的 Hash 对象。
- [x] Outbox/Projection Poison/Delivery DLQ 可查看；重投受 tenant 和稳定 ID 限制且不覆盖历史。
- [x] 数据库短断、Projection 落后、Runtime 崩溃、Lease 丢失、Tool unknown 和 Delivery 5xx 均有
  自动化恢复或 fail-safe 覆盖。
- [x] 架构总览的实例恢复、幂等、DAG 可追踪、Projection 重建、SSE 重连、Delivery 恢复、审批
  绑定和 Secret 隔离完成标准全部映射到自动化回归。
- [x] Canonical Event 仍是业务事实源；Telemetry 与 Runtime Event 不直接修改 Session 状态。
- [x] `domain`/`contracts` 不依赖 FastAPI 或基础设施，运维工具不跨边界写业务事实。

### 数据、质量与运行校验

- [x] `0007_m6_observability_reliability.sql` 与 down migration 已提供，包含 Trace、Metric、Audit、
  Alert、索引和 Retention Policy。
- [x] Ruff 通过。
- [x] Mypy strict 通过，共检查 57 个源码文件。
- [x] M6 专项 8 项通过（含自动观测、200 并发容量与真实 PostgreSQL）；全量 Pytest 53 项通过。
- [x] Release gate 的交付物、架构边界和 Secret 扫描通过，真实 Secret 零命中。
- [x] Timeline API 冒烟通过：同 tenant 返回 200、跨 tenant 返回 404、Trace Context 正常回传。
- [x] M6 验收证据已记录在本阶段清单与对应 Git 提交中。
- [x] [M6 运维与灰度发布 Runbook](../operations/observability-and-canary.md)、README、迁移、SLO、
  告警和回滚说明已同步。

### Git 交付校验

- [x] `.env`、`.history`、虚拟环境、缓存和 Secret 未进入暂存范围。
- [x] 阶段 commit 已完成。
- [x] 当前分支已 push 到 `origin`，远端提交与本地一致。

## 阶段 M7：前端测试与监控工作台

状态：已完成并推送。仓库后续已移除 `frontend/`，本阶段清单保留为历史记录。

### 范围

- 提供与 Python 后端解耦、可静态部署的纯前端 SPA。
- 覆盖任务测试、Runtime SSE、父子 Session、审批响应、Timeline、Metrics 与安全请求历史。
- 本阶段不新增后端 API，不读取数据库/Kafka，不提供高风险运维写操作。

### 功能与架构校验

- [x] API Base URL、tenant、actor 和 correlation 可配置，live/ready 状态可视化。
- [x] 创建、打开、追加消息、运行、取消、恢复和结果查询覆盖现有 Task API。
- [x] 写请求生成唯一 `Idempotency-Key` 并携带当前 `X-Expected-Version`；冲突不会静默覆盖。
- [x] Task/Result 查询适配 `202`、`Retry-After`、`ETag` 和 `304`。
- [x] 基于 `fetch` 的 SSE 携带身份头与 `Last-Event-ID`，支持断流退避重连和 `stream.reset` 提示。
- [x] Runtime Event 明确标注为非权威实时信息；最终状态只读取 Task/Result API。
- [x] Child Session 可查看跳转；已知 Approval ID 可审批或拒绝且遵守并发约束。
- [x] Timeline 合并展示 Canonical Event、Trace、Audit、Alert，并支持 kind/关联字段过滤。
- [x] Metrics 支持名称过滤、趋势展示和 M6 重点异常指标标识，不创建前端权威告警。
- [x] 请求历史仅保留在当前浏览器会话，复制 curl 会移除敏感 Header 并递归脱敏 Payload。
- [x] 取消、恢复和审批展示 tenant/session 并二次确认；生产部署默认只读。
- [x] 前端不依赖 `auraclaw.*` 内部模块，不修改 Canonical Event、Projection 或 Telemetry。

### 数据、质量与运行校验

- [x] 前端不引入 D1/R2、数据库、BFF、认证代理或持久业务数据。
- [x] SSE 分片、多行数据、幂等键、脱敏、Timeline 过滤和 Metric 聚合有自动化单元测试。
- [x] 服务端渲染冒烟测试验证产品壳、元数据和 starter 清理。
- [x] ESLint 通过。
- [x] Vinext 生产构建通过。
- [x] 浏览器端冒烟覆盖“加载工作台 → 健康检查 → 创建任务 → 自动加载 Projection → 查看请求历史”。
- [x] Python 后端 Ruff、Mypy、41 项 Unit Pytest 与 Release Gate 通过；全量回归 52/53 通过，既有
  M2 远程 PostgreSQL 测试因 5 秒测试租约在网络往返期间过期失败，已在 M7 验收记录中说明。
- [x] 根 README 已同步启动、构建、部署、CORS、SSE 和安全边界。
- [x] M7 验收记录已覆盖自动化、浏览器冒烟、后端回归和已知外部时延限制。
- [x] Secret、缓存、虚拟环境、构建产物和本地预览脚本未进入交付范围。

### Git 交付校验

- [x] 阶段变更已作为一个 intentional commit 提交。
- [x] 当前分支已 push 到 `origin`，远端提交与本地一致。

## 阶段 M7.1：协议测试功能页增量

状态：已完成并推送。

### 范围

- 在既有前端工作台新增“智能问答”和“创建任务”两个独立测试入口。
- 智能问答用于验证 Runtime SSE 流式体验与权威 Result 一致性。
- 创建任务用于验证 Query、Task View、Result 以及 HTTP 轮询和缓存语义。

### 功能与架构校验

- [x] 两个页面均有独立导航与 Hash 路由，可刷新后恢复当前测试页。
- [x] 智能问答合并 `model.output.delta`，按事件 ID 去重，并携带 `Last-Event-ID` 重连。
- [x] 智能问答支持停止/取消、原始事件查看、Result 自动查询和流式内容一致性核对。
- [x] 已完成 Session 的追问会创建新 Session，并明确展示后端终态约束，不伪造同会话续聊。
- [x] 创建任务展示脱敏 Query 请求、创建响应、Task View 与 Result 三栏协议数据。
- [x] Result 查询支持 `Retry-After`、`ETag`、`If-None-Match` 与 `304 Not Modified`。
- [x] 页面复用现有公开 HTTP/SSE API，不新增后端事实源或业务状态。

### 数据、质量与运行校验

- [x] 协议辅助函数已覆盖增量提取、事件去重、Result 文本和 `Retry-After` 单元测试。
- [x] 服务端渲染冒烟覆盖两个新页面、导航和产品元数据。
- [x] ESLint、Vinext 生产构建和 7 项 Node 测试通过。
- [x] 浏览器端验证覆盖 Streaming 增量合并、最终 Result 一致性、Query 创建、自动轮询和 ETag 304 命中。
- [x] 浏览器控制台无错误。
- [x] README、前端说明和 M7 验收记录已同步。
- [x] 临时浏览器预览脚本和构建产物未进入交付范围。

### Git 交付校验

- [x] 增量变更已作为一个 intentional commit 提交。
- [x] 当前分支已 push 到 `origin`，远端提交与本地一致。

## 阶段 M7.2：真实 Streaming 联调修复

状态：已完成并推送。

### 功能与架构校验

- [x] 本地前端来源的 CORS 预检允许公开 API 所需 method/header，并暴露缓存与 trace Header。
- [x] 额外跨域来源必须通过 `AURACLAW_CORS_ALLOW_ORIGINS` 显式配置。
- [x] 开发模式启动确定性 Runtime worker，并产生多个 `model.output.delta`。
- [x] 开发 worker 保持 Runnable Queue → Orchestrator → fenced Agent Harness 边界。
- [x] Runtime 最终输出写 Canonical Event，Task/Result 仍是权威完成来源。
- [x] M7.2 首版开发 Runtime 限定纯内存配置；真实基础设施兼容性在 M7.3 补齐。

### 质量与运行校验

- [x] 自动化覆盖本地 CORS 预检、任务完成、多个 delta 回放和流/Result 一致性。
- [x] 真实浏览器验证覆盖健康检查、跨域创建、SSE 增量显示和最终 Result 核对。
- [x] 浏览器中间态收到 56 个字符，最终收到 162 个字符且控制台无错误。
- [x] Ruff、Mypy、前后端自动化与生产构建通过。
- [x] README、前端说明、M7 验收记录与 GitHub issue 已同步根因、启动方式和验证证据。

### Git 交付校验

- [x] 修复已作为一个 intentional commit 提交。
- [x] 当前分支已 push 到 `origin`，远端提交与本地一致。

## 阶段 M7.3：真实基础设施 Streaming 补充修复

状态：已完成并推送。

### 功能与架构校验

- [x] 开发 Runtime 兼容内存与 PostgreSQL 存储，不再因 Kafka 配置而静默停用。
- [x] 确定性测试 delta 直接进入同进程 Replay Bus，不受外部 Kafka 可用性影响。
- [x] 开发 Runtime 仍经 Runnable Queue、Orchestrator、fencing 与 Agent Harness 执行。
- [x] 智能问答持续轮询 Task 直至终态，再读取权威 Result。
- [x] SSE 无游标首次订阅回放当前保留窗口；游标续传和过期 reset 语义保持不变。
- [x] Readiness 暴露真实存储、Runtime Event backend、总线和开发 Runtime 状态。

### 质量与运行校验

- [x] 自动化新增外部开发 backend 启用条件及无游标首次回放覆盖。
- [x] Ruff、Mypy、49 项 Unit/Task API 测试、ESLint、生产构建和 7 项前端测试通过。
- [x] 使用真实 PostgreSQL/Kafka 配置复测原 pending Session，已自动完成并生成 Result。
- [x] 浏览器冷连接已完成 Session，完整回放 162 字符并与 Result 核对一致。
- [x] README、前端说明、M7 验收记录和 GitHub issue 已同步。

### Git 交付校验

- [x] 补充修复已作为一个 intentional commit 提交。
- [x] 当前分支已 push 到 `origin`，远端提交与本地一致。

## 阶段 M7.4：多轮 Session 修复

状态：已完成并通过全量回归，待本次提交同步至远端。

依据：GitHub Issue #9。

### 功能与架构校验

- [x] Root Session 与 Run 生命周期已分离，Run 终态后 Session 回到 `ready`。
- [x] 连续三轮问答保持同一 `session_id`，每轮生成不同 `run_id`。
- [x] Task View 分别暴露 `status` 与 `run_status`，Result 明确关联最新 `run_id`。
- [x] 新 Run 会清空上一轮最新结果投影，旧 Run Delivery 不覆盖当前 Run 状态。
- [x] 模型上下文按 Canonical Event 顺序包含完整 user/assistant 历史。
- [x] Runtime Event sequence 在同一 Session 的多个 Run 间单调递增。
- [x] 完成的 Runtime assignment 释放 Session 租约，等待审批时保持恢复语义。
- [x] `session.closed` 提供显式终态，关闭后拒绝追加消息和新 Run。
- [x] Child Session、Review、Join、tenant、幂等、expected version 与 fencing 语义保持。

### 数据、兼容与安全校验

- [x] `0008_multi_run_sessions.sql` 增加 `run_status` 并将历史 Root 隐式终态迁移为 `ready`。
- [x] down migration 可移除新增投影字段；Canonical Event 无破坏性改写。
- [x] 旧 Snapshot 缺少 `run_status` 时可恢复，并将历史 Root Run 终态解释为 `ready`。
- [x] 新增写接口继续携带 tenant、command id、expected version、actor、correlation 与 causation。
- [x] `.env`、Secret、缓存、虚拟环境与构建产物未进入交付范围。

### 质量与运行校验

- [x] 领域状态机、API 取消/关闭、三轮 Runtime、上下文、Result 和 SSE 序号均有自动化回归。
- [x] 全量 Pytest 通过；仅保留既有 Starlette/httpx2 弃用警告。
- [x] Ruff 与 Mypy 通过。
- [x] 前端 ESLint、生产构建和 7 项 Node 测试通过。
- [x] README、架构源文档、前端说明和 M7 验收记录已同步。

### Git 交付校验

- [x] 本阶段变更作为一个 intentional commit 提交。
- [x] 当前分支 push 到 `origin`，远端提交与本地一致。

## 阶段 M8：Model Gateway 与 Runtime Event Bus 生产接入

状态：已完成并推送。

依据：GitHub Issue #10。

### 功能与架构校验

- [x] Settings、`.env.example` 与 README 只使用 Provider 中立的 `AURACLAW_MODEL_*` 契约。
- [x] OpenAI-compatible Adapter 支持流式 Chat Completions、usage 与 tool calls。
- [x] 鉴权失败、限流/配额耗尽、超时和 Provider 故障映射为稳定应用错误。
- [x] Model Gateway 是唯一 CredentialResolver 调用方，Harness、Session 与 Orchestrator 不接触 Secret。
- [x] development Runtime 继续独占确定性 Model Client 与进程内 Replay Bus 路径。
- [x] 非开发环境启动同进程 MVP production Worker，并按 storage backend 选择 Control Store。
- [x] production Harness 经 Runtime Event Producer SDK 发布到 Kafka 或内存降级总线。
- [x] Kafka producer 与 Streaming Ingestor 共同参与 Runtime Event Bus 就绪判定。
- [x] Runtime Event 失败不影响 Canonical model output 与 run completion。
- [x] 同 Session 多 Run 的公开 sequence、SSE cursor 与 Replay 语义保持不变。

### 配置、安全与可观测性校验

- [x] 本地 gitignored `.env` 已将旧厂商键机械迁移为 `AURACLAW_MODEL_*`，值未回显。
- [x] API Key 不进入 Harness、Canonical Event、Runtime Event payload 或日志。
- [x] `/health/ready` 报告 Model Gateway、producer、ingestor、bus 与两类 executor 状态。
- [x] Provider 日志仅记录逻辑 Provider、模型、耗时和 usage，不记录凭证。
- [x] 多进程 production Worker 拆分、Vault、多模型路由和 fallback 明确留给后续阶段。

### 质量与运行校验

- [x] Provider 配置、流式响应、tool calls、usage、错误映射和生产 Worker 有自动化测试。
- [x] Kafka 集成测试通过，producer→topic→ingestor→Replay Bus 链路可用。
- [x] 全量 Pytest 通过，共 65 项。
- [x] Ruff、Mypy strict 与 import-linter 通过。
- [x] README、环境配置示例、[代码组织与部署映射](../architecture/code-organization.md)与 M8 验收记录已同步。

### Git 交付校验

- [x] `.env`、Secret、缓存、虚拟环境和无关用户改动未进入暂存范围。
- [x] 本阶段变更作为一个 intentional commit 提交。
- [x] 当前分支 push 到 `origin`，远端提交与本地一致。

## 阶段 M8.1：Runtime 环境逻辑统一

状态：已完成并推送。

### 功能与架构校验

- [x] 删除 development/production Worker 双工厂，所有部署只使用 `build_runtime_worker()`。
- [x] 所有部署均经过 Model Gateway、Agent Harness 和 Runtime Event Producer SDK。
- [x] PostgreSQL/内存、Kafka/内存、模型端点和 CORS 仅由当前 `.env` 资源配置决定。
- [x] 应用不再读取 `AURACLAW_ENV` 决定业务或 Runtime 行为。
- [x] 数据库配置统一为 `DB_NAME`，不再识别带开发/生产标签的拆分键。
- [x] Runtime 开关与轮询配置统一为 `AURACLAW_RUNTIME_ENABLED` 和
  `AURACLAW_RUNTIME_POLL_INTERVAL`。
- [x] `/health/ready` 统一报告 `runtime_worker`，不再暴露开发/生产执行器分支。
- [x] 确定性模型只存在于自动化测试替身，不进入运行时 composition。

### 质量与交付校验

- [x] 统一 Runtime 的三轮 Session、Streaming/Result 一致性与资源替换有自动化覆盖。
- [x] 全量 66 项 Pytest、Ruff、Mypy strict 与 import-linter 通过。
- [x] README、前端说明、Python 后端结构、架构代码梳理和配置样例已同步。
- [x] `.env`、Secret、缓存、虚拟环境和无关改动不进入暂存范围。
- [x] 本阶段作为一个 intentional commit 提交并 push 到当前分支。

## 阶段 R1：Managed Agent 模块边界重构

状态：已完成并通过全量回归，待本次提交同步至远端。

依据：[代码组织与部署映射](../architecture/code-organization.md)与 Issue #8。

### 范围与非目标

- 分 R1.1～R1.4 重组 Projection、bounded contexts、composition 和 ports，并增加 import-linter 门禁。
- 建立「架构组件 → 主归属包 → 进程入口」映射，不要求目录与部署单元 1:1。
- 保持 `/v1/*`、现有 CLI、Canonical Event、Projection 和运行语义不变。
- 不新增 `auraclaw runtime run`、`auraclaw delivery run`；生产 Worker 生命周期由后续 feature issue 跟踪。
- 临时 re-export shim 只跨堆叠 PR 保留，最迟在 R1.4 删除；若发现外部 Python 消费者，先另定弃用策略。

### R1.1：Projection 内聚与 PostgreSQL 拆分

- [x] Event Store、Projection Store、Delivery Store 与技术适配器按职责拆分。
- [x] 原 `infrastructure/postgres.py` 按职责拆分，相关 PostgreSQL Event/Projection 文件均不超过 300 行。
- [x] Projection 规则不依赖具体 infrastructure；事实源、幂等、gap、checkpoint 与重建语义不变。
- [x] 数据库 Schema/迁移不涉及：本阶段只移动 Python 模块，现有 migration 与回滚路径不变。
- [x] Ruff、Mypy、56 项 Pytest、PostgreSQL/Kafka 集成测试与 Projection CLI 契约通过。
- [x] 文档与暂存范围已审查；本 R1 作为一个完整 intentional commit 交付。

### R1.2：bounded context 分包

- [x] `application/` 职责迁入 session、gateways、control、action、delivery 与 observability 主归属包。
- [x] API、Canonical Event、Projection、tenant、权限、幂等、并发和敏感信息行为不变。
- [x] 组件/包/进程入口映射及旧路径迁移表已更新。
- [x] Ruff、Mypy、Pytest 与关键 API/Streaming 回归通过。
- [x] 文档与暂存范围已审查；不保留旧路径 shim。

### R1.3：composition 剥离

- [x] entrypoint → composition → api/gateways/adapters 单向装配成立。
- [x] `api`、gateways 和业务包不导入 composition；infrastructure 不导入 api/gateways/composition。
- [x] FastAPI dependency override、lifespan、Development Runtime、Projection/Operations CLI 行为不变。
- [x] 真实 Uvicorn live/ready 返回 200；Streaming/Result 自动化回归与现有 CLI 冒烟通过。
- [x] Ruff、Mypy、Pytest、文档与暂存范围已通过门禁。

### R1.4：端口分组与依赖门禁

- [x] session、projection、control、runtime、action、delivery ports 归属清晰。
- [x] import-linter 使用 8 条小型 forbidden contract，且 `uv run lint-imports` 通过。
- [x] 唯一写入方由 56 项契约/集成测试验证，不以 import lint 代替行为验证。
- [x] 临时旧路径 shim 已删除，仓库内旧 import 为零。
- [x] Ruff、Mypy、Pytest 与关键冒烟通过；安全语义未变，数据库迁移不适用，可按提交整体回滚。
- [x] README、AGENTS、架构说明、Python 后端结构与运维说明已同步。
- [x] 最终暂存范围不含本地配置、Secret、缓存或虚拟环境；intentional commit 将 push 到 `origin`。

## 阶段 S0：生产服务边界 ADR 与映射基线

状态：已完成并推送（Issue #12，commit `de0c326`）。

依据：[Issue #12](https://github.com/sushaofei/AuraClaw/issues/12) 与
[ADR-001](../architecture/decisions/ADR-001-production-service-boundaries.md)。

### 范围与非目标

- [x] 冻结 12 服务生产拓扑、唯一写入者、通信方式、信任域与目标入口。
- [x] 固定 Query/Runnable、MCP 2025-11-25、Policy/Credential、Artifact/SeaweedFS、Ops Admin 生产方案。
- [x] 建立 S0–S5 阶段门禁、迁移顺序、兼容与回滚原则。
- [x] 本阶段只修改文档，不新增入口、数据库迁移或运行行为。
- [x] 用户已有 `.env.example`、`config.py`、SeaweedFS 配置测试改动不纳入 S0 暂存范围。

### 架构与安全校验

- [x] `session` 是 Canonical Event Log 唯一数据库写入者。
- [x] Projection、Control、Hands Invocation、Policy、Credential Audit、Artifact Metadata、Delivery 各有唯一写入者。
- [x] Runtime 不读 Event/Control/Projection 数据库，不读取 Provider/External Secret，不直连第三方 MCP。
- [x] Task Query 固定使用 `task_query_ro`；Orchestrator 固定消费 Projection Runnable Outbox/Feed。
- [x] Lease Assertion、Fencing、Checkpoint、Hands Invocation、Runtime cursor 的持久与恢复责任已定义。
- [x] Policy fail-closed、Credential Proxy 不返回 Secret、SeaweedFS 最小权限与 Artifact ACL 已定义。
- [x] 公开 `/v1/*`、Canonical Event、Projection 业务语义保持兼容。

### 文档与质量校验

- [x] 系统架构总览部署边界更新为 12 服务。
- [x] 架构代码梳理增加“组件 → 包 → 当前装配 → 目标入口”映射和当前缺口。
- [x] ADR 记录同步/异步契约、数据库身份矩阵、兼容、迁移、回滚和被否决方案。
- [x] 文档链接、Markdown 结构、服务计数和术语一致性检查通过；总览与 ADR 均为 12 个服务。
- [x] `uv run ruff check .`、`uv run mypy src/auraclaw`、`uv run pytest`、`uv run lint-imports` 通过。

### Git 交付校验

- [x] 暂存范围限定为 S0 文档，不含用户已有代码/配置改动、`.env`、Secret、缓存或虚拟环境。
- [x] S0 以单个 intentional commit 收口。
- [x] S0 commit 按阶段门禁 push 到当前分支 `origin`。

## 阶段 S1：跨服务契约与客户端基础

状态：已完成，待本阶段提交并推送。

### 契约与功能校验

- [x] `contracts/` 定义 Session、Control、Model、Policy、Credential、Artifact、Admin DTO、版本和稳定错误码。
- [x] 每个内部写入携带 tenant、service identity、command/operation id、actor、correlation、causation。
- [x] Session append 支持 expected version、operation/event allowlist、签名 Lease Assertion 和 Fencing Token。
- [x] Control API 支持 Runtime register/heartbeat/drain、Assignment、Lease、Checkpoint、Cancel、Validate Lease。
- [x] Runtime↔Hands MCP 固定 2025-11-25，支持 initialize、tools/list、tools/call 和 `_meta.auraclaw`。
- [x] Hands 使用稳定 `tool_invocation_id`，不以 JSON-RPC request id 代替业务幂等键。
- [x] Policy/Approval、Credential Proxy、Artifact/SeaweedFS、owner Admin API 契约完整。
- [x] in-process adapter 与 HTTP/MCP adapter 使用同一套 contract tests。

### 架构、数据与安全校验

- [x] 业务包不 import composition；API/gateway 不选择具体基础设施适配器；Runtime 与 Action Hands 双向依赖红线已加入 import-linter。
- [x] Runtime、Hands、Delivery 不接收明文 Secret；模型/工具可控字段不能伪造权威上下文。
- [x] Hands Invocation Store、Policy、Credential Audit、Artifact Metadata 的 owner、schema 和迁移/回滚顺序已设计；物理 schema/migration 按 Issue 计划在 S3 实现，S1 不适用。
- [x] 新旧 Adapter 可按配置回滚，公开 API 与 Canonical/Projection 语义不变。

### 质量与 Git 交付校验

- [x] Ruff、Mypy、77 项 Pytest、10 条 import-linter 与 8 项新增 contract/permission tests 通过。
- [x] 暂存范围不含 `.env`、Secret、用户无关改动、缓存或虚拟环境。
- [x] S1 作为一个 intentional commit 提交并 push 到 `origin`。

## 阶段 S2：12 个入口与本地多进程拓扑

状态：已完成。

### 入口与部署校验

- [x] `api/session/projection/orchestrator/runtime/model-gateway/hands/policy/credential-proxy/artifact/streaming/delivery` 入口可独立启动。
- [x] 每个入口只装配自身对象图、配置和 Worker，不隐式启动其他生产服务。
- [x] `auraclaw serve` 明确为 development combined profile。
- [x] Docker Compose/Ingress 可一键启动 12 进程，并支持外部 SeaweedFS endpoint。
- [x] 每服务具有 live/ready、SIGTERM 优雅停机、硬依赖 readiness 与配置失败语义。
- [x] `.env.example` 只含无密钥占位符；SeaweedFS/Vault/DB/MCP 配置日志不泄漏值。

### 质量与 Git 交付校验

- [x] 12 入口 CLI、live/ready、信号与配置 contract tests 通过。
- [x] Ruff、Mypy（114 个源码文件）、85 项 Pytest、10 条 import-linter、Compose 配置校验与 Docker Policy 服务 SIGTERM smoke 通过。
- [x] S2 作为一个 intentional commit 提交并 push 到 `origin`。

## 阶段 S3：唯一写入者与信任域迁移

状态：已完成；代码与 PostgreSQL/SeaweedFS/Vault 外部门禁全部通过并以 intentional commit/push 收口。

### 数据与调用路径校验

- [x] Task API、Runtime、Orchestrator、Policy、Hands、Delivery 的 Canonical 写路径只经 Session Client；未写事实的服务不持有 Session DB。
- [x] 非 Session 服务没有 Event Store 写凭证或 direct append 装配。
- [x] Orchestrator 是 Control Store 唯一写入者；Runtime 只使用 Control Client。
- [x] Projection/Delivery 通过 Session Outbox claim/feed 消费并 ack，不直接更新 Session schema。
- [x] Model Provider/Secret 迁入 Model Gateway；Runtime 仅保留 Model Client。
- [x] Tool/Sandbox/Invocation Store 迁入 Action Hands；Runtime 仅保留 MCP Client。
- [x] Policy Engine/Approval control 迁入 Policy；Credential/Vault adapter 迁入 Credential Proxy。
- [x] Artifact Metadata/API 迁入 Artifact Service，SeaweedFS S3 adapter 接线；生产禁用本地 Artifact 目录。
- [x] Operations CLI/Task Ops 只调用 owner Admin API，不跨 schema redrive/retention。

### 权限、安全与迁移校验

- [x] 独立 DB roles/schema grants、`task_query_ro`、workload identity 与网络策略已落地。
- [x] 越权 SQL、跨 tenant、伪造 actor、旧 fencing、过期 approval、scope 越权、Artifact ACL 均被拒绝。
- [x] Runtime 不能直连第三方 MCP，Hands/Delivery 不能读取 Vault Secret，Artifact 消费者没有 SeaweedFS 管理凭证。
- [x] 数据迁移、回滚和 N/N-1 兼容测试通过；`0009` 仅 expand 并保留旧表，owner 切换窗口冻结
  Tool/Credential 配置写、先排空旧 owner 再开放新 owner 写入，因此不做跨 owner 双写。

### 质量与 Git 交付校验

- [x] Ruff、Mypy、106 项 Pytest、10 条 import-linter、PostgreSQL grants、SeaweedFS/Vault 集成测试通过。
- [x] S3 作为一个 intentional commit 提交并 push 到 `origin`。

## 阶段 S4：横向扩展、游标与恢复

状态：已完成；多副本共享状态、顺序消费、无黏性恢复、外部资源与阶段总门禁均通过。

### 多副本与恢复校验

- [x] Orchestrator 2+ 副本 claim/lease/reconcile 互斥，过期 Assignment 可恢复。
- [x] Agent Runtime 2+ 副本无黏性接管，Checkpoint/Session 恢复且旧 fencing 失效。
- [x] 生产调度不扫描完整 Event Log，Runnable Outbox/Feed 按 source version 幂等驱动。
- [x] Hands MCP Gateway/Executor 2+ 副本使用持久 Invocation Store；首次调用以 PostgreSQL
  `ON CONFLICT` 原子选主，重复/断连调用返回持久结果或明确的 operator recovery 状态。
- [x] Projection/Delivery 多 Worker 保持同 Session 顺序、幂等、Retry/DLQ/Manual Redelivery。
- [x] Streaming 使用 PostgreSQL 共享 Connection Registry/Replay Store；双实例序列分配、过期游标、
  handoff 与 live cursor 连续性测试通过，Kafka ingestion 在共享边界重分配公共 cursor。
- [x] Policy 共享 active bundle/decision，Model 共享调用幂等与小时 token 配额，Credential 每次权威读取
  撤销状态并持久化 usage 终态，Artifact 使用共享 finalize/scan/GC claim。
- [x] SeaweedFS 真实 multipart/finalize/下载、全对象 SHA-256 scan、重启读取、双副本 GC，以及
  Complete 响应丢失和删除短暂失败恢复测试通过。

### 质量与 Git 交付校验

- [x] 所有声明可横向扩展的服务完成 2+ 副本集成与压力基线测试。
- [x] Ruff、Mypy、122 项 Pytest、10 条 import-linter、Compose、Release Gate 与恢复测试通过。
- [x] S4 作为一个 intentional commit 提交并 push 到 `origin`。

## 阶段 S5：生产部署、滚动升级与故障演练

状态：已完成（生产部署方案固定为 Docker Compose）。

### 部署与运行校验

- [x] Docker Compose 生产模板明确 replica、resource、restart/update/rollback policy、
  service identity、DB role、内部/平台网络和 Secret mount；单机 Compose 不虚构 HPA/PDB。
- [x] N/N-1 内部契约、expand/migrate/contract 数据库迁移与蓝绿并存升级通过。
- [x] 12 服务 E2E：真实 Compose canary 覆盖任务、Runnable、Assignment、Model、Canonical
  Result，MCP、SSE、Delivery 由同构完整集成套件覆盖。
- [x] Kill tests 覆盖 Session、Runtime、Orchestrator、Model、Hands、Policy、Credential、Artifact、Streaming、Delivery。
- [x] PostgreSQL、Kafka、Replay Router、SeaweedFS、Vault 短暂不可用的背压、fail-closed、恢复和告警通过。
- [x] Ops E2E 覆盖在线 Projection rebuild、Retention 与状态审计；Projection/Delivery
  redrive、DLQ 由 PostgreSQL 集成套件覆盖。
- [x] Runbook、架构总览、代码映射、配置、迁移、回滚和扩缩容说明已同步。

### 最终质量与 Git 交付校验

- [x] Ruff、Mypy、133 项 Pytest、10 条 import-linter、Compose/部署 smoke、权限与故障演练全部通过。
- [x] 最终范围检查不含 `.env`、Secret、用户无关改动、缓存或虚拟环境。
- [x] S5 作为一个 intentional commit 提交并 push 到 `origin`；Issue #12 可关闭。

## 阶段 M9：MCP Runtime 能力平面

状态：实施完成，等待质量环境清理、堆叠 PR 审阅与合并。

依据：[MCP Runtime 能力平面](../architecture/system/23%20MCP%20Runtime%20能力平面.md)。

### 范围与非目标

- [x] Runtime 通过一个内部 MCP Capability Gateway 发现和使用 Resource、Tool、Prompt 与 Skill。
- [x] 建立受管 Server Registry、Capability Catalog、Skill Package 和渐进式披露。
- [x] 外部 MCP 连接、鉴权、凭证、策略、脱敏和 Artifact 边界由平台服务管理。
- [x] MCP 不替代 Canonical Event、Control Lease、Hands Invocation Store 或 Result Delivery。
- [x] Runtime 不直连第三方 MCP Server，不接收 Server URL、stdio command 或明文 Secret。

### Phase 1：Catalog 与 Resource

- [x] initialize 协商、分页 list、Catalog 同步和只读 capability search 有契约测试。
- [x] Resource list/read 支持 tenant ACL、Policy、MIME/大小限制、DLP/注入扫描和 Artifact 化。
- [x] Resource revision/digest 可追溯；订阅通知仅用于缓存失效。
- [x] 现有 tools/list 与 tools/call 行为保持 N/N-1 兼容。

### Phase 2：Skill

- [x] Skill Manifest、签名、不可变包、发布准入和撤销流程完成。
- [x] Skill Resolver 固定版本、Tool/Resource 依赖、Policy 版本和 package digest。
- [x] Skill Runner 支持渐进加载、预算、停止条件、Checkpoint 和崩溃恢复。
- [x] `skill.activated/completed/failed/cancelled` 事件和 Projection/查询契约完成。

### Phase 3：远端 MCP 与安全

- [x] Credential Proxy MCP Egress Connector 支持 OAuth/OIDC 发现和 Resource Indicator。
- [x] Token passthrough、任意 URL/命令、SSRF、DNS rebinding 和跨 tenant 请求被拒绝。
- [x] Server/Tool/Resource/Skill 元数据按不可信输入处理；签名 Skill 才能进入受信指令层。
- [x] Skill 与 Tool 组合经过数据流、权限升级、互斥和外发策略检查。

### Phase 4：通知、长调用与生产门禁

- [x] list_changed、Resource subscription 和周期对账不会造成活动绑定静默漂移。
- [x] 实验性 MCP Tasks 保持禁用；未与 AuraClaw Task、Run、Session 或 Delivery 状态机合并。
- [x] 多副本 Catalog/Invocation、Server 断连、响应丢失、撤销和降级恢复测试通过。
- [ ] Ruff、Mypy、Pytest、import-linter、真实 MCP 冒烟和安全测试全部通过。
- [x] README、架构、API、迁移、运维、回滚和阶段测试报告已同步。
- [x] 暂存范围不含 `.env`、Secret、缓存或虚拟环境；阶段按 Issue #21 的堆叠 PR
  拆分为 intentional commits 并 push。

2026-07-24 阶段测试记录：Ruff、Mypy 和 10 条 import-linter contract 通过；M9
Catalog/Reconciliation、Egress、Skill、Runner、PostgreSQL Catalog 与 migration runner
定向测试 15/15 通过。全量测试为 156 passed、3 failed；修正新增 `0016` 后 migration
runner 隔离复跑通过，SeaweedFS multipart 隔离复跑通过。剩余 M2 outbox 用例会被本机
常驻 projection worker 在测试重命名 destination 前消费，隔离复跑仍可复现，属于共享
测试环境冲突；清理该 worker 后需重新执行全量测试，故最终质量门禁保持未勾选。

## 阶段 M10：Model Skill 转换服务

状态：M10a 配置 → Skill → MCP → Runtime Client 最小闭环与单进程周期同步已完成；
生产级持久化、多副本协调和确定性执行仍在后续阶段。

依据：[Model Skill 转换服务](../architecture/system/24%20Model%20Skill%20转换服务.md)。

### Phase 0：源发布契约

- [ ] 固定 `config_snapshot_json` Schema、公式 AST、版本发布事务和定义 Outbox 事件。
- [ ] 修复跨模型版本引用、依赖环、阈值重叠、输出缺失和重复量化配置。
- [ ] 建立 MySQL 只读身份、tenant 强制过滤、连接超时和凭证托管。

### 最小闭环

- [x] 使用固定、tenant-scoped `SELECT` 读取 `ct_model_*`，不执行数据库写操作。
- [x] Draft 配置可编译为带预览版本标识的签名 Skill Package。
- [x] `manifest.json`、`SKILL.md` 和 `references/config.json` 可通过 `skill://` MCP Resource 读取。
- [x] Runtime `HandsMcpClient` 可加载生成的 manifest、说明和配置。
- [x] 真实 MySQL 冒烟生成两套 Skill、8 个 Resource，且 manifest 以 JSON 文本返回。
- [x] 预览 Skill 明确禁止权威计算、自由解释公式和业务回写。

### Phase 1：验证与编译

- [ ] `ModelDefinitionSource` 能在一致快照中装载一个 tenant/model/version。
- [ ] Validator 覆盖引用、DAG、权重、阈值、Schema、开关、Sink 和文本安全。
- [x] Compiler 对相同源快照生成字节一致的 `manifest.json`、`SKILL.md`、references 和 digest。
- [x] Draft 仅生成明确标记的预览 Skill，不声明模型执行 Tool。

### Phase 2：持久发布

- [ ] Skill Publication 和 Sync State 持久化，多副本重启后可恢复。
- [ ] Artifact、签名、Catalog、`skill://` Resource、撤销和不可变版本冲突接入完成。
- [x] 单进程周期全量对账、幂等发布、失效撤销和失败重试完成（Issue #30）。
- [ ] Outbox 提示、持久 Sync State、多副本租约和 Quarantine 完成。

### Phase 3：确定性模型 Tools

- [ ] `ct.model.inputs.read/evaluate/result.get/writeback` 使用固定 Schema 和版本 digest。
- [ ] 公式仅由有界 DSL 执行器计算；禁止 LLM、`eval`、任意 SQL 和动态 Sink。
- [ ] writeback 经过 Policy、Approval、Invocation Store、幂等、Fencing 和副作用审计。
- [ ] Skill Resolver 固定 Tool schema、模型 source digest 和上游模型版本。

### Phase 4：安全、恢复与交付

- [ ] 跨 tenant、提示注入、Secret/PII、依赖环、版本漂移和恶意 Sink 测试通过。
- [ ] MySQL/Artifact/Catalog/Policy 短暂不可用、消息丢失/重复/乱序和多副本恢复通过。
- [ ] Ruff、Mypy、Pytest、import-linter、真实 MCP 冒烟和生产回滚演练全部通过。
- [ ] `.env`、凭证、缓存和虚拟环境不进入暂存；M10 作为 intentional commit 提交并 push。

### M10a / Issue #30 交付门禁

- [x] 功能：启动全量同步、周期扫描、幂等发布、失效撤销、重新激活和失败重试完成。
- [x] 生命周期：进程内重叠扫描串行化，服务退出时周期 worker 正常停止。
- [x] 数据：真实 MySQL `READ ONLY + WITH CONSISTENT SNAPSHOT` 成功装载两套模型。
- [x] 安全：Runtime/Agent 不接触 MySQL 地址、SQL 或凭证；Draft 明确禁止权威执行和回写。
- [x] 架构：Ruff、Mypy 和 10 条 import-linter contract 全部通过。
- [x] 测试：后端全量 167 项、Model Skill 定向 8 项、前端 16 项全部通过。
- [x] 文档：README、环境变量、转换服务设计、页面和阶段清单已同步。
- [x] 迁移：本子阶段只使用内存 Registry，不新增持久结构，因此无数据库迁移。
- [x] 交付范围：暂存不含 `.env`、Secret、缓存或虚拟环境；intentional commit 范围已复核，
  commit/push 结果在 Issue #30 记录。

2026-07-24 M10a 记录：真实 MySQL 只读冒烟把两套 Draft 编译为
`model.supplier-risk-warning/1.0.0-draft.1` 与
`model.supplier-score/1.0.0-draft.2`，MCP 共列出 8 个对应 Resource，manifest 以 JSON
文本成功读取。隔离本机常驻 projection/orchestrator/runtime 容器后，后端全量 167 项全部通过；
随后已恢复所有临时停止的容器。

## 阶段 M11：Capability-Aware Agent Loop

状态：实现与最终质量门禁完成，等待 Draft PR 审阅。

依据：[Issue #31](https://github.com/sushaofei/AuraClaw/issues/31)、
[MCP Runtime 能力平面](../architecture/system/23%20MCP%20Runtime%20能力平面.md) 与
[M11 实施与运维](./implementation/capability-aware-agent-loop.md)。

### 功能闭环

- [x] 简单任务允许模型不搜索能力直接输出最终结果。
- [x] 多轮 Harness 支持 `search -> load -> Tool/Skill/Resource -> result -> next turn`。
- [x] Tool Result 以 `tool_invocation_id` 配对进入下一轮模型 Transcript。
- [x] 只有已加载 Tool 的权威 Schema 进入 `ModelRequest.tools`。
- [x] 签名 Skill 可搜索、固定解析、激活、注入说明并写入激活/终态事件。
- [x] Resource 只从已加载 URI/template 读取并写 `context.resource.used`。

### 恢复、预算与审批

- [x] Checkpoint 保存 turn、phase、累计预算、binding、active Skill 和 pending call。
- [x] Model/Tool 完成后崩溃可恢复，已完成控制调用不会重复执行。
- [x] Token、Cost、模型轮次和 capability calls 按整个 Run 累计。
- [x] 搜索、加载、候选、Schema 和重复无进展调用均有硬上限。
- [x] Capability Loop 保留 `approval_waiting` 与原 invocation/idempotency 语义。

### 安全与架构

- [x] 模型不能提供 tenant、Role、Policy、Credential、Server URL 或任意 Resource URI。
- [x] 未加载 capability id、Tool、Skill 和 Resource fail closed。
- [x] Resource Prompt Injection finding 被 Context Policy withheld，大文本再次截断。
- [x] 中间模型输出只写内部 `model.turn.completed`，不作为最终用户结果或 delta 发布。
- [x] Runtime 继续只连接内部 MCP；未新增生产服务、Secret 路径或业务事实源。
- [x] Runtime 身份允许写 Skill/Resource 证据，Projection 识别新增内部模型轮次事件。

### 测试与交付

- [x] 单元测试覆盖多轮决策、渐进 Tool hydration、Skill 激活、Resource 注入和崩溃恢复。
- [x] In-process MCP 测试覆盖真实 search/load/Tool 和 Skill resolve 路径。
- [x] Ruff、Mypy、Pytest 与 import-linter 全部门禁通过。
- [x] 架构、运维、回滚和阶段校验清单同步。
- [x] M11 作为 intentional commit 提交、push，并创建关联 Issue #31 的 Draft PR。

2026-07-24 M11 记录：Ruff、Mypy 和 10 条 import-linter contract 通过；后端全量收集
173 项，152 passed、21 skipped、0 failed。6 项 M11 定向测试覆盖真实 in-process MCP
search/load/Tool、签名 Skill resolve、Resource Context Policy、multi-turn Transcript 和
Checkpoint 崩溃恢复。

## 阶段 M12：价格洞察业务 Skill 端到端样板（Issue #39）

状态：实现完成，等待最终质量门禁与交付。

依据：[Issue #39](https://github.com/sushaofei/AuraClaw/issues/39) 与
[MCP 开发手册](../guides/mcp-development.md)。

### 业务、数据与能力契约

- [x] 固定历史、区域、市场三维比价和八项首版关键指标。
- [x] DWD 增加 tenant、稳定价格行/匹配对 ID、基准统计类型、物料匹配证据和规则版本表。
- [x] 黄金数据与 MySQL 固定 SQL 适配器遵循同一 `PriceInsightSource` 契约。
- [x] 数据访问强制租户与月份条件，Agent 不接收 SQL、表名或数据库凭证。
- [x] 数据质量覆盖重复粒度、单位/物料缺失、金额不一致、孤儿基准和统计口径缺失。

### Skill、Tool、Resource 与 Agent Loop

- [x] 平台签名 `procurement.price-insight.generate@1.0.0` 携带说明、规则、输出契约和黄金数据。
- [x] snapshot、drilldown、data_quality 三个只读 Tool 完成注册与路由。
- [x] 指标定义、可比规则和输出契约以三个受治理 Resource 暴露。
- [x] Skill 激活按解析 binding 自动 hydration 依赖，失败或超预算时 fail closed。
- [x] Runtime 没有价格场景分支；无关库存 Skill 回归复用同一自动装载机制。

### 测试、安全、文档与交付

- [x] 真实 in-process MCP 流程覆盖 search、load、activate、依赖装载和 snapshot。
- [x] 黄金样本断言八项 KPI、质量状态，以及正负影响金额不抵消。
- [x] `skill-creator` 的 `quick_validate.py` 校验通过。
- [x] Ruff、Mypy、Pytest 与 import-linter 全部门禁通过。
- [x] README、环境变量、DDL、实施运维和阶段清单同步。
- [x] 暂存不含 `.env`、Secret、缓存或虚拟环境；intentional commit 已 push。

2026-07-30 M12 记录：`skill-creator quick_validate`、仓库全量 Ruff、171 个源码文件
Mypy、10 条 import-linter contract 和全量 Pytest 通过；MySQL、SeaweedFS、Vault 集成集为
18 passed、16 skipped，跳过项均为当前 MySQL 主库配置下的 PostgreSQL 专用用例。MySQL 8.4
角色测试补齐 PyMySQL RSA/cryptography 依赖。Action Hands fixture 模式真实进程启动成功，
`/health/ready` 返回 200 并正常关闭；wheel 已确认包含完整 Skill 包。

### M12a：本地真实 DWD 与前端调试闭环

- [x] DDL/黄金数据脚本可重复初始化本机 MySQL，且只清理稳定验证 ID。
- [x] MySQL Source 按稳定业务键读取最新 `dt/etl_load_time` 快照，修订摘要覆盖完整数据内容。
- [x] development combined server 装载同一 Capability/Skill/Tool 能力平面。
- [x] 可选脚本模型在外部模型不可用时仍通过标准 Agent Harness 驱动完整 Loop。
- [x] `/price-insight` 可创建标准 Task，并从 Canonical Timeline 展示五步证据和八项 KPI。
- [x] 浏览器真实联调显示 `MYSQL DWD`、`completed`、质量 `pass` 和 `8 / 8`。
- [x] Ruff、Mypy、Pytest、import-linter、前端 lint/build 全部门禁通过。
- [x] `.env`、Secret 和用户资料未暂存；M12a intentional commit 已 push。

### M12b：数据中心化原子 Tool 与 Skill SOP

- [x] Skill 2.0 将范围画像、质量门禁、逐项指标计算和证据下钻拆成明确 SOP。
- [x] `metric.compute` 每次只允许计算八项指标中的一个 `metric_key`。
- [x] Agent 必须校验所有原子结果的 `source_revision`，禁止拼接跨版本结论。
- [x] Tool 数据表边界硬限制为价格洞察 DDL 声明的四张表。
- [ ] TODO：创建远程 DWD 专用只读账号，仅授予四张白名单表 `SELECT` 并增加授权自检。
- [x] In-process MCP 覆盖 Skill 激活、范围、质量、八次单指标计算和有界证据调用。
- [x] 前端从 Canonical Timeline 聚合原子结果，lint、build 与渲染测试通过。
- [ ] 远程 DWD 完成当前 DDL 对齐后，以真实模型 Provider 执行端到端前端验收。

### M12c：ct_model 配置驱动 Skill

- [x] `PRICE_IMPACT@2.0.0` 配置覆盖四个数据源、请求特征、十个输出和控制塔场景开关。
- [x] `config_snapshot_json.auraclaw_skill` 定义 Skill、原子 Tool、表边界、指标顺序和 Schema。
- [x] 编译器仅接受代码注册执行模板，拒绝未知模板、越界表、Tool 或指标。
- [x] Model Skill Source 启用时不再发布同名平台内置 Skill，避免双来源选择歧义。
- [x] MySQL Source 将四张 DWD 表纳入一致只读快照和 `source_revision`。
- [x] 配置脚本默认 validate、显式 plan/apply，并拒绝覆盖内容不同的已发布同版本。
- [x] tenant 1 的 `PRICE_IMPACT` model id 3 已发布 version id 4 / `2.0.0`。
- [ ] 远程 DWD Schema 对齐后完成原子 Tool 与真实模型 Provider 的端到端验收。

### M12d：可复用原子 Tool 与组合 Skill

- [x] 八项价格指标拆为八个固定 Tool，不再通过 `metric_key` 在一个 Tool 内分派计算。
- [x] 新增数据校验和价格指标两个平台签名子 Skill，场景 Skill 通过 `required_skills` 组合。
- [x] Resolver 递归解析父子 Skill，检测循环并去重固定 Tool、Resource 和子 Skill 版本。
- [x] Runtime 分批加载超过单次 MCP 上限的依赖，并注入所有已解析子 Skill 的签名 SOP。
- [x] `auraclaw.model-skill/v2` 配置以子 Skill 为依赖，拒绝直接 Tool 扩张和未知组合。
- [x] 开发模型和价格洞察前端使用 3.0 原子 Tool Timeline。
- [x] tenant 1 已发布 `PRICE_IMPACT` version id 6 / `3.0.0`，远端快照编译为两个子 Skill。
- [ ] 远程 DWD Schema 对齐后，用真实模型 Provider 完成父子 Skill 端到端验收。

### M12e：模型参数、标签、开关与 DWD 规则治理

- [x] `PRICE_IMPACT@4.0.0` 配置八项解释优先级权重，精确合计为 1，且明确禁止参与 KPI 计算。
- [x] 四组受控发现标签进入 Skill `applies_when`，编译器拒绝未知标签和任意规则文本。
- [x] 控制塔场景只允许一个 `price_insight_agent` 启用开关，优先级固定为 100。
- [x] 配置脚本在事务写入前校验权重、标签和开关，并安全迁移租户全局标签与场景唯一开关。
- [x] DWD 规则驱动默认偏离阈值、最小样本量和最低匹配分；请求覆盖值显式记录来源。
- [x] 多条规则同时匹配时 fail closed，弱市场证据被排除并产生确定性质量告警。
- [x] tenant 1 已发布 model id 3 / version id 7 / `4.0.0`；远端回读为 8 权重、4 标签、1 开关，
  并成功编译 `ct-model/procurement.price-insight.generate@4.0.0`。
- [x] 只读审计确认远端三个同名 DWD 表仍为旧 Schema 且规则表缺失，未覆盖或重建现有数据。
- [ ] 数据侧按当前 DDL 完成兼容迁移后，用真实模型 Provider 和前端完成最终验收。

### M12f：旧 DWD 兼容迁移与数据可信门禁

- [x] 只读审计覆盖行数、分区、来源键、benchmark 关联完整性和上游质量标记。
- [x] 远端 87 条成交、87 条比对、46 条 benchmark 可确定性补稳定 ID，关联口径无不一致。
- [x] 识别全部行业 benchmark 为模拟内部派生数据，禁止将 Schema 对齐误判为权威数据就绪。
- [x] 数据质量 Tool 对模拟市场 benchmark 返回 blocked，对成交未确认和税价未知返回 finding。
- [x] 新增默认只读的兼容迁移工具；apply 要求完整业务语义、目标库确认和演示数据显式覆盖。
- [x] 迁移在远端数据的本机临时克隆演练通过，87/87/46 行保留并由真实 MySQL Source 回读。
- [x] 价格 Tool 的同 idempotency replay 不重复读取 DWD，Policy deny 在 DWD 访问前生效。
- [x] development 脚本模型发现跨 Tool `source_revision` 漂移时停止拼接指标。
- [ ] 经数据所有者确认后，在远端执行增量 Schema 迁移。
- [ ] 用真实外部行业 benchmark 替换模拟内部派生 benchmark，并修复成交确认和税价口径。
- [ ] 使用真实模型 Provider、远端 DWD 和前端完成最终端到端验收。

## 阶段 M9a：MCP 2026-07-28 无状态协议升级

### 协议与兼容

- [x] Runtime↔Hands 默认使用 MCP `2026-07-28`，不再依赖 initialize 或协议 Session。
- [x] 每请求携带协议版本、Client identity/capabilities `_meta` 和 Streamable HTTP 路由 Header。
- [x] Hands 实现 `server/discover`、`resultType`、Server identity 和保守的私有缓存提示。
- [x] `2025-11-25 initialize` 仅作为显式 legacy profile 保留，远端 Server 配置只接受这两个 revision。
- [x] AuraClaw invocation 取消使用自有请求 method，不再误用标准 `notifications/cancelled`。

### 远端、安全与迁移

- [x] Catalog Reconciler 按 profile 选择 discover/initialize，2026 profile 不创建 Resource subscription Session。
- [x] Credential Egress 校验现代请求元数据并发送 `MCP-Protocol-Version`、`Mcp-Method`、`Mcp-Name`。
- [x] PostgreSQL/MySQL migration 将新 Server 的默认 revision 升为 `2026-07-28`，down migration 可恢复。
- [x] workload/lease、Policy、Credential、DNS pinning、Invocation Store 和事实边界保持不变。

### 测试、文档与交付

- [x] 定向 MCP Server/Client、HTTP、Catalog、Egress、鉴权与信任边界测试通过。
- [x] 全量 unit test、相关文件 Ruff/Mypy、全量 import-linter 与前端 lint/build 通过。
- [ ] 仓库全量 Ruff/Mypy 的两个既有非 MCP 问题修复后通过。
- [ ] 依赖 Kafka/MySQL/SeaweedFS/Vault 的外部集成环境恢复后运行完整 Pytest。
- [x] 架构、开发、Java 接入、实施运维文档与前端 Skill Lab 已同步。
- [x] 阶段 intentional commit 已 push，且暂存不含 `.env`、Secret、缓存或虚拟环境。

## 主存储 MySQL 支持（Issue #33）

### 配置与装配

- [x] `AURACLAW_STORAGE_BACKEND` / `AURACLAW_DB_DIALECT` 默认 MySQL，可切回 PostgreSQL。
- [x] 主存储 `DB_*` 与 Model Skill `MYSQL_DB_*` 隔离。
- [x] composition / health 以 `sql_storage_enabled` 上报 `mysql|postgres|memory`。

### 基础设施

- [x] `aiomysql` 连接池与 SQL 方言适配（占位符、前缀表、冲突写入、RETURNING、JSON）。
- [x] `migrations/mysql` 0001–0016 与 `MysqlMigrationRunner`。
- [x] Control 关键路径 MySQL 分支（claim / lease / capacity / assignment / checkpoint）。
- [x] Compose migrate 默认 `migrations/mysql`；角色 DSN preflight 支持 `mysql+aiomysql://`。

### 验证与安全

- [x] 真实库 `auraclaw_dev` migrate + event/projection/dedup/outbox/control 冒烟通过。
- [x] `apply_mysql_roles` 展开前缀授权；角色 owner DML / 跨前缀拒绝 / query RO 矩阵通过。
- [x] `.env.example`、README、S5 Runbook 与 issue 进度同步。
- [x] intentional commit + push + 关联 Issue #33 的 PR。

## 阶段 H1：Runtime 与 MCP 解耦，Hands 协议无关化（Issue #43）

状态：已完成并提交。

### 范围

- Runtime 只依赖 Hands Port / DTO，不再使用内部 `/mcp`。
- Action Hands 对 Runtime 暴露 `/internal/v1/hands/*`，并保留 in-process Adapter。
- 下游 MCP 收敛为 `ManagedMcpConnector`；新增受管 `ManagedJavaApiConnector`。
- 不改变 Canonical Event、Control Lease、Checkpoint 或 Delivery 语义。
- 保留工作区中与本 Issue 无关的既有改动。

### 功能校验

- [x] Hands DTO 不含 `protocolVersion`、`_meta` 或 MCP Session。
- [x] Runtime `HandsClient` 覆盖 list/call/resource/prompt/cancel。
- [x] in-process 与 HTTP Hands Client 跑同一套 contract tests。
- [x] 生产 composition 使用 `HttpHandsClient`；开发 combined 使用 `InProcessHandsClient`。
- [x] Catalog Reconciler 只调用 `CapabilityConnector.snapshot()`，不再 `isinstance` 选路由。
- [x] 下游 MCP 位于 `infrastructure/connectors/mcp`。
- [x] Java API Connector 使用已注册 operation，拒绝 URL/header/method 覆盖。
- [x] 写操作走同一 ToolGateway 并触发 Approval；重试不重复副作用。
- [x] `AURACLAW_HANDS_MCP_URL` 与内部 `/mcp` 已从 src/compose/.env.example 移除。

### 架构与安全

- [x] import-linter：runtime 不得导入 `contracts.mcp`、`infrastructure.connectors`。
- [x] tenant/lease/fencing 只从 workload + signed lease 恢复。
- [x] Java API egress 保留 `is_global` 检查，私网仅显式 allowlist。
- [x] ADR-002 记录决策、备选与回滚。
- [x] ADR-001 §6、架构总览、Hands/MCP 文档与发布手册已同步。

### 质量与交付

- [x] `ruff check src tests`、`mypy src/auraclaw`、`pytest tests/unit` 与 `lint-imports` 通过。
- [x] 阶段 intentional commit 已 push，暂存不含 `.env`、Secret 或虚拟环境。

## 阶段 I44：用户身份归属 chaintower（Issue #44）

状态：完成。

### 范围

- chaintower 是用户身份权威；AuraClaw 只验证 workload + 短期 Assertion。
- 生产 Task API 不信任裸 `X-Tenant-ID` / `X-Actor-ID`。
- MCP Server 执行最终业务鉴权；OAuth 仅为可选 Connector 策略。
- 不实现通用 OAuth Authorization Server，不持久化终端用户 token。
- chaintower 仓库改造见 [chaintower 身份联调](../guides/chaintower-identity-integration.md)。

### 功能校验

- [x] ADR-003 冻结三段调用链、信任边界、HMAC Assertion、密钥轮换与回滚。
- [x] `TrustedUserContext` / `IdentityContextVerifier` 位于 contracts，不依赖 FastAPI/JWT SDK。
- [x] 生产入口要求 chaintower workload + signed context；development 可显式使用 Header adapter。
- [x] Header/body tenant 冲突返回 403；认证失败 401。
- [x] jti+command_id 通过 PostgreSQL/MySQL 唯一约束跨 Task API 副本防重放；N/N-1 kid 轮换可用。
- [x] 已有 Session 强制要求 Assertion session_id 与路径完全一致。
- [x] Runnable 用户从根 Session canonical 创建事实恢复，不能被 runtime/coordinator 最新 actor 覆盖。
- [x] tenant/user 从 CommandContext / HandsTrustedContext 传播，模型参数不能覆盖。
- [x] chaintower MCP 支持 `workload_trusted_context`，OAuth 不再是必选项。
- [x] Assertion/workload/OAuth token 不进入 Event、Artifact 或业务日志。

### 架构与安全

- [x] 内部 workload、lease、fencing、Policy、Approval、Invocation 边界保留。
- [x] 生产禁止 `allow_insecure_identity_headers`。
- [x] MCP Tool arguments 中的 tenant/user 冲突 fail closed。
- [x] ADR-001 / ADR-002 / M9 / 接入手册已同步“无终端用户 OAuth”与“仍有 Connector 认证”。

### 质量与交付

- [x] `ruff check src tests`、`mypy src/auraclaw`、`pytest tests/unit` 与 `lint-imports` 通过。
- [x] Git 暂存不含 `.env`、Secret 或虚拟环境。

## 阶段 I47：本地智问 Ingress 与 Java MCP 价格洞察联调（Issue #47）

状态：完成，待 intentional commit 与 push。

### 范围

- 本地 `auraclaw serve` 增加 `:8080` Ingress，按生产规则分流 Task API 与 Streaming Gateway。
- Java MCP 工具名、输入包装、协议版本、可信用户身份和价格洞察 Skill 对齐。
- 中文能力搜索、schema+json 资源、业务质量状态和 follow-up 加载修复。
- 默认 Runtime 步骤预算统一为 48；显式任务预算仍优先。
- 不提交 `.env.dev`、`.host.env`、Java MCP 私有配置或任何 Secret。

### 功能与回归

- [x] `/v1/streams/*` 进入 Streaming Gateway，其余路径进入 Task API。
- [x] 多进程 Runtime Event 使用共享 SQL 或 Kafka；纯内存组合启动前失败。
- [x] Java MCP canonical alias、`input` 包装和业务 `PASS` 状态有回归测试。
- [x] 中文“价格洞察”搜索、Skill/文档发布和 follow-up capability load 有回归测试。
- [x] Hands 到 MCP 的可信 `user_id` 传播有 contract 与 connector 测试。

### 架构、安全与迁移

- [x] Runtime 仍只通过 Hands 调用 MCP；未绕过 Tool Gateway、Policy 或 Credential Proxy。
- [x] HTTP MCP 仅允许显式白名单中的私网/回环地址，公网 MCP 仍强制 HTTPS。
- [x] 远端工具注解缺失时使用 `write-with-approval` + `high` 保守默认值。
- [x] Canonical Session Event 仍是任务事实源；Runtime Event 不承担结果交付保证。
- [x] 无数据库 Schema 或数据迁移；配置回滚可关闭 Ingress 并恢复原外部入口。
- [x] README、S2 运行说明、MCP 手册和本次 Python 改动说明已同步。

### 质量与交付

- [x] `ruff check .`、`mypy src/auraclaw`、`pytest tests/unit` 与 `lint-imports` 通过。
- [x] `git diff --check` 通过，暂存范围不含 `.env`、Secret、缓存或虚拟环境。
- [x] 本阶段作为一个 intentional commit 提交并 push 到当前分支。

## 阶段 MCP-HC：MCP Server 热配置与本地连接

状态：已完成并推送。

依据：[MCP 开发手册](../guides/mcp-development.md)；实现跟踪：
[GitHub Issue #50](https://github.com/sushaofei/AuraClaw/issues/50)。

### 范围

- 在服务运行期间创建、测试、启用、更新、禁用和退役 Streamable HTTP MCP Server。
- Server 配置以不可变 revision 持久化，Action Hands 与 Credential Proxy 重启后自动恢复。
- 支持 public、private、loopback 网络模式和受策略控制的无认证本地 Server。
- 本阶段不允许 Runtime 注册 URL，不支持任意 stdio command，也不保存明文 Secret。

### 功能与恢复

- [x] 版本化 Admin API 支持幂等命令、expected revision、tenant/actor/correlation/causation。
- [x] 候选配置测试成功后原子切换；失败时旧 active revision 保持可用。
- [x] 配置通知丢失可由周期 revision 对账恢复。
- [x] Action Hands、Credential Proxy 单独或共同重启后自动恢复已启用 Server（代码路径；进程级冒烟待补）。
- [x] 单个 MCP Server 未启动或不可达时不阻断 Hands 启动、其它 Server 恢复/对账，以及 Credential Proxy 出站调用链；该 Server 记为 `unavailable`。
- [x] 禁用撤销新发现与新调用，旧 generation 按 `drain_seconds`（默认 5s）排空。
- [x] MCP Server 只通过热配置 Registry 恢复；环境变量静态装配已删除。

### 本地连接与安全

- [x] development 支持 `loopback + HTTP + auth none`（真实本地 ThreadingHTTPServer 冒烟 + Registry/Egress 单测）。
- [x] public 仅允许 HTTPS 和公网地址；private 同时校验服务 allowlist 与 CIDR；loopback 只允许回环地址。
- [x] DNS rebinding、redirect、userinfo、link-local、公网 HTTP 和请求覆盖 endpoint 均 fail closed。
- [x] Hands→Credential Proxy 调用携带 config revision，不允许不同 revision 的 Connector/Adapter 混用。
- [x] Secret、Token 和完整远端响应不进入 Registry、Session Event、日志、Artifact 或 API 响应。
- [x] 跨 tenant 管理和调用均被拒绝；平台 Server 仅平台管理员可写。

### 数据、质量与交付

- [x] expand/down migration 覆盖 Server、revision、runtime state 和 operation；历史 revision 可审计。
- [x] 配置事实、运行状态和可重建 Capability Catalog 的唯一写入方边界符合架构。
- [x] 单元、SQL 集成、真实本地 MCP 冒烟通过；全量进程重启冒烟可在部署环境补跑。
- [x] Ruff、Mypy、Pytest（unit）和 import-linter 全部通过。
- [x] API、CLI/运维、发布与回滚文档同步。
- [x] 暂存不含 `.env`、Secret、缓存或虚拟环境；intentional commit 已 push。

## 阶段 I45：部门身份快照透传到 MCP（Issue #46）

状态：代码与单测已齐，待 push。

### 范围

- 创建 Root Session 时从已验签 Agent Context 固化 `dept_id`。
- `dept_id` 随 Runnable / Lease / Hands 传到 chaintower MCP Tool、Resource、Prompt。
- MCP 以 `X-CT-Dept-ID` 快照恢复数据权限，不再用用户表覆盖本次任务部门。
- 不修改 `POST /v1/tasks` body；不在 AuraClaw 查询用户服务。

### 功能校验

- [x] `session.created` 与后续 `run.requested` 携带部门快照。
- [x] 子 Session 从 Root 恢复部门，不被 coordinator actor 覆盖。
- [x] MCP 出站 Header 含 `X-CT-Tenant-ID` / `X-CT-User-ID` / `X-CT-Dept-ID`。
- [x] Tool `_meta.io.auraclaw/deptId` 与 Header 一致；冲突 fail closed。
- [x] 参数中的 `dept_id` 不能当授权来源。
- [x] 无部门用户不伪造 `dept_id`。

### 架构与安全

- [x] 身份来源仍是 chaintower Assertion，不是请求体。
- [x] 用户禁用仍由 MCP 实时校验 fail closed。
- [x] Assertion 原文不进入 Event 或业务日志。

### 质量与交付

- [x] AuraClaw `ruff` / 相关 pytest 通过。
- [x] chaintower MCP 单测覆盖 Header 快照优先于用户表部门。

## 阶段 AuraMCP：Hands 登记扩展 MCP Server

状态：已完成并推送。

依据：AuraMCP 仓库 `docs/AuraClaw 接入.md`；本仓 [AuraMCP 接入](../guides/auramcp-integration.md)；
实现跟踪：[GitHub Issue #51](https://github.com/sushaofei/AuraClaw/issues/51)。

### 范围

- AuraClaw Action Hands 用标准 MCP Client 接入 AuraMCP；Runtime 仍只持有 Hands URL。
- 身份为 `workload_trusted_context`；Tool 前缀 `auramcp.`，Resource scheme `auramcp`。
- 不把 AuraClaw 内核 Tool 迁入 AuraMCP，也不让 Runtime / AuraX 直连。

### 功能校验

- [x] 未启用 Server 的 `:test` 先让 Credential Proxy 装载 `mcp:{server_id}` egress，成功或失败后卸掉；`:enable` 探测成功后才保留出口。
- [x] 热配置 `McpServerConfig` 可表达 AuraMCP（`auramcp.` 前缀、`workload_trusted_context`、loopback）。
- [x] Hands `ManagedMcpConnector` 对真实 AuraMCP 完成 discover / list / ping / echo / Resource / Prompt。
- [x] 错误 workload 被 AuraMCP 以 HTTP 401 fail closed。

### 架构与安全

- [x] 出站仍经 Credential Proxy MCP Egress；DNS pinning、前缀 allowlist 与 trusted Header/`_meta` 保持不变。
- [x] AuraMCP 源码不进入本仓依赖；联调测试按兄弟目录或 `AURAMCP_SRC` 发现，缺失则 skip。
- [x] 无数据库 Schema 变更。

### 质量与交付

- [x] `ruff check .`、`mypy src/auraclaw`、相关 pytest 与 import-linter 通过。
- [x] 暂存不含 `.env`、Secret、缓存或虚拟环境。
- [x] 本阶段作为一个 intentional commit 提交并 push 到当前分支。

## 阶段 AuraX-v1 契约：Session 列表、source 元数据与 Skill Admin

状态：AuraClaw 契约已落地，随 AuraX 工作台一并交付。

依据：[公开 API 与身份接入手册](../guides/public-api-and-identity.md)；AuraX [GitHub Issue #1](https://github.com/sushaofei/AuraX/issues/1)。

### 范围

- `POST /v1/tasks` 接受 `source=chat|schedule`，以及 schedule 所需的 `schedule_id` / `occurrence_id`，写入 `session.created`。
- `GET /v1/tasks` 按租户列出 Root Session，支持 `kind` / `status` / cursor 分页。
- Skill Admin 只读 + 启停：列表、详情、`SKILL.md`、`:enable` / `:disable`。不发布签名包。
- 本阶段不包含 AuraAPI Timer、完整 SSO、Skill 编辑器、stdio MCP 本机托管。

### 功能校验

- [x] 创建任务时 `source=chat` 写入投影；`source=schedule` 缺少 `occurrence_id` 返回 422。
- [x] `GET /v1/tasks` 只返回当前租户 Root Session；`kind=chat` 不含 schedule。
- [x] `GET /v1/admin/skills` 列出最新版本；`:disable` 后 `status=revoked`，`:enable` 恢复 `active`。
- [x] 详情返回 `skill_markdown`；启停不改进行中 Run binding（Registry 只改目录状态）。

### 架构与安全

- [x] 列表走 Task 投影，不扫 Event Log；租户隔离。
- [x] `source` / `schedule_id` / `occurrence_id` 不进身份字段；身份仍只走 Header。
- [x] Skill 启停与 Hands 共用同一进程内 Registry 单例。
- [x] 迁移 `0021_task_source_and_list`（Postgres / MySQL）含回滚脚本。
- [x] `GET /v1/admin/mcp-servers/{server_id}/tools` 读 Capability Catalog（不直连 MCP）；未对账为空列表。
- [x] AuraX MCP 卡片可展开列出 tools；走 `@aurax/claw-sdk`，不打 AuraMCP / `/internal/v1/*`。

### 质量与交付

- [x] `ruff check`、`mypy src/auraclaw`、相关 pytest 通过。
- [x] 公开手册已补充列表 API、`source` 字段与 Skill Admin。
- [x] 本阶段与 AuraX 脚手架一并作为一个 intentional commit 提交并 push。

## 阶段 Sync Invoke：同步调用外观（Issue #52）

状态：代码已完成，本次提交。

依据：[公开 API 与身份接入手册](../guides/public-api-and-identity.md)；[Task Query Result Service](../architecture/system/05%20Task%20Query%20Result%20Service.md)；[GitHub Issue #52](https://github.com/sushaofei/AuraClaw/issues/52)。

### 范围

- 新增 `POST /v1/tasks/sync`：经 Task Gateway 接纳任务后，在 Result 投影上等待当前 Run 终态。
- `GET /v1/tasks/{session_id}/result?wait=true` 复用同一 Waiter，供人审/暂停后继续同步等待。
- 现有 `POST /v1/tasks` 仍为 `202`。AuraX / Timer 不改。
- 本阶段不包含 Gateway 同步调度 Runtime、SSE 结果保证、人审上行、替换 Result Delivery。

### 功能校验

- [x] `POST /v1/tasks` 仍返回 202，body 无 `wait_outcome`。
- [x] `POST /v1/tasks/sync` 在 Run 终态时 200，body 与 Result 同源并含 `wait_outcome`。
- [x] 超时返回 202 且 Session 仍在，不取消。
- [x] `waiting_for_human` / `paused` 返回 409；人审仍走既有审批命令。
- [x] 同一 Idempotency-Key 不双开任务。
- [x] `GET .../result?wait=true` 可挂在已有 Session 上等待；未传 `wait` 行为不变。
- [x] `source=schedule` 等多余字段 422；并发超额 429。

### 架构与安全

- [x] 写侧仍只走 Task Gateway / Canonical Event；Waiter 只读 Projection。
- [x] Waiter 不 import Session 写服务，不订 SSE / Kafka。
- [x] 断线 / `CancelledError` 不调用 `cancel_task`。
- [x] Timer 不得使用 sync；身份头与 create 相同。
- [x] 无数据库 Schema 变更。

### 质量与交付

- [x] `ruff check`、`mypy src/auraclaw`、相关 pytest、`lint-imports` 通过。
- [x] 公开手册、Query 架构文档、README 与 env example 已同步。
- [x] 本阶段作为一个 intentional commit 提交；暂存不含 `.env`、Secret、缓存或虚拟环境。

## 阶段 Object Storage Port：S3 抽象与 OBS 接入

状态：代码已完成，待校验与提交。

### 范围

- 抽出 Artifact 生产对象存储端口（presign / multipart / verifier）。
- 泛化 SeaweedFS SigV4 客户端为 S3-compatible adapter，SeaweedFS 与 OBS 共用实现。
- `AURACLAW_ARTIFACT_BACKEND` 支持 `auto | local | seaweedfs | obs`。
- 业务 HTTP 契约、Hands `ArtifactWriter`、Session `artifact_ref` 不变。

### 功能校验

- [x] `artifact_backend=seaweedfs` 与现有 SeaweedFS 集成测试通过。
- [x] `artifact_backend=obs` 配置解析与 factory 选择正确。
- [x] 真实 OBS（若配置）PUT/HEAD/GET/DELETE 与 multipart 冒烟通过。
- [x] Artifact admin status 返回实际 backend 名。

### 架构与安全

- [x] `ArtifactInternalService` 只依赖 `artifact/ports.py` Protocol，不依赖厂商类型。
- [x] 对象存储密钥只注入 Artifact Service；Hands 进程无 `OBS_AK`/`OBS_SK`/`SEAWEEDFS_*`。
- [x] example 与文档无真实密钥；OBS/SeaweedFS 凭证只写在 gitignored 的 `.env.dev` / `.env.test` / `.env.prod`。
- [x] 无数据库 Schema 变更。

### 质量与交付

- [x] `ruff check .`、`mypy src/auraclaw`、`pytest`、`lint-imports` 通过。
- [x] ADR-001、S2 运行说明、`.env.*.example` 已同步。
- [ ] 本阶段作为一个 intentional commit 提交并 push。

## 主存储 KingBase 迁移（Issue #53）

状态：P1 进行中（配置与方言别名）。

### 范围

- 将主存储从 MySQL 切到 KingBase V9（PostgreSQL 兼容模式）。
- 沿用既有 Domain ports + `LazyPool` + PostgreSQL SQL 源方言；`kingbase` 仅为配置别名。
- 不引入 SQLAlchemy/Alembic；不重命名 `Postgres*` 类；不迁移 `MYSQL_DB_*` 外部只读源。

### 配置与抽象

- [x] `AURACLAW_STORAGE_BACKEND=kingbase` 解析为 postgres 方言，`storage_label=kingbase`。
- [x] `.env.test` / `.env.prod` 内 `KINGBASE_*` → `DB_*` 映射（backend=kingbase 时覆盖）。
- [x] `kingbase://` URL 规范化为 `postgresql+asyncpg://`；`detect_dialect` 识别 KingBase。
- [x] Domain / Application 仍只依赖 ports；切换仅改配置。
- [x] 本地开发 `.env.dev` / `.env.dev.example` 默认 Kafka=`localhost:9092`、
  `AURACLAW_STORAGE_BACKEND=postgres`；`.postgresql.local.env` / `POSTGRESQL_*` → `DB_*`。

### 连通与迁移（P2）

- [ ] 真实 KingBase `asyncpg` 连通。
- [ ] `migrations/` up 到最新；advisory lock / jsonb / ON CONFLICT / RETURNING / SKIP LOCKED。
- [ ] Event / Outbox / Control claim / Projection 冒烟。
- [ ] `deploy/postgres/roles.sql` 在 KingBase 上验证或记录差异补丁。

### 文档与交付

- [x] README 主存储章节、Issue #53 方案。
- [x] Issue #54：本地 PostgreSQL/Kafka 默认、统一应用 DSN、清理废弃 Model Skill MySQL 源配置；
  intentional commit（不含 `.env.*` / `.postgresql.local.env` Secret）。
- [ ] Issue #53 P2（真实 KingBase 连通与迁移冒烟）完成后同步关闭。
- [ ] 关联 Issue #53 的进度同步。

## 阶段 M13：Role-aware Agent Runtime（Issue #55）

状态：已完成并推送。

### 调度与恢复

- [x] Runtime 实例统一注册到 `agent` Pool，Assignment 保留 root/worker/reviewer/repair 语义角色。
- [x] Child 终态从 Canonical Root Feed 重算 runnable DAG，不依赖可滞后的 Projection 决定正确性。
- [x] Coordinator `await_children` 持久化 `agent.waiting_children` checkpoint、释放 Lease，且不写
  `run.completed`。
- [x] 所有等待目标终态后，同一个 Root Run 被重新排队；串行依赖只在前置结果发布后解锁。
- [x] PostgreSQL、MySQL 与 memory Control Store 具备 suspend/wake 语义；无 Schema 变更。

### Collaboration Client 与角色 Harness

- [x] Agent Runtime 只通过内部 Collaboration Client 调用 Session Service，不直接写 Event Store。
- [x] 内部命令要求 agent-runtime 身份与签名 Lease Assertion；tenant/root/session/run/runtime/role/
  fencing token 均受签名保护，actor 由服务派生。
- [x] Coordinator 工具覆盖 graph/create/dependencies/review/cancel/await/join；Worker/Repair 只发布
  Result，Reviewer 只发布 Review。
- [x] V1 不向模型开放 `delegate` / `handoff`；Service 能力保留，既有 owner 语义冻结。
- [x] Child 工具权限必须是 Root grant 子集；模型不能提交 actor、owner、tenant 或 Lease 字段。
- [x] Worker Result / Reviewer Decision 与对应 `run.completed` 原子追加；未调用角色终态工具时
  fail closed。
- [x] Coordinator 有 Child 时必须 await 或 join；普通文本不能误完成 Root Run。

### 测试、文档与交付

- [x] M4 串行、并行、树形、混合四类 DAG 端到端回归通过。
- [x] M13 覆盖共享 Runtime Pool、串行解锁、Root suspend/wake、签名角色防伪、命令幂等、
  runtime budget、工具白名单和角色 fail-closed。
- [x] Coordinator、Worker/Reviewer 架构文档与代码梳理已同步。
- [x] `ruff check .`、`mypy src/auraclaw`、完整单元测试与 import lint 通过。
- [ ] 本阶段 intentional commit 已完成；待网络恢复后 push，并在 Issue #55 同步实现与验证结果。

## 阶段 M14a：Skill 生命周期持久化基础（Issue #56）

状态：已完成。本阶段只冻结生命周期契约和持久化基础，不接入发布 API、Source Worker、统一
Catalog 切换或 Runtime 撤销动作；后续能力必须分别建立 M14b+ 阶段门禁并独立交付。

依据：[MCP Runtime 能力平面](../architecture/system/23%20MCP%20Runtime%20能力平面.md)；
[GitHub Issue #56](https://github.com/sushaofei/AuraClaw/issues/56)。

### 契约与语义

- [x] Package、Publication、Installation、Source 和 Sync State 使用独立契约。
- [x] Publication 状态不包含 `purged`；物理清理属于 Package retention/tombstone。
- [x] Disable/Uninstall 与安全 Revoke 分离，为后续应用服务保留明确状态。
- [x] `tests/` 首期仅允许声明式测试向量，不执行包内任意代码。

### 持久化

- [x] 新增 PostgreSQL/KingBase migration 和 down migration。
- [x] Package 使用 tenant + publisher + name + version 不可变键，并校验 digest 冲突。
- [x] Publication、Installation 和 Source 使用 revision 乐观并发语义。
- [x] Source Sync State generation 不允许倒退，且与 tenant/source 外键绑定。
- [x] 提供内存 Store 和 PostgreSQL Store，共享稳定 Action Port。

### 范围边界

- [x] 本阶段不改变现有 `SkillPackageRegistry`、Admin API 或 Runtime 行为。
- [x] 本阶段不把 Skill 搜索切换到统一 Catalog，避免未完成投影时破坏既有闭环。
- [x] Publisher Registry、非对称签名、Outbox、租约、发布 CLI/API 和 Artifact GC 后续分阶段实现。

### 质量与交付

- [x] Lifecycle 单元测试、PostgreSQL 集成测试和 migration roundtrip 通过。
- [x] 全量 Ruff、全量 Mypy、完整单元测试、相关集成测试和 10 条 import-linter contract 通过。
- [x] 架构、migration 和阶段门禁文档同步完成。
- [x] 修正 migration target、生产 SQL fail-closed 测试和既有 Ruff 导入基线；Vault 集成恢复并通过。
- [x] MySQL 服务未部署、PostgreSQL 可选 Role 未安装的环境测试不纳入本地完成判定，失败原因已记录。
- [x] 本阶段作为一个 intentional commit 提交并 push，不包含 `.env`、Secret、缓存或无关改动。

## 阶段 M14b：统一 Skill 发布服务与管理入口（Issue #56）

状态：已完成。本阶段建立唯一发布应用服务和小包管理入口，不宣称完成两阶段
Artifact Upload、统一 Catalog、Source Worker、CLI、Outbox 或完整安全准入。

### 发布应用服务

- [x] `PublishSkillCommand` 携带 tenant、actor、source、command、expected revision、correlation 和
  causation context。
- [x] `SkillPublicationService` 统一执行 Source enabled/publisher allowlist、包校验、签名验证、
  Artifact 写入、Package/Publication 持久化和激活 Installation 创建。
- [x] 新包只允许 `staged` 或 `active`；仅允许 `staged -> active` 且使用 Publication revision 乐观
  并发，publish 不能恢复 quarantined/revoked Publication。
- [x] 相同版本相同 digest 幂等恢复；相同版本不同 digest 拒绝；SQL 重启路径复用持久 Artifact Ref。
- [x] 并发相同发布可收敛到同一权威记录；Artifact 写成功但事务失败的回收仍由后续 staged GC 完成。

### 管理入口与安全边界

- [x] 新增 `POST /v1/admin/skill-publications` 小包便捷入口，文件使用校验型 base64、文件数和编码总量上限。
- [x] tenant/actor 只来自已验证 Identity；command/correlation/causation 来自可信 Header/Identity，Body
  不能覆盖。
- [x] Composition 在 SQL 部署选择 PostgreSQL/KingBase Lifecycle Store，在 memory 配置选择测试适配器。
- [x] 平台 HMAC 仍仅为兼容迁移；外部 Publisher Registry、非对称签名和 key rotation 不在本阶段冒充完成。
- [x] 最终两阶段 staged upload、持久命令审计/Outbox、CLI、Source API/Worker 和统一 Catalog 单独验收。

### 质量与交付

- [x] 发布服务覆盖成功、幂等、不可变冲突、staged 激活、revision 冲突和 Source allowlist。
- [x] 管理 API 通过真实 FastAPI Identity 路径验证发布，并出现在 Task API OpenAPI。
- [x] 全量 Ruff、全量 Mypy、完整单元测试、Skill/Capability 回归和 import-linter 通过。
- [x] 架构说明与阶段门禁同步，未把后续 Phase 标记为完成。
- [x] 本阶段作为一个 intentional commit 提交并 push，不包含 `.env`、Secret、缓存或无关改动。

## 阶段 M14c：Skill 生产发布能力平面（Issue #56）

状态：已完成。本阶段修复生产 Artifact 所有权和多进程发布边界；统一 Catalog、
Resolver 持久恢复、staged upload、CLI 与完整治理继续独立交付。

### 服务边界与生产装配

- [x] SQL profile 的 Admin API 通过 `RemoteSkillPublicationClient` 调用 Action Hands，不在 Task API
  进程内写 Skill Artifact。
- [x] Action Hands 装配 production `ArtifactWriter`、持久 Lifecycle Store、统一
  `SkillPublicationService` 和内部发布服务。
- [x] memory profile 保留进程内适配器，测试不依赖远端基础设施。
- [x] Action Hands readiness 要求 Task API workload identity；内部契约同时校验 bearer identity 与
  `context.service_identity`。
- [x] Task API 只传递已验证 tenant/actor 和命令上下文，不传对象存储凭证。
- [x] 返回结果可刷新 Task API 本副本的只读兼容缓存；明确不把该缓存当作跨副本事实源。

### 准入与错误语义

- [x] 内部请求限制文件数和 base64 编码总量，拒绝非法 base64。
- [x] 包路径、Manifest 和签名在不可变版本冲突判断前完成验证。
- [x] 内部 `INVALID_REQUEST` 与公开 `SchemaValidationError` 双向映射，非法包不会退化为 500。
- [x] Action Hands 返回持久 Artifact Ref，Package/Publication/Installation 由同一服务写入。
- [x] 本阶段不删除 Runtime 双目录；必须等持久包读取、Catalog projection 和 Resolver 回查同时就绪。

### 质量与交付

- [x] Task API client -> workload-authenticated internal route -> Action Hands publication -> Artifact/Lifecycle
  端到端单元测试通过。
- [x] 覆盖非法路径的跨服务 422 映射、actor 保存和 Artifact writer 唯一调用。
- [x] 全量 Ruff、全量 Mypy、完整单元测试、Skill/Capability 回归和 import-linter 通过。
- [x] 架构说明与阶段门禁同步，未把统一 Catalog 或 staged upload 标记为完成。
- [x] 本阶段作为一个 intentional commit 提交并 push，不包含 `.env`、Secret、缓存或无关改动。

## 阶段 M14d：Skill 持久恢复与统一 Catalog（Issue #56）

状态：已完成。本阶段消除 Skill 搜索/加载双目录，并保证 Action Hands 重启后可从
Lifecycle + Artifact 恢复 Registry、Resolver 候选和 Catalog 投影；卸载管理 API、staged upload、
Publisher Registry、Outbox/lease 与 Package GC 继续后续阶段。

### 持久恢复与可见性

- [x] Lifecycle Store 可枚举 tenant 和 Installation，PostgreSQL/KingBase 与内存实现语义一致。
- [x] Action Hands 通过 workload identity、Policy 和 Artifact 内部下载契约读取包，不接触对象存储凭证。
- [x] 下载同时限制声明大小、流式实际大小，并校验 Artifact size/hash；Archive、Manifest、digest 和签名
  在进入 Registry 前重新验证。
- [x] 启动、发布后和周期对账都可重建 tenant Skill 状态；单包失败只记录安全错误类型，不记录正文。
- [x] 普通 disable/uninstall 从 Catalog 和 Resolver 候选移除，但 retained active Package 继续支持已有
  binding 按固定 digest 加载。

### 统一目录与来源收敛

- [x] Skill 以 tenant 专属虚拟 Server 投影到持久 Capability Catalog。
- [x] `auraclaw.capabilities.search/load` 只查询 Catalog，不再回退到进程内 Skill Registry。
- [x] Resolver 从相同 Lifecycle 重建出的 Registry 解析，Catalog/Resolver 不再由两个发布入口维护。
- [x] MCP Skill Reconciler 建立持久 Source 并复用 `SkillPublicationService`，不再直接 publish Registry。
- [x] MCP Server 必须配置 `skill_publisher_allowlist`；缺失或非法时 fail closed。

### 质量与交付

- [x] 恢复、Artifact reader、MCP Source、统一搜索/加载和旧 binding 保留语义的单元测试通过。
- [x] 全量 Ruff、全量 Mypy、完整单元测试、相关 PostgreSQL 集成测试和 import-linter 通过。
- [x] 架构说明与阶段门禁同步，未把卸载 API、安全撤销或物理清理标记为完成。
- [x] 本阶段作为一个 intentional commit 提交并 push，不包含 `.env`、Secret、缓存或无关改动。

## 阶段 M14e：Skill 安装管理与安全撤销（Issue #56）

状态：已完成。本阶段交付 tenant 逻辑删除和 Publication 安全撤销，不开放尚不安全的
物理 Artifact purge。

### 状态机与命令

- [x] disable/enable 只改变 Installation，不再把普通停用误写成 Publication revoke。
- [x] uninstall 表示 tenant 逻辑删除，保留 Package、Publication、Artifact 和历史 binding 解释能力。
- [x] install 只允许 uninstalled -> active；enable 只允许 disabled -> active，非法迁移 fail closed。
- [x] revoke 独立更新指定版本 Publication，并立即从 Registry/Catalog 移除，使旧 binding 不能继续加载。
- [x] 所有命令携带 tenant、actor、command、expected revision、correlation 和 causation；停用、卸载与撤销
  必须带 reason code。
- [x] Publication 新增持久 `updated_by`，migration 对历史行以 `created_by` 回填后设为 NOT NULL。

### API 与服务边界

- [x] Admin API 提供 `:install`、`:enable`、`:disable`、`:uninstall` 和版本级 `:revoke`。
- [x] Admin API 可读取持久 Installation/Publication revision、reason 和 updated actor，支持安全的
  If-Match 式写入流程。
- [x] SQL profile 经 Task API -> workload-authenticated internal contract -> Action Hands 执行管理动作。
- [x] Action Hands 使用持久 Lifecycle Store 和 SkillStateRebuilder；Task API 不直接选择数据库或 Artifact。
- [x] 目标状态幂等重试会再次触发投影修复；revision 冲突和非法迁移保持显式错误。
- [x] 不提供伪 purge：Artifact 删除、retention/legal hold、引用检查和 GC 对账必须后续单独验收。

### 质量与交付

- [x] 状态机、API、内部跨服务契约、Catalog 可见性和 revoke 旧 binding 行为测试通过。
- [x] migration roundtrip、PostgreSQL 集成、全量 Ruff/Mypy/unit/import-linter 通过。
- [x] 架构与阶段门禁说明同步，uninstall、revoke 与物理 purge 语义明确分离。
- [x] 本阶段作为一个 intentional commit 提交并 push，不包含 `.env`、Secret、缓存或无关改动。

## 阶段 M14f：Skill Package 安全物理清理（Issue #56）

状态：已完成。本阶段开放版本级物理 purge，同时保留
uninstall、revoke 和 purge 三种不同语义。

### 保留与引用安全

- [x] Package 持久化 retention deadline、legal hold、retention revision、updated actor/time 和 purge tombstone。
- [x] 新发布 Package 的 retention 同时传入 Artifact Service；历史行由 migration 以创建时间加 90 天回填。
- [x] Purge 只允许已 revoke、已 uninstall、超过 revoke 静默窗口、retention 到期且无 legal hold 的版本。
- [x] Session binding 引用检查在 Canonical Event Store 执行 tenant 级 `EXISTS`，并兼容两种已发布载荷。
- [x] 任一历史 binding 都 fail closed；不使用 Projection、Registry 或进程缓存代替事实源。

### 物理删除与恢复

- [x] Action Hands 通过 Policy 和 Artifact 内部契约删除对象，不读取对象存储凭证或直接写 Artifact metadata。
- [x] Artifact Service 独立复核 retention/legal hold，并使用可过期 delete lease 防止并发重复删除。
- [x] 对象 DELETE 404、已 deleted metadata 和 Package purge 重试均幂等；过期 deleting lease 可被接管。
- [x] 物理对象与 Artifact metadata 删除成功后才写 Package `purged` tombstone；optimistic revision 冲突显式返回。
- [x] Admin API 提供 Package retention 状态查询和版本级 `:purge`，要求 reason、idempotency key 与 expected revision。

### 质量与交付

- [x] Purge 前置条件、历史 binding、retention/legal hold、Artifact 删除幂等和 Session 权威查询单元测试通过。
- [x] 0025 正反迁移在隔离 PostgreSQL 验证通过，PostgreSQL Store 覆盖 retention optimistic update。
- [x] 全量 Ruff、Mypy、unit、相关 integration 与 import-linter 通过。
- [x] 架构、数据库参考与阶段门禁同步，明确删除恢复和竞态边界。
- [x] 本阶段作为一个 intentional commit 提交并 push，不包含 `.env`、Secret、缓存或无关改动。

## 阶段 M14g：Skill 两阶段发布与开发者 CLI（Issue #56）

状态：已完成。本阶段交付 staged Artifact 上传、Artifact Ref 发布和安全的
本地 CLI，不把 Publisher Registry、持久 Outbox 或 finalized orphan GC 冒充为已完成。

### 两阶段上传与统一准入

- [x] Admin API 提供 Skill 专用 create/finalize 上传契约，支持单段与 multipart 预签名直传。
- [x] Task API workload 只能创建和完成 `internal`、24 MiB 内、带 retention 的 `skill-upload:*` Skill Artifact。
- [x] Artifact Ref 发布经 workload-authenticated 内部契约交给 Action Hands，并复用同一
  `SkillPublicationService` 完成 Source、allowlist、Archive、Manifest、测试向量、digest 与签名准入。
- [x] staged 发布复用已 finalize 的 Artifact Ref，不再由 Action Hands 写入重复对象。
- [x] base64 小包入口继续兼容，body 不能覆盖可信 tenant、actor 或命令上下文。

### 开发者 CLI 与安全边界

- [x] 新增 `auraclaw skills validate|test|publish`；publish 始终采用 staged 上传。
- [x] validate 使用与服务端相同的确定性包校验；test 仅接受 `tests/*.json` 声明式向量且不执行代码。
- [x] CLI 拒绝 symlink、文件数/大小越界和非平台 publisher；bearer token 只从环境变量读取且错误不打印正文。
- [x] 当前平台 HMAC 仅为兼容路径；Publisher Registry、Ed25519/key rotation 继续 fail closed。
- [x] finalized 但未发布的 ready Artifact 使用保守 retention；安全 orphan 判定/GC、持久幂等审计与 Outbox
  明确留到后续阶段，pending GC 不得直接删除它们。

### 质量与交付

- [x] staged service、公开/内部 API、Task API Artifact 限权和 CLI 单元测试通过。
- [x] 全量 Ruff、Mypy、unit、相关 integration 与 import-linter 通过。
- [x] README、架构说明和阶段门禁同步，已记录仍未完成的供应链治理范围。
- [x] 本阶段作为一个 intentional commit 提交并 push，不包含 `.env`、Secret、缓存或无关改动。

## 阶段 M14h：Skill 发布可靠性与安全孤儿回收（Issue #56）

状态：已完成。本阶段修复发布跨存储失败后的可恢复性，并为未引用的
ready Skill Artifact 建立带 fencing 的物理回收流程；不把成功命令账本描述成完整安全审计。

### 原子发布与恢复

- [x] Package、Publication、首个 Installation、成功命令账本和发布 Outbox 在一个 PostgreSQL 事务内提交。
- [x] command id + request digest 支持跨副本幂等重放；同 command id 不同请求显式冲突。
- [x] 发布前取得 Artifact publication claim，事务提交后绑定 package digest；即时绑定失败由持久 Outbox 重试。
- [x] Action Hands 启动及周期 Reliability Worker 消费 Outbox，修复 Artifact binding 并重建 tenant Registry/Catalog。
- [x] Outbox 使用可过期 claim、`SKIP LOCKED`、退避重试和安全错误类型，多副本不会同时持有同一记录。

### 安全孤儿回收

- [x] Artifact Service 只 claim retention 到期、无 legal hold、未绑定且无有效 publication claim 的 ready Skill Artifact。
- [x] GC 先原子切换 Artifact 为 deleting，迟到 publisher 不能越过 claim；过期 GC lease 可被接管。
- [x] Action Hands 在 claim 后以持久 Package 为权威引用源；有引用修复 bind，无引用经 Policy 后删除。
- [x] staged `skill-upload:*` 与直接 `skill-registry` 路径均受同一回收规则覆盖，包正文不进入错误日志。
- [x] Artifact Service 不反向读取 Hands 数据库，Action Hands 不读取对象存储凭证或直接改 Artifact metadata。

### 质量与交付

- [x] 0026 正反迁移覆盖命令账本、Outbox 和 Artifact fencing 字段，PostgreSQL 事务回滚/重放测试通过。
- [x] 发布恢复、Outbox 多 owner claim、引用修复、未引用删除和 publish/GC 竞态测试通过。
- [x] 全量 Ruff、Mypy、unit、相关 integration 与 import-linter 通过。
- [x] 架构、README、数据库参考与阶段门禁同步，剩余供应链治理范围未误标为完成。
- [x] 本阶段作为一个 intentional commit 提交并 push，不包含 `.env`、Secret、缓存或无关改动。

## 阶段 M14i：Skill Publisher Registry 与 Ed25519 轮换（Issue #56）

状态：已完成。本阶段把外部 publisher 信任根从平台 HMAC 中分离；
本地 CLI、publisher suspension、Source 多副本租约和完整拒绝审计仍留待后续。

### 信任模型与发布准入

- [x] Publisher 与 Key 均按 tenant 隔离持久化，Registry 只保存 Ed25519 公钥，不接触私钥。
- [x] Manifest 声明 `signature_key_id`；签名覆盖规范化 Manifest 与所有包文件 digest。
- [x] 新发布只接受 active key；retiring key 仅恢复已持久化包；revoked key 对发布和恢复都 fail closed。
- [x] Package 持久化实际验签 key id，恢复时同时校验 Manifest、Package record 与 Registry key identity。
- [x] Admin Upload 的外部 publisher 必须先通过 tenant Registry 验签；其他 Source 仍要求显式 publisher allowlist。

### 管理、轮换与恢复

- [x] Admin API 支持 Publisher 注册、状态读取、Ed25519 key rotation 与 key revoke。
- [x] Task API 通过 workload-authenticated internal contract 调用 Action Hands，不直接访问 Hands 数据库。
- [x] rotation 原子地将旧 active key 改为 retiring 并创建唯一新 active key；revision 冲突 fail closed。
- [x] 管理命令携带 tenant、actor、command、expected revision、correlation、causation，并可跨副本幂等重放。
- [x] revoke 在当前副本立即重建 tenant，其他副本通过周期全量重建移除 Catalog/Resolver 可发现性。

### 质量与交付

- [x] 0027 正反迁移覆盖 Publisher、Key 与命令账本；真实 PostgreSQL rotation/rollback/重放验证通过。
- [x] Ed25519 tenant 隔离、篡改拒绝、active/retiring/revoked 语义与 Admin API 测试通过。
- [x] 全量 Ruff、Mypy、unit、相关 integration 与 import-linter 通过。
- [x] README、公开 API、架构、数据库参考与阶段门禁同步，剩余治理范围未误标为完成。
- [x] 本阶段作为一个 intentional commit 提交并 push，不包含 `.env`、Secret、缓存或无关改动。

## 阶段 M14j：Skill Source 多副本对账租约与恢复（Issue #56）

状态：已完成。本阶段让 MCP Skill Source 的周期发现可在多个 Action Hands 副本间安全接管；
远端缺失版本的自动退役仍需独立确认窗口与审计命令，不能由一次快照直接触发。

### 租约与 fencing

- [x] `(tenant_id, source_id)` 持久租约跨副本互斥，过期接管单调递增 fencing token。
- [x] snapshot 和每个 Package 下载后续租；租约丢失后停止后续发布。
- [x] Publication 提交事务验证 owner/token/expiry，旧副本不能用迟到快照写 Package、Publication 或 Installation。
- [x] 成功与失败 Sync State 使用相同 fencing 校验，旧 generation 不能覆盖新同步证据。
- [x] disabled/retired Source 不拉取远端内容；租约释放与过期均可恢复下一轮执行。

### 恢复与可观测证据

- [x] 完整快照保存 source revision、generation、成功/尝试时间，并在恢复后清零失败计数。
- [x] 失败保留最近成功证据，只记录安全错误类型和连续失败次数，不持久化上游响应正文。
- [x] 部分发布后失败不声称快照完成；下一轮通过不可变版本与 command digest 幂等补齐。
- [x] 单次快照缺失不会自动撤销版本，避免瞬时上游缺页直接破坏现有 binding。

### 质量与交付

- [x] 0028 正反迁移覆盖 Source lease、fencing token 与过期扫描索引。
- [x] 内存与真实 PostgreSQL 覆盖双 owner 竞争、过期接管、旧 token 发布/Sync State 拒绝及失败恢复。
- [x] 全量 Ruff、Mypy、unit、相关 integration 与 import-linter 通过。
- [x] README、架构、数据库参考与阶段门禁同步，缺失版本自动退役和完整审计未误标完成。
- [x] 本阶段作为一个 intentional commit 提交并 push，不包含 `.env`、Secret、缓存或无关改动。

## 阶段 M14k：Skill Source 连续缺失确认与审计化退役（Issue #56）

状态：已完成。本阶段只对连续完整快照确认的来源下架执行普通退役；安全撤销仍保持独立语义。

### 缺失确认与事务

- [x] Source inventory 持久记录 last seen generation、连续完整快照缺失次数、首次缺失与检查时间。
- [x] 首次缺失不改变 Publication；连续第二个严格递增 generation 的完整快照仍缺失才触发退役。
- [x] 中间重新观察到版本会清零缺失计数；失败、不完整快照、同 generation 重放和旧 lease 不推进计数。
- [x] inventory、退役命令账本、Publication revision/status 与成功 Sync State 在同一 fenced 事务提交。
- [x] 退役命令记录 tenant、source、actor、correlation、causation、fencing token、reason 与前后 revision。

### Binding 与安全语义

- [x] 新增 `retired` Publication 状态并从 Catalog/Resolver 新候选移除。
- [x] retained 的 retired Package 仍按固定 digest 可读，既有 binding 不因普通来源下架静默漂移。
- [x] `revoked` 继续 fail closed；安全撤销可以从 retired 状态继续执行，不与普通退役混用。
- [x] 同一不可变版本重新出现不会自动激活，避免远端抖动绕过显式恢复治理。

### 质量与交付

- [x] 0029 正反迁移覆盖 inventory、退役命令账本和 Publication 状态约束。
- [x] 单测覆盖首次缺失、重新出现清零、连续缺失退役、Catalog 隐藏与固定内容可读。
- [x] PostgreSQL 覆盖同 generation 拒绝、连续缺失、命令审计及迁移回滚。
- [x] 全量 Ruff、Mypy、unit、相关 integration 与 import-linter 通过。
- [x] README、架构、数据库参考与阶段门禁同步，剩余恢复与完整审计范围未误标完成。
- [x] 本阶段作为一个 intentional commit 提交并 push，不包含 `.env`、Secret、缓存或无关改动。

## 阶段 M14l：Skill Publisher Suspension 信任断路器（Issue #56）

状态：已完成。本阶段提供 tenant Publisher 的可逆紧急停止；永久 key revoke 与普通 Source retired
继续保持独立语义。

### 状态与信任边界

- [x] Publisher 支持 active/suspended 显式转换，suspended 状态要求 reason 与变更时间证据。
- [x] suspend 后 key rotation、新包 admission 和持久包 restore 全部 fail closed。
- [x] resume 不修改 key 状态，只恢复仍为 active/retiring 且签名有效的包；revoked key 不会复活。
- [x] tenant 隔离保持不变，一个 tenant 的 suspension 不改变其他 tenant 的同名 Publisher。

### 命令、API 与恢复

- [x] suspend/resume 命令携带 actor、reason、expected revision、command、correlation 和 causation。
- [x] Publisher 命令账本支持跨副本幂等重放，同 command id 不同可信请求显式冲突。
- [x] Admin API、Task API 远程客户端和 workload-authenticated 内部合约全部贯通。
- [x] 状态变更后立即重建 tenant Registry/Catalog；即时失败可由命令重放和周期全量重建恢复。

### 质量与交付

- [x] 0030 正反迁移覆盖状态 reason/time 证据及 suspend/resume 命令类型。
- [x] 单测覆盖 admission/restore/rotation 拒绝、resume、Admin API 与内部远程契约。
- [x] PostgreSQL 覆盖 revision、跨 Store 幂等、状态证据、rotation 拒绝及迁移回滚。
- [x] 全量 Ruff、Mypy、unit、相关 integration 与 import-linter 通过。
- [x] README、公开 API、架构、数据库参考与阶段门禁同步。
- [x] 本阶段作为一个 intentional commit 提交并 push，不包含 `.env`、Secret、缓存或无关改动。

## 阶段 M14m：Skill Publication 审核化显式恢复（Issue #56）

状态：已完成。退役版本只能经有审计证据的显式 review 与完整信任复验恢复。

### 状态与安全语义

- [x] 普通 `retired` 只能经显式 review 命令进入 `restoring`，不会因 Source 重新出现自动激活。
- [x] `restoring` 不参与 Catalog/Resolver 新发现，但 retained Package 仍按固定 digest 服务既有 binding。
- [x] 激活前从原 Artifact 重读并复验 digest、Source allowlist/状态、Publisher/key 信任与签名。
- [x] `revoked` 不可通过 restore 恢复；复验失败保持 `restoring`，不会短暂暴露为 active。

### 命令、事务与 API

- [x] restore 命令携带 actor、reason、expected revision、command、correlation 和 causation。
- [x] Lifecycle 事务原子写入 `restoring` revision 与独立 restore 命令账本，支持跨副本幂等重放。
- [x] 同 command id 不同可信请求显式冲突；同一失败命令可在信任条件修复后继续激活。
- [x] Admin API、Task API 远程客户端和 workload-authenticated 内部合约全部贯通。

### 质量与交付

- [x] 0031 正反迁移覆盖 restoring 状态约束、restore 审计账本及安全回滚映射。
- [x] 单测覆盖成功、失败保持 restoring、同命令重试、不同 payload 冲突和内部远程契约。
- [x] PostgreSQL 测试覆盖 restore revision、跨 Store 幂等、命令审计及迁移回滚。
- [x] 全量 Ruff、Mypy、unit、相关 integration 与 import-linter 通过。
- [x] README、公开 API、架构、数据库参考与阶段门禁同步。
- [x] 本阶段作为一个 intentional commit 提交并 push，不包含 `.env`、Secret、缓存或无关改动。

## 阶段 M14n：外部 Publisher 离线签名 CLI（Issue #56）

状态：已完成。外部 Publisher 可在私钥不进入 AuraClaw 服务端的前提下完成离线签名与发布闭环。

### 签名与密钥边界

- [x] `auraclaw skills sign` 为非平台 Publisher 生成规范 Ed25519 Manifest 签名并原子替换 manifest。
- [x] private key 仅从指定 Secret 环境变量读取；CLI 不提供明文 key 参数且输出不包含私钥。
- [x] sign 强制显式 publisher/key id，拒绝冒用 `platform` 或与 Manifest publisher 不一致。
- [x] 输出 32-byte raw Ed25519 公钥、key id、Skill identity 和稳定 package digest，便于 Registry 登记。

### 验证与发布闭环

- [x] validate/test/publish 对外部包使用显式公钥离线验签，平台 HMAC 兼容路径保持隔离。
- [x] 外部 publish 仍使用 staged Artifact 与统一发布服务，服务端以 tenant Registry active key 独立验签。
- [x] 本地签名产物由生产 `SkillPublisherTrustService` 验证通过；错误公钥、身份不匹配均 fail closed。
- [x] CLI 继续拒绝 symlink、非规范路径、越界包和任意测试代码，不扩大私钥或 RCE 攻击面。

### 质量与交付

- [x] 单测覆盖签名、原子 Manifest 更新、生产 verifier 兼容、错误 key、platform 冒用和 parser 密钥边界。
- [x] 全量 Ruff、Mypy、unit、相关 Skill 回归与 import-linter 通过。
- [x] README、架构、公开 API 指南与阶段门禁同步，完整拒绝/安全审计未误标完成。
- [x] 本阶段作为一个 intentional commit 提交并 push，不包含 `.env`、Secret、缓存或无关改动。

## 阶段 Product Activity：产品级对话执行轨迹（AuraX Issue #2）

状态：代码与定向验证完成；完整基础设施集成门禁受现有 Kafka/MySQL/PostgreSQL/Artifact 环境失败阻塞。

### 范围与架构

- [x] 产品 Activity、聊天 transcript、权威 result 与运维 timeline 的职责已分离。
- [x] Canonical Session Events 是唯一事实源；Activity 可重建，SSE 只做失效通知。
- [x] 节点 taxonomy、生命周期折叠键、增量游标和多 Run 隔离规则已定义。
- [x] 模型输入只记录安全证据，不持久化完整 system prompt、Skill 正文或 Chain-of-Thought。
- [x] `model.input.prepared` 与 MCP/Capability 来源元数据已落入稳定事件。
- [x] `GET /v1/tasks/{session_id}/activity` 支持脱敏、大小限制、增量读取和租户隔离。
- [x] 无数据库 Schema 变更：读取模型从 Canonical Event 重建，可后续独立物化且不改变 API。

### 功能与质量

- [x] Tool、Skill、Approval 和 Model 生命周期按稳定 ID 正确折叠。
- [x] 多 Run、失败、取消、重试、旧事件缺字段和超大详情测试通过。
- [x] Task API、公开 API 手册与 AuraX SDK 契约同步。
- [x] Ruff、Mypy、Activity/Runtime 定向 Pytest 通过。
- [ ] 完整 Pytest：功能测试通过；9 项既有基础设施集成测试因 Kafka 无消息、MySQL 断连、
  PostgreSQL 角色/残留数据与 Artifact GC 环境失败，需恢复环境后复跑。

### 交付

- [x] AuraX 产品面板、SDK、响应式与 E2E 验收完成。
- [ ] Git 暂存范围已与现有在途改动隔离，无 Secret、缓存或环境文件。
- [ ] 本阶段作为 intentional commit 提交并 push。
