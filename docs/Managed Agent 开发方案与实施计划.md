# Managed Agent 开发方案与实施计划

> 版本：0.2
> 日期：2026-07-17
> 依据：[Managed Agent 系统架构图](./Managed%20Agent%20系统架构图.png) 与 [Managed Agent 系统架构设计](./Managed%20Agent%20系统架构/00%20Managed%20Agent%20系统架构总览.md)
> 当前状态：已重构为纯 Python 后端，M0 与 M1 的内存竖切版本已完成

## 1. 结论与实施策略

AuraClaw 不应从现有 CLI Agent 直接扩展成一组相互调用的微服务。正确的落地顺序是：

1. 先建立统一契约、Canonical Event Log 与 Transactional Outbox，形成不可丢失的任务事实核心。
2. 再建立 Projection、Read Model 和只读查询，让控制面与用户端不再扫描会话历史。
3. 将现有 AgentLoop 改造成无状态、可租约控制、可恢复的 Agent Runtime。
4. 接入 Orchestrator、Control State Store、Tool Gateway、Hands、Artifact 和 Policy，形成安全的执行闭环。
5. 最后增加 Child Session DAG、Coordinator/Reviewer、实时流和可靠结果交付。

MVP 采用“模块化单体 + 独立 Worker + PostgreSQL”的部署方式，保留所有逻辑边界，不在第一阶段引入完整微服务和 Kafka 集群。Runtime Event Bus 先提供内存/数据库适配器，生产扩容时再替换为 NATS JetStream 或 Kafka。这样能以最小运维成本验证事件、状态、恢复和幂等语义，同时避免形成无法拆分的共享数据库写入。

建议以 4 名工程师组成的核心团队执行，目标为 10～12 周完成可部署 MVP；若只有 2 名工程师，预计需要 18～22 周。计划中的工期是工程估算，不包含采购、合规审查和外部系统接入等待时间。

## 2. 当前基线

### 2.1 已具备能力

- Python 3.11+、FastAPI、Pydantic 与 uv 工程基线。
- Task 创建、查询、取消和恢复 HTTP API。
- Session Aggregate、状态转换与 Canonical Event 信封。
- 内存 Event Store、Command Dedup 与 Transactional Outbox 语义。
- 可重建的 Task Projection、Projection Version、ETag 和 `min_version`。
- PostgreSQL 的 Session、Outbox、Projection、Control 和 Delivery 初始 Schema。
- 单元测试、API 集成测试、Ruff 与 Mypy 严格检查。

### 2.2 主要差距

| 架构能力 | 当前状态 | 关键差距 |
|---|---|---|
| Task Gateway | 最小 API 已实现 | 待接真实身份认证、配额和完整 Admission |
| Session/Collaboration | Root Session 事件核心已实现 | 待接 PostgreSQL、Snapshot、Child/DAG |
| Projection/Read Model/Query | 内存 Task Projection 已实现 | 待改为异步 Worker、持久读模型和全量重建工具 |
| Runtime Event/Streaming | 未实现 | 无事件信封、序列、重放、SSE、背压和可见性过滤 |
| Orchestrator/Control Store | 未实现 | 无 runnable queue、lease、fencing、assignment、reconcile |
| Agent Runtime Pool | 进程内 Agent 对象 | 状态未完全外置，无 run/lease/checkpoint/budget/cancel 语义 |
| Coordinator/Worker/Reviewer | 未实现 | 无角色合同、Child Session、join、review/repair 流程 |
| Model Gateway | 未实现 | 无 Provider、统一路由、预算和用量核算 |
| Tool Gateway/Hands | 未实现 | 无权限、审批、稳定 invocation id、外部副作用状态和隔离 |
| Artifact Store | 未实现 | 大对象只能进入消息或本地文件，无不可变版本和引用 |
| Policy/Approval | 未实现 | 无 action digest、审批状态机和执行前二次校验 |
| Credential Proxy/Vault | 未实现 | 生产接入前必须确保 Runtime 无法接触 Secret |
| Result Delivery | 未实现 | 无 Delivery Job、Sink Adapter、重试与 DLQ |
| Observability/Audit | DEBUG/回调 | 无统一关联 ID、Trace、Metrics 和高风险动作审计 |
| 工程质量 | 基础检查与测试已建立 | 待补 CI、契约兼容性、PostgreSQL 集成和故障恢复测试 |

### 2.3 已完成的代码基线重构

旧 TypeScript、Node、CLI 与重复 Agent 目录已经移除。项目现在只有一个 Python `src/auraclaw` 源码入口；原有 `.env` 被保留且不提交，所有示例配置均不包含真实凭证。

## 3. MVP 边界

### 3.1 MVP 必须交付

- API 创建、查询、取消、恢复任务，写命令具备租户作用域和幂等键。
- PostgreSQL Canonical Event Log、Aggregate Version、Snapshot 和 Transactional Outbox。
- Control、Collaboration、Result、Approval 四类 Projection，可删除重建。
- 单 Root Session 与有限深度的 Child Session DAG。
- Orchestrator 的 runnable、lease、fencing token、assignment、heartbeat 和 reconcile。
- 可恢复 Worker Runtime；Coordinator 和 Reviewer 使用同一 Harness 的不同 Role Profile。
- Model Gateway 统一访问 Anthropic/OpenAI 兼容 Provider。
- Tool Gateway 与本地 Hands Runtime，至少覆盖现有三个工具。
- Artifact 元数据、对象存储适配和不可变版本。
- 高风险工具审批，批准绑定 action digest。
- Runtime Event Bus 抽象、SSE、短期重放和断线恢复。
- Webhook Result Sink，具有 Delivery Job、稳定 delivery_id、重试与 DLQ。
- OpenTelemetry 风格的关联字段、核心指标和审计时间线。
- Runtime、Orchestrator、Projection 或 Delivery Worker 重启后任务可恢复。

### 3.2 MVP 暂缓

- 多区域和跨地域容灾。
- Kafka/Pulsar 的生产级大规模集群；先以可替换适配器实现。
- Email、消息队列等全部 Result Sink；MVP 只实现 Webhook 与 Parent Session。
- 企业级 Vault 产品深度集成；先实现 Credential Proxy 接口和本地开发适配器，生产环境必须接真实 Vault/KMS。
- 任意深度、无限宽度的 Agent DAG；MVP 设置深度、子任务数和预算上限。
- 完整 Web 管理后台；仅提供任务、时间线、审批和结果的最小界面。
- 自动跨 Provider 智能路由；MVP 使用显式 Model Policy 加有限故障回退。

## 4. 技术与边界决策

### 4.1 初始部署拓扑

```text
apps/gateway
  Task Gateway
  Task Query / Result API
  Streaming Gateway

apps/workers
  Outbox Relay
  Projection Workers
  Orchestrator
  Result Delivery Workers

apps/runtime
  Agent Runtime Pool
  Hands Runtime

PostgreSQL
  session schema      Canonical Event Log / Snapshot / Outbox
  projection schema   Read Models / Projector Checkpoints
  control schema      Queue / Lease / Assignment / Capacity
  delivery schema     Delivery Jobs / Attempts

Object Storage
  local filesystem adapter for development
  S3-compatible adapter for production
```

允许上述模块合并进程，但必须遵守：单一写入者、独立 Schema、无跨域外键、无跨边界事务、通过接口或事件通信。未来拆服务时不改变领域契约。

### 4.2 Python 模块结构

```text
src/auraclaw/
  api/                # Task Gateway、Query 和未来 Streaming HTTP 边界
  application/        # 命令与查询用例编排
  contracts/          # ID、命令、事件信封、状态、错误、schema version
  domain/             # Session/Collaboration Aggregate 与端口
  infrastructure/     # Event Store、Outbox、Artifact、外部适配器
  projections/        # projectors、read model、checkpoint/rebuild
  runtime/            # control plane、Agent Runtime、model/tool gateway 端口
  policy/             # 后续增加策略与审批领域
  delivery/           # 后续增加可靠交付 Worker
migrations/           # PostgreSQL Schema 迁移
tests/                # unit、integration、contract、failure injection
```

部署仍可拆为 Gateway、Workers 和 Runtime 三类进程，但共享同一 Python 包。Agent Runtime 只能依赖契约和客户端接口，不能反向拥有 Session、Gateway 或 Orchestrator 实现。

### 4.3 存储选型

| 用途 | MVP | 扩展选择 |
|---|---|---|
| Canonical Event/Outbox | PostgreSQL | PostgreSQL 分区/归档 |
| Read Model | PostgreSQL JSONB + 索引 | 读副本、OpenSearch |
| Control State | PostgreSQL 原子更新/行锁 | 强一致 KV 或独立 PG |
| Runtime Event Bus | 统一接口 + 开发内存适配器；部署环境用 PG Notify/JetStream 二选一 | Kafka/Pulsar/JetStream |
| Artifact | 本地文件适配器 | S3/MinIO + KMS |
| Secret | 开发环境受控适配器 | Vault/云 Secret Manager |

所有适配器必须通过同一契约测试，业务代码不得依赖特定中间件 API。

### 4.4 关键一致性规则

- Session 写入：`expected_version` + 单 Aggregate 事务追加。
- 事务内同时写 Event、Aggregate Version 和 Outbox。
- 命令幂等：`tenant_id + operation + idempotency_key` 唯一。
- Projection：at-least-once、按 Session version 有序、event_id 去重、发现 gap 停止该 Aggregate。
- 调度：原子 claim + lease + 单调 fencing token；执行端校验 token。
- 工具：稳定 `tool_invocation_id` 和 idempotency key；副作用不确定时返回 `unknown`。
- 审批：`approval_id + session_id + action_digest + policy_version` 全量匹配。
- 交付：Outbox 保证触发不丢，Delivery Job 保证投递过程可恢复。

## 5. 里程碑计划

### M0：工程基线与共享契约（第 1 周）

目标：让后续模块基于一套可测试、可演进的公共语言开发。

交付物：

- 清理或迁移 `packages/agents` 中未参与编译的重复目录。
- 引入单元测试、集成测试、代码检查、数据库迁移和 CI。
- 新建 `packages/contracts`，定义统一 ID、Command、Canonical Event、Runtime Event、状态机和错误模型。
- 使用运行时 Schema 校验，并建立 schema version 与兼容性测试。
- 为所有请求预留 tenant、actor、correlation、causation、run 和 visibility 字段。

验收：

- 构建、检查、测试和迁移校验可一条命令完成。
- 非法状态转换、缺失 tenant、未知关键事件和不兼容 Schema 均被测试拒绝。
- 原有 CLI 单 Agent 回归通过。

### M1：事实核心与查询闭环（第 2～3 周）

目标：先跑通 `POST task → Canonical Event → Projection → GET task`。

交付物：

- Session Aggregate、状态转换、Event Store、Snapshot 和 Transactional Outbox。
- create、append message、request run、cancel、resume 命令。
- Task Gateway 的最小 HTTP API、幂等表和 Admission 接口。
- Outbox Relay、Control/Result Projector、Checkpoint、幂等与 gap 检测。
- Read Model DAO 和 Task Query API，支持 projection version、ETag、min_version。
- Projection rebuild 管理命令。

验收：

- 100 个并发相同幂等请求只产生一个 Root Session。
- Event 与 Outbox 不能出现单边提交。
- 删除 Read Model 后可以完整重建，重复投递不产生重复记录。
- Gateway 提交超时后重试返回同一个 session_id。

### M2：Managed Runtime 与控制平面（第 4～5 周）

目标：把现有 Agent 从进程内对象改造成可调度、可恢复的 Runtime。

交付物：

- Control State Store：runnable queue、lease、fencing、assignment、heartbeat、capacity。
- Orchestrator：watch、claim、schedule、provision、cancel 和 reconcile。
- Agent Runtime Client：Session、Model、Tool、Runtime Event 四类端口。
- Harness 支持 run_id、lease、budget、deadline、cancel 和 checkpoint。
- Model Gateway 封装现有 Provider Adapter，Secret 不再由 Agent Runtime 读取。
- 完整模型输出写 Canonical Event；Token Delta 进入 Runtime Event Bus。

验收：

- 两个 Orchestrator 竞争同一任务只有一个取得执行权。
- Lease 过期后的旧 Runtime 写入或执行工具被 fencing token 拒绝。
- 在模型调用前、模型完成后、工具前后四个注入点杀死 Runtime，任务均能恢复到可解释状态。
- 更换模型 Provider 不修改 Session 和 Orchestrator。

### M3：工具、Artifact 与审批安全闭环（第 6～7 周）

目标：Agent 无法绕过受控边界执行动作或读取真实凭证。

交付物：

- Tool Registry、Schema 校验、权限模型、Dispatcher、Result Normalizer。
- Hands 本地 Sandbox/Process/File 执行适配器，将现有工具迁入 Hands。
- Artifact Metadata、对象适配器、Hash、版本、lineage 和受控下载。
- Policy 接口、风险分类、action digest、Approval Aggregate 与 Projection。
- Human Response 通过 Task Gateway 写入 Session。
- Credential Proxy 接口、credential_ref 和日志/结果脱敏。

验收：

- write/destructive 工具没有有效审批时无法执行。
- 修改任一参数后旧审批失效。
- 相同工具幂等键不会重复产生外部副作用。
- Sandbox 环境、Session Event、Tool Result 和日志中均不存在真实 Secret。
- 大型工具输出只通过 artifact_ref 进入 Session。

### M4：多 Agent 协作与评审（第 8～9 周）

目标：实现有限但完整的 Root/Child DAG 协作。

交付物：

- Collaboration Aggregate：Child、dependency、delegate、handoff、publish result、join。
- DAG 无环、同 Root/tenant、深度/宽度/预算限制。
- Collaboration Projector 与 runnable 计算。
- Coordinator Role：分解、合同、join、汇总、动态修复。
- Worker Role：按 Output Contract 产出并发布 Child Result。
- Reviewer Role：独立上下文、证据和 accepted/changes_requested/rejected 决策。

验收：

- 串行、并行、树形和混合四类 DAG 场景通过端到端测试。
- Coordinator 重启不会重复创建同一 Child。
- Worker 不能写 Root，Reviewer 不能覆盖 Worker Artifact。
- Root Result 可以追溯到 Child Result、Review 和 Artifact lineage。

### M5：实时体验与可靠交付（第 10 周）

目标：网页断线不影响任务，服务停机不丢最终通知。

交付物：

- Runtime Event Producer SDK、sequence、visibility、大小限制和 Token 合并。
- Streaming Gateway SSE、订阅授权、Last-Event-ID、重放、背压和慢连接处理。
- Delivery Job Store、Webhook/Parent Session Sink、签名、重试、Circuit Breaker 和 DLQ。
- Delivery 状态回写 Session 并投影到查询接口。

验收：

- 关闭或重启 Streaming Gateway 不取消任务。
- 断线重连能在保留期内补齐事件；过期时明确回退到 Query。
- Delivery Worker 停机后恢复仍能投递已提交结果。
- 重复 Outbox Event 不产生重复业务交付。

### M6：可靠性、观测与发布门禁（第 11～12 周）

目标：达到可灰度发布和可运维标准。

交付物：

- 跨组件 Trace Context、核心 Metrics、结构化日志、Audit Event Store。
- Session Timeline 与失败定位视图。
- 故障注入：数据库短断、Projection 落后、Runtime 崩溃、Lease 丢失、Tool unknown、Delivery 5xx。
- 数据保留、Artifact GC、Outbox/Projection/Delivery DLQ 运维工具。
- 容量测试、SLO、告警、Runbook、安全检查和灰度方案。

验收：

- 架构总览中的七项完成标准全部自动化覆盖。
- 任一失败可由 root/session/run/event/tool/delivery/approval ID 串联定位。
- Projection lag、lease lost、unknown side effect 和 delivery DLQ 均有告警。
- 日志与 Trace 的 Secret 扫描为零命中。

## 6. 工作流与团队并行方式

建议四条工作流并行，但通过里程碑门禁合并：

| 工作流 | 主要负责 | 前置依赖 |
|---|---|---|
| A：事实与查询 | Contracts、Session、Outbox、Projection、Query | M0 |
| B：控制与 Runtime | Control Store、Orchestrator、Agent/Model Gateway | M0；M2 集成依赖 M1 |
| C：行动与安全 | Tool Gateway、Hands、Artifact、Policy、Credential | M0；M3 集成依赖 M1/M2 |
| D：体验与运维 | Streaming、Delivery、Web、Observability、CI | M0；M5 集成依赖 M1/M2 |

接口可以并行实现，但任何跨工作流集成必须使用 `packages/contracts` 中已版本化的契约，禁止通过直接访问对方数据库表“临时打通”。

## 7. 首个两周迭代拆解

### Sprint 1：基线与 Event Store

1. 确认并清理未编译的重复 agents 目录。
2. 建立测试框架、CI、代码检查与 PostgreSQL 测试容器。
3. 创建 contracts 包并实现 ID、CommandEnvelope、CanonicalEventEnvelope、RuntimeEventEnvelope。
4. 实现 Session 状态机和表驱动状态转换测试。
5. 设计并迁移 event_stream、session_head、snapshot、command_dedup、outbox 表。
6. 实现 append(expected_version)、load、getEvents 和命令幂等。
7. 验证 Event + Version + Outbox 原子提交。

### Sprint 2：API、Projection 与 Query

1. 实现 Task Gateway 的 create/cancel/resume API。
2. 实现 Outbox Relay 与 Control/Result Projector。
3. 实现 projector checkpoint、event_id 去重和 version gap 检测。
4. 实现 task/result Read Model DAO 与查询 API。
5. 实现 ETag、min_version、Retry-After 和 tenant authorization hook。
6. 实现 projection rebuild CLI。
7. 建立第一个端到端测试：创建任务后最终在 Query API 可见。

两周结束时不要求调用模型；要求“事实写入、派生、读取、重建和幂等”全部可靠。模型执行只在此基础上接入。

## 8. 测试策略

### 8.1 测试金字塔

- 单元测试：状态机、DAG、action digest、错误分类、重试判定、权限交集。
- 契约测试：每个 Store/Bus/Provider/Hands/Sink Adapter 共享同一测试套件。
- 集成测试：真实 PostgreSQL 事务、并发 claim、lease/fencing、outbox/projector。
- 端到端测试：Task API 到 Query/Stream/Delivery 的完整路径。
- 故障注入：在每个持久化边界前后终止进程并验证恢复。
- 安全测试：跨租户访问、审批复用、Secret 泄漏、Sandbox 越界、Artifact URL 过期。

### 8.2 必须长期保留的架构回归用例

- 并发相同 idempotency key 只创建一个 Root Session。
- Projection 全删重建结果一致。
- 多 Orchestrator 单 Session 单所有者。
- 旧 fencing token 永远无法写入或产生副作用。
- Runtime 在任意步骤崩溃后恢复，完成事实不丢。
- Human Response 只能走 Task Gateway。
- Runtime Event 丢失不影响最终查询与交付。
- Delivery 重启不丢、不重复业务效果。
- Approval 不能跨 Session、参数或 policy version 复用。
- Agent/Sandbox 无法读取真实 Secret。

## 9. 发布与迁移策略

### 9.1 纯后端边界

旧 CLI 与嵌入式 Agent 已移除，不再承担兼容路径。所有用户、Timer 和集成端均通过 Task Gateway 提交命令，通过 Query、Streaming 或 Result Delivery 获取状态和结果；开发工具也必须调用相同的后端契约。

### 9.2 灰度顺序

1. 将当前内存 Event Store/Projection 替换为 PostgreSQL 适配器并做影子对照。
2. 内部任务使用 managed 单 Agent，无外部写工具。
3. 开启只读工具和 Artifact。
4. 开启审批后的写工具。
5. 开启 Child DAG 与 Reviewer。
6. 开启外部 Webhook Delivery。

每一步都以错误率、恢复率、Projection lag、unknown side effect 和 Secret 扫描作为继续放量门禁。

## 10. 风险与控制

| 风险 | 影响 | 控制措施 |
|---|---|---|
| 一开始拆成过多服务 | 交付慢、联调和运维成本高 | 模块化单体起步，逻辑边界先于部署边界 |
| 把消息历史直接升级成事件源 | Event 语义混乱、无法演进 | 新建领域事件；旧 Session 只做导入适配 |
| Projection 被当事实源 | 恢复和一致性失败 | 唯一写入者、可删除重建、版本水位测试 |
| Runtime 重试重复副作用 | 外部数据损坏 | invocation id、幂等键、unknown 状态、审批/人工决策 |
| Orchestrator 与 Coordinator 职责混淆 | 调度层耦合自然语言与 DAG | 用接口和权限强制分离语义规划与资源调度 |
| Secret 继续进入 Agent 环境 | 严重安全风险 | Model/Tool Credential Proxy，泄漏扫描，生产 fail closed |
| 实时流被误作可靠事实 | 结果或通知丢失 | Canonical Log + Outbox + Delivery Job；Bus 仅短期实时 |
| 公共契约过早僵化 | 演进成本高 | schema version、兼容字段、契约测试、Poison Queue |
| DAG 无限制扩张 | 成本和资源失控 | 深度、宽度、并行度、Token、时间和成本预算 |

## 11. Definition of Done

任一功能只有同时满足以下条件才算完成：

- 明确唯一写入方、事实源和可重建边界。
- 命令、事件、错误和状态具有版本化契约。
- tenant、授权、visibility、脱敏和审计已经实现。
- 幂等、并发、超时、取消、重试和恢复路径有测试。
- 指标、Trace 和结构化日志包含统一关联字段。
- 不通过跨模块表写入、全局配置或进程内不可恢复状态完成业务闭环。
- 文档、迁移、Runbook 和回滚方案随代码提交。
- 对照 `docs/开发阶段校验清单.md` 完成该阶段的功能与质量门禁。
- 阶段内容使用独立 Git commit 提交，并将当前分支 push 到 `origin`。

## 12. MVP 成功指标

- Canonical append 可用性 ≥ 99.9%，P95 延迟 < 100 ms（不含跨区域）。
- Projection 正常负载下 P95 新鲜度 < 2 s。
- runnable 到 runtime started 的 P95 < 5 s（不含冷启动大型 Sandbox）。
- Runtime 非计划终止后的 P95 恢复时间 < 30 s。
- SSE 实时事件 P95 端到端延迟 < 1 s。
- Webhook 在目标 Sink 可用时 99% 于 60 s 内成功。
- 幂等、审批越权和过期 fencing token 的重复业务效果为 0。
- Session、日志、Trace、Tool Result 和 Sandbox 中真实 Secret 泄漏为 0。

这些指标应在 M0 确认测试环境和负载模型后固化为 SLO；若实际业务量级不同，调整数值但不降低一致性与安全完成标准。
