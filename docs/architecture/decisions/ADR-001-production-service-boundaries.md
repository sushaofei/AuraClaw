# ADR-001：生产服务部署边界与通信契约

- 状态：Accepted（Issue #12 / S0）
- 日期：2026-07-22
- 适用范围：AuraClaw 正式开发 S0–S5
- 架构真源：`docs/architecture/system/`

## 背景

AuraClaw 已完成 MVP 与模块化单体包边界重构。当前 `auraclaw serve` 仍在同一进程装配 Task API、
Session 写路径、Streaming、Orchestrator、Agent Runtime 和 Model Gateway；Projection 只有独立 CLI，
Delivery、Hands、Policy、Credential Proxy 与 Artifact 尚无生产服务入口。

包边界已经降低拆分成本，但它不能提供进程故障隔离、独立扩缩容、数据库写权限隔离或 Secret 信任域。
Issue #12 将生产部署边界固定为 12 服务。本 ADR 先冻结服务所有权、通信方式、数据边界、兼容策略和迁移顺序，
避免先增加入口、后补契约而形成多个直连同一数据库的“伪微服务”。

## 决策

### 1. 生产拓扑

生产使用同一 monorepo、同一应用镜像和 12 个不同 entrypoint：

| 服务 | 目标入口 | 唯一职责 |
|---|---|---|
| task-api | `auraclaw api run` | 公开 Admission/Command/Query/Ops；不拥有业务 Store 写权限 |
| session | `auraclaw session run` | Canonical Session/Collaboration 写入、Snapshot、Session Outbox |
| projection-worker | `auraclaw projection relay --watch` | Projection 与 Runnable Outbox 唯一写入者 |
| orchestrator | `auraclaw orchestrator run` | Control Store、Lease、Assignment、Checkpoint、Reconcile |
| agent-runtime | `auraclaw runtime run` | Agent Harness；只使用 Session/Control/Model/Hands/Artifact 客户端 |
| model-gateway | `auraclaw model-gateway run` | Model Provider、Secret、Quota、Usage、Fallback 信任域 |
| action-hands | `auraclaw hands run` | 托管 Hands Gateway、下游 Connector、Hands/Sandbox、Invocation/Attempt/Side Effect |
| policy | `auraclaw policy run` | Policy/Approval/Budget/Egress 决策与证据 |
| credential-proxy | `auraclaw credential-proxy run` | Vault/KMS 引用、代理调用、签名、刷新、Credential Audit |
| artifact-service | `auraclaw artifact run` | Artifact Metadata/ACL/Version/Lineage/Scan；SeaweedFS 对象访问 |
| streaming-gateway | `auraclaw streaming run` | Runtime Event Replay/Router 与 SSE；不接收业务命令 |
| delivery-worker | `auraclaw delivery run` | Delivery Outbox、Job/Attempt、Retry/DLQ 与 Sink |

`auraclaw serve` 只保留为 development combined profile，不是生产拓扑。服务可以共享 PostgreSQL/Kafka 集群，
但不能共享 schema owner、超级用户凭证或跨 Store 事务。

### 2. 同步与异步通信

- Session、Control、Model Gateway、Policy、Credential Proxy、Artifact 与 Admin 使用 HTTP/JSON、OpenAPI 和 typed client。
- Runtime 到 Action Hands 使用协议无关的内部 HTTP/JSON（`/internal/v1/hands/*`）。下游 MCP 仅作为 Hands Connector。
- Session Transactional Outbox 保证 Projection/Delivery 触发；Projection Transactional Runnable Outbox 保证调度触发。
- Runtime Event Bus 只承载 Token Delta、Typing、实时进度等可丢弃事件，不保证结果交付。
- 所有消费者采用至少一次传递、稳定业务幂等键、显式 claim/ack/nack、visibility timeout 和 poison/DLQ。

每个内部请求必须传播 `tenant_id`、service identity、trace、correlation、causation。所有写请求还必须携带稳定
`command_id` 或 `operation_id`；Session append 携带 `expected_version`；Runtime/Hands 请求携带 Lease Assertion、
`lease_id` 与 `fencing_token`。

### 3. Session 与 Fencing

Session 是 Canonical Event Log 的唯一数据库写入者。Task API、Orchestrator、Runtime、Policy、Hands、Delivery 通过
身份受限的 Session Internal API 写入，每个身份只有允许的 operation/event allowlist。

Orchestrator 签发短期 Lease Assertion，至少包含 audience、tenant/session/run、lease id、fencing token、expiry 和 key id。
Session 与 Hands 使用轮换公钥验签，并在持久层维护每个资源已接受的最高 fencing token。旧 token 不能覆盖新 Checkpoint、
追加 Canonical Event 或继续外部副作用。

高水位分别由 `session_core`、`hands` 和 `control` owner schema 按 `(tenant_id, resource_id)` 持久化。
接受 token 使用单条原子 upsert：相同 token 幂等，更高 token 单调推进，低 token 即使落到其他副本或服务重启后也被拒绝。
Control 的 Assignment/Lease 数据仍是执行归属权威来源；其高水位只加强 assertion replay 防护，不替代当前 lease 校验。
生产 Session、Orchestrator 和 Hands 缺少 SQL-backed ledger 时必须拒绝启动，不能回退到进程内 ledger。

### 4. Query 与 Runnable

生产 Query 采用以下唯一方案：

- Task Query/Result 归属 `task-api`，使用 `task_query_ro` 只读 Projection schema。
- Projection schema 是版本化 N/N-1 读取契约；迁移遵守 expand/migrate/contract。
- Projection Worker 在更新 Read Model 的同一事务内写 Runnable Outbox。
- Runnable Relay 通过 Orchestrator Internal API 投递 `tenant/session/run/source_version`；Orchestrator 写 Control Store。
- Orchestrator、Runtime、Delivery、Hands 不直接读取 Projection 数据库；需要查询时调用内部 Query API。

### 5. Orchestrator 与 Runtime

Orchestrator 独占 Control Store 写权限。Control API 包含 Runtime register/heartbeat/capacity/drain、Assignment claim/ack、
Lease renew、finish/fail、cancel observation、`save/load checkpoint`、`isCancelled` 和 `validateLease`。

Checkpoint 必须携带 fencing token，保存 phase、resume cursor、Artifact references 和安全可恢复 Harness state。
生产调度不得扫描完整 Event Log；Orchestrator 只消费 Runnable Feed 并通过 Reconcile 恢复过期 Lease 与 Assignment。

### 6. Action Hands 与下游 Connector

Agent Runtime 只依赖协议无关的 Hands Contract。生产使用内部 HTTP/JSON
（`/internal/v1/hands/*`），开发 combined profile 使用 in-process Adapter。
Runtime 的网络策略禁止直连第三方 MCP 或 Java API endpoint。下游 MCP Server 与 Java REST API
只能作为 Hands 的受管 Connector 接入。决策见
[ADR-002](./ADR-002-hands-boundary-and-connectors.md)。

tenant、session、run、lease 与 fencing 只能从 workload identity 与 signed lease assertion 恢复，
不能信任请求 body 或模型可控 arguments。公开 Task API 的 tenant/user 同样不能信任裸
`X-Tenant-ID` / `X-Actor-ID` 或请求体；生产必须从 chaintower workload + 短期签名 Assertion
恢复。决策见 [ADR-003](./ADR-003-trusted-identity-context.md)。`tool_invocation_id` 是业务幂等键。Hands Invocation Store
持久保存 Invocation、Attempt、normalized argument digest、result/artifact refs、status、lease、
deadline 和 `side_effect_status`。连接断开不等于取消。MCP Task/Progress 不替代该 Store、
Canonical Event 或 Delivery Job。

Invocation Store 同时持久保存 execution owner、claim token、heartbeat/expiry、取消请求和等待审批的
标准化结果。只有未过期 claim owner 可以 dispatch 和提交结果；`accepted` claim 过期可在副作用前重领，
`executing` claim 过期必须进入 `unknown_side_effect` 人工恢复。Cancel 以 tenant-scoped 共享状态跨副本
传播，状态查询读取 Store。进程内 task、cache 与 keyed lock 不是生产正确性依据。

下游 MCP 默认使用 `2026-07-28` 无状态 profile；`2025-11-25 initialize` 仅作为显式 legacy Connector
配置保留。Java API Connector 只允许已注册 operation 的 method/path template，禁止模型覆盖
scheme、host、port、method 或 headers。

MCP Lifecycle 管理命令在 Runtime apply/revoke 前按 tenant/command digest 原子 claim。Enable、Disable、
Retire 与 Reconcile 先持久提交 desired-state intent，再由各 Hands 副本 reconcile 实际 Connector、Catalog
和 Egress；即时副作用失败进入 `reconciling`，claim owner 失联进入 `unknown_side_effect`，均不盲目重放。

### 7. Policy 与 Approval

Policy 是 Task Admission、Model/Data Residency、Runtime Placement、Tool/Side Effect、Delivery Egress、Artifact Download/Share
的强制决策平面。接口包含 evaluate、request/validate/cancel/expire approval 和 record human response。

Approval 绑定 `approval_id + tenant/session/run + action_digest + policy_version`。重要状态通过 Session API 写 Canonical Event；
Human Response 只经 Task API -> Session。Policy 不可用时，高风险、写入、凭证和外发操作 fail closed；只读降级必须由显式
版本化策略声明。

### 8. Credential Proxy

Credential Proxy 提供受控 invoke、sign、initializeResource、revoke 和 usage audit，不提供返回明文 Secret 的 API。
Hands/Delivery 使用 workload identity、`credential_ref`、Policy decision 与 operation scope 调用。Proxy 负责 Vault/KMS resolve、
OAuth refresh、目标/方法 allowlist、请求签名、响应脱敏和 unknown-side-effect 证据。Model Provider Secret 留在 Model Gateway/Vault
信任域。Vault 不可用时写操作 fail closed。

Production composition 必须配置外部 Vault/KMS，禁止 `InMemoryVault` 与 debug secret fallback；缺少地址、
token 或连接不可用时启动/readiness fail closed。受管 Java Connector 的 Credential Reference 在服务
initialize 阶段幂等写入 PostgreSQL Registry；并发 seed 必须定义一致，配置冲突拒绝启动，已撤销引用
不得因重启重新激活。development 才允许显式内存 Vault。

### 9. Artifact 与对象存储

生产对象存储使用 S3-compatible adapter（SeaweedFS 或华为 OBS），由 `AURACLAW_ARTIFACT_BACKEND`
切换；Artifact Service 使用 PostgreSQL 保存 Artifact Metadata、ACL、Version、
Lineage、Classification、Scan State、Retention 和 Audit；对象存储只保存对象字节。生产禁用本地共享 `artifact_root`。
`auto` 解析为 local、显式 local 或缺少后端 credential 时 Artifact Service 必须拒绝启动；development
local backend 不得被 readiness 标记为 production-ready。

上传协议为 createUpload -> presigned PUT/multipart -> finalize。Finalize 校验 tenant/object key、size、checksum、media type，经过
扫描/分类后才进入 ready。对象 key 使用不可猜测的 tenant/root/artifact/version 前缀；ready version 不可覆盖。下载先经
Policy/ACL 校验，再签发短 TTL、最小权限 URL。Hands 输出先提交 Artifact，再向 Session 追加 `artifact_ref`；Delivery 不持有
对象存储管理凭证。

本地 `.env` 已存在 SeaweedFS / OBS 变量名。Settings 使用显式字段/alias 映射；Access/Secret Key 只注入 Artifact Service，配置值不进入
Issue、文档、日志或测试输出。`.env.example` 只保留无密钥占位符，生产使用 Secret/Workload 配置注入。

### 10. Operations

公开 Ops 与 CLI 只作为 Admin API Client。Projection rebuild/redrive、Delivery redrive/DLQ、Retention、Hands recovery 等操作由
数据 owner 服务执行，携带 tenant、actor、`operation_id`、幂等和审计上下文。Task API/CLI 不直接使用跨 schema Operations Store。

Projection、Delivery 与 Artifact Owner 在执行 handler 前，必须先以 `operation_id` 和规范化 request digest 原子 claim 持久记录。
相同请求在其他副本返回 `running` 或最终结果，不重复副作用；相同 ID 的不同 tenant/owner/operation/parameters 返回 conflict。
claim 由 owner heartbeat 延续。owner 到期后不得自动重放无法确认的运维副作用，而是持久化
`unknown_side_effect` / `manual_recovery_required`，由操作员核对后以新 operation id 恢复。

### 11. Streaming 与 Delivery

Runtime producer 不分配依赖进程内存的公开 Session sequence。共享 Replay/Router 分配可恢复 cursor；Runtime handoff、Gateway
重启和 Kafka rebalance 后 cursor 继续前进。Connection Registry 保存 owner 与 TTL，Gateway 实例不全量消费 Kafka。

Delivery 只消费持久 Delivery Outbox/Job。相同 `delivery_id` 重试，Attempt History 不覆盖；Sink 凭证经 Credential Proxy，
Artifact 链接经 Artifact Service，策略经 Policy。Sink 熔断按 tenant/sink 持久化为全局状态，半开探针由原子
claim 保证跨副本唯一，不能用 Worker 内存计数决定生产外呼。Runtime Event Bus 丢失不影响交付。

## 数据库与身份矩阵

| 数据域 | 唯一写入身份 | 其他访问 |
|---|---|---|
| Session/Event/Snapshot/Outbox | session | 只经 Session API/feed |
| Projection/Runnable Outbox | projection-worker | task_query_ro 只读；Runnable Relay 投递 API |
| Control/Lease/Assignment/Checkpoint | orchestrator | Runtime 只经 Control API |
| Hands Invocation/Attempt | action-hands | Runtime 只经 Hands Contract |
| Policy/Decision/Approval Control | policy | Canonical 审批事实经 Session API |
| Credential Reference/Audit | credential-proxy | Secret 在 Vault/KMS |
| Artifact Metadata | artifact-service | Object bytes 在 S3-compatible Object Store |
| Delivery Job/Attempt/DLQ | delivery-worker | Ops 经 Admin API |

每个生产服务使用独立 workload identity；数据库连接使用统一应用 DSN（不再按服务注入分角色
DSN）。自动化仍须验证跨 tenant、伪造 Actor、过期 Lease、旧 fencing token、Credential 泄漏和
Artifact ACL 均被拒绝。

## 兼容、迁移与回滚

- 公开 `/v1/*`、Canonical Event 与 Projection 业务语义保持兼容。
- 内部 API 路径和 DTO 显式版本化，服务支持 N/N-1 滚动升级。
- 数据库迁移采用 expand/migrate/contract；删除旧字段前完成双读/双写窗口与回滚验证。
- S1 先建立 in-process 与 HTTP/MCP Adapter 的同一套 contract tests；S2 再新增入口；S3 才移除 direct Store 装配。
- S3 的 `0009_s3_owner_boundaries.sql` 只做 expand：Outbox claim 字段使用 `IF NOT EXISTS`，新增
  Hands/Policy/Credential owner schema、Artifact 状态字段和持久 Admin Operation；M1–M8 表与公开
  DTO 不删除。滚动顺序为 migration → owner services → callers → DB grants/network policy。
- S3 回滚顺序为 callers 回退 → owner services 回退 → 执行 down migration；若已经
  产生 Invocation/Decision/Credential/Artifact/Admin 新状态，先导出审计并保留 owner schema，不执行
  destructive down。N/N-1 窗口内旧进程仍可读取原表，但生产 Secret/写 role 不回授旧 Runtime。
- Tool Registry/Credential Reference 从旧 `security` schema 迁入新 owner schema 时冻结配置写，迁移后
  先排空旧 Hands/Credential owner，再开放新 owner 的生命周期写入；禁止为滚动窗口授予跨 owner
  双写权限。`0009` 保留旧表用于只读回退，并在 down 前将可兼容的新状态回填旧表。
- 每个阶段保留 development combined profile 作为行为对照，但不能让生产入口回退为共享写凭证。
- 每个 S0–S5 阶段独立通过清单、形成一个 intentional commit 并 push 到 origin。

## 后果

正面：各故障域可独立扩缩，数据库和 Secret 边界可由运行时权限验证，Runtime/Hands/Delivery 可恢复，协议可以滚动升级。

代价：本地开发、部署、契约测试和可观测性复杂度显著上升；同步调用需要超时、重试、Circuit Breaker 和服务身份；
SeaweedFS、Vault、Kafka、PostgreSQL 与 Replay Router 成为需要演练的外部依赖。

## 被否决的方案

- 仅增加多个 CLI、仍共享 Event Store/Control Store 写凭证：不能形成真实服务边界。
- 第一阶段延后 Model Gateway、Policy、Credential Proxy：无法满足 Secret 与 fail-closed 红线。
- Runtime 直连第三方 MCP：绕过 Policy、Fencing、Invocation Store、Credential 和审计。
- 所有服务直接读取 Projection DB：形成 schema 耦合；生产只允许 task_query_ro，调度使用 Runnable Feed。
- 直接共享本地 Artifact 目录：不能支持横向扩展、不可变版本和受控下载。

## 验证入口

阶段门禁见 [开发阶段校验清单](../../development/stage-gates.md) 的 S0–S5。目标实现与当前缺口映射见
[代码组织与部署映射](../code-organization.md)。
