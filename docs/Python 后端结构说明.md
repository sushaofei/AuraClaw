# Python 后端结构说明

本项目已移除旧 TypeScript CLI/Agent 实现，重构为纯 Python Managed Agent 后端。

## 当前完成范围

当前实现覆盖事实/查询、Managed Runtime、安全行动、多 Agent 协作和 M5 实时/交付链：

```text
HTTP Command
  -> Task Application Service
  -> Session Aggregate
  -> Canonical Event Store + Transactional Outbox
  -> Task Projection
  -> Query API

Control Projection
  -> Runnable Queue / Atomic Claim
  -> Lease + Monotonic Fencing Token
  -> Orchestrator / Runtime Assignment
  -> Recoverable Agent Harness
  -> Session / Model / Tool / Runtime Event Ports

Tool Invocation
  -> Tool Registry / JSON Schema / Permission Policy
  -> Action Digest / Approval Validation
  -> Hands Sandbox or Credential Proxy
  -> Secret Redaction / Result Normalizer
  -> Inline Result or Immutable Artifact Reference

Runtime Event Producer SDK
  -> Kafka Runtime Topic / at-least-once
  -> Shared Streaming Ingestor / Replay Buffer
  -> Tenant-authorized SSE / Last-Event-ID / Backpressure

Canonical terminal event + Transactional Delivery Outbox
  -> Idempotent PostgreSQL Delivery Job
  -> Webhook or Parent Session Sink
  -> Retry / Circuit Breaker / DLQ / Attempt History
  -> delivery.* Canonical Event -> Task Query Projection
```

内存适配器用于快速测试。PostgreSQL 适配器负责 Canonical Event、Aggregate Version、
Command Dedup 与 Outbox 的单事务提交，并保存可选 Snapshot。Outbox Relay 以
at-least-once 方式驱动幂等投影；投影记录 source version、processed event 和 checkpoint，
检测 gap，并将未知关键事件放入 Poison Event Queue。

## 依赖方向

以下是 Issue #8 完成后的代码结构；具体依赖由 import-linter 持续约束。

```text
entrypoint -> composition -> api -> gateways
                   |                 |
                   v                 v
             infrastructure    session / projection
                   |                 |
                   +---- implements ports ----+
                                     v
                                domain -> contracts
```

- `contracts`：跨边界稳定类型，不依赖框架。
- `domain`：Session 聚合和状态机，仅依赖 contracts。
- `session` / `control` / `action` / `delivery`：按 bounded context 编排业务用例。
- `infrastructure`：Event Store、Outbox、Hands、Artifact、Credential 与 PostgreSQL 适配器。
- `projection`：可删除、可重建的查询模型与 relay。
- `gateways`：命令、查询与 Streaming 接入边界。
- `api`：鉴权上下文、Header/DTO 和 HTTP 表示，不选择具体实现。
- `composition`：唯一对象图与进程装配根。
- `runtime`：Runtime 端口、fenced client、可恢复 Harness 和 Model Gateway。

重构后的装配约束为：entrypoint → `composition` → `api`/gateways/业务包/infrastructure adapters。`api` 和 gateways 不导入 `composition` 或具体 infrastructure；infrastructure 可以依赖其实现的 ports，但不能依赖 `api`、gateways 或 `composition`。M8.1 在 API lifespan 内统一装配 Runtime Worker；部署间只替换资源配置。拆分为独立 `runtime run` 进程与 `delivery run` 仍是后续功能。

## 与目标架构的对应关系

| 目标组件 | 主归属模块 | 状态 |
|---|---|---|
| Task Gateway / Admission | `gateways/task/` | 命令转发与准入边界已实现 |
| Sync invocation facade | `gateways/task/invocations.py`、`gateways/query/waiter.py` | `POST /v1/tasks/sync` 写命令后只读投影等待终态 |
| Session / Collaboration | `session/`、`domain/` | Canonical Session 写侧已实现 |
| Canonical Event / Outbox | `infrastructure/persistence/*event_store.py` | PostgreSQL 与内存适配器已实现 |
| Projection / Read Model | `projection/`、`infrastructure/projection/` | 幂等、gap、checkpoint、重建已实现 |
| Task Query / Result | `gateways/query/reader.py`、`gateways/query/waiter.py` | 只读 Projection 查询；可选受控等待 Run 终态 |
| Control State | `control/ports.py`、`infrastructure/persistence/*control_store.py` | Queue、Lease、Fencing、Assignment、Heartbeat、Capacity、Checkpoint |
| Orchestrator | `control/orchestrator.py` | watch、claim、schedule、provision、cancel、heartbeat、reconcile |
| Agent Runtime | `runtime/harness.py`、`runtime/clients.py` | Budget、Deadline、Cancel、Checkpoint 与四类端口 |
| Model Gateway | `runtime/model_gateway.py`、`infrastructure/model/openai_compatible.py` | 生产 Provider Adapter、流式调用、错误映射和 Gateway 内 Credential 解析 |
| Tool Gateway | `action/`、`contracts/tools.py` | Registry、Schema、权限、审批、幂等、取消、标准化 |
| Hands | `infrastructure/hands/local.py` | 无 Shell 进程、受限环境、文件根隔离和取消 |
| Artifact Store | `infrastructure/artifacts/store.py` | Hash、去重、不可变版本、lineage、ACL 和短期下载令牌 |
| Policy / Approval | `action/policy.py`、`domain/approval.py`、`projection/approval/` | Action Digest、Aggregate、可重建 View、Human Response |
| Credential Proxy | `infrastructure/credentials/proxy.py` | credential_ref、scope、撤销、代调用和递归脱敏 |
| Collaboration | `session/collaboration_service.py`、`domain/collaboration.py`、`projection/collaboration/` | Child DAG、合同、委派、交接、Join、Runnable 与 Review |
| Runtime Event Bus | `infrastructure/kafka/runtime_events.py` | Harness→Producer SDK→Kafka/Memory→Ingestor/Replay，含 sequence、visibility、大小限制与 Token 合并 |
| Streaming Gateway | `gateways/streaming/gateway.py`、`api/routes/streams.py` | 租户授权、SSE、公开 Cursor、重放、过期回退和有界背压 |
| Result Delivery | `delivery/worker.py`、`infrastructure/delivery/` | Outbox、持久 Job、Webhook/Parent Sink、签名、重试、Circuit、DLQ |

## M2～M5 运维与安全边界

- API 请求会尝试即时 relay，以提供开发和单进程部署下的快速可见性；可靠恢复仍以
  Transactional Outbox 为准。
- 独立 Worker 使用 `auraclaw projection relay --watch` 持续消费。
- `auraclaw projection rebuild [--tenant ...]` 从 Canonical Event Log 重建 Task Read Model。
- Snapshot 仅优化聚合加载；损坏或缺失时仍可从完整事件历史恢复。
- Collaboration Aggregate 从同一 Root 下的 Canonical Session Events 重建；
  `projection.collaboration_view` 可删除并重放恢复。
- Coordinator 只调用 Collaboration Service，不直接启动 Runtime；Orchestrator 可消费
  Collaboration Projection 的 runnable Child，但不参与语义拆分。
- Worker 写自己的 Child Result；Reviewer 写独立 Review Session 和证据决策，不覆盖 Worker
  Artifact。Root Join 保存 Child/Review/Artifact lineage。
- Control State Store 与 Canonical Event Store 使用独立 Schema/写入边界，不做跨 Store 事务。
- Orchestrator 只调度资源，不解析目标、不拆分 Task DAG。
- Runtime Event Bus 故障不阻止完整模型输出与最终结果写入 Canonical Event Log。
- 所有部署由同一个 `RuntimeWorker` 注入 Model Gateway 与 Runtime Event Producer SDK；
  PostgreSQL/内存、Kafka/内存、模型端点等只由当前 `.env` 选择。Provider API Key 只由
  Gateway 内的 CredentialResolver 读取，不进入 Harness、事件或日志。
- Kafka Offset 仅用于 Streaming Ingestor 恢复；浏览器只使用 `session_id:sequence`，超出保留期
  时收到明确 Query 回退信号。关闭 SSE 不等于取消 Session。
- Streaming Gateway 使用共享 Consumer 和每连接有界队列，慢客户端不阻塞 Runtime、Kafka
  Partition 或其他订阅者；`secret` visibility 事件不能进入重放缓冲。
- 可靠通知只由 Canonical Event 的 delivery Outbox 触发，不扫描 Session 状态猜测任务完成。
  稳定 `delivery_id`、唯一 `(event_id, sink_id)` 和 Attempt History 保证服务重启/重复 Outbox
  不重复业务交付；429/5xx/timeout 重试，耗尽进入 DLQ。
- Webhook 使用 timestamp + HMAC-SHA256 和稳定 Idempotency-Key；Job/Sink 只保存受控目标与
  `credential_ref`，Secret 不写入 Event、Job、响应摘要或 Runtime Event。
- Runtime checkpoint、模型调用 ID 和工具调用 ID 都稳定，接管后不会重复产生业务事实；
  Tool Gateway 进一步以 tenant-scoped idempotency key 阻止重复外部副作用。
- `write-with-approval` 与 `destructive/admin` 权限默认 fail closed；批准绑定 Session、动作摘要
  和策略版本，参数变化或策略升级都会使旧审批失效。
- Human Response 通过 Task Gateway 形成 `human.response.recorded` 与审批决策 Canonical Event；
  Approval View 不是事实源，可从事件重建。
- Hands 不接收 Secret；进程禁用 Shell、只传固定最小环境，文件访问限制在配置根目录。
- Credential Proxy 校验 tenant、过期时间和 operation scope，Vault Secret 只在代理调用栈内
  使用；返回值、日志载荷和 Tool Result 在离开代理前脱敏。
- Tool Result 超过配置上限时在进入 Session 前写为 Artifact；对象内容不可变，外部只见
  Artifact Reference，下载令牌绑定 tenant、actor 和短 TTL。
