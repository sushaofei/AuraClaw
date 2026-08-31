# S4 横向扩展与恢复运行说明

## 生产拓扑建议

S4 的进程均为无本地业务状态实例；Canonical Event、Control、Projection、Delivery、Hands、
Model、Policy、Credential、Artifact 和 Streaming 状态由各自 owner schema 持久化。生产环境建议
Session、Orchestrator、Agent Runtime、Projection、Streaming、Delivery 与 Action Hands 至少 2 个
副本，入口类服务至少 2 个副本；PostgreSQL、Kafka、OBS 与 Vault 使用平台提供的高可用
部署。S5 已以 Docker Compose 固化副本、资源限额、内部网络、Secret mount 和蓝绿升级；
Compose 不提供 HPA/PDB/NetworkPolicy，跨故障域高可用由两套独立 Compose 集群承担。

Agent Runtime 在生产模式下若未显式配置 `AURACLAW_RUNTIME_ID`，会使用容器/Pod hostname 生成
实例 ID；`AURACLAW_RUNTIME_NODE_ID=local` 同样解析为 hostname。Lease Assertion 对 runtime ID、
tenant、session、run、lease 和 fencing token 签名，Action Hands 不再把共享 workload token 误当成
单个 Runtime 身份。每个进程还生成独立 `registration_id`；同一 `runtime_id` 的上一注册在 30 秒
registration lease 内仍活跃时，新进程注册会 fail closed。因此显式 Runtime ID 必须保证副本间唯一，
滚动替换固定 ID 时必须先等待旧实例退出和 registration lease 到期。

## 调度与消费恢复

- Session 在 Canonical append 同一事务写入 `control` Runnable Outbox。Orchestrator 只消费该 Feed
  并按 source version 幂等入队，不扫描完整 Event Log。
- Orchestrator 使用可过期 claim、Session lease 与单调 fencing token；任一副本可回收过期
  Assignment。Runtime 从共享 checkpoint 恢复，旧实例的 Session、Tool、heartbeat 和 checkpoint
  写入均被拒绝。
- Session、Control 和 Hands 分别在 owner schema 持久化 `(tenant_id, resource_id)` fencing 高水位；
  副本切换和进程重启不会清零。相同 token 可安全重试，低于高水位的 assertion 在业务 handler 前拒绝。
- Runtime 为每个 Assignment 原子创建 `execution_claim_token`，不再按 `running.started_at + 5s`
  猜测孤儿。活跃 claim、Runtime registration 与 Session lease 必须同时匹配；执行心跳每 10 秒续租
  claim、Session lease 和签名 Lease Assertion。超过 claim 安全窗口后 Runtime 取消 Harness，停止新副作用。
- `AURACLAW_RUNTIME_CAPACITY=N` 表示同一 Runtime tick 最多同时执行 N 个 Harness；Control 的
  `assigned|running` 计数与此槽位上限一致。关闭 Runtime 前先停止领取，并给在途 Harness 至少一个
  lease TTL 的排空时间；强制退出的任务在 registration/claim lease 到期后从 checkpoint 恢复。
- 无可用 Runtime 槽位是背压而非故障。Orchestrator 将原队列项延迟 100–500ms（带 jitter）后按原
  priority/partition 重排，避免多副本热循环且不改变公平顺序。
- Projection Outbox 每个 destination/tenant/session 只释放最早未完成记录。claim、retry delay 或
  poison 会阻断后续版本，避免多个 Worker 产生 version gap。
- Delivery 在 Outbox ingestion 后按 tenant/session/sink 串行领取 Job；attempting、retry_wait 和过期
  claim 均可恢复，DLQ 与人工 redelivery 使用稳定 delivery ID。
- Streaming 的 sequence、Replay Event 与 Connection Registry 位于 PostgreSQL；实例切换不依赖
  进程内 cursor。Hands、Model、Policy、Credential 与 Artifact 同样使用共享状态和原子 claim。

## 生产配置门禁

1. 依次应用 PostgreSQL/KingBase `0010`～`0043` expand migration。`0040`
   `0022` 增加 registration 与 execution claim 字段和索引；先迁移 Control 数据库，再滚动升级
   Orchestrator，最后升级 Agent Runtime。可选执行 `deploy/postgres/roles.sql` 做硬化，
   当前部署不按服务注入分角色 DSN。
   `0041` 将 MCP observed health 扩为实例级主键，并加入 Catalog generation；必须在滚动 Action Hands
   前应用。升级后观察每个 Server 至少一个 active instance、active generation 单调增长和 stale 告警。
   `0042` 增加 Session、Control、Hands fencing 高水位表；必须先迁移数据库，再滚动三个服务，
   不允许新版本在生产环境回退到内存 ledger。
   `0043` 扩展 Projection、Delivery、Artifact Admin Operation claim；迁移后先滚动 owner 服务，
   再恢复 Task API/CLI 运维流量。
2. 各服务共享统一 `AURACLAW_DATABASE_URL`（Compose `database_url` secret）；migration 使用
   独立的 `AURACLAW_MIGRATION_DATABASE_URL`。
3. 所有 Control、Session 与 Hands 副本必须使用相同的 `AURACLAW_LEASE_SIGNING_KEY`，并通过平台
   Secret mount 注入。
4. OBS bucket 权限只授予 Artifact Service；Vault token 只授予 Credential Proxy。Vault KV
   多字段引用必须使用 `path#field`。集成测试通过 `TEST_VAULT_CREDENTIAL_FIELD` 指定一次性测试值
   的字段名。
5. `AURACLAW_ORCHESTRATOR_LEASE_TTL_SECONDS` 必须大于正常单次模型/工具网络超时；默认 300 秒。
   健康执行会自动续租；到期任务由新 Runtime 从 checkpoint 接管，旧 fencing 不能继续提交结果。

升级前检查并修复旧数据：同一部署中若多个容器显式共享 `AURACLAW_RUNTIME_ID`，先改为未配置（使用
hostname）或注入唯一实例 UID。停止全部旧 Runtime，等待 30 秒，然后将无对应健康实例且状态为
`assigned|running` 的 Assignment 交给 `recover_expired()`，不要手工复制或重置 fencing token。
观察 `runtime.capacity_saturated`、claim conflict、lease renewal failure、queue wait 与 active Harness；
升级期间 duplicate-attempt prevented 应只在故障注入时增长。

## 回滚

先停止新版本 Runtime 领取并等待 claim/Assignment 排空，再回滚 Runtime 与 Orchestrator。确认没有
`running` execution claim 后，才可执行 `0040_runtime_execution_claims.down.sql`（KingBase 对应
`0022...down.sql`）；否则旧版本可能把运行任务按 5 秒规则重复领取。生产数据库禁止在未备份、未排空
或仍有新版本进程时执行 down migration。旧版固定 Runtime ID 只能以单副本运行，直至重新升级。

回滚 Action Hands 前先停止 Catalog reconcile，确认没有 generation 切换事务，并将每个 Server 排空到
单个 Hands 实例；随后才可执行 `0041_capability_catalog_consistency.down.sql`。down migration 会折叠
实例健康行并移除 generation 字段，不删除当前 active Capability 行。

回滚 `0042` 前必须停止所有 Runtime 和 Session/Orchestrator/Hands 新请求并完成排空；删除高水位表会失去
对仍未过期旧 assertion 的重放防护，因此只允许在所有旧 Lease Assertion 都已过期后执行 down migration。

回滚 `0043` 前停止新的 Owner Admin 请求并等待所有 `running` claim 完成。存在未完成 claim 或
`unknown_side_effect` 时禁止删除 claim/audit 字段；先完成人工核对并记录恢复结果。
