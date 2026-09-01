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
  claim 均可恢复，DLQ 与人工 redelivery 使用稳定 delivery ID。Sink 熔断状态按 tenant/sink 共享，
  半开探针通过持久 claim 保证全局至多一个，不随 Worker 重启丢失。每轮只领取副本空闲容量，
  全局/per-tenant 并发上限分别由 `AURACLAW_DELIVERY_MAX_CONCURRENT` 与
  `AURACLAW_DELIVERY_MAX_CONCURRENT_PER_TENANT` 控制；长投递按
  `AURACLAW_DELIVERY_CLAIM_TTL_SECONDS` 续租。副作用开始后丢 owner 进入 reconciliation，不自动重投。
- Streaming 的 sequence、Replay Event 与 Connection Registry 位于 PostgreSQL；实例切换不依赖
  进程内 cursor。Hands、Model、Policy、Credential 与 Artifact 同样使用共享状态和原子 claim。
- Hands 以 Invocation execution claim 约束每个副作用 owner，并持续 heartbeat。Cancel 落到非 owner
  副本时写共享请求，由 owner 在轮询窗口内停止；`executing` claim 过期不得自动重放，而是进入
  `invocation_recovery_required`。waiting approval 的 payload 持久化并在重启后复用。
- Hands 不在 replica-wide 锁内等待 Policy、Approval 或 Tool I/O。每个副本使用
  `AURACLAW_HANDS_MAX_CONCURRENT` / `AURACLAW_HANDS_MAX_CONCURRENT_PER_TENANT` 控制执行槽位，使用
  `AURACLAW_HANDS_MAX_QUEUED` / `AURACLAW_HANDS_MAX_QUEUED_PER_TENANT` 限制等待者。队列等待超过
  `AURACLAW_HANDS_QUEUE_TIMEOUT_SECONDS` 时返回可重试背压；相同 idempotency key 的等待同样受该
  时间与队列上限约束，不形成无界 single-flight waiter。
- Resource Gateway 的 cache miss 按 tenant/session/URI/revision single-flight；invalidate generation 防止
  旧 load 重新发布 cache。Policy、扫描和 Artifact 写入不持有全局 cache lock，且受独立并发/队列上限约束。
- Runtime Event Producer 只按 tenant/session 串行 sequence 与 send；慢 Kafka publish 不阻塞其他
  Session。Delta buffer 按 run 隔离，Kafka timeout 或 caller cancellation 后 keyed state 会回收。
- Skill 管理读取始终经 Hands 查询 PostgreSQL lifecycle，`SKILL.md` 经 Artifact 边界读取；Task API
  的进程内 Skill Registry 不是管理事实源，清空缓存或重启 Task API 不需要预热才能提供管理查询。
  Hands 使用 `AURACLAW_SKILL_CONTENT_CACHE_MAX_BYTES`、
  `AURACLAW_SKILL_CONTENT_CACHE_MAX_ENTRIES` 和 `AURACLAW_SKILL_CONTENT_CACHE_TTL_SECONDS` 控制按
  tenant/package digest 隔离的本地不可变包缓存。管理详情、冷副本重建和并发 read-through 共享该
  cache；同 digest 并发只允许一次 Artifact 下载。周期 `skill-state` 对账会复用 Registry 中 digest
  一致的包，观察 `skill.package.download.*`、`skill.cache.*`、`skill.load.singleflight.waiters`、
  `skill.rebuild.packages.*` 和 `skill.rebuild.duration.seconds`。未变化 digest 持续下载表示副本频繁重启、
  cache 容量不足或 Registry 未能收敛，应先排查这些原因，不应直接增加 Redis。
  Runtime resolve 或 `skill://` read 在冷副本 miss 时会触发 tenant 增量重建后重试。该 read-through 是
  可用性兜底，不替代启动 readiness：新 Hands 副本只有完成初始 lifecycle snapshot 后才能接流量。
  Redis 当前不是依赖；未来可选 L2 必须经 ADR、容量基准和机密数据评审，且故障时回退到 Artifact。
  多副本 lifecycle 加速使用 `AURACLAW_KAFKA_SKILL_LIFECYCLE_TOPIC`（默认
  `managed-agent.skill-lifecycle`）。PostgreSQL `hands.skill_lifecycle_broadcast_outbox` 只保存 tenant、单调
  revision、change type、snapshot digest 和 origin replica，不保存 Skill 正文。relay publish 失败会保留
  outbox；观察 pending 数、`skill.lifecycle.signal.applied.count`、`skill.lifecycle.signal.stale.count` 和
  `skill.trusted_messages.latency.seconds`。每个 Hands 进程必须有唯一 replica id，从而生成独立 consumer group；
  同一 group 被多个副本共享会退化为竞争消费。Kafka 不可用时不要清除 outbox，Hands 依靠启动 snapshot、
  read-through 和周期 reconciliation 运行，并定期恢复 consumer。恢复后重复/乱序消息由 tenant revision fence
  丢弃。决策和 Redis 退出条件见
  [ADR-004](../architecture/decisions/ADR-004-skill-cache-and-replica-convergence.md)。
  Publication Reliability 每轮只领取 `AURACLAW_SKILL_RELIABILITY_MAX_CONCURRENT` 条 Outbox，按 tenant
  合并 rebuild、跨 tenant 并行，并以 `AURACLAW_SKILL_RELIABILITY_CLAIM_TTL_SECONDS` 续租；complete/fail
  影响零行视为 owner 丢失，不把旧 worker 的结果冒充成功。
  Skill Source/Publication/Publisher 写事务使用固定数据库锁层级；多 publication 始终按
  tenant/publisher/name/version 排序加锁。`40P01`/`40001` 从事务入口按原 command 语义有限重试，
  不在失败语句中间继续。观察 `postgres.transaction.retry{operation=skill.*}` 与
  `postgres.transaction.retry_exhausted`；持续增长时检查长事务、缺失索引和数据库 lock wait。
- Model Gateway 为每个 Model Call 持久化 execution owner、claim token 与 heartbeat。任意副本可写
  cancel request，owner 协作取消 Provider；断线或 owner 失联进入 `reconciling` 并保留 token
  reservation，禁止在结果未知时自动重放或释放额度。
- Artifact Service 的 ready/deleted/scan/hold/retention 读始终以 PostgreSQL 为准。长对象操作续租
  finalize/GC claim，并在副作用开始前记录 marker；owner 丢失且对象结果未知时进入 reconciliation，
  后续 cleanup 周期通过对象 HEAD/checksum 收敛，不凭租约过期直接重复 complete/delete。

## 生产配置门禁

1. 依次应用 PostgreSQL/KingBase `0010`～`0053` migration。`0040`
   `0022` 增加 registration 与 execution claim 字段和索引；先迁移 Control 数据库，再滚动升级
   Orchestrator，最后升级 Agent Runtime。可选执行 `deploy/postgres/roles.sql` 做硬化，
   当前部署不按服务注入分角色 DSN。
   `0041` 将 MCP observed health 扩为实例级主键，并加入 Catalog generation；必须在滚动 Action Hands
   前应用。升级后观察每个 Server 至少一个 active instance、active generation 单调增长和 stale 告警。
   `0042` 增加 Session、Control、Hands fencing 高水位表；必须先迁移数据库，再滚动三个服务，
   不允许新版本在生产环境回退到内存 ledger。
   `0043` 扩展 Projection、Delivery、Artifact Admin Operation claim；迁移后先滚动 owner 服务，
   再恢复 Task API/CLI 运维流量。
   `0044` 扩展 Hands Invocation execution claim、heartbeat 与 cancellation；先迁移数据库，再滚动
   Action Hands。升级期间不得同时运行会绕过 claim 的旧 Hands 副本。
   `0045` 增加 MCP Catalog 持久同步失败与 quarantine 字段；先迁移再滚动 Action Hands，观察失败
   计数只增不退、阈值隔离和成功清零。
   `0046` 扩展 MCP Lifecycle Operation command digest、claim/heartbeat/expiry 和恢复状态；先迁移再
   滚动 Hands 管理面，升级窗口暂停 MCP enable/disable/retire/reconcile 命令。
   `0047` 增加 Delivery tenant/sink 全局熔断、generation 和半开探针 owner；先迁移再滚动 Delivery，
   升级窗口不得混跑仍以进程内计数决定外呼的旧 Worker。
   `0048` 扩展 Model Call execution owner、claim/heartbeat/expiry、cancel/reconciliation 与终态时间；
   先迁移再滚动 Model Gateway，升级窗口不得混跑会绕过 claim 或将 cancel request 直接视为成功的旧副本。
   `0049` 增加 Artifact finalize/GC heartbeat、对象副作用 marker 和 reconciliation 状态；先迁移再滚动
   Artifact Service。升级窗口不得混跑会用旧 `_ready` 授权或在过期 claim 后直接重放对象操作的副本。
   `0050` 增加 Delivery/Skill Outbox claim heartbeat、Delivery side-effect marker 与 reconciliation 原因；
   先迁移，再滚动 Delivery Worker 与 Action Hands。升级窗口不得混跑不会续租的旧批处理 Worker。
   `0051` 增加 MCP Catalog config revision、reconcile owner/fencing/expiry、active snapshot digest/source
   revision；先迁移再滚动 Action Hands。升级窗口不得混跑会绕过 Catalog CAS 的旧 Reconciler。
   `0052` 增加 Approval request digest/generation/decision metadata 与 transition audit；先迁移 Policy
   数据库，再滚动 Policy 和 Task API。升级窗口不得混跑会无条件覆盖 Approval 终态的旧 Policy 副本。
   `0053` 是 Action Hands 数据一致性迁移：删除已退役的 `auraclaw-price-insight` Provider，并清除
   非 active generation 的 Capability 残留；迁移后滚动 Action Hands，确认
   `capability.catalog.backing_missing` 不持续增长。该数据清理不可通过 down migration 恢复，若需恢复
   Provider，必须重新注册并发布经过完整校验的新 snapshot。
2. 各服务共享统一 `AURACLAW_DATABASE_URL`（Compose `database_url` secret）；migration 使用
   独立的 `AURACLAW_MIGRATION_DATABASE_URL`。
3. 所有 Control、Session 与 Hands 副本必须使用相同的 `AURACLAW_LEASE_SIGNING_KEY`，并通过平台
   Secret mount 注入。
4. OBS bucket 权限只授予 Artifact Service；Vault token 只授予 Credential Proxy。Vault KV
   多字段引用必须使用 `path#field`。集成测试通过 `TEST_VAULT_CREDENTIAL_FIELD` 指定一次性测试值
   的字段名。
5. `AURACLAW_ORCHESTRATOR_LEASE_TTL_SECONDS` 必须大于正常单次模型/工具网络超时；默认 300 秒。
   健康执行会自动续租；到期任务由新 Runtime 从 checkpoint 接管，旧 fencing 不能继续提交结果。
6. Hands 默认每副本 32 个执行槽、每 tenant 8 个槽、全局 256 个等待者、每 tenant 32 个等待者，
   队列超时 5 秒。per-tenant 值不得超过相应全局值。扩容前先观察
   `tool.gateway.queue.depth`、`tool.gateway.queue.latency.seconds`、`tool.gateway.in_flight` 与
   `tool.gateway.backpressure.count`；持续全局饱和优先增加副本，单 tenant 饱和应调整该 tenant 工作负载
   或显式容量策略，不能取消隔离上限。
7. Resource Gateway 默认 32 个 load 槽、128 个 unique-key waiter、5 秒等待超时；Runtime Event
   Producer 默认 64 个 publish 槽、1024 个 waiter、5 秒队列超时和 10 秒底层 publish 超时。分别观察
   `resource.gateway.*` 与 `runtime.event.*` queue/in-flight/latency/backpressure 指标。单 Session Kafka
   超时应只增加该 key 的 timeout，不应伴随其他 Session queue latency 同步上升。
8. Delivery 默认每副本 8 个槽、每 tenant 2 个槽、claim TTL 30 秒；Skill Reliability 默认每副本
   8 条 Outbox、claim TTL 30 秒。观察 `delivery.worker.*` 与 `skill.reliability.*`；renew failure 或
   duplicate prevention 持续增长时先查数据库延迟和 owner 切换，不能直接缩短 TTL 或重放 reconciliation。
9. Skill 事务默认最多重试 3 次，基础抖动延迟 10ms。调大重试预算前先检查 PostgreSQL
   `deadlocks`、`pg_stat_activity.wait_event`、statement/lock timeout 和最慢事务；预算耗尽返回可重试
   conflict，调用方应保留同一 command id/request digest，不能生成新命令绕过幂等检查。
10. MCP Catalog/Skill/Connection reconcile 默认全局 8、每 tenant 4、每 host 2 个槽，每 server timeout
    60 秒。调整 `AURACLAW_MCP_RECONCILE_*` 前先核对 Credential Proxy、Policy 和远端 host 限额；单 host
    变慢应只造成该 partition 排队。`stale_capability_snapshot` 表示 owner/config/generation CAS 被拒绝，
    不得人工递增 generation 或清空 last-good，应等待当前 owner 完成或下一轮 reconcile。
11. Approval 冲突排查先查询 `policy.approval` 的 request digest、终态、decided actor/time，再按
    `policy.approval_transition_audit` 的 request/correlation/causation ID 重建 winner/loser 顺序。Canonical
    Event 已终结而 Policy 仍 waiting 时，以原完整绑定和相同决定重放通知；Policy 已有相反终态时停止 Tool
    副作用并升级人工调查，禁止直接 UPDATE 或删除审计行。`approved` 过期后 validate 会 fail closed，但
    历史状态仍保持 approved，不运行 expire 覆盖它。

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

回滚 `0044` 前停止 Hands 新调用并排空所有 `accepted|executing|waiting_approval` Invocation；逐项核对
`unknown` 和已请求取消的外部副作用。存在活跃 execution claim 时禁止删除 owner/heartbeat/cancel 字段。

回滚 `0045` 前停止 Catalog reconcile，并确认没有处于 `quarantined` 的 Server。回滚会删除持久失败
历史；若仍需隔离，必须先通过 desired state 显式 disable，不能依赖旧副本的内存计数。

回滚 `0046` 前暂停 MCP Lifecycle 管理命令，等待 `running` claim 完成，并核对所有 `reconciling` 与
`unknown_side_effect` Operation。未完成人工核对时禁止把扩展状态折叠为 failed 或删除 claim 证据。

回滚 `0047` 前停止 Delivery 新 Job 领取并排空 attempting attempt。记录仍处于 open/half-open 的 Sink，
确认外部依赖恢复后才能删除共享状态；回滚到旧版本时 Delivery 只能单副本运行，否则熔断阈值不再全局一致。

回滚 `0048` 前停止 Model Gateway 新调用并排空 active claim。逐项核对 `cancel_requested` 与
`reconciling` 调用、确认 Provider 最终结果并完成额度结算；存在活跃 claim 或未知结果时禁止删除
owner/heartbeat/cancel 字段。回滚到旧版本后 Model Gateway 只能单副本运行。

回滚 `0049` 前停止 Artifact finalize/delete/GC/orphan 新操作，排空 active claim，并逐项解决所有
`reconciling` 或 `object_state=unknown` 记录。未知对象结果未与存储后端核对前禁止删除 marker/heartbeat
字段；回滚后 Artifact Service 只能单副本运行，且不得恢复 production `_ready` 快捷授权。

回滚 `0052` 前停止审批请求、Human response 和 Tool 新副作用，确认无 `waiting` Approval 且 Canonical
审批事件与 Policy 终态一致，并导出 transition audit。回滚后 Policy 失去 CAS/审计保护，只允许单副本
运行；恢复多副本前必须重新应用迁移并核对 request digest。
