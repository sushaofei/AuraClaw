# 数据库 Schema 与迁移索引

本文提供导航，不复制完整字段定义。数据库结构的唯一真源是 `migrations/` 中按版本排序的 SQL；
运行时实现只能补充表的使用方式，不能覆盖迁移定义。

## 查看当前状态

```bash
uv run auraclaw migrate status
uv run auraclaw migrate up
```

生产环境先执行独立 migration job，再滚动服务。应用进程不得在启动时隐式修改 Schema。

## Schema 职责

| Schema | 唯一写入边界 | 主要对象 |
|---|---|---|
| `session_core` | Session Service | Session Head、Canonical Event、Command Dedup、Outbox、Snapshot |
| `projection` | Projection Worker | Task/Approval/Collaboration View、Checkpoint、Processed/Poison Event、Admin Operation |
| `control` | Orchestrator / Control Store | Runnable、Assignment、Runtime Lease/Instance/Checkpoint、Capacity、Cancellation |
| `delivery` | Delivery Worker | Delivery Job、Sink、Attempt、Admin Operation |
| `hands` | Action Hands | Tool、Invocation、Capability Catalog、MCP Server Registry/Revision/Runtime/Operation |
| `policy` | Policy Service | Decision、Approval、Active Bundle |
| `credential` | Credential Proxy | Credential Reference 与 Usage Audit |
| `artifact` | Artifact Service | Metadata、Access Audit、Admin Operation 与生命周期 claim |
| `streaming` | Streaming Gateway | Session Sequence、Runtime Event Replay、Connection Registry |
| `model_gateway` | Model Gateway | Usage Budget 与 Model Call |
| `observability` | Observability Service | Trace、Metric、Audit、Alert、Retention Policy |
| `security` | 迁移兼容与入口安全 | Agent Context Replay；早期 Tool/Credential 表由 `0009` 迁入 owner Schema |

任何服务都不得因为“能连接同一个数据库”而越过 owner Schema 写入。生产角色和默认权限由
`deploy/postgres/roles.sql` 与迁移共同约束。

## 迁移时间线

| 版本 | 主题 |
|---|---|
| `0001` | Canonical Session、基础 Projection、Control 与 Delivery |
| `0002` | M1 事实查询、Poison Event 与幂等响应 |
| `0003` | M2 Managed Runtime、Lease、Assignment 与 Checkpoint |
| `0004` | M3 Tool、Artifact、Approval 与 Credential 基线 |
| `0005` | M4 Collaboration / Review Projection |
| `0006` | M5 Streaming 结果字段与可靠 Delivery |
| `0007` | M6 Observability、Audit、Alert 与 Retention |
| `0008` | 多 Run Session 投影 |
| `0009` | S3 owner Schema 与信任域迁移 |
| `0010` | S4 claim 过期接管与恢复索引 |
| `0011` | S4 Streaming 共享状态 |
| `0012` | S4 Model Gateway 状态 |
| `0013` | S4 Artifact 生命周期与 GC claim |
| `0014` | S4 Policy Bundle 版本 |
| `0015` | M9 Capability Catalog |
| `0016` | M9 Skill Projection |
| `0018` | MCP 协议版本信息 |
| `0019` | 可信 Agent Context 防重放 |
| `0020` | MCP Server 热配置 Registry |
| `0021` | Task Source 与列表查询 |
| `0022` | MCP Server 删除操作状态 |
| `0023` | Skill Package、Publication、Installation 与 Source 生命周期 |
| `0024` | Skill Publication 更新 actor |
| `0025` | Skill Package retention、legal hold 与 optimistic revision |
| `0026` | Skill 发布命令账本、事务 Outbox 与 Artifact 发布/孤儿回收 fencing |
| `0027` | tenant Publisher Registry、Ed25519 公钥、rotation/revoke 与命令幂等 |
| `0028` | Skill Source 对账租约、单调 fencing token 与过期接管索引 |
| `0029` | Skill Source 连续缺失清单、审计化自动退役命令与 `retired` Publication 状态 |
| `0030` | Publisher suspend/resume 状态证据与管理命令账本扩展 |
| `0031` | 退役 Skill Publication 显式恢复状态、review 命令账本与回滚约束 |
| `0032` | Skill 发布准入成功/拒绝审计、阶段化安全错误码与 tenant 时间索引 |
| `0033` | Skill 内容扫描 quarantine 审计结果约束与安全回滚映射 |
| `0034` | Skill admission 内容策略版本、运维查询索引与聚合读取基础 |
| `0035` | Skill admission keyset/保留清理的全局时间索引 |
| `0036` | Skill Publication 撤销后的 `continue|pause|cancel` Runtime Policy 证据与查询索引 |
| `0037` | Skill Source priority、Publication 多来源引用与 Source 管理命令审计账本 |
| `0038` | Skill Installation draining/force 策略证据、drainer 索引与安装命令幂等账本 |
| `0039` | Publisher 永久撤销、Publisher/key 批量 Runtime Policy 证据与约束 |
| `0053` | 清理已移除 Resource Provider 与非 active generation Catalog 残留 |

PostgreSQL / Kingbase 序列保留 `0017` 版本号空位，后续迁移继续按既有编号递增。
不要为了填补编号而重命名已经发布的迁移。

每个正向迁移原则上有同版本 `.down.sql`。新增迁移时同时更新本表，并在
[开发阶段校验清单](../development/stage-gates.md)记录迁移、回滚和真实数据库验证证据。

## 关键不变量

- `session_core.canonical_event` 是任务事实源；Projection、Control、Streaming 和 Observability 都不能反向定义业务状态。
- 所有租户数据按 `tenant_id` 隔离，查询和唯一键必须保留租户作用域。
- Canonical Event 与 Session Outbox 同事务写入；下游以幂等消费承受至少一次投递。
- Runtime Lease 与 Fencing Token 决定当前执行权，旧 Runtime 的写入必须被拒绝。
- Tool、Delivery、Model 和外部副作用使用稳定业务幂等键；不能用进程内锁代替持久去重。
- Credential 表只保存引用和审计元数据，不保存明文 Secret。
- Artifact 内容不可变；生命周期、扫描、下载权限和访问审计通过 Metadata 管理。
- 所有写入都携带 tenant、command id、expected version、actor、correlation 和 causation 上下文。

## 代码入口

- 迁移执行：`src/auraclaw/infrastructure/persistence/migration_runner.py`
- PostgreSQL / Kingbase 迁移：`migrations/`
- 生产角色：`deploy/postgres/roles.sql`
- Store 与投影适配器：`src/auraclaw/infrastructure/persistence/`、`src/auraclaw/infrastructure/projection/`
