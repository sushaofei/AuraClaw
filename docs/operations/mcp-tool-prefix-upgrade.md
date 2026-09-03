# MCP Tool 前缀移除升级（Issue #91）

本版本移除 `allowed_tool_prefixes`。所有合法的远端 Tool 都参与 Catalog 对账；tenant、
Policy、Approval、Schema、工具来源归属及 Credential/Egress 控制继续生效。
Resource scheme 和 Prompt 前缀配置保持不变。AuraX 与 SDK 同步删除该字段。

## 数据与 API

- `0058` 删除 `hands.downstream_mcp_server` 的派生列，不修改既有 migration。
- `hands.mcp_server_revision.config_json` 和 `config_digest` 保留原始值。读取历史 revision
  时剥离退休字段；历史 digest 描述原始存档，不是剥离字段后的配置。新 revision 不含该键。
- API/OpenAPI 不再声明或输出该字段，旧客户端带该字段写入会收到 422；部署须配套更新 AuraX/SDK。
- 旧列表为非空、空数组、包含空字符串或缺失时均可读取，不需要手工编辑历史 JSON。

## 维护窗口发布

1. 备份注册配置、revision 和派生 Server 表。准备包含 `0058` 的同一不可变镜像及配套 AuraX。
   新增工具可能触发现有数量上限或名称冲突，应预先核对远端清单。
2. 暂停配置写入，停止所有旧 SQL 服务实例（含 Task API、Hands、Credential Proxy）。
   按 [生产部署](production-deployment.md) 执行 migrate up/check 到 `0058` 后统一重建实例。
   Compose、环境模板、环境生成器及一键发布脚本默认目标均为 `0058`。
3. 对每个 enabled Server 用 MCP 页面“同步”或
   `POST /v1/admin/mcp-servers/{server_id}/reconcile` 触发完整对账。API 沿用管理员身份、
   `Idempotency-Key`、`X-Expected-Revision` 和 correlation/causation 上下文；不直接改目录表。
   周期对账也会重新取完整快照。disabled/retired Server 保持原状态。
4. 查看 publication 的 generation、工具清单和各 Hands 实例加载结果，确认此前被过滤的工具
   已登记且路由刷新；用既有 Policy 允许的只读工具验证 search/load/call。
   如新目录与旧目录完全相同，可复用 generation；存在新增工具时应产生新 generation。
5. 远端不可达时保持现有 last-known-good 策略并重试；旧目录仍可用不表示本次扩展已完成。
   Schema drift、名称冲突或数量超限继续按现有错误处理，不发布半成品目录。

不得让新旧实例混跑：旧代码依赖已删除的列，且仍执行前缀判定。启动检查只证明 schema 一致，
不能代替全量对账及各副本路由验证。

## 回滚

停止新实例后，按现有数据库迁移回退流程执行 `0058_remove_mcp_tool_prefixes.down.sql`，
将迁移账本与目标版本恢复至 `0057`，再配套回退镜像、AuraX/SDK 和部署目标。当前 CLI 不提供
自动 down 命令，不得只执行 DDL 而保留 `0058` 已应用的账本；由迁移管理账号在维护窗口完成。

down 从对应 active revision 恢复旧前缀数组，历史记录及 digest 不变。没有可恢复数组的
Server（包括升级后新建/更新配置和未注册的旧派生条目）将被停用；派生状态置为 quarantined，
注册 desired state 从 enabled 改为 disabled，避免重启后立即重新启用。

对这些 Server，管理员须从发布前备份恢复有效配置或在旧版重新配置前缀，再显式启用并完整
对账；空数组只是旧列的默认值，不代表数据已恢复。旧配置本来为空数组时，其旧版本语义仍为
过滤全部 Tool。升级后新增的调用历史、Session Events 和配置 revision 均不删除。

## 验证记录

实现回归覆盖多种 Tool 名称的发现/调用、非法名字在出站前拒绝、Resource/Prompt 限制、
旧 JSON 读取及严格写入契约。临时 PostgreSQL 集成测试覆盖 `0058` up/down/up、旧数组恢复、
不可恢复条目停用、SQL Store 往返和启动快照；KingBase 实例发布前仍需实际维护窗口验证。
