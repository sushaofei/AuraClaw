# 自研 MCP 注解直接生效升级

当前版本只接入自行开发的 MCP。Tool 权限直接来自 `tools/list` 的
`annotations.readOnlyHint`：true 为 `read-only`，false 或缺失为
`write-with-approval`。风险采用 `_meta.auraclaw.riskLevel`；缺失时只读默认
`low`，其余默认 `high`。写入审批、租户隔离、身份、ACL、Policy 显式拒绝、
Credential Proxy 和网络边界继续生效。

## 删除的配置

- MCP Server、HTTP Server 和统一能力契约不再提供 `trust_level`。
- 删除 `AURACLAW_MCP_TRUST_REMOTE_TOOL_ANNOTATIONS`；部署环境可移除旧变量。
- 不再使用 `metadata.tool_policy_overrides`，不需要逐个维护工具名。
- AuraX SDK 不再返回能力信任等级；只读和权限标签随新目录更新。

旧 MCP 配置 revision 和其 digest 保持原样。持久化读取时仅丢弃退休字段，
新 API / 配置中应删除 `trust_level`，不再提交覆盖规则。

## 发布步骤

1. 构建并准备完整新镜像，备份数据库并记录旧镜像。此迁移删除字段，采用维护窗口发布，
   不在旧进程仍访问目录表时执行，不使用旧版本与新版本混跑的滚动升级。
2. 停止旧 AuraClaw 进程，对该环境数据库执行 migration up 到 `0057`。
   两个表的信任等级列被删除，Server 投影的旧覆盖被清理；不改 Session Events、
   MCP 配置历史、工具 Schema 或工具版本。
3. 使用新镜像启动各服务，让 Hands 正常获取 `tools/list` 并重新对账。
   不手工改目录权限，不绕过租约或同版本 Schema 漂移检查。
4. 确认目录发布 active、同步成功，管理接口 `read_only` 与 MCP 声明一致。
   在本次已检查的 ChainTowerMCP 快照中，预期为 20 个只读、5 个写入需审批；
   工具集合变化时以新的 `tools/list` 为准。

## 回滚

停止新进程，执行 `0057` down migration 后再启动旧镜像。down 会从仍存在的
active 配置 revision 恢复 Server 信任和覆盖，Catalog 继承 Server 的信任等级；
没有配置历史的条目回落到 `external_untrusted`。重新对账以恢复旧版有效权限。
如需逐值还原没有配置历史的投影，应使用发布前数据库备份。

正向和反向迁移都不应与另一版本应用并发执行。本次代码修改不等同于测试环境已发布。
