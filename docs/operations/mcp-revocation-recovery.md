# MCP 撤销与删除恢复

Issue #98 的清理顺序是阻止新执行、移除本机目录/路由、撤销 Egress、关闭连接、确认副本已撤销。
任一步失败都会保留待处理状态，由 Hands 周期对账重试；连接关闭失败不应恢复准入，也不自动重放业务请求。
共享目录已被另一副本删除时，本机仍按稳定 owner 清除工具、路由和发现快照。
超时累计触发 quarantine 与普通发现失败走相同的隔离准入，Tool/Resource/Prompt 均受限制。
在途请求允许结束；无法确认远端副作用的请求继续使用既有 unknown 恢复规则。

删除先将权威 desired state 设置为 retired，并持久化 reconciling 操作。进程重启后读取待删除操作继续清理。
删除最终写入检查配置 revision、创建时间和 retired 状态；旧操作遇到重建/新配置时标为 superseded，不能删除新配置。
普通禁用依靠持久 desired state 与每个副本的本地加载集合对账；删除重试不依赖消息投递成功。
状态仍为 pending/degraded 时不代表连接已关闭。旧版本遗留的 failed 删除需要管理员重新提交删除命令，不能自动重放可能已过时的意图。

Egress revoke 增加可选 expected_revision；迟到的旧撤销不能移除新修订。Egress 同时核对 Hands 权威启用快照，
不接受过期 apply，也不移除仍被启用的相同修订。临时探测 adapter 使用独立的修订键，最多存活 60 秒，
只允许发现请求，不参与业务执行。测试新认证目标且复用正在使用的 credential reference 时，必须提供独立引用，
以免更改其 scope 导致正式连接失效；无认证或相同 scope 不受影响。

## 发布与兼容

无数据库 DDL：复用现有 desired state、operation reconciling 状态和运行时错误码。
严格 DTO 增加 expected_revision，因此先升级 Credential Proxy，再升级 Hands；混用不支持该字段的旧 Proxy 会拒绝请求。
滚动窗口保留 pending，待新版对账收敛后再确认卸载。旧进程必须全部退出，避免旧逻辑重新发布已撤销入口。
真实双副本的五秒收敛目标、在途未知结果及 #99 冷启动恢复仍须联合部署验收，不能仅凭管理页状态关闭 issue。

## 验证

内存故障注入覆盖 Egress 撤销失败重试、关闭失败隔离、探测隔离/过期、共享目录消失、超时 quarantine 和新旧修订交错。
隔离 PostgreSQL 验证待删除操作跨 store 重建继续执行及原子删除校验；无测试/生产数据库写入。
全量回归 637 passed / 55 skipped，Ruff、Mypy 和 10 条架构合同通过。
