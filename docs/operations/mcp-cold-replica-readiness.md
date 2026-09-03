# MCP 冷副本就绪恢复

Issue #99 将共享目录的发布与本地路由安装分开。Catalog Store 提供同一数据库快照内的 committed generation、
源修订、摘要、配置及 descriptors；PostgreSQL 使用 repeatable read，内存适配器使用一致读取。
冷启动优先安装已提交目录；远端发现租约竞争失败时也能安装，不再用共享 active 推断本机已加载。

安装验证 tenant/server/config revision、内容摘要、目录摘要和执行状态，并在安装前重读 generation 与本地撤销 epoch。
路由/工具安装与本机 snapshot 提升在无 await 的区间完成。被删除、隔离、篡改、过期或安装失败的快照不会成为可用入口。
读取错误保持非就绪，后台周期对账继续恢复；单 Server 故障不会阻止其他 Server。
Skill 搜索/加载所依赖的 MCP backing 同样检查本地已安装 generation。旧会话通过原来的完整目标身份继续执行或收到 stale/blocked。

持久快照重建工具别名、只读提示与完整 Tool/Resource/Prompt 描述符。标准旧协议的冷连接首次调用先进行初始化，
无需重新 tools/list。协议握手和入参标准化的其余工作继续由 #93 完成。
不变目录的摘要排除每次同步的 updated_at，避免仅因时间变化持续创建 generation；读取兼容旧版含时间的摘要。

## 部署

先执行 `0060_mcp_local_applied_generation.sql`，再协调升级读取新 runtime DTO 的服务和 Hands。
`McpServerRuntimeRecord.applied_generation` 表示此副本已安装的 committed generation，NULL 表示尚未证明就绪，
不能以 loaded_revision 或共享目录 active 代替。字段可空，旧行无需推测或回填。
Compose、部署脚本和示例配置默认迁移目标同步至 0060。旧服务必须先退出再回滚字段；down 不改变权威目录。

## 验证

冷副本租约竞争、无快照、摘要漂移、修订变化、并发删除与 quarantine 回归通过。
两个真实本机 HTTP MCP 同名工具，经 Runtime → HTTP Hands → Gateway → Connector 正确路由；
新建独立 PostgreSQL store 与冷连接后恢复，安装期间没有远端发现请求，首次业务调用正确初始化。
已加载 generation 跨 store 重建读取通过；0060 up/down/up 在隔离 PostgreSQL 验证。
全量 645 passed / 55 skipped；最终摘要稳定性和恢复专项另行验证。Ruff、Mypy 与 10 条架构合同通过。
真实环境双 Hands 进程滚动恢复时限仍应联合 #98 部署验收，当前证据不替代该验收。

## 独立进程验收

`tests/integration/test_mcp_replica_processes.py` 使用 multiprocessing spawn 创建两个独立 Hands 进程，
以真实 HTTP 经 Runtime adapter 调用 search/load/call，共享仅限 PostgreSQL。两个同名工具的本机 MCP
Server 记录实际请求数。持有远端发现租约时，冷启动及杀死后重启仍从 committed snapshot 装载，
不增加 tools/list。关闭一台 Server 且预先删除共享目录后，无通知的周期对账在 5 秒断言内清除
两个进程的旧路由/Egress adapter，旧绑定不触发远端调用，另一 owner 不受影响。

本测试使用真实 McpConnectionManager/McpEgressManager/受管网络 adapter；CredentialProxy 在各 Hands
测试进程内运行，未以此冒充部署拓扑下独立 Credential HTTP 服务的网络故障验收。
