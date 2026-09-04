# Skill/MCP 测试环境部署记录（2026-09-04）

Vault 访问已恢复，workload 配置 revision 4 已测试并启用；Java 仍返回身份上下文不可用。业务成功闭环及 Skill 安装迁移尚未完成。

## 已部署内容

- AuraClaw 源码：`b6cd8b9`，分支 `dev_20260717`，已推送。
- 镜像：`auraclaw:skill-mcp-b6cd8b9`，linux/amd64，镜像 ID
  `sha256:2e4417d965f1a6e69277d5eaecba8c037bcbc19cfc3f8fda54743b18ebe00881`。
- 从已提交文件的独立构建目录构建；未包含环境文件、编辑器修改或临时业务诊断文件。
- 镜像内迁移 latest 为 0063；一次性镜像测试容器中 51 项相关冒烟通过。
- 将镜像加载到测试主机，以 `dev_service_deploy.sh --skip-sync --skip-build`
  和上述镜像标签执行维护停止、迁移、check、全服务 recreate、健康等待。
- 0060、0061、0062、0063 应用成功，schema ready 为 0063；30 个容器健康，入口 readiness 成功。
- AuraX `d91e001` 已推送 `dev_20260820`。此前 SDK 55 项、类型检查和 Skill 浏览器测试已通过；本次没有重新发布桌面安装包。

## 现场调用证据

使用原故障会话的可信用户和部门上下文，通过签名入口创建新的只读验收会话。
正式 Agent 搜索、加载成功，仓库列表调用生成嵌套 `input.type=warehouse`、`input.limit=10`。
调用经过部署的 Hands/Credential HTTP 链路，返回 `mcp_tool_error`，
`error_details.stage=remote_tool`、`origin=downstream`，摘要为“身份上下文不可用”。
总览合法 `input={}` 也得到相同下游错误。参数类型错误则在本地返回路径明确的
`tool_schema_invalid`，没有被吞为默认空参数。

管理 API 返回 76 项能力，其中 20 项写工具需要审批。仓库合法嵌套参数测试返回
`status=failed`；错误类型参数返回 HTTP 422。本次没有成功的库存业务查询，不能关闭 #93。

## 当前认证配置和待办

AuraClaw 保存的 `chaintowermcp` 平台配置 revision 3 是 `auth_strategy=none`。
该策略不会发送 workload 凭据或可信 tenant/user/dept 头；Java 工具需要已恢复的身份上下文。
因此必须修正显式认证配置，不能让 none 模式隐式发送可信身份，也不能向业务 arguments 注入身份。

用户已授权使用测试 Vault：已创建专用引用 `vault/chaintower-mcp-test#workload`，
使用 KV v2 CAS=0 防止覆盖；未复用其他开发端点的凭据，未输出密钥值。
Vault 只负责保管和提供凭据，下游是否校验对应 workload token 仍须独立核实和配置。

通过管理 API 保存候选时，当前业务租户身份被平台写权限检查拒绝，服务返回 403，
外层呈现 HTML 404。已只读确认 latest/active revision 仍为 3，候选未提交，旧配置仍生效。
需要合法平台管理员通过管理入口保存 `workload_trusted_context` 和上述引用，
随后 test、enable、完整 reconcile，再对照管理测试和新聊天的成功结果。
不直接修改数据库，不伪造平台管理员断言，不绕过网关或授权检查。

## Skill 和其他未完成验收

- 受信 v2 包与现有 publication 摘要一致，installation 仍 pin v1；本次未修复 pin 或触发旧包删除。
- 六次旧 Skill unknown 调用的业务结果尚未确认。相关 Credential audit 的 completed 仅说明出口请求完成，不能证明业务成功或无副作用。
- 不重放这些旧调用、不修改历史 Canonical Events、不以 Run 终态直接释放旧包引用。
- 真实 create/resume、旧对象物理清理、撤销/恢复故障矩阵继续由 #89–#100 跟踪。
- 各 issue 已更新部署证据、认证诊断和剩余条件，保持开放；离线测试通过不替代现场验收。

## 后续部署与当前阻塞（2026-09-04）

用户明确将 MCP 配置管理授权交给外层，并授权向既定测试主机传输镜像及部署。
上文平台写权限阻塞已由 `286ab06` 解决；1/1/100 身份成功保存 workload 候选 revision 4。
继续现场验收交付了 `c2bc9e5`（共享凭据 owner）、`315da8f`（候选 probe 隔离）、
`462b9e6`（控制 RPC 30 秒超时）、`ef6cd69`（内部 HTTP 服务发现副本控制分发）。
代码均已推送。Docker Hub 超时后，以已验证的本地/远端镜像及相同依赖生成新不可变镜像。

当前组件版本：Hands `ef6cd69`，Task API `315da8f`，Credential Proxy `c2bc9e5`，
其他业务服务 `286ab06`。各次更新健康检查通过，schema 仍为 0063。这些补丁不改变内部 DTO，
按组件职责部署；后续完整发布应统一到最新镜像。

当前候选测试已经到达 Vault，但运行时 token 的精确路径读取和 lookup-self 都返回 403。
此证据不足以区分过期与策略缺失。候选未启用，active 仍为 revision 3；库存业务成功验收未完成。
自动审批禁止提取旧 token，并再次拦截挂载位置/sudo 检查；没有执行被拒绝操作或绕过访问限制。
已请求用户明确授权在测试 Vault 创建专用只读 token、更新 Credential Proxy 的
`/run/secrets/vault_token` 对应部署凭据并重启。新的服务 token 尚未创建或替换，旧 token 未展示。

相关回归与 Ruff/Mypy 通过；全量 unit 中本地 socket 沙箱受限的真实 HTTP 项单独提权重跑通过。
#93/#98/#99/#100 已更新此次证据及待办，保持开放；旧 Skill pin/unknown/物理清理仍未改动。


## Vault 访问恢复与后续验收（2026-09-04）

用户授权后创建仅专用 MCP KV 路径 read 的测试 token，无 default policy、不可续期、
最长 24 小时。Vault capabilities 验证其他凭据路径、创建 token、管理策略均 deny。
初次以非目标路径 HTTP 状态做检查未通过，临时 token 当即撤销，未安装；
随后用 Vault 权限接口确认实际权限，获准安装。旧 token 未读取，密钥未写入仓库或日志。

两个 Credential Proxy 已使用 ef6cd69 镜像重建并健康启动。
候选 revision 4 TEST 与 ENABLE 均 succeeded，active_revision=4。
之前等待授权、Vault 403、active revision 3 的描述仅为历史诊断，不再是当前状态。

当前测试 token 预计在北京时间 **2026-09-05 10:50** 到期。
这是用于继续验收的短期凭据，长期自动续发/重新认证尚未配置，不能作为长期修复验收完成。
到期前需要平台确定受限工作负载自动认证方案；不得把管理/root token 配置给服务。

真实只读管理测试仍收到 mcp_tool_error / remote_tool / downstream，摘要“身份上下文不可用”。
原先输出 Schema 校验把这个错误覆盖为 `$ violates anyOf`，现修正为仅校验成功结果，保留原始错误。
新聊天第一次错误限定 kinds=skill/resource 后耗尽搜索预算；补充搜索说明及空结果提示，
明确普通 MCP 查询使用 tool 或不限制 kinds。明确 Tool 的新验收中 search/load 成功，
总览生成合法 input={}，location 生成 input.type=warehouse/input.limit=10；
两项执行仍返回相同 Java 身份错误，没有库存查询成功证据。

Java 本地源码的 McpTrustedContextFilter 读取 X-CT-Tenant-ID/X-CT-User-ID，
再向 AdminUserApi 查询启用用户及部门。AuraClaw 出站代码头名与之匹配；
尚未取得部署端收到的头、Filter 生效和 Java 用户事实源查询结果，不能猜定其中某一步的原因。
管理探测错误保真回归及目录/Agent/管理联合 45 项通过，Ruff/Mypy 通过，无 DDL。
