# MCP Server 热配置需求设计

> 实现跟踪：[GitHub Issue #50](https://github.com/sushaofei/AuraClaw/issues/50)。

## 1. 背景与结论

当前 AuraClaw 已有 `hands.downstream_mcp_server` 和 Capability Catalog 持久化能力，但远端
MCP Connector、Credential Proxy Egress Adapter 和 Credential Reference 仍主要在服务启动时根据
`AURACLAW_MCP_EGRESS_SERVERS_JSON` 装配。运行中写入 Server 记录不会自动生成可调用连接器；服务
重启后也仍依赖环境变量重新装配。因此，现状不满足“运行时配置、持久化恢复”的要求。

代码中的“本地限制”原意是防止 SSRF 和 DNS rebinding，并非 MCP 协议要求。当前实现其实存在
`allowed_private_hosts` 例外，可显式允许回环 HTTP；但该例外绑定在静态启动配置中，而且现有 Egress
Adapter 默认仍要求 `credential_ref`，普通本地 MCP Server 很难通过正常产品入口接入。因此本需求是
把本地/私网连接提升为正式配置模型，而不是删除整层网络校验。

本需求引入一个由 Action Hands 拥有的受管 MCP Server Registry：

- 管理员可在服务运行时创建、测试、启用、更新、禁用和退役 MCP Server。
- 配置先持久化为不可变 revision，再由运行时异步加载，不要求重启任何服务。
- 已启用 Server 的更新先验证候选 revision，验证成功后原子切换；失败时旧 revision 继续服务。
- Action Hands 和 Credential Proxy 重启后从 Registry 恢复当前生效 revision，不再要求重新提交配置。
- 本地、回环和私网 MCP Server 成为正式支持的网络模式，不再依赖“必须是公网 HTTPS”的隐式限制。
- 开放本地连接不等于删除 SSRF 防护；网络目标仍必须由受管配置固定，Runtime 和模型不能提交 URL。

本文中的“热配置”只指受管的 Streamable HTTP MCP Server。允许 Runtime 动态提交 URL、stdio
command、宿主环境变量或明文 Secret 仍是非目标。若后续支持 stdio MCP，应使用独立 Sandbox 和平台
签名命令模板另行设计。

## 2. 用户故事

### 2.1 配置与恢复

作为平台或 tenant 管理员，我可以在 AuraClaw 运行期间登记一个 MCP Server，完成连接测试并启用，
无需修改 `.env`、重启 Action Hands 或重启 Credential Proxy。

配置成功后，即使 AuraClaw 全部服务重启，Server 仍自动恢复为重启前的生效 revision，并重新完成
连接器装配和目录对账，不需要管理员再次配置。

### 2.2 安全更新

作为管理员，我可以修改 endpoint、认证引用、协议版本和能力 allowlist。更新不会直接覆盖正在工作的
连接器；候选配置验证通过后才接管新请求。验证失败时，管理操作显示失败原因，旧配置保持可用。

### 2.3 本地 MCP Server

作为开发者或平台管理员，我可以登记 `localhost`、`127.0.0.1`、`::1` 或明确的私网服务地址，
不需要伪造公网域名或修改源码。系统应明确提示：“loopback 是相对 Credential Proxy 所在网络命名空间
而言”；容器部署中宿主机服务通常应使用受管的宿主机别名或私网服务名，而不是容器内的
`127.0.0.1`。

## 3. 范围与非范围

### 3.1 本期范围

- Streamable HTTP MCP Server 的版本化管理 API。
- Server 非密配置、revision、期望状态、运行状态和操作结果的持久化。
- Action Hands Connector 与 Credential Proxy Egress Adapter 的运行时增加、替换、禁用和重启恢复。
- 公网、私网和回环三种网络模式。
- `oauth_client_credentials`、`workload_trusted_context` 和受策略控制的 `none` 认证模式。
- 配置校验、连接测试、目录对账、审计、指标和失败恢复。
- 现有环境变量配置到持久化 Registry 的兼容迁移。

### 3.2 非范围

- Runtime 或模型自行注册 MCP Server。
- 接受任意请求 URL、Header、Token、stdio command 或进程环境变量。
- 在 Registry、日志或 API 响应中保存明文 Secret。
- 本期新增 stdio MCP 生命周期管理。
- 因配置变化而修改已开始 Run 中固定的 Skill/Capability binding。

## 4. 配置模型

### 4.1 Server 配置

建议将当前 `McpServerDefinition` 拆分为“管理员期望配置”和“系统观测状态”。管理员可写字段示例：

```json
{
  "server_id": "local-order-mcp",
  "tenant_id": "tenant-a",
  "title": "Local Order MCP",
  "endpoint": "http://127.0.0.1:48080/mcp",
  "network_mode": "loopback",
  "protocol_revision": "2026-07-28",
  "auth_strategy": "none",
  "credential_ref": null,
  "oauth": null,
  "allowed_tool_prefixes": ["order."],
  "allowed_resource_schemes": ["order"],
  "allowed_prompt_prefixes": ["order."],
  "trust_level": "tenant_verified",
  "metadata": {}
}
```

`enabled`、`status`、`last_sync_at` 和错误计数不再混在同一可任意覆盖的 JSON 中：

- `desired_state`：`disabled | enabled | retired`，由管理命令写入。
- `observed_state`：`pending | loading | active | degraded | quarantined | disabled | unavailable`，由运行时写入。
- `config_revision`：每次配置修改单调递增。
- `active_revision`：当前实际承接新请求的 revision；可暂时落后于最新候选 revision。

`server_id` 本期继续保持全局唯一，避免改变现有 Capability ID、Tool owner 和策略资源标识语义。

### 4.2 网络模式

| 模式 | 允许的 endpoint | 地址判定 | 典型用途 |
| --- | --- | --- | --- |
| `public` | 仅 HTTPS | DNS 全部结果必须为公网地址，并固定已验证 IP/SNI | 第三方 SaaS MCP |
| `private` | HTTP 或 HTTPS | 主机必须匹配精确服务 allowlist；解析地址必须落入配置的 CIDR/地址集合 | Kubernetes、VPC、内网 Java 服务 |
| `loopback` | HTTP 或 HTTPS | 主机解析结果必须全部为 loopback；不接受解析到私网或公网地址 | 同机开发、同 Pod sidecar |

`allowed_private_hosts` 不再作为“知道隐藏开关才能连本地”的用户接口。它由 `network_mode`、固定 endpoint
host 和可选 `allowed_cidrs` 生成底层网络策略。底层仍保持：

- endpoint 必须是绝对 URL，不含 userinfo 和 fragment；
- 禁止 HTTP redirect；
- DNS 每次连接重新校验并固定已批准 IP，防止 DNS rebinding；
- 调用请求不能覆盖 endpoint、Host、Header、认证信息或网络模式；
- link-local、multicast、unspecified、保留地址默认拒绝；
- `private` 不得只凭 hostname 放行任意私网地址，必须同时命中服务/CIDR 策略。

### 4.3 无认证本地连接

新增 `auth_strategy: "none"`，使不带认证的本地开发 MCP Server 可以接入。它不是全局旁路：

- 只允许用于 `loopback` 或 `private`，`public` 永远不允许 `none`；
- 是否允许 `private + none` 由平台网络策略决定，而不是源码硬编码；生产默认拒绝，管理员可在受控环境显式开启；
- 即使无远端认证，调用仍经过 AuraClaw tenant、Policy、Approval、Tool allowlist 和 Invocation Store；
- 无认证调用仍通过 Credential Proxy 的受管 Egress Adapter 发出，但不解析 Vault Secret。

## 5. 持久化与事实归属

### 5.1 数据表

建议用 expand migration 演进现有表，逻辑模型如下：

```text
hands.mcp_server
  server_id PK
  tenant_id
  desired_state
  latest_revision
  active_revision nullable
  created_by / created_at / updated_at

hands.mcp_server_revision
  server_id + revision PK
  immutable_config_json
  config_digest
  created_by / created_at

hands.mcp_server_runtime
  server_id PK
  loaded_revision nullable
  observed_state
  last_test_at / last_sync_at
  consecutive_failures
  safe_error_code / updated_at

hands.mcp_server_operation
  operation_id PK
  server_id / target_revision
  command_id / actor / correlation_id / causation_id
  operation / status / safe_error_code
  created_at / completed_at
```

配置 revision 不可原地修改。回滚是“复制一个历史 revision 的内容并创建新 revision”，因此审计链和
单调版本保持完整。`hands.capability_catalog` 仍是可重建目录，不承担 Server 配置事实源职责。

所有写命令必须携带 tenant、command id、expected config revision、actor、correlation 和 causation。
相同 command id 重试返回同一 operation；expected revision 不一致返回 `409 Conflict`。

### 5.2 唯一写入方

Action Hands 的 `McpServerRegistryService` 是 Server 配置、revision 和 desired state 的唯一写入方。
Catalog Reconciler 只写 observed state 和可重建 Capability Catalog，不能反向覆盖管理员配置。
Credential Proxy 不直接修改 Registry；它只加载指定 revision、管理 Egress Adapter 并返回加载结果。

Secret 继续由 Vault 保存。Registry 只保存 `credential_ref` 和 OAuth 非密元数据。Credential Reference
由 Credential Proxy 的现有持久化 Registry 保存，且 provider、account scope、operation 必须与
Server revision 匹配。

## 6. 管理 API

对外管理入口通过受认证的 Ops/Admin facade 暴露，业务实现归 Action Hands；不要把具体数据库适配器
放进 `api`。建议的版本化资源 API：

```text
POST   /v1/admin/mcp-servers
GET    /v1/admin/mcp-servers
GET    /v1/admin/mcp-servers/{server_id}
PUT    /v1/admin/mcp-servers/{server_id}
POST   /v1/admin/mcp-servers/{server_id}:test
POST   /v1/admin/mcp-servers/{server_id}:enable
POST   /v1/admin/mcp-servers/{server_id}:disable
POST   /v1/admin/mcp-servers/{server_id}:reconcile
POST   /v1/admin/mcp-servers/{server_id}:retire
GET    /v1/admin/mcp-operations/{operation_id}
```

约束：

- 创建和更新只写候选 revision，默认不直接启用。
- 写请求要求 `Idempotency-Key`、`X-Expected-Revision`（创建固定为 `0`）和权威管理员身份。
- 返回 `202 Accepted + operation_id`；调用方通过 operation 查询装载、测试和切换结果。
- `test` 执行 DNS/网络策略、认证、协议握手和受限 list，不发布 Capability 或 Tool 路由。
- `enable` 只有在目标 revision 测试成功后才设置为 active；可将“测试并启用”实现为同一 operation。
- `disable` 先阻止新调用并撤销发现/路由，再异步排空旧连接。
- `retire` 是软删除；保留 revision、Operation、Invocation 和审计证据。本期不提供硬删除 API。
- tenant 管理员只能管理本 tenant 的 Server；平台 Server 只允许平台管理员操作。
- API 响应不回显 Secret、Token 或 Vault 内容，错误只返回稳定 `safe_error_code`。

## 7. 热加载和原子切换

### 7.1 组件

```text
Admin API
  -> McpServerRegistryService
     -> Registry Store + Transactional Outbox
        -> Action Hands McpConnectionManager
        -> Credential Proxy McpEgressManager
           -> Catalog Reconciler
              -> Capability Catalog / Tool Registry / Routes
```

- `McpConnectionManager` 根据 active revision 构建 `ManagedMcpConnector`。
- `McpEgressManager` 根据同一 revision 构建网络和认证 Adapter。
- 配置变更通过持久化 Outbox 通知；通知丢失时，两个 Manager 至少每 30 秒按 revision 对账。
- 服务启动时先读取完整 active snapshot，再接收增量通知。环境变量不再是运行事实源。
- Action Hands 提供只读的内部 Registry snapshot/revision contract；Credential Proxy 通过该契约启动恢复，
  不跨服务写 Hands 数据表。Action Hands readiness 不依赖任一远端 MCP 可达，避免形成启动死锁。
- Hands 发给 Credential Proxy 的受管调用必须携带 `server_id + config_revision`。Proxy 发现 revision
  不匹配时拒绝请求并触发重载，不能用新路由误调用旧 endpoint。

### 7.2 更新流程

```text
持久化候选 revision
 -> 两侧做结构校验并预构建 Connector/Adapter
 -> test: DNS/认证/server discover/list
 -> 在 Registry 事务中推进 active_revision
 -> 两侧以 copy-on-write 原子替换 active generation
 -> Catalog 对账并发布新目录
 -> 旧 generation 排空后关闭
```

新请求只使用新 generation；已进入 Tool Gateway 的调用继续持有旧 generation，直到完成、超时或取消。
对于可能产生外部副作用的调用，不因配置更新而盲目重试。若切换后 Catalog 对账失败，新 revision 进入
`degraded/quarantined` 并撤销新路由；是否自动回切旧 revision 必须由明确策略决定，默认不自动回切，
避免把管理员已撤销的 endpoint 或凭证重新启用。

### 7.3 禁用流程

禁用命令持久化成功后应立即：

1. 将 desired state 设为 `disabled`；
2. 从 Tool Registry、Resource/Prompt 路由和发现结果撤销 Server；
3. 拒绝该 Server 的新 Egress 调用；
4. 保留已开始 Invocation 的幂等与副作用状态，按原 deadline 排空；
5. 清理短期 OAuth Token 和连接池，但不删除 Vault Secret、配置 revision 或审计记录。

## 8. 启动恢复与旧配置迁移

- Action Hands 启动时从持久化 Registry 恢复所有 `enabled` Server；先建立路由骨架，再完成远端目录对账。
- Credential Proxy 启动时读取相同 active snapshot 接口，装配 Egress Adapter；不依赖进程内 seed。
- 远端暂时不可用不会丢失配置。Server 显示 `degraded/unavailable`，后台按退避策略恢复。
- Readiness 不应因某个可选 MCP Server 不可用而让整个 Action Hands 或 Credential Proxy 退出服务；
  应通过 Server 状态和告警暴露局部故障。
- 首次升级时，将 `AURACLAW_MCP_EGRESS_SERVERS_JSON` / file 中的条目作为 bootstrap source 幂等导入。
  仅当 `server_id` 尚不存在时创建 revision 1；数据库已有记录时绝不被环境变量覆盖。
- 完成迁移后，环境变量只保留一版兼容期并标记 deprecated；最终删除运行时静态装配路径。
- `storage_backend=memory` 仅用于测试，明确不提供跨重启保证；所有可部署 profile 必须使用 SQL Registry。

## 9. 状态、失败与可观测性

### 9.1 失败语义

| 场景 | 预期行为 |
| --- | --- |
| 候选 revision 连接测试失败 | operation 失败，错误安全化，旧 active revision 不变 |
| 配置通知丢失 | 周期 revision 对账补偿 |
| Hands 已加载、Proxy 未加载 | revision mismatch，调用 fail closed 并触发重载 |
| 重启时远端 Server 不可达 | 配置保留，状态 degraded/unavailable，退避重试 |
| disable 与调用并发 | 禁止新调用；已开始调用按 Invocation 语义排空，不盲目重试 |
| DNS 从允许地址漂移到禁止地址 | 本次连接拒绝，Server 降级；不沿用旧 DNS 结果绕过策略 |
| active revision 内容摘要异常变化 | 隔离并告警；不可原地覆盖 revision |

### 9.2 指标与审计

至少增加：

```text
mcp_config_operation_total{operation,status}
mcp_config_apply_latency
mcp_loaded_revision{component,server_id}
mcp_revision_mismatch_total
mcp_server_observed_state{server_id,state}
mcp_connection_test_total{network_mode,result}
mcp_connector_reload_total{result}
mcp_connector_drain_seconds
```

审计记录包含 tenant、server id、前后 revision、actor、command、correlation、网络模式和结果；endpoint
可记录规范化值，但不得记录 Secret、Authorization Header、Token 或完整远端响应。

## 10. 验收标准

### 10.1 功能

- 在服务运行期间创建并启用一个 MCP Server，无需重启即可搜索并调用其 Tool。
- 更新已启用 Server 时，新配置验证成功后接管新请求；验证失败时旧 Server 仍可调用。
- 禁用后新发现和新调用在目标 5 秒内失效，已开始调用不产生重复副作用。
- Action Hands 与 Credential Proxy 分别重启、共同重启后，已启用 Server 能从数据库自动恢复。
- 环境变量导入只发生一次，重启不会覆盖管理员后续修改。
- 操作幂等、revision 冲突、跨 tenant 访问和软删除均有自动化测试。

### 10.2 本地连接

- development profile 可用 `loopback + http + auth none` 连接真实本地 MCP Server。
- `localhost`、`127.0.0.1` 和 `::1` 均有解析与连接测试。
- `loopback` 配置解析到私网或公网地址时拒绝；`private` 地址不在配置 CIDR 时拒绝。
- 公网 HTTP、redirect、userinfo URL、DNS rebinding、link-local 元数据地址和请求覆盖 endpoint 均被拒绝。
- 容器内 loopback 语义在 API/CLI 错误提示和运维文档中明确。

### 10.3 架构与安全

- Runtime 仍只调用内部 Hands Contract，不读取远端 URL、网络模式或 Secret。
- Action Hands 是 Server 配置唯一写入方；Credential Proxy 是 Secret 和 Egress Adapter 所有者。
- 配置事实、运行状态与可重建 Capability Catalog 分离。
- 所有写入包含 tenant、command id、expected revision、actor、correlation 和 causation。
- Secret 不进入 Registry、Session Event、Prompt、Artifact、日志和 API 响应。
- Ruff、Mypy、Pytest、import-linter、SQL migration up/down 和真实进程冒烟全部通过。

## 11. 建议实施拆分

1. **Registry 与迁移**：配置 revision、operation、observed state、Admin application service 和 API 契约。
2. **动态加载**：Action Hands Connection Manager、Credential Proxy Egress Manager、revision fencing 和启动恢复。
3. **网络模式**：public/private/loopback 策略与 `auth none`，补齐 SSRF/DNS/容器网络测试。
4. **切换与对账**：候选测试、原子 promote、排空、禁用、Catalog reconcile 和失败补偿。
5. **兼容迁移与运维**：环境变量一次性导入、CLI/API 使用说明、指标告警和回滚演练。

每一拆分都必须补充 [开发阶段校验清单](./开发阶段校验清单.md)；全部适用项完成后才能把本需求标记为完成。
