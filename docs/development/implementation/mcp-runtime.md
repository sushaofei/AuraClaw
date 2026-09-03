# M9 MCP Runtime 实施与运维

## 1. 交付范围

M9 把 Agent Runtime 的数据、工具、Prompt 和 Skill 接入统一到内部 Action Hands Gateway。
Issue #43 之后 Runtime 只持有 Hands URL 和 workload identity，不接受远端 URL、stdio command
或 Secret；MCP wire 只存在于下游 Connector。

实现分为以下边界：

- `contracts/hands.py`：Runtime↔Hands 的协议无关 DTO 与内部 HTTP 路径。
- `infrastructure/connectors/mcp/wire.py`：下游 MCP 2026-07-28 JSON-RPC；保留
  2025-11-25 legacy profile 供滚动升级。
- `contracts/capabilities.py`：受管 Server、可选 OAuth/OIDC 或 workload trusted-context
  认证策略，以及统一 Capability 描述符。
- `contracts/skills.py`：Skill Manifest、发布状态、固定依赖和激活绑定。
- `action`：Capability Catalog、Resource Gateway、Skill Package/Resolver 和周期对账。
- `runtime`：单一 Hands Client、渐进式 Skill 加载和可恢复 Skill Runner。
- `infrastructure/credentials`：Credential Proxy 内的 OAuth/OIDC MCP Egress Connector。

MCP 不成为新的业务事实源。Skill 激活与终态、重要 Resource 采用和 Tool 结果仍写入
Canonical Session Event；目录、缓存、通知和步骤进度都可丢弃并重建。

## 2. 远端 Server 配置

远端 Server 用热配置 Admin API（`POST /v1/admin/mcp-servers`，再 `:test` / `:enable`）登记。
配置只能包含非密信息和 `credential_ref`，不能包含 client secret 或 access token。Action Hands
与 Credential Proxy 从 Registry 恢复已启用 revision，不再读取环境变量静态清单。

```json
{
  "server_id": "github-mcp",
  "tenant_id": "tenant-a",
  "title": "GitHub MCP",
  "endpoint": "https://mcp.example/v1/mcp",
  "protocol_revision": "2026-07-28",
  "network_mode": "public",
  "credential_ref": "vault/github-mcp#client_secret",
  "oauth": {
    "protected_resource_metadata_url": "https://mcp.example/.well-known/oauth-protected-resource",
    "authorization_server_metadata_url": "https://auth.example/.well-known/oauth-authorization-server",
    "issuer": "https://auth.example",
    "token_endpoint": "https://auth.example/oauth/token",
    "client_id": "auraclaw-hands",
    "resource": "https://mcp.example/v1/mcp",
    "scopes": ["tools.read"]
  },
  "allowed_tool_prefixes": ["github."],
  "allowed_resource_schemes": ["github"],
  "allowed_prompt_prefixes": ["github."]
}
```

OAuth `client_credentials` 是**可选 Connector 策略**，不是 AuraClaw 用户身份系统。
第三方 MCP 可继续使用 OAuth；chaintower MCP 应使用 `auth_strategy: "workload_trusted_context"`，
由 Hands 从 `HandsTrustedContext` 构造受控 Header/`_meta`，MCP Server 做最终业务鉴权。
见 [ADR-003](../../architecture/decisions/ADR-003-trusted-identity-context.md)。

```json
{
  "server_id": "chaintower-mcp",
  "title": "chaintower MCP",
  "endpoint": "https://mcp.chaintower.example/mcp",
  "credential_ref": "vault/chaintower-mcp#workload",
  "auth_strategy": "workload_trusted_context",
  "allowed_tool_prefixes": ["order."]
}
```

AuraMCP 扩展面同样走 `workload_trusted_context`，前缀锁定 `auramcp.` / `auramcp://`。
见 [AuraMCP 接入](../../guides/auramcp-integration.md)。

```json
{
  "server_id": "auramcp",
  "title": "AuraMCP extensions",
  "endpoint": "https://auramcp.internal/mcp",
  "protocol_revision": "2026-07-28",
  "network_mode": "public",
  "credential_ref": "vault/auramcp#workload",
  "auth_strategy": "workload_trusted_context",
  "allowed_tool_prefixes": ["auramcp."],
  "allowed_resource_schemes": ["auramcp"],
  "allowed_prompt_prefixes": ["auramcp."]
}
```

对应 Credential Reference 的 provider 必须等于 `server_id`。OAuth 路径的 account scope 必须等于
OAuth `resource`；workload trusted-context 路径的 account scope 等于 MCP endpoint origin。
allowed operations 必须包含 `mcp.invoke`。Secret 只由 Vault 和 Credential Proxy
解析。Hands 只传 credential ref、受信身份、JSON-RPC 摘要和 Policy decision id。

`AURACLAW_MCP_RECONCILE_INTERVAL_SECONDS` 控制周期全量对账，默认 60 秒，最小 5 秒。

## 3. 安全与恢复

每次远端连接都执行：

```text
Server Registry / tenant
 -> Policy
 -> Credential provider + account scope
 -> 认证策略：
    - oauth_client_credentials：OAuth metadata + client_credentials + Resource Indicator
    - workload_trusted_context：AuraClaw/Hands workload Bearer + 受控 tenant/user Header
 -> DNS 全量公网地址校验
 -> 固定已校验 IP + 原 Host/SNI
 -> 禁止重定向
 -> MCP method / Tool prefix / Resource scheme / Prompt prefix
 -> 响应大小、Content-Type、JSON/SSE 与脱敏
```

任一 DNS 结果为 loopback、private、link-local、reserved 或其他非公网地址时整次请求失败。
Runtime 不能覆盖 endpoint、Header、Authorization 或 Token。当前仅接入自行开发的 MCP，
直接采用 `annotations.readOnlyHint`：true 为 `read-only`，false 或缺失为 `write-with-approval`。
风险等级采用 `_meta.auraclaw.riskLevel`，未提供时只读为 `low`、其余为 `high`。
不再配置 Server 信任等级、全局信任开关或工具级权限覆盖。

2026-07-28 Server 只有在 `server/discover` 和全部分页 list 成功后才发布快照；显式登记为
2025-11-25 的 legacy Server 仍走 `initialize`。同名同版本内容漂移会失败，不会覆盖
旧摘要。新快照只改变发现集合，已固定 Skill 仍可使用旧 Tool 版本；Server 连续三次同步失败后
进入 `quarantined`，届时旧路由也被撤销。`list_changed` 只加速对账，
`notifications/resources/updated` 只失效 Resource cache；通知丢失由周期对账修复。

MCP Tasks Extension 未启用，也不在逐请求 client capabilities 中声明。Hands Invocation Store、Runtime
Checkpoint 和 Session 状态机仍是长调用恢复边界。

## 4. Skill 生命周期

Skill 包必须包含 `manifest.json` 和 `SKILL.md`，可包含 `references/`、`scripts/*.workflow.json`、`assets/` 和 `tests/`。
发布会校验 canonical path、UTF-8、文件数、总大小、Manifest、版本约束和发布者签名，然后把
不可变包写入 Artifact。相同 publisher/name/version 不允许替换内容。

Resolver 固定 package digest、Tool schema digest、Resource digest、Policy version/decision 和
版本。Runner 只把稳定证据写入 `skill.activated/completed/failed/cancelled`；当前步骤游标与
完成步数写入 Control Checkpoint。进程死亡后从游标继续，不重新解析依赖。

含 Workflow 的包在发布阶段完成 schema、依赖、selector、reference、步骤预算、timeout 和稳定 digest 校验。
Runtime 只执行固定 binding 中的 entrypoint/digest；每个步骤通过 CapabilityClient/Hands 调用，使用稳定
invocation/idempotency key。Workflow 不执行 Python/Shell/JS/Wasm，不直连 MCP，不读取 credential。

## 5. 迁移、发布与回滚

M9 新增：

- `0015_m9_capability_catalog.sql`：受管 Server 和 Capability Catalog。
- `0016_m9_skill_projection.sql`：Task Projection 的 `skill_activations`。
- `0018_mcp_protocol_revision.sql`：新注册远端 Server 的数据库默认 revision 升为 `2026-07-28`。
- `0041_capability_catalog_consistency.sql`：实例级 MCP observed state、Catalog generation 和
  last-known-good publication 时间。

发布顺序是先执行 expand migration，再滚动 Credential Proxy、Action Hands、Runtime 和
Projection。回滚应用时先停用远端 Server 配置，再回滚服务；只有确认没有旧版本读取
`skill_activations` 后才执行 `0016_m9_skill_projection.down.sql`。`0015` 的 down migration
会删除 Catalog，只能在确认不再使用 M9 后执行。Canonical Event 和 Artifact 不随 Catalog
回滚删除。

## 6. 验证与告警

必须关注：

- Server status、连续同步失败、同步耗时和 capability count。
- Resource cache hit/deny、内容大小、DLP 和 prompt-injection finding。
- Skill resolve/activation/step cursor/terminal outcome。
- Tool approval、Invocation unknown side effect 和远端协议错误。
- OAuth discovery/token failure、DNS 拒绝、redirect 拒绝和响应大小拒绝。

发布门禁命令：

```bash
uv run ruff check .
uv run mypy src/auraclaw
uv run pytest
uv run lint-imports
```

真实生产 Secret、Token、完整 Resource/Skill 内容和 Tool 参数/结果不得写入日志或阶段报告。
