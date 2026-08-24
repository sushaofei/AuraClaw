# AuraMCP 接入

AuraMCP 是标准 MCP `2026-07-28` Server，承载 AuraClaw **内核以外** 的扩展 Tool / Resource / Prompt。

接入方向固定：

```text
Runtime ──Hands HTTP──► Action Hands ──ManagedMcpConnector──► Credential Proxy
                                                              └── POST /mcp ──► AuraMCP
```

- Runtime / AuraX **不得**直连 AuraMCP。
- AuraClaw **不得**把内核 Tool（如价格洞察）迁入 AuraMCP。
- chaintower 业务 MCP **不得**指到 AuraMCP。
- 身份仍是 Hands workload + trusted context；不要为 AuraMCP 配固定租户的 OAuth client。

登记走 **MCP Server 热配置 Admin API**（`/v1/admin/mcp-servers`）。配置写入 Hands Registry，Action Hands 与 Credential Proxy 无需重启即可加载；重启后从 Registry 恢复。

配套：AuraMCP 仓库 `docs/AuraClaw 接入.md`。

## 登记

Vault 中先放入 `vault/auramcp#workload`（与 `AURAMCP_HANDS_WORKLOAD_TOKEN` 相同）。然后：

```http
POST /v1/admin/mcp-servers
Idempotency-Key: auramcp-create
X-Expected-Revision: 0
```

```json
{
  "server_id": "auramcp",
  "title": "AuraMCP extensions",
  "endpoint": "https://auramcp.internal/mcp",
  "protocol_revision": "2026-07-28",
  "network_mode": "public",
  "auth_strategy": "workload_trusted_context",
  "credential_ref": "vault/auramcp#workload",
  "trust_level": "platform",
  "allowed_tool_prefixes": ["auramcp."],
  "allowed_resource_schemes": ["auramcp"],
  "allowed_prompt_prefixes": ["auramcp."]
}
```

创建成功后 `POST /v1/admin/mcp-servers/auramcp:test`，再 `:enable`。Catalog 出现 `auramcp.health.ping`、`auramcp.example.echo`、`auramcp.ops.catalog_snapshot` 即登记成功。

AuraMCP 侧：

- `AURAMCP_HANDS_WORKLOAD_TOKEN` 与 Vault workload 相同
- 生产必须 `AURAMCP_DEPLOYMENT_PROFILE=production` 且 `AURAMCP_ALLOW_INSECURE_IDENTITY=false`

Hands 用标准 MCP 方法访问，并发送 `Authorization: Bearer <workload>`、`X-CT-*` 与 `_meta.io.auraclaw/*`。

## 本地联调

AuraMCP 默认听 `127.0.0.1:8020`。热配置 body 与生产相同，只改网络：

```json
{
  "endpoint": "http://127.0.0.1:8020/mcp",
  "network_mode": "loopback"
}
```

`loopback` 相对 **Credential Proxy 所在网络命名空间**。容器内的 `127.0.0.1` 不是宿主机。开发可 `AURAMCP_PLATFORM_STORE=memory`。Hands 出站契约由 `tests/unit/test_auramcp_egress.py` 覆盖。

## 不要做的事

- 不要让 Runtime 持有 AuraMCP URL 或 workload。
- 不要把 `tenant_id` / `user_id` / `dept_id` 放进 Tool arguments 当授权来源。
- 不要为 AuraMCP 发明规范外 JSON-RPC 方法。
