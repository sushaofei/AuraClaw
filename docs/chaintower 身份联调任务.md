# chaintower 身份联调任务（Issue #44 关联）

AuraClaw 仓库内已冻结 ADR-003：chaintower 是用户身份权威，MCP Server 是业务权限最终裁决者。
chaintower 不在本 GitHub 仓库中，请在其项目管理系统创建关联任务并回链本 Issue。

## AuraClaw 已提供的契约

调用 Task API：

```http
Authorization: Bearer <chaintower-workload-credential>
X-CT-Agent-Context: <base64url(canonical-json)>.<base64url(hmac-sha256)>
X-Correlation-ID: <id>
```

声明字段：`iss=chaintower`，`aud=auraclaw-task-api`，`tenant_id`，`user_id`，`scopes` 含
`agent.task.invoke`，`iat`/`exp`（默认 ≤5 分钟），`jti`，`kid`。可选 `session_id`、`dept_id`、
`permission_version`。create 之后的调用应绑定 `session_id`。

密钥与 AuraClaw `AURACLAW_AGENT_CONTEXT_SIGNING_KEYS_JSON` 的 N/N-1 `kid` 对齐。

## chaintower 需要完成

1. 入口从已登录用户构造 Agent Context，不接受前端 tenant/user。
2. 提供签发/轮换/撤销短期 Assertion 的系统服务。
3. MCP Server 验证 AuraClaw/Hands workload 与 per-request trusted context
   （`Authorization` + `X-CT-Tenant-ID` / `X-CT-User-ID` / `X-CT-Session-ID`，以及
   `_meta.io.auraclaw/tenantId|userId`）。
4. Tool/Resource/Prompt 执行前恢复 LoginUser、tenant、dept 和数据权限。
5. 请求 DTO 中兼容 tenant/user 字段不得作为授权来源。
6. 用户禁用、Grant 撤销、租户变化、权限版本失效时 fail closed。
7. 对 discover/list/read/get/call 分别定义最小 scope。
8. 与 AuraClaw 建立双仓 contract/integration tests。

## 灰度

1. chaintower 增加签发与 MCP 验证，保留旧 client_credentials。
2. AuraClaw 已支持 verifier 与 development Header 双路径。
3. 灰度 chaintower → AuraClaw signed context。
4. 灰度 Hands → chaintower MCP trusted context。
5. 监控 401/403、tenant mismatch、replay、MCP deny。
6. 全量后关闭生产裸 tenant/user Header。
7. 删除固定 tenant/user 的 OAuth client 配置。
