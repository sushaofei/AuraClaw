# Java 服务 MCP 接入手册

> 适用仓库：AuraClaw  
> 评审日期：2026-08-19  
> 结论基于当前代码、MCP `2026-07-28` 规范和 MCP Java SDK 2.x。  
>
> 给 Agent / Java 开发人员分享、按步骤接入时，优先使用
> [MCP 开发手册](./MCP%20开发手册.md)。本文保留评审结论、协议细节和基础设施重构项。

## 1. 先给结论

Java 服务接口应在 **Java 服务自身的适配层**，或紧邻 Java 服务部署的独立 MCP Adapter
中封装为 MCP；AuraClaw 只负责受管注册、目录同步、策略、审批、凭证代理和调用路由。

不要在以下位置逐接口编写 Java REST 调用：

- `runtime/`：Runtime 只能连接内部 Action Hands MCP，不能直连业务服务。
- `contracts/`：这里只放稳定协议 DTO，不放 HTTP Client 或业务映射。
- `api/`：这是 AuraClaw 的交付面，不应选择或实现下游基础设施适配器。
- `composition/services.py`：这里只装配实现，不应继续堆积每个 Java 服务的接口逻辑。

推荐调用链：

```text
Agent Runtime
  -> AuraClaw Action Hands MCP
  -> Tool Gateway / Policy / Approval / Invocation Store
  -> Managed Remote MCP Transport
  -> Credential Proxy / Egress
  -> Java MCP endpoint (/mcp)
  -> Java application service
  -> repository / downstream system
```

当前架构方向是正确的，但接入过程还不够简洁。正式接入 Java 服务前，建议先完成一组
小而集中的 MCP 基础设施重构，见第 8 节。

## 2. MCP 原语如何映射 Java 接口

不要机械地把每个 REST Endpoint 变成一个 Tool。先按语义分类：

| Java 能力 | MCP 表达 | 例子 |
| --- | --- | --- |
| 执行动作、查询参数复杂、可能产生副作用 | Tool | `order.create`、`order.cancel`、`order.search` |
| 由稳定 URI 定位的只读内容 | Resource | `order://orders/123` |
| 参数化只读内容 | Resource Template | `order://orders/{orderId}` |
| 可复用交互模板 | Prompt | `order.review` |

推荐规则：

- 命令接口使用 Tool，名称采用 `<domain>.<aggregate>.<verb>`。
- 简单 GET 不必全部做 Tool；如果结果天然由 URI 标识，优先 Resource。
- 一个 Tool 对应一个业务意图，不对应一个 Controller 方法。
- Handler 调用 Java application service，避免在同一进程内再次 HTTP 回调自己的 REST API。
- 如果 MCP Adapter 是独立服务，再通过生成的 OpenAPI Client 或稳定内部 Client 调 Java API。
- 不向模型暴露数据库字段、内部枚举、分页实现和凭证字段。

## 3. Java 侧推荐目录

### 3.1 可以修改原 Java 服务

```text
com.example.order
├── application/
│   └── OrderApplicationService.java
├── domain/
├── infrastructure/
├── web/
│   └── OrderController.java
└── mcp/
    ├── McpServerConfiguration.java
    ├── OrderToolDefinitions.java
    ├── OrderToolHandlers.java
    ├── OrderResourceHandlers.java
    └── McpResultMapper.java
```

`web/` 和 `mcp/` 是两个平级适配层，共用 `application/`，两者不互相调用。

### 3.2 不能修改原 Java 服务

创建独立部署的 `order-mcp-adapter`：

```text
order-mcp-adapter
├── client/       # 由 OpenAPI 或稳定契约生成/维护的 Java Client
├── mcp/          # Tool、Resource 和 /mcp transport
├── mapping/      # 外部 DTO <-> MCP DTO
├── security/     # OAuth、mTLS、审计上下文
└── observability/
```

不要把这个特定业务 Adapter 放进 AuraClaw 的 `action/` 或 `infrastructure/`；否则每接入一个
Java 服务都要发布 AuraClaw，Capability Plane 会退化成集成代码仓库。

## 4. Java MCP Server 实现步骤

### 4.1 选择 SDK 和 Transport

- 使用官方 MCP Java SDK 2.x；如果是 Spring Boot，可使用 Spring AI 2.0+ 提供的
  WebMVC/WebFlux transport。
- 远程服务使用 Streamable HTTP，统一暴露 `POST /mcp`。
- 不为远程生产接入使用 stdio；不要新建 Legacy HTTP+SSE 接口。
- SDK 版本必须锁定，不能使用 Maven 动态版本。

官方 Java SDK 提供 Servlet、WebMVC 和 WebFlux 的 Streamable HTTP transport，也会校验
Tool 输入 JSON Schema。参考：

- [MCP Java SDK Server](https://java.sdk.modelcontextprotocol.io/latest/server/)
- [MCP Java SDK repository](https://github.com/modelcontextprotocol/java-sdk)

### 4.2 定义 Tool 契约

下面是结构示例，具体 import 以锁定的 SDK 小版本为准：

```java
var getOrderTool = SyncToolSpecification.builder()
    .tool(Tool.builder("order.order.get", inputSchema)
        .description("Get one order visible to the authenticated tenant")
        .build())
    .callHandler((exchange, request) -> {
        var orderId = (String) request.arguments().get("orderId");
        var order = orderApplicationService.getOrder(orderId);

        return CallToolResult.builder()
            .content(List.of(new TextContent("Order loaded")))
            .structuredContent(orderResultMapper.toMcp(order))
            .isError(false)
            .build();
    })
    .build();
```

每个 Tool 必须具备：

- 稳定且带命名空间的名称，例如 `order.order.get`。
- 清晰描述，包括适用场景，不包含诱导模型绕过策略的文字。
- 根类型为 `object` 的 `inputSchema`，默认 `additionalProperties: false`。
- 明确 `required`、字符串长度、数值边界、枚举和数组上限。
- `outputSchema` 与 `structuredContent` 一致。
- 业务错误返回 `isError=true` 和安全摘要；异常堆栈不进入 MCP 响应。

### 4.3 JSON Schema 示例

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object",
  "properties": {
    "orderId": {
      "type": "string",
      "minLength": 1,
      "maxLength": 64,
      "pattern": "^[A-Za-z0-9_-]+$"
    }
  },
  "required": ["orderId"],
  "additionalProperties": false
}
```

输入 Schema 是安全边界，不只是给模型看的说明。Java Handler 仍需执行租户、权限、对象状态和
幂等校验。

### 4.4 副作用和幂等

写操作必须：

- 接收业务幂等键，或把 AuraClaw invocation id 映射为 Java 服务幂等键。
- 在 Java 服务中持久化去重，不能只依赖进程内缓存。
- 超时后把副作用状态视为 `unknown`，不能自动重试非幂等命令。
- 删除、付款、发布等动作必须在服务端再次鉴权；MCP annotation 不是授权依据。
- 返回稳定业务标识和状态，避免返回整份内部实体。

### 4.5 身份和凭证

Java MCP Server 应验证调用方身份，并把以下上下文传给 application service 或审计设施：

- tenant / subject；
- correlation / trace context；
- MCP method 和 Tool name；
- policy/approval 相关的受信断言（如采用）；
- 幂等键和 deadline。

不得把 OAuth access token、client secret、下游数据库凭证或完整 Authorization Header 写入
日志、Tool 输出、Resource 或 Artifact。

## 5. 接入当前 AuraClaw 的步骤

这一节描述的是 **当前代码真实支持的兼容路径**，不是目标设计。

### 5.1 当前前置条件

Java MCP Server 必须同时满足：

1. 默认支持 MCP `2026-07-28` 的 `server/discover` 和逐请求元数据；若显式登记 legacy
   profile，也可使用 `2025-11-25 initialize`。
2. 使用绝对 HTTPS Endpoint。
3. DNS 的全部解析结果都是公网地址；当前 Egress 明确拒绝私网、loopback 和 link-local 地址。
4. 使用 OAuth `client_credentials`。
5. 提供 OAuth Protected Resource Metadata 和 Authorization Server Metadata。
6. Tool、Resource Scheme、Prompt 名称命中 AuraClaw allowlist。
7. 最好使用无状态的 2025 兼容模式；当前 Egress 没有保存和回传 `Mcp-Session-Id`。

如果 Java 服务只在 Kubernetes/VPC 私网中，这条路径目前不能直接使用。不要通过放开所有私网
地址来绕过；按第 8.2 节增加受管私网 Transport。

### 5.2 配置受管 Server

通过 `AURACLAW_MCP_EGRESS_SERVERS_JSON` 配置非密信息：

```json
[
  {
    "server_id": "order-mcp",
    "tenant_id": "tenant-a",
    "title": "Order Service MCP",
    "endpoint": "https://order-mcp.example.com/mcp",
    "protocol_revision": "2026-07-28",
    "credential_ref": "vault/order-mcp#client_secret",
    "oauth": {
      "protected_resource_metadata_url": "https://order-mcp.example.com/.well-known/oauth-protected-resource",
      "authorization_server_metadata_url": "https://auth.example.com/.well-known/oauth-authorization-server",
      "issuer": "https://auth.example.com",
      "token_endpoint": "https://auth.example.com/oauth/token",
      "client_id": "auraclaw-hands",
      "resource": "https://order-mcp.example.com/mcp",
      "scopes": ["order.read", "order.write"]
    },
    "trust_level": "tenant_verified",
    "allowed_tool_prefixes": ["order."],
    "allowed_resource_schemes": ["order"],
    "allowed_prompt_prefixes": ["order."],
    "status": "active",
    "enabled": true
  }
]
```

`credential_ref` 对应的目录记录必须满足：

- `provider = server_id`；
- `account_scope = oauth.resource`；
- `allowed_operations` 包含 `mcp.invoke`；
- 未过期且未撤销；
- Vault 对应路径只保存 client secret，不放入 Server JSON。

当前仓库没有完整的 MCP Server/Credential 自助管理 API。生产接入需要通过受控运维流程写入
Vault 和 Credential Registry；不要把 SQL 或 Vault Token 写进部署脚本和文档。

### 5.3 启动后的自动流程

Action Hands 启动后会：

1. 读取 Server JSON 并写入 Capability Catalog Store。
2. 通过 Credential Proxy 获取 OAuth Token。
3. 对 2026 profile 调用 `server/discover`；只有显式 legacy profile 调用 `initialize`。
4. 分页调用 `tools/list`、`resources/list`、`resources/templates/list` 和 `prompts/list`。
5. 对描述符做名称、URI Scheme、大小、深度和版本漂移校验。
6. 原子替换 Catalog 快照并注册远端 Tool 路由。
7. 周期性全量对账；连续三次失败后 quarantine 并撤销路由。

所有远端 Tool 在实际执行层会被保守注册为 `write-with-approval/high`。不要依赖远端 Tool
annotation 自动降低权限。

### 5.4 冒烟验证

在 Java Server 自身先验证：

```bash
npx @modelcontextprotocol/conformance server \
  --url https://order-mcp.example.com/mcp \
  --suite active
```

然后验证 AuraClaw：

- Server 由 `quarantined/degraded` 进入 `active`。
- Catalog 中只出现 allowlist 内的能力。
- 同名同版本 Schema 不发生静默漂移。
- 读 Tool 仍按当前保守策略触发审批，除非完成风险策略重构。
- 写 Tool 的幂等、审批、超时和未知副作用均有测试。
- 禁用 Server 后，目录和 Tool 路由不可再用。

## 6. 推荐测试矩阵

### Java 单元测试

- Schema 必填、边界、未知字段和错误枚举。
- application service 只被调用一次。
- 业务异常映射为 `isError=true`，不泄露堆栈。
- tenant 越权和对象级越权被拒绝。
- 写操作幂等和并发去重。

### Java MCP 契约测试

- `server/discover`（现代协议）或 `initialize`（兼容协议）。
- `tools/list` 描述符稳定且顺序确定。
- `tools/call` 的 `structuredContent` 满足 `outputSchema`。
- `MCP-Protocol-Version`、`Mcp-Method`、`Mcp-Name` 与请求体一致。
- 超时、取消、限流、认证失败和 5xx 的协议映射。

### AuraClaw 集成测试

- OAuth discovery、Resource Indicator、issuer 和 scope。
- DNS/IP 约束、重定向拒绝、响应大小和 Content-Type。
- Tool prefix、Resource scheme、Prompt prefix 过滤。
- Catalog 全量分页、重复项、游标循环和 Schema 漂移。
- Policy deny、approval required、approval success、idempotency conflict。
- Server 降级、quarantine、恢复和路由撤销/恢复。

## 7. 当前代码中的实现位置

| 职责 | 当前文件 | 判断 |
| --- | --- | --- |
| MCP DTO/版本 | `src/auraclaw/contracts/mcp.py` | 位置正确，版本模型需升级 |
| 受管 Server/OAuth 描述 | `src/auraclaw/contracts/capabilities.py` | 位置正确，认证/网络模型过窄 |
| 内部 Hands MCP Server | `src/auraclaw/action/mcp.py` | 位置正确，不应加入 Java 业务逻辑 |
| 远端目录对账 | `src/auraclaw/action/catalog_reconciler.py` | 位置正确，应拆协议协商与规范化策略 |
| 远端 Tool 路由 | `src/auraclaw/action/remote_mcp.py` | 位置正确，可保留 |
| OAuth/网络 Egress | `src/auraclaw/infrastructure/credentials/mcp_egress.py` | 位置正确，应拆公网/私网 Transport |
| Runtime MCP Client | `src/auraclaw/runtime/mcp_client.py` | 位置正确，只连接内部 Hands |
| 生产装配 | `src/auraclaw/composition/services.py` | 装配职责正确，但函数过大、条件耦合过多 |
| Server 注册入口 | 环境变量 + composition | 不完整；与架构文档中的版本化 Admin API 不一致 |

## 8. 是否需要重构

需要，但不需要推倒 MCP 能力平面。建议保留边界，分三组重构。

### 8.1 P0：协议兼容层

当前代码已将默认协议升级到 `2026-07-28`。新规范移除 `initialize/initialized` 和协议
Session，引入 `server/discover`、逐请求 `_meta`、`Mcp-Method`/`Mcp-Name` Header 以及
可缓存 list 结果；`2025-11-25` 仅作为显式 legacy profile 保留。

当前实现仍有这些兼容边界：

- legacy profile 不实现 `Mcp-Session-Id` 持久化和完整 SSE 双向 Session；依赖这些能力的
  旧 Server 应升级，而不是长期停留在 legacy profile。
- SSE 响应是整包缓冲解析，不是真正的订阅流。

建议：

1. 已实现 `2026-07-28` 默认 profile 和 `2025-11-25` 显式 legacy profile。
2. 已由 Client/Transport 生成逐请求 `_meta` 与标准路由 Header，对明确的 legacy Server
   再走 `initialize`。
3. 后续把 profile 编解码从 Client/Reconciler 进一步收敛为独立对象，并按 `ttlMs` / `cacheScope`
   增加实际响应缓存。
4. Catalog Reconciler 只做目录同步，不自行拼协议生命周期细节。
5. 内部 Hands MCP 与外部 Egress 可分阶段升级，避免一次改动所有 Runtime 契约。

最新规范依据：

- [MCP 2026-07-28 release](https://blog.modelcontextprotocol.io/posts/2026-07-28/)
- [server/discover](https://github.com/modelcontextprotocol/modelcontextprotocol/blob/main/docs/specification/2026-07-28/server/discover.mdx)

### 8.2 P0：受管私网连接

当前 `_approved_addresses` 只允许公网 IP，因此典型 Kubernetes/VPC Java 服务无法接入。

建议把 Egress 拆成：

```text
McpTransportFactory
├── PublicOAuthMcpTransport      # 保留现有公网 DNS pinning
└── PrivateWorkloadMcpTransport  # 服务发现 + mTLS/SPIFFE + 明确 CIDR/服务 allowlist
```

私网 Transport 必须绑定：

- server id 与服务发现名称；
- tenant/namespace；
- 允许的精确 CIDR 或 workload identity；
- mTLS/SPIFFE 身份；
- 禁止重定向；
- Tool/Resource/Prompt allowlist；
- 独立连接与响应大小限制。

不能简单把 `address.is_global` 检查删除。

### 8.3 P1：注册和开发体验

文档规定 Server 注册应走版本化 Admin API，但代码目前只从环境变量加载。Credential Registry
也缺少完整的受控接入命令/API；开发模式不会装配远端 Transport 和 Reconciler。

建议增加：

- `McpServerRegistryService`：register/update/disable/test/reconcile。
- owner-scoped Admin handler：携带 tenant、actor、command id、expected version 和审计上下文。
- Credential Reference 管理命令，只保存引用和 scope，不接受 Secret 正文。
- `auraclaw mcp validate-config`、`test-connection`、`reconcile` CLI。
- 开发专用的 Transport Factory，可注入 MockWebServer/Testcontainers；生产安全策略保持不变。

### 8.4 P1：风险策略一致性

当前 Catalog 将远端 Tool 描述为 `read-only/medium`，实际注册到 Tool Gateway 时统一变成
`write-with-approval/high`。执行侧保守是正确的，但发现信息与执行策略不一致，会造成模型选择和
运营展示混乱。

建议在受管 Server 配置中增加平台控制的 Tool policy override：

```json
{
  "tool_policy": {
    "default": {"permission": "write-with-approval", "risk_level": "high"},
    "rules": [
      {
        "prefix": "order.order.get",
        "permission": "read-only",
        "risk_level": "low"
      }
    ]
  }
}
```

只有管理员审核过的规则可以降级；远端 annotation 只能作为提示，不能提升权限或自动降级。
Catalog 和 ToolRegistry 必须使用同一份已解析策略。

### 8.5 P2：缩短装配代码

`_hands_app` 同时承担 Store 选择、Skill、业务能力、远端 MCP、认证、周期任务和 HTTP 装配。
建议提取但不改变包边界：

- `build_capability_plane(...)`
- `build_remote_mcp_registry(...)`
- `build_hands_gateway(...)`
- `build_mcp_reconcile_jobs(...)`

这会让“新增一个 Java MCP Server”最终只涉及管理面注册，不再修改 1800 多行的
`composition/services.py`。

## 9. 推荐实施顺序

1. **先做协议 profile 和会话修复**：保证官方 Java SDK Server 能通过真实握手和目录同步。
2. **根据部署网络选择 Transport**：公网 OAuth 使用现有通道；私网 Java 服务新增 mTLS 通道。
3. **实现一个 Java 试点 Tool**：优先只读、幂等、输出小，例如 `order.order.get`。
4. **补管理入口和本地测试工具**：去掉生产环境试错式接入。
5. **统一 Catalog/执行风险策略**。
6. **再批量封装写接口**，逐个增加审批、幂等和未知副作用测试。

不建议先自动扫描 OpenAPI 并批量生成几十个 Tool。可以用 OpenAPI 生成 Java Client 和 Schema
草稿，但 Tool 的业务边界、描述、权限、风险和输出裁剪必须人工审核。

## 10. 验收标准

- Java MCP Server 通过锁定协议版本的官方 conformance suite。
- AuraClaw 能在开发和生产环境执行受控连接测试。
- Runtime 仍只知道内部 Hands MCP 地址，不接触 Java Server URL 或 Secret。
- 私网和公网连接使用不同安全策略，均无任意 URL/Header 注入入口。
- Catalog、ToolRegistry 和 Policy 对 permission/risk 的判断一致。
- Tool Schema 版本漂移会阻断发布；版本升级可并存和回滚。
- 写操作具备服务端幂等、审批绑定、deadline 和未知副作用语义。
- Secret 不进入 Runtime、Prompt、Session Event、日志或 Artifact。
- 禁用或 quarantine Server 后，发现和执行路径都被撤销。
- 相关阶段检查项补入 `docs/开发阶段校验清单.md`，完成后按项目规则提交并推送。

## 11. 本次代码核查证据

本次只做了评审和文档，不修改运行代码。执行了：

```bash
.venv/bin/pytest -q \
  tests/unit/test_m9_catalog_reconciliation.py \
  tests/unit/test_m9_mcp_egress.py \
  tests/unit/test_m9_mcp_primitives.py \
  tests/unit/test_m9_resource_gateway.py
```

结果：`13 passed`。

这说明现有 `2026-07-28 + 公网 OAuth Egress` 的受测行为是稳定的；它不能证明所有 MCP
规范、官方 Java SDK 的实际互操作、私网 Java 服务或生产 OAuth 部署已经可用。
