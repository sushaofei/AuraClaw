# AuraClaw MCP 开发手册

> 面向：维护 Agent Runtime / Action Hands 的开发人员，以及要把现有 Java 服务接到 Agent 的开发人员。  
> 代码基线：当前仓库实现，Runtime→Hands 使用协议无关内部 HTTP；下游 MCP 默认协议为
> `2026-07-28`，并保留显式 `2025-11-25` legacy Connector profile。  
> 配套文档：[MCP Runtime 能力平面](./Managed%20Agent%20系统架构/23%20MCP%20Runtime%20能力平面.md)、[M9 实施与运维](./M9%20MCP%20Runtime%20实施与运维.md)、[Java 服务 MCP 接入手册](./Java%20服务%20MCP%20接入手册.md)（后者偏评审与基础设施缺口）。

本文回答三件事：

1. AuraClaw 里 MCP 是什么、请求怎么走。
2. Tool 名字如何被注册、发现、路由到真正的实现。
3. 把现有 Java 服务接进来时，两边各自改什么、不要改什么。

---

## 0. 先选路

| 你要做的事 | 正确做法 | 不要做 |
|---|---|---|
| 给 Agent 增加 AuraClaw **自己拥有** 的能力（如价格洞察） | 在 Hands 里注册 `ToolCapability` + `HandsExecutor` | 改内部 Hands HTTP 路由或 Runtime Client |
| 给 Agent 增加 **扩展能力**（不属于内核，也不属于 chaintower 业务） | 在 AuraMCP 写 Extension；AuraClaw 只登记 `auramcp` MCP Server | Runtime / AuraX 直连 AuraMCP；把扩展写进 Hands 内核 |
| 把 **已有 Java 服务** 交给 Agent 调用 | Java 暴露 MCP **或** 登记受管 Java API operation；AuraClaw 只登记 Connector | 在 `runtime/` 里直连 Java URL |
| 本周必须打通 1～2 个内网接口，Java 还不能改 | 过渡：Credential Proxy adapter 出站，或 Java 旁挂独立 MCP Adapter | 按价格洞察模式把业务 HTTP Client 堆进 Hands |

判断标准：**业务契约属于谁，谁就拥有 Tool 定义。**  
价格洞察的数据和计算在 AuraClaw，所以 Tool 写在 Python。订单/库存的数据和权限在 Java，所以 Tool 写在 Java MCP，AuraClaw 只做登记、策略、凭证和对账。

```text
Agent Runtime  ──Hands HTTP/JSON──►  Action Hands Gateway /internal/v1/hands/*
                                      │
                                      ├─ 本地 Tool  → HandsExecutor（本进程）
                                      ├─ 远端 MCP   → ManagedMcpConnector → Java / AuraMCP POST /mcp
                                      └─ 远端 API   → ManagedJavaApiConnector → 已注册 REST operation
```

Runtime **只连** `AURACLAW_HANDS_URL`（默认 `http://127.0.0.1:8006`）。它不接收 Java URL、stdio 命令或明文 Secret。

---

## 1. 给两个角色的最短路径

### 1.1 维护 Agent / Hands

按这个顺序读代码：

1. `src/auraclaw/contracts/hands.py` — 协议无关 Hands DTO 和内部路径
2. `src/auraclaw/runtime/hands_client.py` — Runtime 怎么发 `call_tool`
3. `src/auraclaw/action/hands.py` / `action/hands_http.py` — Hands Gateway 与内部 HTTP
4. `src/auraclaw/action/tool_gateway.py` — 校验、策略、审批、幂等
5. `src/auraclaw/action/capability_catalog.py` 中的 `RoutedHandsExecutor` — 按名字找执行器
6. 本地范例：`src/auraclaw/action/price_insight.py`
7. 装配：`src/auraclaw/composition/services.py` 里 Hands 的 Registry / Router

### 1.2 维护 Java 服务

按这个顺序做事：

1. 把业务意图映射成 MCP Tool（不要一对一映射 Controller）
2. 在 Java 进程或独立 Adapter 暴露 `POST /mcp`
3. 实现 `server/discover`、`tools/list`、`tools/call`
4. 准备 HTTPS、受管认证（chaintower：workload + trusted context；第三方：可选 OAuth）、名称前缀
5. 交给 AuraClaw 运维通过 `POST /v1/admin/mcp-servers` 热配置登记 + Vault `credential_ref`
6. 用对账结果确认 Catalog / Registry 里出现了你的 Tool

Java 开发 **不需要** 改 `HandsGateway`，也不需要在 Python 里为每个接口写 Executor。

---

## 2. MCP 在 AuraClaw 里是什么

### 2.1 协议，不是业务事实源

Hands Contract 是 Runtime 发现和调用「数据 / 工具 / 提示 / 技能」的内部运输协议。下游 MCP 只服务 Java/第三方 Server。下面这些东西 **不** 由 Hands 或 MCP 保证：

- Canonical Session Event（任务事实）
- Control Lease / fencing token（谁有权跑这次 Run）
- Hands Invocation Store（Tool 幂等与恢复）
- Result Delivery（最终交付）

Harness 在 `tools/call` 前后写 checkpoint（`tool_pending` / `tool_completed`）。MCP 响应可以丢，任务不能丢。

### 2.2 官方原语和 AuraClaw 概念

MCP 规范只有三类 Server 原语。AuraClaw 的 Skill 不是第四类原语，而是用 Resource + 平台清单表达：

| AuraClaw 概念 | MCP 表达 | 控制方 | 例子 |
|---|---|---|---|
| 数据 | Resource / Resource Template | Application / Runtime | `order://orders/123` |
| 工具 | Tool | Model 选择，平台治理 | `order.order.cancel` |
| 提示模板 | Prompt | 用户或产品显式选用 | `order.review` |
| 技能 | Resource + Skill Manifest | Runtime Skill Runner | 价格洞察 SKILL.md |
| 远端长调用句柄 | MCP Tasks（当前禁用） | — | 不是 AuraClaw Task/Session |

映射规则：

- 有副作用、复杂查询、要校验输入的，做成 Tool。
- 能用稳定 URI 重复读取的，优先 Resource，不要做成只读 Tool。
- 一个 Tool 对应一个业务意图，名称用 `<domain>.<aggregate>.<verb>`。
- 不要把每个 REST Endpoint 做成一个 Tool。

### 2.3 契约

`src/auraclaw/contracts/` 是跨服务的稳定数据结构，不依赖 FastAPI 或数据库。Hands 与下游 MCP 相关主要有：

| 文件 | 内容 |
|---|---|
| `contracts/hands.py` | Runtime↔Hands DTO、内部 HTTP 路径、`HandsTrustedContext` |
| `infrastructure/connectors/mcp/wire.py` | 下游 MCP JSON-RPC、`McpTrustedContext`、`McpTransport` |
| `contracts/tools.py` | `ToolCapability`、`ToolInvocation`、`ToolResult`、权限与风险 |
| `contracts/capabilities.py` | 受管 Server 定义、OAuth、Catalog 描述符 |

`ContractModel` 是冻结且禁止多余字段的 Pydantic 模型。内部用 Python 字段名（`mime_type`），Hands 对外 JSON 用 camelCase；下游 MCP 转换集中在 Connector。

身份不放进 Hands 请求 body。`HandsToolCall` 只说「做什么」；`HandsTrustedContext` 由 Hands 从 workload token + lease assertion **自己还原**，说「凭什么做」。

---

## 3. 内部 Hands Contract 怎么跑起来

### 3.1 进程角色

| 进程 | 入口 | 职责 |
|---|---|---|
| `agent-runtime` | 连 `hands_url` | `HttpHandsClient` / `HandsRuntimeAdapter`：list / call / 读 Skill Resource |
| `action-hands` | `/internal/v1/hands/*` | `HandsGateway`：鉴权、转 `ToolInvocation`、调 Gateway |
| `policy` | 内部 HTTP | 允许 / 拒绝 / 要求审批 |
| `credential-proxy` | 内部 HTTP | 持有 Secret，出站打远端 MCP 或 Java API |
| Java MCP | `POST /mcp` | 业务 Tool 的真正实现（Hands 下游） |

开发/单测可以用进程内 `InProcessHandsClient`，生产 Runtime 用 `HttpHandsClient`。Gateway 代码相同，只换 Client。

### 3.2 HTTP 入口

```python
# src/auraclaw/action/hands_http.py
@app.post("/internal/v1/hands/tools/call")
async def call_tool(request, authorization, lease_assertion):
    trusted = await authenticator.authenticate(authorization, lease_assertion)
    return await gateway.call_tool(trusted, payload)
```

请求体是 Hands DTO。头里另带：

- `Authorization: Bearer <runtime workload token>`
- `X-AuraClaw-Contract-Version` / `X-AuraClaw-Hands-Contract: 2026-08-19`
- `X-AuraClaw-Lease-Assertion`（生产：带签名的租约）

tenant / session / run / lease / fencing **只来自** token + lease，不信任 body。

Server 当前支持的内部路径：

`/internal/v1/hands/tools/list`、`/tools/call`、`/resources/list`、
`/resources/templates/list`、`/resources/read`、`/prompts/list`、`/prompts/get` 和
`/invocations/cancel`。

### 3.3 `tools/call` 全程

**Runtime 封包**（`HttpHandsClient.call_tool`）：

```python
HandsToolCall(
    tool_invocation_id=...,
    name=call.name,
    version=call.version,
    arguments=dict(call.arguments),
    idempotency_key=...,
    approval_id=...,
    credential_ref=...,   # 只传引用，不传 Secret
)
```

**Hands 拆包**（`HandsGateway.call_tool`）：

- `name` / `arguments` 来自 `HandsToolCall`
- `tenant_id` / `session_id` / `run_id` / `fencing_token` **只来自** `trusted_context`
- 组装 `ToolInvocation`，交给 `ToolGateway.execute()`

Hands HTTP 适配器到这里就停了。它不是执行器。

**Gateway 过门顺序**：

```text
Registry.get(name, version)
 → JSON Schema 校验 arguments
 → 幂等（内存缓存或 Invocation Store）
 → Policy：ALLOW / DENY / REQUIRE_APPROVAL
 → 超时与 HandsExecutor.dispatch
 → 脱敏、校验 output schema、大结果落 Artifact
 → 返回 ToolResult
```

结果再封回 `HandsToolResult`：摘要给模型，结构化内容给 Runtime 写 checkpoint。协议错误走 Hands error；工具业务失败仍是一次成功的 Hands 调用，`is_error=true`。

---

## 4. 名字如何找到实现

名字 **不是** DNS，也不是 URL。它是 Hands 进程里两张表的 key。

```text
Tool 名字
 ├─ ToolRegistry[(name, version)]     → 说明书（schema、权限）  用于发现和校验
 └─ RoutedHandsExecutor._routes[name] → Executor 实例           用于真正执行
```

漏了第二张表：`tools/list` 能看到它，`tools/call` 会掉进默认 `LocalHandsService`，报 tool is not installed。

两张表由谁填写，取决于 Tool 类型：

| 类型 | 谁在何时写入 |
|---|---|
| 平台 / 本地业务 Tool | `composition/services.py` 启动时写死 |
| Java / 远端 MCP Tool | `CapabilityCatalogReconciler` 对账成功后动态 `replace_owner` |

### 4.1 用例 A：本地 Tool `procurement.price.metric.evidence.list`

这是「能力属于 AuraClaw」的范例。Agent 维护者加同类 Tool 就抄这条路径。

**① 定义说明书**

```python
# src/auraclaw/action/price_insight.py
PRICE_METRIC_EVIDENCE_LIST_TOOL = "procurement.price.metric.evidence.list"

ToolCapability(
    name=PRICE_METRIC_EVIDENCE_LIST_TOOL,
    version="1.0.0",
    description="List bounded evidence for one governed procurement-price metric; ...",
    input_schema={...},
    output_schema={...},
    permission=ToolPermission.READ_ONLY,
    risk_level=RiskLevel.LOW,
    owner="business-skill:price-insight",
)
```

**② 定义执行器**（多个价格 Tool 共用一个对象，内部再按名字分方法）

```python
class PriceInsightToolExecutor:
    async def execute(self, invocation, capability):
        filters = PriceInsightFilter.model_validate(invocation.arguments["filter"])
        if capability.name == PRICE_METRIC_EVIDENCE_LIST_TOOL:
            return await self.service.evidence(...)
        ...
```

执行器签名必须符合 `HandsExecutor`：

```python
async def execute(self, invocation: ToolInvocation, capability: ToolCapability) -> Any
```

**③ 启动时同时写入两张表**

```python
# src/auraclaw/composition/services.py
price_tools = price_insight_tools()
registry = ToolRegistry((capability_search_tool(), ..., *price_tools))

price_executor = PriceInsightToolExecutor(PriceInsightService(source))
routed_hands = RoutedHandsExecutor(
    LocalHandsService(...),
    {
        "auraclaw.capabilities.search": CapabilitySearchExecutor(...),
        **{tool.name: price_executor for tool in price_tools},
        # 展开后：
        # "procurement.price.metric.evidence.list": price_executor
    },
)
```

`price_insight_source`（fixture JSON 或 MySQL）在 new executor 时就已经注入。调用时不会再按名字去「发现服务」。

**④ 发现**

- MCP `tools/list` 遍历 `registry.discover()`，模型看到这串名字。
- 价格洞察 Skill 文档也会写死该名字，Runtime 按 Skill 指导调用。

**⑤ 调用时三次查找**

1. `ToolRegistry.get(name, version)` → schema  
2. `routes[name]` → 同一个 `PriceInsightToolExecutor`  
3. executor 内 `if name == evidence.list` → `service.evidence()`

### 4.2 平台自带的三个 Tool

不要改它们，模型发现远端能力时会用到：

| 名字 | 作用 |
|---|---|
| `auraclaw.capabilities.search` | 按策略可见范围搜索 Catalog |
| `auraclaw.capabilities.load` | 按 id 加载完整契约，单次最多 8 个 |
| `auraclaw.skills.resolve` | 解析并绑定一个 Skill 版本 |

---

## 5. 把 Java 服务接进来

目标结构：

```text
Java 拥有：Tool 名、JSON Schema、业务权限、幂等、application service
AuraClaw 拥有：Server 登记、前缀白名单、Policy/审批、OAuth/Egress、目录对账、Invocation Store
```

AuraClaw **启动时登记的是 Server，不是单个 Tool 名字。** 名字来自 Java 的 `tools/list`，对账成功后才写入 Registry / Router。

### 5.1 Java 侧：做成 MCP，而不是给 AuraClaw 一份 REST 清单

#### 可以改原服务时

```text
com.example.order
├── application/          # 业务逻辑，MCP 与 REST 共用
├── web/OrderController   # 给人用的 HTTP
└── mcp/
    ├── McpServerConfiguration
    ├── OrderToolDefinitions
    ├── OrderToolHandlers
    └── McpResultMapper
```

`web/` 与 `mcp/` 平级，都调 `application/`，彼此不互相 HTTP 回调。

#### 不能改原服务时

单独部署 `order-mcp-adapter`：用稳定 Client 调现有 REST，再暴露 MCP。这个 Adapter **不要** 放进 AuraClaw 的 `action/`。

#### Transport 与协议

- 官方 MCP Java SDK 2.x（Spring 可用 Spring AI 的 WebMVC/WebFlux transport）
- 生产用 Streamable HTTP：`POST /mcp`
- 新接入默认实现 `2026-07-28` 无状态 profile 和 `server/discover`；只有登记为
  `2025-11-25` 时才走 legacy `initialize`
- 不要用 stdio，不要新建 Legacy HTTP+SSE
- SDK 版本锁定，禁止动态版本

#### Tool 定义要点

```java
// 结构示例，import 以锁定的 SDK 小版本为准
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

每个 Tool 必须有：

- 带命名空间的稳定名字，且命中稍后登记的 `allowed_tool_prefixes`（如 `order.`）
- 根类型为 `object` 的 `inputSchema`，默认 `additionalProperties: false`，含长度/枚举/数组上限
- 与 `structuredContent` 一致的 `outputSchema`
- 业务错误：`isError=true` + 安全摘要；禁止返回堆栈、Secret、内部枚举
- 写操作：接收幂等键（或映射 AuraClaw `toolInvocationId`），在 Java 侧持久化去重
- Schema 变更必须 bump `_meta.auraclaw.version`（semver）。AuraClaw 对账发现同名同版本 digest 变了会失败

版本可放在 list 描述符里：

```json
{
  "name": "order.order.get",
  "description": "...",
  "inputSchema": { "type": "object", "...": "..." },
  "outputSchema": { "type": "object" },
  "_meta": { "auraclaw": { "version": "1.0.0" } }
}
```

未提供合法 semver 时，AuraClaw 记为 `0.0.0`。

#### Java 必须实现的 method

对账与调用最少需要：

| Method | 谁调用 | 用途 |
|---|---|---|
| `server/discover` | 对账器 | 核对支持 `2026-07-28` 并读取 Server capabilities |
| `tools/list` | 对账器 | 拉 Tool 目录（支持 `cursor` / `nextCursor`） |
| `tools/call` | Agent 调用时 | 执行业务 |
| `resources/*`、`prompts/*` | 可选 | 有稳定 URI 或交互模板时再实现 |

身份：校验调用方 OAuth，把 tenant、correlation、Tool name、幂等键、deadline 传给 application / 审计。不得把 access token 写进日志或 Tool 输出。

### 5.2 AuraClaw 侧：登记 Server

配置入口：Task API 的热配置 Admin API（`/v1/admin/mcp-servers`）。配置以不可变
revision 写入 Hands Registry，Action Hands 与 Credential Proxy **无需重启**即可加载。
配置里 **只能有非密信息 + `credential_ref`**；本地开发可用 `network_mode=loopback`。
工作台可用 `GET /v1/admin/mcp-servers/{server_id}/tools` 查看对账后的 Catalog tools，
这不是对远端 `tools/list` 的实时探测。

`loopback` 地址是相对 **Credential Proxy 所在网络命名空间** 而言。容器内的 `127.0.0.1`
不是开发者宿主机；跨网络命名空间应使用私网服务名或受管宿主机别名。

```json
{
  "server_id": "order-mcp",
  "tenant_id": "tenant-a",
  "title": "Order Service MCP",
  "endpoint": "https://order.example/mcp",
  "protocol_revision": "2026-07-28",
  "network_mode": "public",
  "credential_ref": "vault/order-mcp#client_secret",
  "oauth": {
    "protected_resource_metadata_url": "https://order.example/.well-known/oauth-protected-resource",
    "authorization_server_metadata_url": "https://auth.example/.well-known/oauth-authorization-server",
    "issuer": "https://auth.example",
    "token_endpoint": "https://auth.example/oauth/token",
    "client_id": "auraclaw-hands",
    "resource": "https://order.example/mcp",
    "scopes": ["tools.read"]
  },
  "trust_level": "tenant_verified",
  "allowed_tool_prefixes": ["order."],
  "allowed_resource_schemes": ["order"],
  "allowed_prompt_prefixes": ["order."]
}
```

字段含义：

| 字段 | 作用 |
|---|---|
| `server_id` | Hands 与 Credential Proxy 共用的内部键 |
| `endpoint` | Java MCP 的绝对 HTTPS URL；OAuth `resource` 的 origin 必须与它一致 |
| `credential_ref` | Vault 中 client secret / workload 的引用 |
| `allowed_tool_prefixes` | 对账和出站都会过滤；`order.create` 能进，`admin.delete` 会被丢掉 |
| `auth_strategy` + `credential_ref` | workload 路径必填 credential_ref；OAuth 路径还要 `oauth` |

本地联调若 Java MCP 仍发布旧工具名，可在 Server 的 `metadata.tool_name_aliases` 中配置
“远端名 → AuraClaw Skill 标准名”，并用 `metadata.search_tags` 补中文检索。Connector 只在
MCP 边界做名称转换；若远端 schema 要求单一 `input` 参数，也会在同一边界补齐包装。生产配置
应优先让 Java 直接发布标准名，不要把别名规则写进 Skill 或模型。

本机 loopback 示例（`POST /v1/admin/mcp-servers`）：

```json
{
  "server_id": "java-mcp",
  "tenant_id": "development",
  "title": "Local Java MCP Gateway",
  "endpoint": "http://127.0.0.1:48080/rpc-api/agent-runtime/mcp",
  "protocol_revision": "2025-06-18",
  "network_mode": "loopback",
  "credential_ref": "vault/java-mcp#client_secret",
  "auth_strategy": "workload_trusted_context",
  "trust_level": "tenant_verified",
  "allowed_tool_prefixes": ["procurement.price.", "price_insight."],
  "metadata": {
    "tool_name_aliases": {
      "price_insight.dataset.profile": "procurement.price.dataset.profile",
      "price_insight.metric.comparability": "procurement.price.metric.comparability"
    },
    "search_tags": ["价格洞察", "采购价格", "price_insight"]
  }
}
```

Credential Registry 中对应引用必须满足：

- `provider` == `server_id`（`order-mcp`）
- `account_scope` == OAuth `resource`
- `allowed_operations` 包含 `mcp.invoke`

### 5.3 启动时从 Registry 恢复，此时还没有 Java Tool 名字

Hands 初始化从 MCP Registry 的 active snapshot 恢复已启用 Server，Credential Proxy
通过 `/internal/v1/mcp-registry/snapshot` 装配同一批 Egress Adapter。此时 Catalog 里只有
Server 定义。`order.order.get` **还不在** Registry / Router 里。

远端对账还要求 Hands 能连 **Credential Proxy** 且 Policy 是 `RemotePolicyClient`。开发单进程默认不会跑这条路径。

### 5.4 对账：这时才按名字写入两张表

`McpCatalogReconciler.reconcile_server()`：

1. 对 2026 Server 发 `server/discover`（legacy Server 才发 `initialize`），协议版本必须一致  
2. 分页 `tools/list`（以及 resources/prompts）  
3. 名字必须匹配 `allowed_tool_prefixes`，否则丢弃  
4. 写入 Capability Catalog（给 `auraclaw.capabilities.search` 用）  
5. 动态填 Registry 和 Router：

```python
# src/auraclaw/action/catalog_reconciler.py
owner = f"mcp:{server.server_id}"          # "mcp:order-mcp"
executor = RemoteMcpToolExecutor(server, transport)
self._tool_registry.replace_owner(owner, capabilities)
self._hands_router.replace_owner_routes(
    owner,
    {capability.name: executor for capability in capabilities},
)
```

对账成功后：

```text
ToolRegistry
  ("order.order.get", "1.0.0") → ToolCapability(
        owner="mcp:order-mcp",
        permission=WRITE_WITH_APPROVAL,   # 远端一律先按高风险+审批
        risk_level=HIGH,
        runtime_location="remote-mcp",
     )

RoutedHandsExecutor._routes
  "order.order.get"    → 同一个 RemoteMcpToolExecutor(order-mcp)
  "order.order.cancel" → 同一个 RemoteMcpToolExecutor(order-mcp)
```

这与价格洞察的 `{tool.name: price_executor for tool in price_tools}` 是同一模式，差别是：

- 名字来自 Java，不是 Python 常量
- Executor 不跑业务，只是把 `tools/call` 转给这台 Server
- 连续对账失败 3 次会隔离，并 `revoke_owner` 摘掉这些名字

`AURACLAW_MCP_RECONCILE_INTERVAL_SECONDS` 控制全量对账周期，默认 60 秒，最小 5 秒。Java 侧 `notifications/tools/list_changed` 只会把 Server 标脏，真正更新仍靠全量对账。

### 5.5 用例 B：调用 `order.order.get`

模型发出：

```json
{
  "jsonrpc": "2.0",
  "id": 3,
  "method": "tools/call",
  "params": {
    "name": "order.order.get",
    "arguments": { "orderId": "A1001" }
  }
}
```

Hands 侧：

1. Registry 取 schema，校验 `orderId`  
2. Policy / 审批（远端 Tool 当前默认需要审批）  
3. `routes["order.order.get"]` → `RemoteMcpToolExecutor`  
4. Executor 原样转发 `tools/call`，**不再按名字选 URL**  
5. Transport 向 Policy 申请 `mcp.remote.invoke`，资源为 `mcp:order-mcp`  
6. Credential Proxy 用适配器名 `mcp:order-mcp` 找到 `ManagedMcpEgressAdapter`  
7. Adapter 再查一次前缀白名单，走 OAuth、DNS 公网校验、钉 IP、禁止重定向  
8. `POST https://order.example/mcp`  
9. Java MCP 按名字调 `orderApplicationService.getOrder`

所以：

```text
order.order.get
  → 字典：这个名字属于 order-mcp 这台 Server
  → Executor 固定打 https://order.example/mcp
  → Java 进程内再分到 getOrder()
```

AuraClaw 从不把 `order.order.get` 解析成 REST 路径。换一台 Java 服务，是换 `server_id` + `endpoint`，不是改 Tool 名拼 URL 的规则。

---

## 6. 开发时不要踩的坑

1. **不要改 `src/auraclaw/action/mcp.py` 来加业务 Tool。** 它只做协议翻译。  
2. **不要在 `runtime/` 里连 Java。** Runtime 只能连内部 Hands MCP。  
3. **不要在 `composition/services.py` 堆 Java HTTP Client。** 那里只装配。  
4. **本地 Tool 必须同时改 Registry 和 Router。** 只 `register` 不够。  
5. **远端 Tool 的 URL 在 Server 定义里，不在 Tool 名字里。**  
6. **Java 改 schema 必须升版本。** 否则对账报 “changed without version bump”。  
7. **当前 Egress 拒绝私网 IP。** 典型 K8s/VPC Java 还接不进这条路径；需要私网 Transport（见第 8 节）或旁挂可公网/专线暴露的 MCP Adapter。  
8. **开发 profile 默认不对账远端 Server。** 需要 Credential Proxy + 远端 Policy。  
9. **远端 Tool 一律 `write-with-approval/high`。** Java 的 `readOnlyHint` 不能降权。只读查询现在也会走审批，除非后续做了策略覆盖。  
10. **不要用 Python executor 封装 Java REST 作为正式接入标准。** 过渡可以走 Credential Proxy adapter，正路仍是 Java MCP。理由：每改一个 Java 字段都要发 AuraClaw 版，Hands 会变成集成仓库。

---

## 7. 联调与验收清单

### 7.1 Java 自己先过

- [ ] `server/discover` 返回 `supportedVersions` 含 `2026-07-28`、capabilities、
  `resultType`、私有缓存提示与 `_meta.io.modelcontextprotocol/serverInfo`
- [ ] 每请求校验 `_meta` 和 `MCP-Protocol-Version` / `Mcp-Method` / `Mcp-Name`
- [ ] `tools/list` 名称稳定、带前缀、顺序确定，含 `inputSchema`
- [ ] `tools/call` 的 `structuredContent` 满足 `outputSchema`
- [ ] 未知字段、缺必填、越权、业务失败均有明确 `isError` 或协议错误
- [ ] 写操作幂等；超时后副作用视为 unknown，不盲目重试
- [ ] 日志与输出中无 token / secret / 堆栈

可用 MCP 一致性套件对 Server 做冒烟（以当时锁定的 CLI 为准）。

### 7.2 AuraClaw 侧

- [ ] `POST /v1/admin/mcp-servers` 创建后 `:test` / `:enable` 成功
- [ ] Vault 与 Credential Registry 的 provider / scope / `mcp.invoke` 匹配
- [ ] Hands 启动后 Catalog 中该 `server_id` 进入 `active`
- [ ] 只出现 allowlist 内的 Tool
- [ ] `tools/list`（Hands 内部）能看到 `order.order.get`
- [ ] 一次真实 `tools/call` 打到 Java application service，且只一次
- [ ] 禁用 Server 或对账隔离后，路由被撤销，模型不能再调

### 7.3 Agent 维护者加本地 Tool

- [ ] 新增 `ToolCapability`（名字、schema、权限、风险、owner）
- [ ] 实现 `HandsExecutor.execute`
- [ ] `ToolRegistry` 与 `RoutedHandsExecutor` 同时挂上
- [ ] 单测覆盖 schema 拒绝、策略拒绝、成功路径
- [ ] 不把 Secret 放进 arguments 或返回值

---

## 8. 当前实现缺口（分享时要讲清楚）

这些不是「方向错了」，是「现在就接内部 Java 会疼」：

| 缺口 | 影响 | 文档位置 |
|---|---|---|
| Egress 只允许公网 IP | 内网 Java 无法走现有 MCP Egress | 手册建议拆 `PublicOAuth` / `PrivateWorkload` 两种 Transport |
| 2025 legacy 只保留基本兼容 | 依赖 Session、SSE 双向请求的旧 Server 不适合长期保留 | 迁移到 2026 无状态 profile |
| 开发模式不对账 | 本地难试远端接入 | 需要开发用 Transport / Mock |
| 远端读 Tool 也要审批 | 模型调用体验差 | 需要管理员审核过的 policy override |
| 注册只有环境变量 | 没有版本化 Admin API | 与架构文档不一致 |

在这些补齐之前：内部 Java 更稳妥的做法是 **紧邻 Java 部署 MCP Adapter**（映射工作仍在 Java 侧），而不是在 AuraClaw 里用 executor 调 REST。

---

## 9. 代码地图

| 职责 | 路径 |
|---|---|
| Hands DTO / 内部路径 | `src/auraclaw/contracts/hands.py` |
| 下游 MCP JSON-RPC / Trusted Context | `src/auraclaw/infrastructure/connectors/mcp/wire.py` |
| Tool 契约 | `src/auraclaw/contracts/tools.py` |
| 受管 Server / Catalog DTO | `src/auraclaw/contracts/capabilities.py` |
| Runtime Hands Client | `src/auraclaw/runtime/hands_client.py` |
| 进程内 Hands Client | `src/auraclaw/internal/hands.py` |
| Hands Gateway | `src/auraclaw/action/hands.py` |
| 内部 Hands HTTP | `src/auraclaw/action/hands_http.py` |
| Resource / Prompt 注册 | `src/auraclaw/action/mcp_primitives.py` |
| Registry / Gateway | `src/auraclaw/action/tool_gateway.py` |
| 按名路由 | `src/auraclaw/action/capability_catalog.py` |
| 下游 MCP Connector | `src/auraclaw/infrastructure/connectors/mcp` |
| 下游 Java API Connector | `src/auraclaw/infrastructure/connectors/http` |
| 目录对账 | `src/auraclaw/action/catalog_reconciler.py` |
| OAuth / DNS pinning Egress | `src/auraclaw/infrastructure/credentials/mcp_egress.py` |
| 本地业务范例 | `src/auraclaw/action/price_insight.py` |
| 生产装配 | `src/auraclaw/composition/services.py` |
| 配置 | `src/auraclaw/config.py`（`hands_url`、`java_api_servers_json`） |
| Hands 契约单测 | `tests/unit/test_hands_contract.py` |
| Java API Connector 单测 | `tests/unit/test_java_api_connector.py` |
| Egress 单测 | `tests/unit/test_m9_mcp_egress.py` |
| 对账单测 | `tests/unit/test_m9_catalog_reconciliation.py` |
| 价格洞察单测 | `tests/unit/test_m12_price_insight_skill.py` |

---

## 10. 分享时可用的三句话

1. Runtime 只认内部 Hands Contract；Java 永远不直连 Agent 进程。  
2. Tool 名字是字典 key：本地 Tool 启动时挂上 Executor，Java Tool 对账时挂上「这台 Connector 的转发器」。  
3. 业务契约在哪边演进，Tool 就定义在哪边；AuraClaw 负责治理，不负责翻译每一个 Java Controller。
