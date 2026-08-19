# ADR-002：Hands 是稳定能力边界，MCP/HTTP 是下游 Connector

- 状态：Accepted（Issue #43）
- 日期：2026-08-19
- 适用范围：AuraClaw Runtime ↔ Action Hands 内部契约，以及 Hands 下游能力接入
- 架构真源：`docs/Managed Agent 系统架构/`

## 背景

AuraClaw 真正依赖的是受治理的 Hands 能力，不是 MCP 协议本身。此前 Agent Runtime 到 Action Hands
的内部通信也被建模为 MCP，导致 Runtime、内部 Transport、Capability Reconciler 和部署配置都直接感知
MCP 生命周期与版本。下游 Java 服务既可能暴露 MCP，也可能只暴露稳定 REST/gRPC API；继续让 Runtime
依赖 MCP 会把协议升级扩散到内部契约，并迫使非 MCP 服务先做协议改造。

## 决策

Hands 是 AuraClaw 的稳定能力边界。调用链固定为：

```text
Agent Runtime
    │  AuraClaw Hands Contract（协议无关 HTTP/JSON 或 in-process）
    ▼
Action Hands Gateway
    ├── Workload / Lease / Fencing
    ├── Policy / Approval
    ├── Invocation Store / Idempotency
    ├── Audit / Redaction / Artifact
    └── Capability Router
          ├── LocalHandsExecutor
          ├── ManagedMcpConnector ──► Java MCP Server
          └── ManagedJavaApiConnector ──► Java REST API
```

依赖方向：

```text
runtime -> HandsClient / Hands DTO
action  -> CapabilityConnector
infrastructure/connectors/mcp  -> MCP wire / OAuth egress
infrastructure/connectors/http -> 受管 Java API client
composition -> 选择具体 Client 与 Connector
```

约束：

- Runtime 不得导入 `contracts.mcp`、MCP SDK 或 `infrastructure.connectors`。
- tenant / session / run / lease / fencing 只能从 workload token + signed lease 恢复，不能信任 body。
- 公开 Task API 的 tenant/user 只能从 chaintower 签名身份上下文恢复，见
  [ADR-003](./ADR-003%20用户身份归属与可信上下文.md)。
- 不实现任意 REST 代理；Java API 只能调用已注册 operation。
- Policy、Approval、Invocation Store、Artifact 与 Canonical Event 语义仍留在 AuraClaw，不迁入 Java 业务服务。
- MCP 2026-07-28 仅作为下游 Connector profile 保留，不再作为 Runtime→Hands 内部协议。

## 备选方案

1. **继续用内部 MCP。** 否决：MCP 版本升级会穿透 Runtime 与内部契约，也无法接入纯 Java API。
2. **Runtime 直连 Java。** 否决：会绕过 Policy、Approval、幂等、Lease/Fencing 和 Artifact 边界。
3. **任意 OpenAPI 代理。** 否决：模型可生成 URL/method/header，形成 SSRF 与凭证泄漏面。

## 回滚

- 删除内部 `/mcp` 之前：可将 Runtime 切回旧 Hands MCP Client（需同时回滚 Runtime 与 Hands 镜像）。
- 删除内部 `/mcp` 之后：只能整套回滚 Runtime + Hands 版本；下游 `ManagedMcpConnector` 不受影响。
- 配置只保留 `AURACLAW_HANDS_URL`；旧 `AURACLAW_HANDS_MCP_URL` 不再作为兼容 alias。

## 后果

- 生产 Runtime 使用 `HttpHandsClient` 调用 `/internal/v1/hands/*`。
- 开发 combined profile 使用 `InProcessHandsClient`。
- Catalog Reconciler 只依赖 `CapabilityConnector.snapshot()`。
- Java MCP 与 Java API 对 Runtime 呈现同一套 Hands DTO。
