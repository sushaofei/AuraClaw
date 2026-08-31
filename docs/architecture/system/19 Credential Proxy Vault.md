# Credential Proxy / Vault

## 定位

Credential Proxy 代 Agent 使用凭证调用外部系统；Vault 保存 Secret、OAuth Token 和密钥。Agent Runtime、Sandbox、Prompt、Tool Result 和 Session Event 均不得接触真实凭证。

## 信任边界

```text
Agent / Sandbox
  -> 受控 Tool Invocation
Tool Gateway / Credential Proxy
  -> Policy Check
  -> Vault Resolve
  -> External Service
  -> Redacted Result
```

## Credential Proxy 核心模块

```text
Service Authentication
Credential Reference Resolver
Policy / Scope Validator
Delegated Request Builder
OAuth Refresh Coordinator
Request Signing
Response Redaction
Egress Allowlist
Usage Audit
Rate Limit / Circuit Breaker
```

## Vault 核心模块

```text
Secret Storage
Encryption / KMS
Versioning
Lease / Dynamic Credentials
OAuth Token Lifecycle
Rotation
Revocation
Access Policy
Audit Log
Break-glass Governance
```

## Credential Reference

Session 或 Tool Invocation 只保存：

```text
credential_ref
provider
account_scope
allowed_operations
expires_at
```

`credential_ref` 不能被外部客户端解析为 Secret。

## 调用模式

### 代理调用

Proxy 直接构造外部 API 请求，最适合 SaaS、数据库和消息系统。

### 资源初始化

Provision 阶段使用凭证准备受控资源，例如 Clone Repo；Sandbox 获得工作副本，但不能读取初始化 Secret。

### 短期动态凭证

必要时向受信任 Runtime 发放最小范围、短 TTL、可撤销凭证；不向模型可读环境发放。

## 防泄漏要求

- Secret 不进入环境变量、Prompt、文件和 Tool Result。
- 日志、Trace、错误和 HTTP Header 必须脱敏。
- External Response 中可能回显的 Token 需要扫描。
- Proxy 仅允许目标域、方法和资源范围。
- 不同 Tenant、Session 和 Account 的权限上下文隔离。

## 失败处理

- Policy validator 未配置、不可达、超时、返回异常或决定无效：在解析 Vault Secret、写 usage audit 或调用外部适配器前 fail closed。
- 生产 Credential Proxy 缺少任一调用方 workload identity、服务自身 Policy workload identity 或 Policy 地址时拒绝启动。
- Secret 过期：Proxy 协调刷新并记录，不把刷新 Token 返回调用方。
- 权限不足：返回标准 denied，不泄漏账户信息。
- 外部副作用未知：记录 request id 和 side-effect status，禁止盲目重试。
- Vault 不可用：写操作 fail closed，按策略允许已缓存的短 TTL 能力。

## 观测指标

```text
credential_resolve_latency
vault_error
token_refresh
scope_denied
egress_denied
redaction_hit
credential_rotation_age
unknown_side_effect
```

## 验收条件

- 在 Sandbox 中执行 `printenv`、读文件或抓取进程信息无法获得 Secret。
- Tool Result 和日志不出现真实 Token。
- 凭证撤销后新调用立即失效。
- 每次凭证使用可以追溯到 Session、Tool、Actor 和策略决策。
