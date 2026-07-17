# Tool Gateway / Dispatcher

## 定位

Tool Gateway 是 Brain 与 Hands 之间的同步行动数据平面。它负责工具发现、授权、路由、标准化和传输可靠性，不负责 Runtime 生命周期，也不替 Agent 判断语义上是否应重试。

## 核心模块

```text
Tool Registry
Capability Discovery
Schema Validator
Permission / Policy Enforcement
Approval Validator
Invocation Correlator
Runtime Router
Credential Reference Resolver
Timeout / Cancellation
Transport Retry
Circuit Breaker
Result Normalizer
Artifact Extractor
Audit / Event Publisher
```

## Tool Capability

```text
name
description
input_schema / output_schema
read_permissions / write_permissions
side_effects
risk_level
approval_policy
runtime_location
timeout / quota
owner / version
```

权限等级至少区分：`read-only`、`suggest-only`、`write-with-approval`、`write-autonomous` 和 `destructive/admin`。

## 接口

```text
discoverTools(sessionContext)
getToolSchema(name, version)
execute(toolInvocation)
cancel(toolInvocationId)
getStatus(toolInvocationId)
```

Tool Invocation：

```text
tool_invocation_id
session_id / run_id
tool_name / version
arguments
expected_side_effect
approval_id
idempotency_key
deadline
fencing_token
```

统一结果：

```text
status: success | error | denied | timeout | cancelled | unknown
content: string | json | artifact_ref
summary
metadata
error_code
side_effect_status
```

## 调用流程

```text
Agent Runtime
 -> Tool Schema Validation
 -> Permission / Policy Check
 -> 必要时 Approval Check
 -> Runtime Route
 -> Credential Proxy / Hands
 -> Result Normalization
 -> Tool Result 返回 Agent Runtime
 -> Canonical Tool Event + Runtime Progress Event
```

Tool Result 返回 Agent Runtime 的同步/关联响应不可由 Runtime Event Bus 替代。

## 重试边界

- 连接建立失败等传输错误可由 Gateway 重试。
- 有幂等键且明确未执行的调用可安全重试。
- 外部副作用未知时返回 `unknown`，由 Agent/Human 决定补偿或查询。
- Orchestrator 只恢复 Runtime，不盲目重放 Tool Call。

## 审批绑定

执行高风险动作前校验：

```text
approval_id
session_id
action_digest(tool + normalized arguments)
policy_version
expires_at
```

任何参数变化都必须重新审批。

## 观测指标

```text
tool_call_latency
tool_error_by_code
approval_required
permission_denied
transport_retry
side_effect_unknown
circuit_open
schema_validation_failure
```

## 验收条件

- Tool Gateway 与 Hands 之间存在清晰请求/结果双向链路。
- Agent 无法绕过 Gateway 直接调用受控外部系统。
- 相同幂等键不会重复产生外部副作用。
- Secret 不进入 Tool Result、Session 或 Runtime Event。
