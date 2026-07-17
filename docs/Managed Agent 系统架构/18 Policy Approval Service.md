# Policy / Approval Service

## 定位

Policy / Approval Service 是横切入口、模型、调度、工具和交付的安全决策平面。它把权限、配额、风险策略和 Human-in-the-Loop 建模为可审计、可恢复的决策，而不是依赖 Prompt 约束。

## 核心模块

| 模块 | 功能 |
|---|---|
| Policy Engine | 依据主体、资源、动作、上下文和策略版本决策 |
| Risk Classifier | 对工具、参数、数据和副作用分类 |
| Approval Request Manager | 创建、去重、过期和取消审批 |
| Decision Store / Projector | 保存批准、拒绝和策略证据 |
| Human Assignment | 指定允许审批的用户、组或岗位 |
| Action Digest | 规范化动作并计算不可变摘要 |
| Quota / Budget Policy | 成本、Token、并发和资源策略 |
| Enforcement Adapter | 为 Task、Tool、Model、Delivery 提供检查接口 |
| Audit Publisher | 记录策略版本、输入摘要和决策 |

## 决策接口

```text
evaluate(subject, action, resource, context)
requestApproval(actionDigest, allowedDecisions, expiresAt)
recordHumanResponse(approvalId, actor, decision, feedback)
validateApproval(approvalId, sessionId, actionDigest)
cancelApproval(approvalId, reason)
```

决策结果：

```text
allow
deny
allow_with_constraints
require_approval
```

## Approval 模型

```text
approval_id
tenant_id
session_id / run_id
action_digest
tool_name + redacted_arguments
risk / reason / expected_effect
allowed_decisions
assigned_approvers
policy_version
expires_at
status
decision / feedback
```

批准绑定 `approval_id + session_id + action_digest + policy_version`。参数、目标或权限变化必须重新审批。

## Human-in-the-Loop 流程

```text
Tool Gateway -> Policy: evaluate
Policy -> Session: approval.requested + waiting_for_human
Session/Runtime Bus -> Streaming Gateway -> Web: 通知
Human -> Task Gateway: response
Task Gateway -> Session: human.response
Projection -> Approval View
Orchestrator: 恢复 runnable Session
Tool Gateway -> Policy: validateApproval
Tool Gateway: 执行动作
```

Streaming Gateway 只通知，不接收审批结果。

## 策略执行点

- Task Gateway：接纳、租户、配额和命令权限。
- Model Gateway：数据驻留、模型允许列表和预算。
- Orchestrator：资源上限和 Runtime Placement。
- Tool Gateway：动作、参数、副作用和审批。
- Result Delivery：目标、数据级别和外发策略。
- Artifact Store：读取、下载和分享权限。

## 失败与过期

- Policy 服务不可用时，高风险写操作默认 fail closed。
- 低风险只读行为可按明确策略降级，不能默认放行。
- Approval 过期产生 Canonical Event，不自动视为拒绝或失败。
- 拒绝后 Agent 可以修改计划，不必终止整个任务。

## 观测指标

```text
policy_decisions_by_result
approval_wait_time
approval_expired
action_digest_mismatch
deny_reason
policy_evaluation_latency
budget_exceeded
```

## 验收条件

- 批准不能用于不同参数或不同 Session。
- Human Response 必须经过 Task Gateway 鉴权和幂等处理。
- 高风险工具无法绕过 Policy/Approval。
- 每个决策可追溯到策略版本和输入摘要。
