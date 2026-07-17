# Coordinator Agent Runtime

## 定位

Coordinator Agent 是复杂任务按需启用的 Agent Role，负责语义拆分、依赖、分派、汇总和动态调整。它不是任务入口，也不是常驻平台服务。

## 启用条件

出现以下情况时启用：

- 子任务可以并行。
- 需要不同角色、模型、Harness、工具或权限。
- 需要隔离上下文、失败和重试。
- 有独立输出契约或 Artifact。
- 需要独立 Reviewer。

普通单 Agent 或单 Session 的顺序步骤不必创建 Child Session。

## 核心模块

```text
Complexity Evaluator
Task Decomposer
Dependency Planner
Role / Profile Selector
Output Contract Builder
Collaboration Tool Client
Join / Completion Monitor
Result Validator
Result Aggregator
Dynamic Replanner
```

## 协作工具

```text
createChildSession(parent, role, goal, outputContract)
setDependencies(childId, dependencyIds)
delegate(childId, agentProfile)
publishResult(childId, resultRef)
join(childIds)
cancel(childId)
handoff(sessionId, targetRole)
requestReview(targetSessionId, contract)
```

所有工具调用进入 Session / Collaboration Service，由服务校验 DAG、权限、版本和所有权。Coordinator 不直接修改数据库或启动 Runtime。

## Task DAG 规则

- DAG 必须无环。
- Child Goal 和 Output Contract 必须明确。
- 依赖只引用同一 Root Task 内允许的 Session。
- 只有依赖满足的 Child 才能成为 runnable。
- Child 可以继续分解，但必须受到深度、数量和成本限制。
- 汇总结果写回 Root Session。

## 核心流程

```text
读取 Root Session
 -> 判断是否需要拆分
 -> 生成 Child Goals / Contracts
 -> Collaboration Service 创建 DAG
 -> 等待 Collaboration Projection
 -> Orchestrator 调度 Runnable Children
 -> 读取 Child Result Projection / Artifact
 -> 验证输出契约
 -> 必要时创建 Review / Repair Child
 -> 汇总并发布 Root Result
```

## 失败处理

- Child 暂时失败：依据合同和预算请求重试或替代 Worker。
- Child 结果不合格：创建 Repair 或 Review Session。
- 部分成功：根据 Root Output Contract 决定降级、补充或失败。
- Coordinator Runtime 故障：新实例从 Collaboration Projection 和 Session 恢复。
- DAG 修改使用 expected version，防止并发 Coordinator 冲突。

## 权限

- Coordinator 只能使用 Collaboration Tools 和显式授权的业务工具。
- 创建 Child 时不能扩大 Root Session 的权限边界。
- 高风险子任务需要 Policy/Approval 决策。
- Coordinator 不持有 Runtime 基础设施权限。

## 观测指标

```text
children_created
dag_depth / dag_width
parallelism
join_wait_time
replan_count
child_contract_failure
aggregation_latency
```

## 验收条件

- 简单任务可以不启动 Coordinator。
- Coordinator 不能绕过 Collaboration Service 修改 DAG。
- Coordinator 重启后不会重复创建相同 Child。
- Root Result 能追溯到所有 Child Result 和 Artifact。
