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
auraclaw.collaboration.get_graph
auraclaw.collaboration.create_child
auraclaw.collaboration.set_dependencies
auraclaw.collaboration.request_review
auraclaw.collaboration.cancel_child
auraclaw.collaboration.await_children
auraclaw.collaboration.join
```

所有工具调用进入 Session / Collaboration Service，由服务校验 DAG、权限、版本和所有权。Coordinator 不直接修改数据库或启动 Runtime。

V1 不向模型开放 `delegate` / `handoff`。这两个 Service 能力继续保留，既有 `owner` 事件语义冻结，
但 Runtime 的模型工具清单中没有对应入口。执行归属由 Control Plane 的 Lease 和 fencing token
确定，避免模型同时操纵业务 owner 与实际执行租约，后续只有在两者语义、恢复和冲突策略明确后才开放。

## Runtime 执行与恢复

- 所有语义角色共用 `agent` Runtime Pool；`root`、`worker`、`reviewer`、`repair` 仍保留在
  Assignment 中，由 Harness 决定可见工具和终态合同。
- Coordinator 每一轮都读取同一 Root 的 Canonical Collaboration Graph；`task_key` 是创建 Child
  的稳定幂等键。
- `await_children` 写入 `agent.waiting_children` checkpoint，释放 Assignment Lease，并且不写
  `run.completed`。
- Child 终态事件使 Orchestrator 从 Canonical Root Feed 重算 runnable DAG；所有等待目标终态后，
  同一个 Root Run 被重新排队并从 checkpoint 继续。
- `join` 是 Coordinator 唯一的协作终态工具；只要存在 Child，普通文本输出不能把 Root 标成完成。

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
- 等待 Child 时 Root Run 释放 Lease，恢复后不重复已提交工具调用。
- 模型无法调用 `delegate` / `handoff`，也不能提交 actor、owner、tenant 或 fencing token。
- Root Result 能追溯到所有 Child Result 和 Artifact。

## 当前实现对照

- 归属：`runtime/collaboration_controller.py`、`runtime/execution_engine.py` 与
  `session/collaboration_service.py`；通过 Capability-Aware Agent Loop 暴露受控协作动作。
- 已实现 create child、dependencies、delegate、join、review、handoff、publish result 等契约，所有变化经
  Session Service 成为 Canonical Events。
- Coordinator role 与 Worker/Reviewer 共用模型和执行引擎，Orchestrator 仅调度资源。

## 现有缺陷与待完善

- “是否拆分、如何拆分”的质量由模型与 Prompt/Skill 决定，尚无独立 Planner 评估、成本预测或 DAG 优化器。
- 大规模 DAG 的并发窗口、失败传播、部分结果接受和动态重规划策略仍较基础。
- 缺少针对多 Agent 语义质量的离线 eval、可重复 benchmark 和策略回归门禁。
- 待补：DAG 规模/深度限制、预算分配、child 取消传播矩阵、review gate 策略与协调质量指标。
