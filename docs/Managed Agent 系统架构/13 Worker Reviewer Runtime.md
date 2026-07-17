# Worker / Reviewer Runtime

## 定位

Worker 和 Reviewer 是使用通用 Agent Runtime 框架实现的两类执行角色。Worker 负责产出，Reviewer 使用独立上下文和标准验证产出。两者不负责全局调度和基础设施生命周期。

## Worker Runtime

核心职责：

- 读取 Child Session Goal、Input Refs 和 Output Contract。
- 构建最小任务上下文。
- 调用模型和工具完成子任务。
- 写入工具结果、模型完成输出和 Artifact 引用。
- 发布符合合同的 Child Result。
- 在失败、审批或缺少输入时产生明确状态。

核心模块：

```text
Goal Interpreter
Context Builder
Execution Harness
Tool Client
Artifact Producer
Contract Validator
Result Publisher
Checkpoint Manager
```

## Reviewer Runtime

核心职责：

- 使用独立 Context 读取待审 Result、Artifact 和 Output Contract。
- 执行验证、测试、反例检查和政策检查。
- 输出 `accepted`、`changes_requested` 或 `rejected`。
- 提交证据、问题列表和修复建议。

核心模块：

```text
Review Contract Loader
Evidence Collector
Validation Harness
Test / Static Check Tool Client
Finding Publisher
Decision Publisher
```

Reviewer 不应直接覆盖 Worker Artifact；应提交 Review Event、Patch 或 Repair Request。

## 输入输出契约

```text
Child Input
  goal
  input_refs
  constraints
  output_contract
  tool_permissions
  budget

Child Result
  status
  summary
  result_ref
  artifact_refs
  evidence_refs
  limitations
  contract_version
```

## 所有权与隔离

- 一个 Child Session 同时只有一个执行 Lease 持有者。
- Worker 写自己的 Child Session，不直接写 Root。
- Reviewer 使用独立 Session 或 Review Branch。
- 外部资源写入遵循 Artifact Ownership 和工具幂等键。
- Tool Permission 不得超过 Child Profile 和 Root Policy 的交集。

## 错误与重试

- Runtime/网络错误：Orchestrator 恢复基础设施。
- 工具副作用未知：不自动重放，交给 Harness 或 Human 决策。
- Output Contract 不满足：Worker 修复或 Coordinator 创建 Repair Child。
- Reviewer 拒绝：记录证据，不直接把任务标成系统失败。

## 观测指标

```text
worker_completion_rate
contract_validation_failure
review_accept_rate
review_turnaround
repair_cycles
tool_side_effect_unknown
```

## 验收条件

- Worker 和 Reviewer 具有不同权限和 Context。
- Reviewer 结论包含可追溯证据。
- Worker Runtime 崩溃不会丢失已提交事实。
- Child 结果只有通过规定合同后才进入 completed。
