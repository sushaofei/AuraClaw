# Agent Runtime Pool

## 定位

Agent Runtime Pool 是可横向扩展的 Brain 运行环境集合。`Agent Runtime = Role + Harness + Model Client + Context Policy + Tool Client`。它执行任务语义，但不保存不可恢复的唯一状态。

## Runtime 类型

```text
Coordinator Agent Runtime
Worker Agent Runtime
Reviewer Agent Runtime
Specialist Runtime
```

它们可以共享同一 Runtime 镜像和框架，通过 Role Profile、Harness、Model Policy、Tool Permission 和资源声明形成不同实例。

## 通用核心模块

| 模块 | 功能 |
|---|---|
| Role Loader | 加载 Coordinator、Worker、Reviewer 等角色契约 |
| Harness | 稳定执行 facade；保持 composition 和 worker 的调用入口不变 |
| Execution Engine | 选择并推进一个合法状态转换，协调角色、能力与协作流程 |
| Execution State | 集中定义 checkpoint phase 与合法后继状态 |
| Execution Guard | 在模型、工具和持久化副作用边界统一检查 fencing、cancel 与 deadline |
| Model / Tool Round | 使用明确结果类型执行或恢复单个模型/工具回合 |
| Context Builder | 从 Session、Artifact 和 Retrieval 构建当前上下文 |
| Model Client | 通过 Model Gateway 发起模型调用和接收增量 |
| Tool Client | 发现工具、发起调用、关联结果和取消 |
| Capability Client | 经内部 MCP 发现和读取 Resource、Tool、Prompt 与 Skill Package |
| Skill Resolver / Runner | 固定技能版本和依赖，渐进加载说明并驱动 Harness 步骤 |
| Session Client | 读取历史、追加模型/计划/状态事件 |
| Checkpoint Manager | 保存可恢复 Harness 状态和引用 |
| Runtime Event Publisher | 发布 Token、进度和短期状态 |
| Budget Controller | Token、时间、步骤和成本预算 |
| Cancellation Handler | 响应取消、Deadline、Lease 丢失 |

## 可恢复状态机

```text
读取 Canonical Events + durable checkpoint
 -> 检查 Lease / Fencing / Budget / Cancel / Deadline
 -> 从显式 RuntimePhase 图选择一个合法转换
 -> 执行一个 Model Round、Tool Round 或 Commit Action
 -> append-once Canonical Event 并持久化 checkpoint
 -> 继续、等待审批/Child、完成或交接
```

`runtime/harness.py` 只提供稳定 `AgentHarness` facade。状态推进位于
`runtime/execution_engine.py`；phase 图、guard、模型/工具回合、Canonical Event 提交和
checkpoint/suspend 分别位于独立模块，可以单独测试。`RuntimePhase` 是 `str` 枚举，因此已有
checkpoint 的持久化值与跨版本恢复格式不变。

## 状态外置

必须外置：

- Session 事实和 Task DAG。
- 完成模型输出和工具结果。
- Artifact、Patch 和大型日志。
- Checkpoint 和恢复位置引用。
- 资源规格和 Tool Permission。

Runtime 内仅允许：当前 Prompt、短期缓存、连接状态和可丢弃中间计算。

Runtime 只连接内部 MCP Capability Gateway，不注册或直连第三方 MCP Server。Skill 激活后固定包摘要、
Tool/Resource 依赖和 Policy 版本；Catalog 更新不得在同一 Run 内静默替换绑定。详细设计见
[[23 MCP Runtime 能力平面]]。

## 幂等与所有权

- 每次运行携带 `run_id`、`lease_id`、`fencing_token` 和持久化的 `execution_claim_token`。
- Runtime 注册携带进程 incarnation；同一 `runtime_id` 的活跃 incarnation 只能有一个。
- `running` Assignment 只在 execution claim、registration 或 Session lease 失效后恢复，不能按执行时长猜测孤儿。
- Session 写入携带 `expected_version`。
- Tool Call 携带稳定 `tool_invocation_id`。
- Runtime 失去 Lease 后立即停止写入和外部副作用。
- 恢复后由 Harness 根据 Session 判断语义重试，不由 Orchestrator盲目重放工具调用。
- Canonical Event append-once 与 checkpoint/suspend 由独立提交边界负责；checkpoint 不替代事实事件。
- 模型前后、工具前后崩溃恢复必须产生相同的规范化业务事件轨迹，且外部 Tool 副作用最多一次。

## Runtime 事件

持久：完整模型输出、计划决定、工具意图、完成/失败状态。

短期：Token Delta、当前步骤、Typing、部分 Usage、实时进度。

## 扩缩容与隔离

- Runtime Pool 按 Role、Model、Tool、租户和资源规格分池。
- 高风险 Tool Profile 使用独立执行域。
- Runtime 实例无黏性；任何实例可根据 Session 恢复。
- 单实例故障只影响当前运行尝试，不影响 Session。
- `runtime_capacity=N` 对应进程内有界并发 N，Control 的 `assigned|running` 数不得超过同一上限。
- 执行心跳刷新 registration、execution claim、Session lease 和签名 assertion；续租超过安全窗口即停止副作用。

## 观测指标

```text
runtime_startup_latency
context_build_latency
model_latency / token_usage
tool_calls / tool_failures
checkpoint_age
lease_lost
lease_renewal_failure / claim_conflict / duplicate_attempt_prevented
capacity_saturation / active_harness / queue_wait
run_steps / budget_exhausted
```

## 验收条件

- Runtime 在任意步骤崩溃后能够由新实例接管。
- Runtime 无法直接读取 Vault Secret。
- Coordinator、Worker 和 Reviewer 的权限、输出契约可独立配置。
- Runtime 不直接创建进程或 Sandbox，资源生命周期由 Orchestrator 管理。
