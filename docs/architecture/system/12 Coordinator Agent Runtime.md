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

## Agent Router

Router 位于 Coordinator 的语义边界内，在首轮生成式模型调用前形成结构化
`RoutingDecision`。Orchestrator 不读取用户语义，也不参与 Skill、Tool 或 Agent Role 的选择。

路由模式为 `off | shadow | assist | enforce`：

- `off` 不计算路由；
- `shadow` 计算并记录决定，但不改变执行；
- `assist` 只采用通过高置信度门禁的决定，其余回退 Capability-Aware Agent Loop；
- `enforce` 为通过离线 eval 和交大环境灰度后的目标模式，首阶段不扩大自动执行范围。

确定性 Router 处理两类高置信度输入：可信任务上下文中被目标精确选择的唯一 Skill，以及目标中唯一、
完整的 dotted canonical Tool/Skill 引用。`skill_names` 本身只是授权范围，不代表用户一定选择了 Skill；
多个 allowlist 条目中仅精确命中一个时只采用该项。显式 canonical 引用必须先经过受管 Capability
Search 的策略可见性和唯一性检查，再通过受管 Load 或 Skill activation 固定当前 binding、版本与 digest。
采用路由后仍逐次执行 resolver、Policy、包完整性、依赖加载、Approval、binding disposition 和撤销检查，
且 Skill 激活写入正常的 `skill.activated` canonical event。

Router 决定及 `decision_digest` 进入 Runtime checkpoint。进程恢复时先从 canonical Skill 事件恢复
active state，再复用相同 activation key，避免重复激活。采用单 Skill 快速路径后，首轮模型不再暴露
search/load/activate/resolve-and-activate 控制工具；唯一 Tool 命中时也在首轮前完成受管 Search/Load，首轮
只暴露固定的业务 Tool。Resource、业务 Tool 与必要的协作工具仍按绑定可见。

Normalizer 不把 URL、自由文本相似词或客户端提供的 capability id 当作选择证据。2–8 个显式能力只有
在全部经策略可见性检查、固定当前 binding 且为 `read-only` Tool 时，才编译为当前 Session 内的有界
`sequential_plan`；计划包含稳定 task key、依赖、输出合同、预算、精确 binding 和 digest。Skill、写能力、
模糊候选或校验失败仍回退。当前顺序计划不会创建 Child；只有后续语义 Planner 产生并通过受管
Role/Profile、权限、预算和 DAG 校验的计划，才能经 Collaboration Service 创建 Child。候选查询复用按
tenant、Catalog revision、策略、Role 和过滤条件隔离的短 TTL Search L1，Load、激活、最终授权与执行不缓存。

后续结构化 Planner 必须沿用同一契约，只有并行、权限/上下文隔离、不同 Role/Profile、独立输出合同
或 Reviewer gate 时才产生 Child DAG。多候选、低置信度、校验失败或超预算继续澄清或回退，不允许
Router 绕过 Collaboration Service 直接修改 DAG。

语义 Planner 支持 `off | shadow | submit`。它只接收 Runtime 生成的候选名称 allowlist 和受管
Profile/Model/Harness 注册表，通过虚拟 `auraclaw.router.submit_plan` 返回严格 Schema 提案；
提案不能携带 tenant、actor、credential、URL、capability id、server id 或配置 revision。Runtime 再从
当前 Catalog 解析精确 binding，校验无环 DAG、深宽、Root 预算、角色/注册表、Child 工具权限以及写
操作的高风险 Reviewer gate。`shadow` 通过的提案只进入 checkpoint 和指标；`submit` 进一步只接受
低/中风险、只读、每步精确绑定 capability 且全部为 worker Child 的 DAG，并调用内部原子提交边界，
记录稳定 plan digest/Child IDs 后让 Root 进入 `agent.waiting_children`。其余合法 DAG 仍为 shadow；
Reviewer、写操作与动态重规划不在首轮自动采用范围。Planner 模型调用以内部 canonical
`model.turn.completed` 记录并计入 Run 用量。模型输出异常、越权、循环、超预算一律保留原回退决定。
budget policy v2 在模型调用前
使用稳定 model call id 写入幂等 `runtime.budget.reserved`；预留失败时不调用 Planner 模型并保留原
fallback，崩溃恢复复用同一 reservation 和 Model Gateway 幂等结果。

Coordinator DAG 的物化入口是 Session Service 内部 `submit_plan` 命令。它重新校验 plan digest、Child
scope、task key、依赖拓扑、输出合同和协作额度，将 task key 映射为稳定 Child Session ID，并通过
`AtomicBatchEventStore` 在一个 Root 事务中写入全部 `child.created` / `run.requested`、Outbox、Session
head 和 command dedup。PostgreSQL 对 command、Root budget 和所有目标 Session head 取确定性锁；任一
版本、预算或治理错误回滚整张 DAG。完整同规格 DAG 可幂等复用，部分重叠或规格漂移拒绝。该内部命令
只接受持 Root lease 的 root/coordinator Runtime，尚未加入模型协作工具。`submit` 模式使用
`run_id + plan_digest` 生成稳定 command id；Child 的精确 capability/version/digest 和 Skill 名称进入
canonical `child.created`，Control Plane 再把它们带入 Child Runtime assignment，先完成精确 preload，
不让 Child 重新执行无边界 Search/Load。

## 协作工具

```text
auraclaw.collaboration.get_graph
auraclaw.collaboration.create_child
auraclaw.router.submit_plan  # 虚拟 Planner 工具；不是模型可调用的 DAG 写接口
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

- 已有高置信度单 Skill 的首轮前 Router 快速路径；自然语言候选召回、复杂度评估与结构化 Planner
  尚未进入 enforce，“是否拆分、如何拆分”的其余路径仍主要由模型与 Prompt/Skill 决定。
- 大规模 DAG 的并发窗口、失败传播、部分结果接受和动态重规划策略仍较基础。
- 缺少针对多 Agent 语义质量的离线 eval、可重复 benchmark 和策略回归门禁。
- 待补：DAG 规模/深度限制、预算分配、child 取消传播矩阵、review gate 策略与协调质量指标。
