# AuraClaw 模块内聚与依赖重构

跟踪：[Issue #105](https://github.com/sushaofei/AuraClaw/issues/105)。

## 目标与约束

让一个业务规则的修改集中在拥有该规则和状态的模块中，让调用方依赖明确的协议。
保持现有生产进程拓扑、CLI、HTTP 路径、事件与检查点格式；Canonical Session Events
仍为任务事实源，Runtime 不直接改变业务 Session 状态。重构不包含数据库迁移、生产部署或数据清理。

每阶段独立完成验证、文档和提交；第一阶段完成不表示整个重构完成。

## R1：服务装配单向依赖

原先 `services.py` 分发到 builders，而全部 12 个 builder 又从它导入规格和私有函数。
这使服务分发、生命周期、安全、Skill/MCP 装配和 Runtime worker 的变更集中在同一个模块。

调整后的依赖为：

```text
CLI -> services -> builders -> service_spec / service_host / security
                           -> skill_services / credential_services / adapters
```

| 模块 | 所有权 |
|---|---|
| `services.py` | builder 注册、薄分发、生产存储前置检查 |
| `service_spec.py` | 服务名称、端口规格、依赖分类、worker 分类 |
| `service_host.py` | FastAPI 服务外壳、readiness、worker 循环、唤醒与关闭 |
| `security.py` | workload 身份、租约密钥与 fencing ledger 的装配 |
| `skill_services.py` | Skill/MCP registry、目录存储、发布服务的装配与开发 registry 生命周期 |
| `credential_services.py` | 凭据代理的 MCP 快照恢复与受管 Connector 凭据初始化 |
| `adapters/remote_runtime_worker.py` | 远端任务领取、续租、失败处置和 Runtime 实例身份 |
| `adapters/unavailable_model.py` | 未配置模型时的失败适配器 |

跨模块辅助函数通过公开名称导入；`services.create_service_app`、`services.service_spec`、
`ServiceSpec` 以及 CLI 使用的服务分类常量保留兼容出口。原有开发 Skill registry 缓存归属
`skill_services`，不新增跨进程共享状态。后续状态治理须单独处理该缓存的配置与生命周期，
不在此次结构迁移中改变行为。

新增 import-linter 合同禁止 builders/support 依赖 services，并禁止 support 依赖 builders。
CI 执行 import-linter。新解释器测试阻断 services 导入后加载全部 builders，验证依赖确实单向。
原有 topology、readiness、worker 唤醒、Runtime 续租与 fencing、Skill 管理测试验证行为保持。

## R2：能力控制器与状态所有权

从 `RuntimeCapabilityController` 提取内容缓存、提示词构建、Skill 激活和资源读取。
控制器保留能力操作分发；缓存拥有锁、加载合并、TTL 与容量淘汰；运行状态拥有已加载能力、
激活记录与计数。先明确读写端口，再逐步引入类型化状态，禁止多个组件直接修改同一可变字典。

持久化边界保留旧检查点字段，使用显式序列化/反序列化适配；若需要新格式，先制定版本与
N/N-1 读取策略。验收覆盖缓存并发与租户隔离、激活幂等、绑定撤销、提示词预算和恢复。

### R2 实现边界

`runtime/capability_controller.py` 保留原有公共入口、确定性预加载、Router inspection、
search/load/tool 分发与 resolve-and-activate 组合流程，组件位于 `runtime/capabilities/`：

| 模块 | 所有权 |
|---|---|
| `contracts.py` | 操作名称、执行结果、进度回调与 admission 异常；原 controller 导入路径仍可用 |
| `state.py` | `CapabilityState` 的类型化读取与检查点副本导入/导出，保留未知字段和缺省字段形状 |
| `loading.py` | 已加载能力的数量与 Schema 字节预算；输入和输出描述符不共享嵌套可变数据 |
| `model_tools.py` | 模型可见控制工具、已加载工具描述和参数指导 |
| `content_cache.py` | 租户、版本、digest、path 隔离的内容缓存；Run 内加载合并、TTL、LRU、容量与取消 |
| `prompts.py` | Skill 说明与声明预加载引用的拼装、提示词预算、缓存 key 和提示词指标 |
| `skills.py` | 可信/模型 Skill 激活、依赖加载、工作流进度、事件恢复、绑定撤销检查与工作流指标 |
| `resources.py` | 已加载 Resource/Template 与已激活 Skill 声明引用的受控读取和证据 |

组件入口接收拥有自身数据的 `CapabilityState`，需要写入时导出私有工作副本；
Skill 工作流给进度接收方的 state/events 也是独立副本，接收方保留或转换检查点不会影响后续步骤。
`restore_skill_events` 的原公共入口继续更新调用方映射，但恢复结果不引用 Canonical Event 的可变 payload。
工作流内部继续使用原字段布局；本阶段不宣称将所有工作流字段替换成新的 DTO。

内容加载合并限制在同一 Run；不同 Run 的在途读取独立授权、独立取消，完成后的不可变内容可跨 Run 复用。
两个 Run 同时完成同一冷加载时，替换已有内容先扣除旧字节，避免重复计数导致误淘汰。
释放一个 Run 仅取消它的在途读取并清理它的指标，不删除共享的已完成内容。

新增架构合同禁止组件依赖 controller、execution engine 或 task router，并使激活、提示词、
资源读取、模型工具展示四种职责相互独立。验证新增检查点往返、嵌套隔离、进度快照隔离、
Canonical payload 隔离、加载合并、取消、跨 Run 字节计数、TTL/LRU 与租户/digest/path 隔离测试；
原有 activation 幂等、generation 切换、审批恢复、未知写结果补录、撤销和预算用例继续执行。

## R3：执行引擎与恢复语义

从 `RuntimeExecutionEngine` 提取恢复、预算结算、消息构建；引擎只协调有明确输入输出的
状态迁移。复用已有 Guard、EventCommitter、ProgressStore 和 Model/Tool Round 边界，
不再创建同职责的第二套协调层。预算预留/结算与事件提交顺序必须保持。

验收覆盖模型/工具执行前后故障恢复、待确认结果补录、审批/子任务等待、取消、过期租约、
旧 fencing token、截断恢复与预算耗尽；现有检查点可继续恢复。

### R3 实现边界

执行引擎从 2009 行缩减为 1169 行，保留阶段迁移、Router 接入、撤销处置、故障注入和
Model/Tool Round 调度。新增组件位于 `runtime/execution/`：

| 模块 | 所有权 |
|---|---|
| `recovery.py` | 待确认结果补录、失败与未派发工具结算、审批证据和等待恢复、Canonical/Control 检查点选择 |
| `budget.py` | 预留、用量校验、结束原因、`TurnBudget`；不拥有执行游标或本地预算账本 |
| `context.py` | `RuntimeModelContext`、可信说明顺序、Router 首轮说明、发现工具过滤与结束轮禁止工具 |
| `messages.py` | Canonical 历史消息、输入摘要、工具来源和请求身份摘要 |
| `serialization.py`、`contracts.py` | 检查点兼容编解码、finish reason 分类与注入点兼容枚举 |

组件共用原 Guard、Committer、ProgressStore，不引入第二套持久化或状态机。
恢复能力端口仅提供 invocation 状态查询和 Run 资源释放；协作恢复端口仅提供 Child 状态和
既有 DAG 的 join，不提供通用工具执行。上下文端口只提供说明和 Schema。
预算服务返回原因和分配结果，由引擎写入执行 state；v1 终结预留和截断提示、v2 结束轮
预算与无工具约束保持原样。v2 最新 Canonical progress 优先于控制检查点，v1 保持旧恢复路径。

保留 `AgentHarness`、engine/harness 的注入枚举出口、事件和检查点字段。
原测试直接调用的私有辅助函数改为验证对应组件的公开函数，不保留重复实现。
新增 13 项组件测试覆盖 v1/v2 分配矩阵、预留与硬耗尽的不同错误、原因优先级与状态只读、
Canonical progress 选择及 Run 隔离、恢复先校验 fencing、可信说明顺序/工具过滤和旧响应往返。
新增两条架构合同约束组件无反向依赖且恢复、预算、上下文互不依赖。

## R4：目录与管理 API

分离 Capability Catalog 的存储适配器、索引发布、搜索服务与工具执行器。
保留索引 generation、租约提交和搜索证据契约。Skill Admin 按发布者、包发布、安装与准入查询
组织路由；窗口聚合和告警规则归应用服务，HTTP 层保留身份、参数与响应映射。

验收覆盖搜索 golden fixtures、目录并发对账与缓存失效、权限可见性、管理查询过滤/分页、
发布与安装 expected revision、tenant 隔离及 HTTP 契约兼容。

## 阶段交付与回滚

每阶段运行 Ruff、Mypy、Pytest、import-linter，更新架构映射和 stage-gates，按单一意图
提交并推送当前分支。外部数据库、Kafka、Vault 或对象存储用例若缺少环境，明确记录跳过；
不得把跳过当作生产验收通过。

R1/R2/R3 不修改持久化数据、检查点格式或部署配置，可整体回退对应阶段提交并重新构建。
后续状态/检查点格式变化必须先补充迁移与回滚策略，再实施。
