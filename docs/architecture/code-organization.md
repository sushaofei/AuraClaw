# 代码组织与部署映射

本文是架构组件、Python 包和生产进程之间的当前映射。系统语义仍以
[系统架构文档](./system/00%20Managed%20Agent%20系统架构总览.md) 为准；本页用于回答“代码放在哪里、由哪个进程装配”。

## 依赖方向

```text
entrypoint
  -> composition
      -> api / gateways / business packages
      -> infrastructure adapters

domain -> contracts
business packages -> stable ports
infrastructure -> implements stable ports
```

硬约束由 `pyproject.toml` 中的 import-linter 合同执行：

- `contracts` 不依赖 FastAPI、业务包或基础设施；
- `domain` 只依赖 `contracts`；
- `api` 和 `gateways` 不选择具体基础设施适配器；
- 业务包不导入 `composition`；
- `infrastructure` 不依赖 `api`、`gateways` 或 `composition`；
- Query/Streaming 只读网关不调用 Session 写服务；
- Runtime 只调用协议无关的 Hands Contract，不嵌入 MCP Connector；
- Action Hands 不反向依赖 Runtime 或 Control。

## 包职责

| 包 | 主要职责 |
|---|---|
| `contracts/` | 跨边界 DTO、命令、事件和稳定协议 |
| `domain/` | Session、Collaboration、Approval 等领域状态机 |
| `session/` | Canonical Session 写路径与协作应用服务 |
| `projection/` | 可删除、可重建的投影规则与维护逻辑 |
| `control/` | Runnable、Assignment、Lease、Fencing 和调度 |
| `runtime/` | 可恢复 Agent Loop、角色执行和受控客户端 |
| `action/` | Capability Catalog、Tool/Resource/Skill、Admin Catalog 查询与 Hands 应用逻辑 |
| `policy/`、`credential_proxy/`、`artifact/` | 独立信任域的业务边界 |
| `delivery/`、`observability/` | 可靠结果交付、观测和审计 |
| `gateways/` | Task、Query、Streaming 接入边界 |
| `api/` | HTTP 表示、路由和鉴权上下文 |
| `infrastructure/` | SQL、Kafka、对象存储、模型和下游 Connector 适配器 |
| `composition/` | 唯一对象图、服务装配和 CLI 实现选择 |

`application/`、`admin/`、`internal/` 及各服务外观包承载兼容入口或跨边界用例；新增核心规则应优先进入上表对应的明确边界，而不是扩展通用杂项包。

Skill 控制面按业务链路组织，而不是按 HTTP 页面堆叠规则：`action/skill_lifecycle.py` 定义生命周期
Port 与内存参考实现，发布、管理、Source 对账分别由明确的应用服务负责，Admin 列表的 latest-version、
filter 与 dependency availability 语义归 `action/skill_admin_catalog.py`。HTTP route 只做身份/参数与响应
映射。PostgreSQL 生命周期适配器保留事务协调，Package、Publication、Installation、Source 的行映射
分别位于 `infrastructure/persistence/postgres_skill_*_records.py`，避免单一适配器同时承担全部聚合适配。

跨服务客户端统一通过 `infrastructure/clients/internal.py` 建立 timeout、workload bearer、请求上下文与
有限瞬时故障重试；稳定 command id 使写命令重试保持幂等。业务客户端只声明契约路径和 DTO，不各自
复制认证、correlation/causation 或 retry 策略。

## 生产进程映射

`auraclaw serve` 在本地启动与生产同构的 12 进程拓扑。`composition/services.py`
只保留 builder 注册、生产存储前置检查和薄分发入口；服务规格在 `service_spec.py`，
公共生命周期、readiness 与 worker 唤醒在 `service_host.py`，安全装配在 `security.py`，
Skill/MCP 与凭据初始化分别归 `skill_services.py`、`credential_services.py`，
远端 Runtime worker 归 `composition/adapters/remote_runtime_worker.py`。12 个生产对象图分别由
`composition/builders/` 下的独立 builder 装配。builder 之间不互相依赖，development 与
production 复用同一 builder，仅由 Settings/provider 选择不同 adapter。
builders 与公共装配模块不依赖 `services.py`，公共装配模块也不依赖 builders；
两条方向约束由 import-linter 在 CI 执行。

分阶段重构方案与兼容约束见
[模块内聚与依赖重构](../development/implementation/modularity-refactor.md)。

Runtime 能力控制器保留稳定入口和操作分发，内容缓存、提示词预算、Skill 激活/恢复、
资源读取、加载预算和模型工具展示归 `runtime/capabilities/` 的独立组件。
组件通过 `CapabilityState` 接收拥有自身数据的快照，持久化仍导出原检查点字段；
进度接收方与 Canonical Event payload 不共享可变工作流状态。
组件不得反向依赖 controller/engine/router；激活、提示词、资源读取和工具展示的独立性由
import-linter 约束。

| CLI | 服务 | 主归属代码 |
|---|---|---|
| `auraclaw api run` | Task API | `api/`、`gateways/task/` |
| `auraclaw session run` | Session Service | `session/`、`domain/` |
| `auraclaw projection run` | Projection Worker | `projection/`、`infrastructure/projection/` |
| `auraclaw orchestrator run` | Orchestrator | `control/` |
| `auraclaw runtime run` | Agent Runtime | `runtime/` |
| `auraclaw model-gateway run` | Model Gateway | `model_gateway/`、`infrastructure/model/` |
| `auraclaw hands run` | Action Hands | `action/`、`infrastructure/connectors/` |
| `auraclaw policy run` | Policy Service | `policy/` |
| `auraclaw credential-proxy run` | Credential Proxy | `credential_proxy/`、`infrastructure/credentials/` |
| `auraclaw artifact run` | Artifact Service | `artifact/`、`infrastructure/artifacts/` |
| `auraclaw streaming run` | Streaming Gateway | `gateways/streaming/`、`infrastructure/kafka/` |
| `auraclaw delivery run` | Delivery Worker | `delivery/`、`infrastructure/delivery/` |

包与部署单元不是 1:1。组件归属以职责和稳定端口为边界，具体适配器只在 `composition` 中选择。

## Capability Catalog 与 Skill Admin

`action/capability_catalog.py` 是兼容入口与薄分发；核心职责位于 `action/catalog/`：

| 模块 | 所有权 |
|---|---|
| `memory.py` | 原内存参考存储：Server、generation、reconcile lease 和 committed snapshot |
| `publication.py` | 校验发布范围、租约竞争、embedding 索引构建、摘要复用和 generation 提交 |
| `search.py` | 权限/角色/环境可见性、混合搜索、证据、live get、按 revision 隔离的有界缓存 |
| `executors.py` | Search/Load/Skill resolve/binding status 的 Hands 适配与工具路由 |
| `tools.py`、`contracts.py` | 模型工具 Schema、稳定名称和只读/解析/安全查询协议 |
| `telemetry.py` | 有界尽力写入搜索、缓存与索引发布指标 |

PostgreSQL Catalog 仍实现原 `CapabilityCatalogStore`，未改变事务、租约、CAS 或表结构。
组件不反向导入兼容入口；存储、发布、搜索、执行器与 Schema 互不依赖，通过 Port 和公共契约交接。

`api/routes/admin_skills.py` 保留原 factory 参数与兼容出口，只组装 `skill_admin/` 下的
catalog、admissions、publishers、installations、publications 五组路由。
`contracts.py` 保存 HTTP 请求模型和服务协议，`presentation.py` 保存响应映射、配置缺失错误和游标编码。
过滤与管理视图聚合归 `action/skill_admin_queries.py`；latest-version 与依赖可用性继续归
`action/skill_admin_catalog.py`；准入时间窗口、计数和 quarantine 告警归 `action/skill_admission_query.py`。
路由继续从认证上下文构建命令，保留 command id、expected revision、tenant 和 correlation/causation。
路由组相互独立且不导入 factory；应用查询不依赖 API 或具体基础设施。上述边界由五条新增合同约束。

## Runtime 执行组件

`runtime/execution_engine.py` 负责阶段推进、路由接入和 Model/Tool Round 调度。
`runtime/execution/` 中的组件复用同一 Guard、CanonicalEventCommitter 和 ProgressStore：

| 模块 | 所有权 |
|---|---|
| `recovery.py` | 待确认写结果只读补录、失败结算、未派发工具终结、审批/Child 等待恢复、检查点选择 |
| `budget.py` | Canonical 预算预留、累计用量校验、结束原因和单回合输出分配 |
| `context.py` | 可信说明拼装、Router 首轮计划、可见工具过滤与结束轮约束 |
| `messages.py` | Canonical 历史到模型消息的映射、输入摘要证据、请求身份与工具来源信息 |
| `serialization.py` | 原检查点 response/tool call 形状的编解码、finish reason 分类 |
| `contracts.py` | 故障注入点和 finish reason 枚举；原 engine/harness 出口保留 |

恢复组件的能力端口只允许查询 invocation 状态和释放 Run 资源；它不持有 Model/Tool Client。
协作端口只允许读取 Child 状态和 join 已完成的 Router DAG。Context 的端口只提供可信说明和
工具 Schema。三个组件相互独立，且整个 execution 包不得导入 engine、harness 或具体 controller/router；
两条新增 import-linter 合同在 CI 执行。业务事件仍经原 Session/Committer 边界写入。

## 变更检查

调整包边界或进程归属时，至少同步：

1. `pyproject.toml` 的 import-linter 合同；
2. 本页的包/进程映射；
3. 受影响的系统架构或 ADR；
4. [开发阶段校验清单](../development/stage-gates.md)。


Runtime v2 预算治理：`domain/runtime_budget.py` 为 Canonical 事实的纯预留/根树配额规则；
EventStore 实现在根事务内调用规则。`contracts/json_schema.py` 为 Runtime 与 Action 共享的
受限离线校验器。`model_gateway/pricing.py` 和 ModelStateStore 拥有可信价格及成本结算，
Runtime 仅传递 Run 预算，不读取凭据。`projection/task` 以事实重建公开用量。

## Agent 任务评测

`evaluation/` 定义 τ-bench/AppWorld 环境端口、固定工具 profile、消息适配、记录契约与
pass^k 汇总；不依赖 API、具体适配器或 composition。`composition/evaluation.py` 是独立、
显式调用的评测装配，复用真实 AgentHarness、内存参考 Control/Event Store 和 FencedSessionClient。
`benchmarks/run_agents.py` 管理官方 SDK 的可选加载、版本预检、Model Gateway 客户端、进程隔离
和本地产物，不被生产 builder 导入。生产 Runtime/Action/API/Session 不依赖评测工具，
两条 import-linter 合同约束上述方向。评测不新增生产依赖。

方案、成绩语义和实际运行限制见 [Agent 任务评测](../development/agent-evaluation.md)。

E2 的 `benchmarks/deploy/` 是显式选择的开发评测部署入口，不被生产包导入；
`run_service.py` 调用既有 composition 子进程生命周期，生产 CLI 的部署 profile 约束不变。
分布式 Collaboration 投影从 Session root-feed 获取 Canonical Events，SQL 只维护可重建视图；
单库参考拓扑保留本地事件读取。评测环境统一 SQL 账号不改变上述组件职责。
