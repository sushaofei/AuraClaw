# Runtime 预算与重复调用治理改善方案

状态：A–F 已实现并通过联合测试发布验收，跟踪 #101。v2 仅对新 Run 启用；已有 Run 保持原快照。
范围：AuraClaw Runtime/Control/Model Gateway/Session 投影，以及 AuraX 运行进度与结束原因展示。
关联：#93 调用稳定性、#96 执行收尾与恢复、#100 可信身份。

## 1. 已确认的问题

测试会话 ses_1d451deff7e54a07b1945fbda24145c9 的 Run 实际配置为48步、8192输出token、
无 max_cost、无 deadline。失败前使用21步、1303输出token，根因是 inventory.arrival.list
相同参数第4次请求。前3次全部成功，且调用并非必须连续。

现有 execution_engine.py 以工具名和排序后的参数 JSON 计算签名，在执行前累计；不检查结果、
新旧数据、任务依赖变化或合理轮询意图。重复次数超限、步数/token/cost超限共用
runtime_budget_exceeded，导致误诊。当前 model.call 和工具执行各计一步；校验失败也计一步。
异常可能发生在 tool.call.requested 已提交后，需补齐该调用明确未执行的结算事实。

根本问题：资源预算、重复尝试、无进展循环和业务失败混用了同一退出机制。

## 2. 目标与边界

1. 按可观测结果判断重复调用是否合理，避免误杀成功查询、合法分页、批准后的继续执行和有界轮询。
2. 尽早阻止无法修复的重复错误；优先明确反馈和正常收尾，不以增加预算掩盖循环。
3. 硬预算始终有界；预算分配、剩余额度和停止原因可解释、可审计、可从checkpoint恢复。
4. 结束时保留成功结果和未完成事项；不会把失败或未知副作用声明为业务成功。
5. 不以模型自称“有进展”作为依据，不以相同参数代替业务幂等键，不自动重放未知写操作。
6. 不在本次方案里重写Agent框架或替换为LangChain；先修复明确的执行策略与契约。

## 3. 三套独立机制

### 3.1 资源预算 BudgetPolicy / BudgetUsage

第一阶段兼容原 max_steps=48、max_output_tokens=8192，不改变实际计费/默认额度。
细分展示 model_turns、tool_attempts、tool_dispatches、output_tokens、cost、deadline；
兼容 steps_used = model_turns + tool_attempts，并公开每项定义。

- model_turn：一个新的模型调用计一次；恢复同一个已完成model_call_id不重复计数。
- tool_attempt：模型提出且进入执行决策的一次调用计一次，包括本地校验失败、去重拦截。
  旧实现对执行前重复拦截未计数，迁移时以budget_policy_version区分，不改旧统计。
- tool_dispatch：实际进入工具执行器的一次调用（包括发现/加载类工具）；同一次在途请求恢复不重复dispatch或计数。
- 累计输出token包含本Run所有模型轮次，包括最终总结和工具调用参数；输入token独立记录。
- Model Gateway 支持可信价格快照、Decimal 金额预留/结算。未配置价格或缺少最终输入/输出用量时 fail closed；未知消费保留预留，不当零。价格货币与 Run 限额必须一致。
- deadline、用户取消、lease失效、预算耗尽分别处理；等待审批不消耗模型/工具次数，墙钟deadline继续有效。
- 每次调用前检查/预留，完成后结算。并行工具批次逐个预留，不允许先并发后发现超额。
- 新Run可获新预算，同Run重启/审批恢复/副本迁移沿用用量；用户补发消息不能抹掉旧未知副作用。
- 子任务有自己的Run预算，且消耗根任务树批准的总配额；发起子任务必须先分配配额，
  不允许靠无限子Run绕过总量。成本全局口径与Model Gateway现有租户限制对齐，避免重复扣款。

配置通过可信入口创建的Run budget快照保存；模型不能修改额度、取消deadline或提升配额。
管理员可调整后续Run默认值。当前Run追加预算必须走带actor/expected_version/command_id的显式命令，
记录变更事件；已终止Run不能通过修改数据库伪装恢复。提供平台上限，用户请求不能超出授权上限。

### 3.2 重复调用策略 RepeatPolicy

识别键不是简单函数别名。至少绑定tenant、可信user/dept摘要、Run、server/owner、
capability identity、目标/配置revision、Schema digest、canonical arguments digest。
只对对象键排序，不把字符串数字强转为数字、不注入defaults、不更改数组顺序。
调用幂等键独立保留：相同参数的两次写入可能是不同业务意图，不能自动合并。

执行决策矩阵：

| 情况 | 决策 | 给模型的信息 |
| --- | --- | --- |
| 首次合法调用 | 通过Gateway正常授权和执行 | 正常结果 |
| 同一invocation仍在途 | 查询已有执行状态，不生成新业务调用 | 在途状态及原invocation引用 |
| 成功的只读调用被重复请求 | 先检查新鲜度/依赖变化/刷新意图，优先提醒使用本Run已取得的结果 | 原结果引用、取得时间、数据版本；明确本次未执行 |
| 数据源变化或有明确刷新需求 | 在额度、频率、权限允许时执行 | 新调用结果；记录刷新依据 |
| 参数校验失败、未开始执行 | 返回Schema纠错路径；允许生成有实质修正的参数 | 字段/类型、正确入口，不猜业务值 |
| 不可重试的下游错误 | 阻止同一错误条件下无意义重试，给解释并切换其他已授权工作/收尾 | 错误来源、是否可通过参数修正、未完成项 |
| 可重试瞬时失败 | 仅在工具契约、幂等与结果确定性允许时有界重试 | retry-after/次数/执行状态 |
| unknown副作用 | 进入已有reconciliation流程，禁止自动重放 | 原invocation状态，需查询或人工处理 |
| Tool误走Resource/Skill入口 | 不自动路由执行，返回正确入口；计入无效尝试 | 已加载精确工具名 |

成功只读结果复用限制：

- 默认不是跨Run缓存。先给“已有结果”的控制反馈，而不是伪造一次新的工具执行成功。
- 仅在声明了可复用/稳定快照及有效freshness范围时允许由框架透明复用结果。
  动态库存等未声明时效的读取不能凭固定TTL默认视为仍然新鲜。
- 复用前仍检查最新撤销、租户/用户/部门、权限、发布/安装状态；不因缓存绕过Gateway。
- 结果通过受控artifact/Canonical事件引用读取，引用本身不授予访问权；记录来源调用、时间和数据版本。
- 执行路径中发生影响该数据的写入，或观察到catalog/data revision变化，必须失效。
- 模型提供的“我要刷新”仅是请求，不能成为突破限流/授权的证明；可由用户明确要求刷新、
  已授权轮询任务或可观测依赖变化支撑。无法确认时先解释已有数据时间，再请用户明确需求。
- 合法分页游标变化、不同查询参数、已批准的有界轮询不是机械重复；轮询使用后端调度，
  有最大次数、最小间隔、截止时间，不在AuraX加计时器。

### 3.3 无进展策略 ProgressPolicy

以已结算的执行事实构造有界进展记录，不让模型评价自己：

- 新的分页游标/数据版本、满足新的依赖、有效批准、子任务状态变化可作为进展。
- 相同数据的重新输出、trace/requestId变化、时间戳噪声、模型改写解释不算进展。
- 错误类型、Schema路径与错误码形成错误族；不解析自由文本来猜业务意图。
- 同一已加载目标的“缺input”，只换limit=3到5仍是同类未修复错误；改成合法嵌套输入属于纠正。
- 不能只用短窗口：同时记录按目标的Run累计无效尝试，避免A/B工具交替绕过检测。
- 不同工具独立失败不能一概认定为循环；允许任务在已授权范围内完成一次有意义的排查。

建议首版默认策略（均为待灰度验证的新策略，不是现有配置）：

- 第一次重复成功查询：给已有结果引用/刷新条件，不直接终止Run。
- 同一目标同类未修复本地错误：原始失败后最多2次纠正机会；耗尽后阻断该目标当前错误条件。
- 同一目标相同不可重试错误：不允许自动再次发出相同业务请求；避免仅改无关参数绕过。
- 瞬时失败默认最多2次重试、尊重retry-after；副作用未知时即使retryable=true也不自动重放。
- 8次最近已结算工具尝试中至少4次被判定为同一无效循环，且无可信进展：进入收尾模式。
  与每目标累计计数联合使用。此阈值只适用于可判定循环，不替代总步数上限。
- 不提供一键“无限制”。用户可发起新的明确请求或经授权追加资源预算，但不能解除未知副作用约束。

## 4. 停止探索与收尾

增加Runtime内部阶段：executing → concluding → terminal；它不直接修改业务Session状态。
软阈值、可判定无进展或重复保护先阻断相关新工具动作，保留现有结果；模型仍可处理其他目标，
确认没有可继续的合法工作后进入一次有界总结。

- root/coordinator同样预留收尾预算，统一已有worker terminal reserve，避免重复预留。
- 建议总48步中预留2步，8192输出token中预留1024，均从原总额中扣除而非额外赠送。
  小预算按比例且必须容纳实际收尾操作；配置校验不能出现“预留耗尽全部探索额度”。
- 收尾阶段不再暴露业务工具。只有具备明确失败语义的 collaboration 终态才可进入白名单。当前 publish_result 会生成 Child 完成事实，不能用于强制失败收尾；首版所有角色由 Runtime 写 run.failed 并保留 partial 输出，不调用成功发布工具。正常完成时原角色输出协议不变。
- 总结包含：已完成、失败原因、未完成项、是否存在unknown副作用、是否需要用户补充信息。
- 如果已触及绝对硬额度、成本未知无法预留或模型不可用，采用Canonical事实生成固定模板收尾，
  不再发额外模型请求。不得为了总结超过硬token/cost/deadline上限。
- 原始用户取消/失效lease不允许恢复代码开始新执行；固定摘要由合法业务事件路径/投影生成。

业务终态首版不新增枚举：任务未完成仍run.failed，但携带partial result引用和具体停止原因；
仅确认目标完成时run.completed。查询成功不等于用户目标全部完成，生成总结不等于执行成功。
已有成功步骤和产物仍可见，失败不会清空结果。未知写操作始终保留unknown事实。

## 5. 错误契约、事件与恢复

新增明确分类，兼容旧runtime_budget_exceeded显示：

- runtime_step_budget_exceeded
- runtime_output_token_budget_exceeded
- runtime_cost_budget_exceeded
- runtime_deadline_exceeded（与用户主动cancel区分，终态映射需契约冻结）
- runtime_no_progress_detected
- tool_repeat_suppressed（一次调用的控制结果，不直接等于Run失败）
- tool_retry_exhausted / tool_execution_unknown

结构化details包括：category、reason_code、policy_version、scope、limit、used、remaining、
tool/capability、source_invocation_id、decision、retryability、side_effect_status、partial_result_ref。
不要把高基数ID放指标label，不输出密钥、完整业务参数或原始错误栈。

Canonical Session Events仍是唯一任务事实来源：

1. 工具attempt/requested、预算预留、控制决策、执行结算和Run终态必须可关联且幂等。
2. 已提交requested但随后被本地策略拒绝：产生对应settled/completed事实，明确not_started；
   不能留下“似乎仍在执行”的悬空调用。严格ToolResult枚举若无reused/suppressed，优先以
   现有denied及结构化reason表示；不得未经DTO演进直接塞入新status。
3. 同一invocation的checkpoint恢复不重复累加、重复扣款或重新执行；事件写入失败按原幂等ID补交。
4. 在checkpoint保存前崩溃，可从Canonical执行事实和已有Model/Hands执行账本重建计数；
   不能依赖Runtime内存次数，也不能把日志当结算依据。
5. 新增预算和进展投影必须可重建；跨服务费用reservation留在Model Gateway账本，
   Session事件保存引用和已确认消费，不创建相互竞争的第二套账本。
6. 同一可信调用上下文/版本绑定在审批、恢复、结果复用中保持一致；未知或旧身份缺失明确拒绝复用。

旧Session历史不改写。旧generic错误最多标注“旧版本未区分类型”；只有存在原始错误证据时，
详情页才可显示辅助解释，不能伪造新Canonical失败事件。

## 6. AuraX 交互

- 运行详情显示“步骤21/48、输出1303/8192”，区分已执行工具次数和被阻止的尝试。
- 重复保护显示“已阻止重复查询，已有3次成功结果”，而不是“预算耗尽”。
- 每个工具状态分清：执行成功、参数错误、下游错误、复用已有结果、未执行、结果未知。
- Run结束时保留部分结果，列出未完成工具和可采取行动；不把run.failed改名成成功。
- 支持“查看已有结果”“查看停止原因”；“重新查询/继续任务”通过明确新命令处理，
  不在前端自动重发旧工具请求，不向未知写操作提供无条件重试按钮。
- 预算展示只读为第一阶段；后续按权限开放新Run额度选项及显式预算追加，并展示上限和来源。
- SDK/API先提供稳定结构，UI不解析错误自然语言、也不自行推断预算或计费。

## 7. 实施阶段与优先级

| 阶段 | 优先级 | 工作 | 完成条件 |
| --- | --- | --- | --- |
| A | P1 | 分离错误码与详情；补齐本地阻断结算；UI准确显示 | 原故障不再被误报为48步/8192token耗尽 |
| B | P1 | 替换硬编码repeat>3；结果分类、目标级有界记录、纠正策略 | 成功重复读取先明确反馈；未知写不重放；非法循环可终止 |
| C | P1 | root/worker统一预留、concluding阶段、固定模板兜底 | 软停止有总结，硬停止不超额，已有结果不丢失 |
| D | P1 | 幂等计数、checkpoint重建、并发预留、混合版本门禁 | kill/restart及副本切换无重复执行/计费/预算重置 |
| E | P2 | 合法刷新/轮询契约、可复用结果新鲜度、任务树配额 | 动态读不过度缓存，子Run不能绕过授权总量 |
| F | P2 | 受控配置入口、指标灰度与阈值校准 | 正常任务误拦截和循环消耗都有实测对照 |

建议A–D作为第一批联合上线，E中的结果透明复用默认关闭，F逐步开放配置。
D虽单独列出，不允许B/C先绕过恢复/幂等门禁上线。各阶段均先列stage-gates，
功能/架构/测试/安全/文档/迁移通过后形成独立提交并推送，再更新对应issue状态。

主要代码落点：contracts/errors及事件/结果DTO、control/ports与Runnable预算快照、
runtime/execution_engine/execution_guard及独立budget/progress policy模块、
Model Gateway reservation复用、Session投影/API、AuraX SDK/工具轨迹/运行详情。
策略模块返回纯决策，网络/凭据/业务授权仍经过Gateway，避免把更多逻辑堆进execution_engine。

## 8. 数据迁移、发布与回滚

- 先冻结DTO/error_details/事件契约，再实现后端读取兼容、存储、Runtime写入，最后SDK/UI。
- budget_policy_version、progress_policy_version写入Run/assignment/checkpoint；同一Run固定策略，
  恢复不因进程升级改变阈值或计数语义。新策略不得随意接管未知执行状态的旧Run。
- 如新增列/索引，使用新的增量migration，不改已执行0063及此前迁移；确定具体字段后再编号。
- 旧checkpoint有call_signatures但无结果分类：结合本Run Canonical事实重建，缺证据不猜结果。
- 灰度按明确测试Run启用，不能按副本随机启用而造成迁移后策略漂移。
- 先部署Reader/Projection兼容，再切新Run策略；不支持新checkpoint的旧镜像禁止接管该Run。
- 回滚停止向新Run分配v2，已有v2 Run由兼容版本排空/正常终止；不能清预算或删除checkpoint。

## 9. 验收矩阵

- 原故障fixture：同名同参成功3次后第4次请求，不报资源预算耗尽，不丢成功结果，明确复用/刷新决策。
- 同一成功结果合理刷新：依赖写入、数据版本变化、时效到期、用户明确刷新均按契约处理。
- 分页与有界轮询：不同cursor继续，同cursor无变化受控等待，达到poll预算有明确结束原因。
- 输入错误：缺input/字符串数字提示可纠正，合法修正可执行；只换无关字段不重置同类错误计数。
- Java输出Schema错误、授权拒绝、瞬时429/503、业务错误分别处理，retryable不覆盖副作用未知保护。
- 连续及交替A/B循环、空结果、时间戳噪声、反复search/load均不无限扩张；不同工具正常排查不过度阻断。
- 同参数写操作不同业务意图不合并；同invocation恢复只查既有结果；unknown、撤销及跨身份不能复用。
- 48步和8192token边界、一步多工具、超额provider返回、缺失usage、并行子任务配额均有测试。
- 在reservation、dispatch、result收到、事件提交、checkpoint保存各边界kill/restart；重复消费不重复结算。
- 正常总结、模型截断、硬限额、deadline、cancel、lease失效，部分结果/工具终态仍准确。
- AuraX显示精确停止原因、当前用量与已有结果；旧服务/旧错误向后兼容。
- 不重放原测试Session的历史业务调用；使用隔离fixture和新的明确只读测试Session。

指标：首调参数合法率、纠正成功率、相同远端失败重复dispatch次数、每完成任务模型/工具次数、
各类预算/循环退出、部分结果保留率、正常分页/刷新误阻断、unknown自动重放次数（必须为0）。
上线前以固定场景集和相同模型/配置对照，明确样本量；不能用单次成功声称稳定性已保证。

## 10. 此次故障按新方案的预期行为

前面的Java输出错误明确归类并停止无意义参数试探；arrival首次输入错误得到纠正提示，
合法查询成功后，重复请求收到已有结果引用与时间。如果没有新的刷新条件，阻断重复dispatch，
继续其他尚未测试的工具或收尾。即使最终无法完成全部测试，也保留成功结果，准确说明
Java输出契约问题/无效循环；不再让用户误以为21/48步、1303/8192token已经耗尽。

## 11. 实施记录（#101）

### A：错误契约与执行前结算

- 新增 step/output token/cost/deadline/no-progress 精确错误类；保留原 generic 类型供旧事件和未迁移路径使用。
- deadline 走失败语义，区别用户取消；停止后未知 Skill 写入仍仅查询原 invocation 进行 reconciliation。
- 现有重复策略与步数执行前拒绝先提交同 invocation 的 tool.call.completed，status=denied、side_effect_status=not_started。恢复重试幂等补交，不重新调用业务工具。
- run.failed 附加 error_details：当前 v1 策略、预算快照、checkpoint 用量、成功调用引用；未知 token/cost 使用 null，不猜成零。
- Task 投影将 details 原样投影进 error；AuraX 按错误码展示原因并按结构化数字展示用量，旧 generic 显示未区分类型。
- 无新增业务终态或 DDL。读取兼容先发布，重复策略仍 v1；阶段 A 单独通过不代表 B–F 完成。

### B/C 与 D 基线：v2 受控实现

- 可信 TaskService 配置 `AURACLAW_RUNTIME_BUDGET_POLICY_VERSION`（默认 `1`，可选 `2`）写入每个新 Run 的 budget 快照；公开请求和模型不能改此配置。旧事件缺字段按 v1 解释，恢复不改历史预算。
- `repeat_policy.py` 从当前 Run 的 requested/completed 事实计算决策，身份/完整加载绑定/参数分别做摘要；没有跨 Run 缓存，不把模型的 expected_side_effect 当权限依据。
- 成功只读同参请求返回 denied/not_started 与源 invocation/时间；不复制原数据、不宣称刚完成新查询。成功写入不按参数合并，未知效果即使 retryable 也阻止新 invocation 重放。
- 必填包装/基础类型的明显错误最多三次尝试，变更无关参数不清零；合法修正仍交 Gateway 做完整 Schema 验证。策略层不解析远程 ref、regex 或注入默认值。复杂 Schema 错误族、可信 retry-after 与依赖变化策略仍待补齐。
- 最近八次结算中至少四次抑制且没有新的成功事实时进入 concluding；search/load 同参也受控。默认从原总额预留最多两步和1024输出 token，小预算按比例缩小。
- 收尾屏蔽全部工具，模型绕过时仍返回未执行；部分输出写 model.output.completed(partial=true)，随后 run.failed。硬边界/模型故障在失败详情给固定事实摘要，不额外调用模型。
- 先保存模型 usage/响应，再记录模型事实并检查额度；超额 provider 结果不会进入工具执行。v2 缺失 usage 时停止，不能视为零。
- 修复最后一步 checkpoint 恢复被 while 条件跳过的问题；完成原结果结算后再拒绝新执行。批次因本地限额停止时逐个结算剩余未执行调用。
- Control/数据库/远程 DTO 保存 policy_version；Runtime 注册 budget_policy_v2 能力；PostgreSQL 与内存选择/领取双门禁阻止旧 Runtime 接管 v2。run.started 保存 budget_snapshot，恢复发现预算变化拒绝继续。
- 无 DDL。已完成34项针对性回归、全量 unit 回归、一次性 PostgreSQL Control 并发/租约/版本门禁集成、Ruff/Mypy/架构检查。发布开关仍为1，不宣称已完成真实模型灰度或完整任务树限额。

### 后续必须完成的门禁（不随基线提交关闭 #101）

1. D：从 Canonical/Model/Hands 账本完整重建细分用量，显式预留与所有崩溃点矩阵；成本金额账本及可信价格接入。当前只验证既有 checkpoint 与 Gateway 幂等恢复、最后一步边界和版本门禁，不能替代完整验收。
2. B/E：结构化错误族、retry-after、有权威依据的刷新/依赖失效、后端有界轮询与任务树总配额。透明结果复用仍关闭。
3. F：运行中只读预算投影、低基数指标、固定模型场景对照与误拦截校准。当前 AuraX 仅失败时展示结构化用量。
4. Reader/Control/Runtime 协调发布并逐 Run 灰度后才切 v2 默认；不清理 checkpoint 或重放测试 Session 的旧业务调用。


### D/E/F：完整治理实现与验收（2026-09-04）

- Canonical `runtime.budget.reserved` 以 model_call_id / tool_invocation_id 幂等预留；
  `runtime.progress.recorded` 保存可重建执行游标。Control checkpoint 丢失时恢复最新事实收据。
  未获得最终用量的模型预留保留；原 Model/Hands invocation 账本负责在途/未知结果，禁止新 ID 重放。
- Session EventStore 在根任务事务锁内完成额度检查和事实追加；子任务创建及再次申请 Run
  都分配根 Run 的有限总额，不能伪造 scope 或通过并发 Child 绕过。配额为保守整轮分配，
  已完成子 Run 的分配不返还，避免新增 Run 无限消耗。
- Model Gateway 使用可信 `AURACLAW_MODEL_PRICING` JSON：provider/model/currency/version、
  max_input_tokens、input_per_million、output_per_million；金额以该货币计。
  按最大输入窗口和最大输出保守预留，最终按已确认输入/输出 token 结算。
  精确 Decimal 存储；未知/取消未确认消费保留金额和 token，跨小时不释放；
  明确未发送的 Policy 拒绝释放。未配置价格时只有设置 max_cost 的 Run 会被拒绝。
  真实服务费率必须由运营提供，fixture 的 TEST 费率不发布到测试或生产。
- 复杂参数错误复用受限离线 JSON Schema 校验器（移至 contracts，无网络引用或强制转换）；
  错误族绑定结构化路径/keyword。目标级不可重试输出/身份/授权错误不因无关参数变化重试。
- `POST /v1/tasks`、`POST /v1/sessions/{id}/runs` 可带 `read_refresh`：
  `[{"capability_id":"已加载工具的精确ID","max_calls":2,"min_interval_seconds":5,"duration_seconds":60}]`。
  最多8个不同目标，每目标2–10次、间隔1–300秒、授权期限1–1800秒；固定绝对到期时间，
  同一命令重试不延长。只适用可信只读工具且仍走 Gateway；模型不能自行授予。
  等待由后端执行且持续检查取消/lease/deadline；Retry-After 超过300秒直接抑制，不提前重试。
  成功写入使之前的读失效；catalog/配置版本变化产生新绑定。透明缓存复用保持关闭。
- 可信默认配置：runtime_max_steps / runtime_max_output_tokens / runtime_max_cost；
  runtime_tree_max_steps / runtime_tree_max_output_tokens / runtime_tree_max_cost。
  默认48步/8192 token，根树480步/81920 token；金额默认不限制。
  额度只在新 Run 创建时快照，当前 Run 不支持直接改配置追加额度，用户可明确发起新 Run。
- task_view.runtime_budget 投影提供 limits/usage，去除内部计数索引，不复制业务输出。
  AuraX 区分模型轮次、工具尝试、实际外部调用、已用输出及在途预留。
- 低基数指标：runtime.budget.{model,tool}.reserved.count、runtime.repeat.suppressed.count、
  runtime.tool.dispatched.count、runtime.stop.<固定错误码>.count；ID仅作为追踪上下文。
- 调度/领取同时要求 runtime_governance_v2，避免旧基线 Runtime 接管新收据。
  迁移0064新增金额账本/调用价格快照/用量投影；先部署所有 reader，再将测试新 Run 切为v2。
  回滚先把新 Run 开关改回1，保持兼容镜像处理存量v2；存在未决成本预留时禁止 down migration。

固定场景验证（脚本化模型输入，不冒充真实 LLM 准确率评测）：

| 场景 | 原策略 | v2 断言 |
| --- | --- | --- |
| 同参成功只读4次 | 3次外部调用后中止 | 1次外部调用、3次明确抑制，可正常结束 |
| 长期相同只读循环 | 第4次触发混淆错误 | 有界收尾、partial保留、无新工具 |
| 分页/参数合法修正 | 未区分条件 | 新参数/合法纠正可执行 |
| 同参写入与unknown | 参数计数 | 成功写不合并，unknown不重放 |
| 2个并发Child仅剩1份配额 | 无根树限制 | 仅1个创建成功，另1个原子拒绝 |
| 2个并发priced model仅够1份 | 仅token限额 | 仅1个金额预留成功；重复结算仅扣1次 |
| 最后一步结果收据后Control丢失 | 依赖checkpoint | Canonical恢复，外部调用仍仅1次 |

测试入口：test_runtime_budget_ledger、test_runtime_repeat_policy、test_m11_capability_agent_loop、
PostgreSQL test_runtime_cost_ledger；同时复跑原 Model 多副本和 Control 门禁集成。
所有业务复现使用 fixture 或新只读 Session，不重放历史测试 Run。


### 联合发布验收结果

2026-09-04 已发布：数据库0064；基础服务 `budget-d2df425`，Hands/Runtime
`budget-0e74e1f`，Projection Worker `budget-befc14e`；AuraX `9a6500f`，
Web资源 `index-eiDtxA0F.js`。测试新Run policy_version=2；所有相关副本健康，
后端与AuraX上游ready均200。最终默认镜像固定为 `budget-befc14e`。

- `ses_167f235ec7794a7f916c981002efaff2`：两次相同发现请求，首次成功、第二次
  `tool_repeat_suppressed / existing_read_result`；源调用引用存在；3轮模型、2次工具尝试、
  1次工具执行，5/48步、602/8192输出token、预留0，正常结束。
- `ses_37063c8000f44137a74734611375bdd5`：Java MCP返回错误/未知后的重试被抑制，
  11/48步、1078/8192 token、预留0；未误报预算耗尽。此记录不代表Java业务查询成功。
- `ses_5e86a0448ef448079e4c0bcd6077f100`：对指定PLANNED参数先检查当前Schema后停止，
  未强行执行不符合当前契约的业务查询；5步、625输出token、预留0。
- `ses_2b1e3ae540d7494f925e4bf622e84084`：1次模型完成，分布式worker自动落库
  1条 `runtime.budget.model.reserved.count`。发布验收发现单体/分布式观测接线差异后已补齐，
  不以手工补写指标替代自动链路验收。

全量unit通过（1 skipped）；最后工作流依赖与指标接线补丁分别59/37项回归通过。
PostgreSQL9项集成、Ruff/Mypy259文件/10条架构合同、AuraX55项SDK测试、类型检查、
2项Playwright及截图检查通过。前端5项既有Hook警告保留。
真实费率未配置且max_cost默认空；金额预留/未知消费使用隔离TEST费率验证，未虚构真实价格。
