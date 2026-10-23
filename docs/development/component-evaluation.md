# Memory、Skill 与 MCP 评测

跟踪 [issue #107](https://github.com/sushaofei/AuraClaw/issues/107)，与 #106 的 τ-bench/AppWorld 成绩独立。

## 当前交付边界

B1 提供可由现有 AgentHarness 执行的独立适配器、端口、脱敏输入及样本冻结 CLI。
已用自建 fixture 验证真实 Runtime / Canonical Session Events；这不是官方任务成绩。
尚未提供生产 Memory 服务、官方环境部署或三项基准的正式评测结果。
B2 需要在 composition 装配具体 Memory、Skill/Sandbox、MCP Gateway 和官方 grader；
不能把直接模型调用、上游自带 Agent 或简单提示注入称作 AuraClaw 模块成绩。

## 固定来源

| 基准 | 官方仓库 | 提交 | 源码许可 |
| --- | --- | --- | --- |
| LongMemEval | xiaowu0162/LongMemEval | 9e0b455f4ef0e2ab8f2e582289761153549043fc | MIT |
| SkillsBench | benchflow-ai/skillsbench | f860ecf02ef53412a030f8661c83d644b739ff01 | Apache-2.0 |
| MCP-Universe | SalesforceAIResearch/MCP-Universe | 0df6c42717f2a618d34f5b271efd05be462b9e4d | Apache-2.0 |

源码固定值由 `evaluation/components/catalog.py` 管理；三套 SDK 不进入生产依赖。
LongMemEval 使用官方 `xiaowu0162/longmemeval-cleaned` 数据集提交
`98d7416c24c778c2fee6e6f3006e7a073259d48f` 的 `longmemeval_s_cleaned.json`。
数据文件不入库，冻结计划记录字节 SHA-256，发布前另核对数据使用条款。
SkillsBench 取 registry **1.1**（87 任务），其任务包各自带 `git_commit_id`、path、digest；
不得将 registry 仓库提交误当作任务执行提交。BenchFlow 版本范围为 `>=0.6.3,<0.7`。
MCP 仅选择原始 `mcpuniverse/3d_design` 本地 Blender 任务，不替换成单独的 MCPMark。

## 预注册协议

选择只依赖任务 ID、题型等元数据，按 `SHA256(auraclaw-components-v1:20261022:<id>)` 排序，
Memory 按题型（拒答单列）轮询分层。12 题 × 3 条件 × 3 次 = 108 次；
SkillsBench 6 题 × 2 条件 × 3 次 = 36 次；MCP 6 题 × 3 次 = 18 次，共 162 次。

条件：Memory 为 no-memory / retrieval-memory / full-history；Skill 为 without-skill / with-skill；
MCP 为 gateway-tools。每个 question/task/trial/condition 必须新建隔离环境；不同条件同题同模型，
随机种子及执行顺序在 B2 后冻结。默认每次 30 动作、24576 输出 tokens、每轮 4096、600 秒；
SkillsBench 官方任务可能要求 900 秒，必须运行前对全部所选任务审阅预算并冻结修订，不能按成绩调参。
全历史参考需先验证模型输入窗口，不能静默截断历史或删掉题目。

候选计划不可作为环境已就绪证明。冻结后遇到缺包、镜像、凭据、oracle 或 grader 未就绪，
保留任务和分母，停止准入；修复同一任务环境后再启动，不按难度/成绩换题。

```bash
PYTHONPATH=src python benchmarks/plan_components.py \
  --benchmark longmemeval --upstream-root /private/upstreams/LongMemEval \
  --dataset /private/longmemeval_s_cleaned.json --output /private/memory-plan.json
PYTHONPATH=src python benchmarks/plan_components.py \
  --benchmark skillsbench --upstream-root /private/upstreams/SkillsBench \
  --output /private/skill-plan.json
PYTHONPATH=src python benchmarks/plan_components.py \
  --benchmark mcp-universe --upstream-root /private/upstreams/MCP-Universe \
  --output /private/mcp-plan.json
```

CLI 核对工作树固定提交且无 tracked 修改，只创建新的 0600 计划，不执行模型或上游程序。
旧 `run_agents.py` 明确拒绝新三项，防止落入原 AppWorld 分支；不能只改 benchmark 名称启动。

## 装配与评分要求

- `MemoryCase.from_official` 只投影 role/content/session id/date，删除 `answer_session_ids`、
  `has_answer` 和其他注释。MemoryPort 只接收净化历史；grader 回调持有 gold，Agent 看不到它。
  上游 `evaluate_qa.py:get_anscheck_prompt` 与固定 judge 应由评分进程使用；
  Qwen 可作为已授权 solver，但不自动冒充官方 `gpt-4o-2024-08-06` judge。
  若采用替代 judge，必须另行标注评分口径，不与官方数字直接比较。
- SkillsBench 官方 oracle 先验证环境，验证结果与任务包摘要绑定；两个条件环境一致，
  without-skill 的文件系统也不能残留 Skill。Agent 容器不能挂载 oracle/verifier；
  SkillPort 只列元数据、按需读取已准入技能，网关负责路径/符号链接与版本权限验证。
  官方 verifier 仅由外部评分侧在执行结束后运行。B1 的端口测试不代表生产安装/升级链路已验收。
- MCP 工具路由通过 Server/name 散列映射避免同名冲突。具体客户端和清理机制由 composition 装配；
  官方 Task.prepare/evaluate/reset 在 runner 侧，隐藏 evaluators 不进入 Agent 输入。
  所选本地任务需要隔离 Blender 4.4 和官方 Blender MCP；不接个人业务账号、不开放宿主目录。
- 每次模型和工具调用复用 Runtime 预算与规范事件，不另写 Agent 循环。同步旧环境继续同步调用；
  新端口支持 await，结束和失败时 await close。进程级超时仍由后续专用 runner 承担，
  不依赖阻塞线程取消。grader 异常为 evaluation_error / reward=null，不能算成功。

## 报告与 AuraX

正式发布前必须完成：题目/条件/重复覆盖、固定数据摘要、执行 profile、官方评分来源、
judge 版本、输入预算和异常分母核验。Memory 报告分题型准确率/检索与回答耗时；
Skill 报告配对成功率差和调用/token 成本；MCP 报告任务成功、工具失败与认知失败，
并保留未启动/准入拒绝与失败尝试，不以可评分子集替代完整分母。

AuraX 已开始增加三套基准名称、简介、官方链接及正确的子集说明；只有已有真实报告才出现卡片。
当前 schema v2 尚未承载条件对照、judge 和额外诊断，B4 必须补齐发布合同并验证，
不能将适配 fixture 强标为 official-sdk-adapter 后导入。既有 E2/E3 报告不改写。

## 验收

- B1：端口和 Runtime 联通、gold 隔离、Skill 延迟读取与路径拒绝、MCP 命名空间、
  样本固定 CLI、异步清理及现有评测回归。
- B2：具体网关/记忆/官方 sandbox 和 grader，逐项 canary；真实准入而非只判断工具存在。
- B3：冻结协议后执行全部受控样本，保留失败、原始产物和评分回执。
- B4：报告合同、界面配对展示、受限部署及实际浏览器核验后更新 issue。

### LongMemEval canary worker

`benchmarks/run_longmemeval.py` 提供单进程、单问题真实 canary：检查固定 cleaned S 摘要，
从固定上游代码只提取官方纯评分提示函数，经同一 Model Gateway 调用已授权 Qwen judge。
严格接收 yes/no；其他输出为评分异常。结果标为 reference-adapter / custom-judge-not-official-comparable，
不满足当前 AuraX official-sdk-adapter 发布合同，不自动发布。
检索条件使用独立词法参考 Memory（top-5），不是生产 Memory 服务；不依赖额外 embedding 模型。
输入字节上限仅用于准入拒绝，不是 token 计数，不静默截断；full-history 仍需额外模型窗口核验。
600 秒超时由 worker 和外部进程监督共同限制。canary 选用候选 cohort 之外的题目，不回填正式成绩。

### Blender 独立环境

`benchmarks/containers/blender/` 提供专用镜像及真实 MCP 环境探针。
固定 Blender 4.4.3、上游 addon/server 和 MCP SDK；镜像仅包含任务执行依赖。
评分器、oracle、模型凭据和宿主目录不进入容器。运行默认非 root、只读根、
无网络、无 capabilities，并限制 CPU/内存/进程数；工作区为每次新建的 tmpfs。
MCP initialize / list_tools / 创建与读取物体 / 保存场景通过只能证明环境准入，
仍须完成官方 Task/evaluator 与 AuraClaw Runtime canary 后才能开启正式批次。

SkillsBench 的固定 BenchFlow 0.6.3 要求 Python >=3.12；官方本地后端使用
Docker Engine 与 Docker Compose。Podman 能运行 Blender 不代表其已满足该后端。
正式执行前需运行官方结构检查、官方 task_digest 比对，再执行 oracle/verifier。

## B2 验收与首轮执行冻结（2026-10-23）

六项官方 oracle 全部通过：bike 20/20、shock 9/9、GDP 27/27、dialogue 6/6、
data-to-d3 15/15、Scala 10/10。固定任务 digest 不变；Scala 镜像仅替换依赖传输
（已校验 Coursier 归档、Maven/JVM 代理配置），不改任务或评分器。环境失败独立保留。
SkillsBench Runtime canary 完成，19/20 测试通过而任务 reward=0，验证评分链路，
不计正式成绩；bike-rebalance 标记为 development-exposed。

首轮仍为原计划 162 次，不换题、不按成绩重抽：LongMemEval 108、SkillsBench 36、MCP 18。
各基准串行执行、基准之间最多三个并行通道，每次间隔 3 秒；每个样本独立环境。
LongMemEval 600 秒/30 动作/24576 总输出/4096 单轮；SkillsBench 遵循任务官方
600–2700 秒/30 动作/49152 总输出/8192 单轮；MCP 600 秒/30 动作/32768 总输出/4096 单轮。
SkillsBench shell 每条限时 120 秒、输出 32000 字符；参考 Skill 文本每部分上限 256KB。
LongMemEval 输入超过 800000 字节时拒绝，不截断；已实测最长候选之外的全历史输入
117518 input tokens 成功进入模型。Qwen 仍为可变别名，不能视为不可变上下文配置保证。

LongMemEval 独立 judge 限流最多重试两次、间隔 45 秒，只评同一个已保存答案；不重跑 solver。
正式调度在发出任务前记录 started，完成后保留退出状态；崩溃未决任务拒绝自动重复，
子进程失败保留 infra_error/null reward，不能从分母删除。全部原始 trace/回答/评分日志
只留在私有评测目录，公开报告仅包含白名单汇总字段。无新生产依赖。

### 批次中止与 MCP 素材依赖修正（2026-10-23）

首轮在 91/162 次已有记录时因上游推理持续返回 HTTP 403
`AccessDenied.Unpurchased` 停止；/models 仍返回 200，不能将模型列表可见当作推理准入。
原始记录与未完成监督回执保留，`publication-hold.json` 禁止发布；没有选择性重试或删除失败。
调度器现对 ModelAuthenticationError 停止新派发，让已在运行的通道收尾，并阻止误恢复。
模型服务权限恢复后重新准入；任何替换模型须明确新模型及独立批次，不能混合汇总。

MCP 固定 task_0010 明确要求搜索和下载 PolyHaven 公共 HDRI/纹理，原先 network=none
不满足该任务环境要求。六题原样保留；原 18 次仅作为环境诊断，不作为有效基准成绩发布。
Agent 可显式选择经运维准入的 internal network + 无凭据 HTTP 代理，只有 PolyHaven
官方 API/下载域名允许访问；禁止直接外网及其他宿主端口。评分容器仍 network=none，
非 root、只读根、资源限制、隐藏 grader 隔离保持不变。修复后须全量重跑六题三轮，
不能只重跑失败题或把网络拒绝算作 Agent 任务失败。

素材联调另发现官方 MCP socket 的 15 秒等待短于 2K 多贴图下载；
`Containerfile.transport` 从已准入镜像构建，断言原始两处超时后仅调整为 120 秒，
记录原始/运行 server SHA-256。没有修改工具参数、题目或评分器，正式重跑需使用新镜像 ID。

恢复策略：旧 91 次记录作为明确停用的诊断批次保留，不改写记录和原始评分。
权限恢复并通过推理准入后，使用修正环境开启独立的 162 次完整批次，保持原任务、条件、
三轮和预算；不将两批的成功样本拼接。新批次仍须声明可变模型别名与开发子集限制。
