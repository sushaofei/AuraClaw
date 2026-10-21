# AuraClaw Agent 任务评测（Issue #106）

跟踪：[Issue #106](https://github.com/sushaofei/AuraClaw/issues/106)。

## 交付范围

E1 提供实际 AuraClaw Runtime 驱动的环境适配器、运行命令、事件证据与汇总协议。
自动化验证使用自建协议夹具，**没有运行官方数据集或付费模型，不能报告 Benchmark 成绩**。
E2 完成上游依赖/数据/服务镜像核验及小规模 dev 联调；E3 建立相同条件的多次运行基线与按需 CI。
E1 不新增生产依赖、API、数据库迁移或生产 worker 行为。

| 基准 | 固定源码 revision | 接口 |
|---|---|---|
| [τ-bench](https://github.com/sierra-research/tau-bench/tree/59a200c6d575d595120f1cb70fea53cef0632f6b) | `59a200c6d575d595120f1cb70fea53cef0632f6b` | `get_env/reset/step`、`Action`、环境返回的 reward |
| [AppWorld](https://github.com/StonyBrookNLP/appworld/tree/42b5bcf3cd334fee33f0c37c02070a9f5807add5) | `42b5bcf3cd334fee33f0c37c02070a9f5807add5` | `AppWorld/execute/task_completed/evaluate` |

使用用户指定的原始 τ-bench，支持 retail/airline；不把 τ²-bench 的结果混入。
源码版本核验不等于数据或环境镜像核验，尤其 AppWorld 含安装时解包的数据/代码；
E1 将 `dataset_revision` 视为声明，并在结果标注 `dataset_revision_verified=false`。

## 可比较的执行协议

Profile 为 `auraclaw-fixed-tools-single-action-v1`：

1. 每个 task/trial 建立独立进程、Session、Run、内存 Event Store、Control 租约与检查点。
2. 使用现有 `AgentHarness`、`RuntimeExecutionEngine`、Guard、Committer 和 Model/Tool 回合，
   不另写任务求解循环。此 profile 评测 Runtime/固定工具执行，不覆盖生产调度、持久恢复或动态 Skill 发现。
3. 官方工具作为固定能力预准入；控制器沿用实际加载、名称/版本匹配与派发逻辑。
   模型仅看到官方工具，不额外看到 search/load/Skill 工具。适配器在派发前验证工具参数。
4. 每轮只采用首个 Tool Call，与固定 τ-bench 工具调用基线一致；AppWorld 使用同样串行 profile。
   因此模型的并行工具能力不在本轮测量范围，不能直接与其他 Harness 的不同工具协议混比。
5. τ-bench 的自由文本映射成隐藏的 `respond` 环境动作，下一次观察还原为 user 消息；
   工具观察还原为官方原始文本。`EnvInfo.task/actions/reward_info` 不进入消息或工具结果。
6. AppWorld 只暴露公开任务说明、Supervisor 信息、App 描述和一个 `appworld_execute(code)` 工具。
   代码交给官方隔离环境执行，状态跨调用保留。适配器不预置登录/API 解题脚本。
7. 环境终止后用本地控制回合结束 Runtime，不再付费调用模型、不再派发环境动作。
   该控制回合保留在事件轨迹，`model_calls` 只计实际委派调用。
8. 评分在求解后发生，结果只给评测器。AppWorld 使用官方 TestTracker，保留默认断言收集，
   不把正常失败断言当作评分基础设施故障；拒绝零测试的空评分结果。

Runtime 使用明确记录的 budget policy v1：`max_steps=2*max_actions+2`，覆盖模型和工具回合及
终止控制回合；另设总输出 Token、单轮输出和动作数上限。
合成终止回合仍遵循引擎的正剩余 Token 前置检查：刚好耗尽输出预算时，即使环境完成，
Runtime 仍可能为 budget_exhausted；官方 reward 单独保留，E2 应核对该边界与对照组口径。`max_actions` 对 τ-bench 包含 respond。
CLI 的独立子进程 timeout 才是硬时限，包含导入/初始化/模型/动作/评分；直接调用 async helper
只具备 Runtime 边界的 deadline 检查，不能中断同步官方 SDK。

固定模型版本、任务 split/id、域、重复次数、seed、预算和 Harness commit。官方用户模拟器另记录
model/provider；每次 trial 的 Python/random seed 为 `seed+trial`。Gateway 管理的温度、采样参数和
Provider 版本未由 E1 客户端单独验证，**E2 必须保存对应 Gateway 配置及收据，再宣称条件固定**。

## 先运行无外部依赖冒烟

```bash
PYTHONPATH=src .venv/bin/python benchmarks/run_agents.py smoke --benchmark tau-bench --output .data/agent-evaluation
PYTHONPATH=src .venv/bin/python benchmarks/run_agents.py smoke --benchmark appworld --output .data/agent-evaluation
```

每条命令用自建任务夹具做两次独立尝试；输出 `validation_scope=fixture`，不加载官方 SDK、数据或模型。
CI 通过单元测试执行同样的进程级冒烟，不引入付费调用或公开官方题目。

## 准备官方 dev 联调（E2）

两个上游分别使用独立 Python 3.11 环境，避免上游依赖约束污染生产 `uv.lock`。从上表固定 commit
检出上游，按其 README 安装依赖、解包与数据；在对应环境安装当前 AuraClaw 源码。不要把第三方
数据、答案、解包后的受保护源码或 Provider 凭据提交到本仓库。

τ-bench 的 user simulator 由官方 SDK 调用，按上游配置其 Provider 凭据；被测 Agent 的模型经
AuraClaw `RemoteModelClient` 调用 Model Gateway。CLI 仅读取内部 Gateway bearer 环境变量
`AURACLAW_EVAL_GATEWAY_TOKEN`，模型 Runtime 不读取 Provider Secret。

AppWorld 必须使用专用于本次串行评测的隔离 Environment Server；不要共享已有任务服务器。
例如在已安装上游的独立环境中使用官方 `appworld serve environment --docker`，并为服务提供
数据与输出专用目录。确认服务端源码、数据版本、镜像 digest 与上表一致。默认的语法/执行安全
检查保持开启，客户端不在 AuraClaw 宿主机执行模型代码。

复制相应配置模板并填写实际值：

- `benchmarks/configs/tau-bench.dev.example.json`
- `benchmarks/configs/appworld.dev.example.json`

模板的地址只是示例，不代表当前机器存在这些服务。`harness_revision` 填 `git rev-parse HEAD`；
`dataset_revision` 填完整数据清单摘要；模型与用户模型使用固定版本。AppWorld task id 必须从官方
`dev` split 获取，不从 test 挑选样本做调优。τ-bench task id 为规范的非负整数索引。

```bash
# 凭据预先在本地安全环境配置，勿写入 manifest、命令行或聊天。
PYTHONPATH=src /path/to/eval-env/bin/python benchmarks/run_agents.py run \
  --manifest /path/to/local-dev-manifest.json --output .data/agent-evaluation
```

CLI 验证 Harness HEAD、上游 HEAD 和 tracked 文件未修改、SDK import 路径属于指定 checkout，
再启动每个尝试的独立子进程。禁止在 manifest URL 中嵌入认证信息、查询参数或片段。
正式 dev 预检尚未运行：需要实际 Gateway/模型、τ 用户模拟器以及隔离 AppWorld 服务和数据。
E1 未安装或验证这些外部依赖的兼容性，不应将此命令的存在等同于 E2 联调完成。

## 结果与分母

每次运行创建 UUID 目录，保存配置、每次尝试摘要、Canonical trace 与 summary。新文件权限为
0600，新运行目录为 0700；拒绝覆盖现有文件。产物可能包含模拟账户口令和任务私有内容，不能
作为普通 CI 日志或公开附件上传。trace 保存在本地指定目录，不自动写入 GitHub。

- `official_reward` 为适配器收到的官方环境/评分器结果；fixture 同名字段来自自建评分桩，
  必须同时检查 validation_scope。运行成功且环境终止、reward=1 才计 `success=true`。
- `completed`、`incomplete`、`budget_exhausted` 与 `infra_error/runtime_error/evaluation_error/timeout`
  分开记录。错误的 reward 为 null，不记成官方 0 或 1；总成功率仍以所有请求尝试为分母。
- `pass_power_k` 使用 `C(c,k)/C(n,k)` 的跨任务平均，表示 k 次都成功的估计，**不是 pass@k**。
  trial 不齐时不计算；重复身份、不同配置结果不混合汇总。
- E1 的 `cost` scope 为 `agent_model_only`，任一实际模型响应缺 cost 则为 null；
  用户模拟器、环境与基础设施费用未核验，`total_cost` 保持 null。缺输出用量时在工具派发前停止，
  不以 0 Token 继续运行。
- `model_calls`、`tool_calls`、usage、耗时和 input digest 在每次摘要中保存；
  本地统计不冒充 AppWorld 的官方 TGC/SGC 汇总。E2 应同时保存官方 `appworld evaluate` 报告。
- CLI 完成后写完整报告；有基础设施、Runtime、评分错误或硬超时则退出码 2，正常任务失败不导致
  CLI 自身失败。硬超时直接终止 worker，禁止在同一环境上自动重试未知副作用。

## E1 验证记录（2026-10-21）

17 项专项测试通过，包含两个环境各两次独立进程夹具冒烟。
完整 Pytest 978 passed、74 skipped；Ruff、Mypy strict（325 源码文件加 CLI）、
23 条架构合同和仓库 release gate 通过。跳过项不作为外部服务或生产验收。
该证据验证适配与工程行为，不是两个官方 Benchmark 的任务成绩。

## 验收与后续

E1：离线协议/Runtime/进程冒烟、质量门禁与文档；无正式分数。
E2：分别安装固定依赖，核验数据清单和服务器镜像；以 1–3 个 dev 任务检查工具、用户回合、
终态和官方评分，补充 SDK 真正的集成测试与 Gateway 配置证据。
E3：固定 dev 协议后，在保留集上做重复评测；报告任务完成率、pass^k、费用/耗时、故障类别，
增加官方场景分组指标、按需 CI 与控制变量对照。AppWorld test 仅用于最终汇总，不查看逐题评分
来调优或进行错误分析。公开报告仅发布允许公开的汇总与配置摘要。

回滚为整体回退该阶段提交；没有数据库或生产配置迁移。既有镜像安全扫描发布阻断单独跟踪，
评测夹具通过不能替代生产发布门禁。
