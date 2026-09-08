# 审批模式与升级（AuraClaw #92 / AuraX #13）

## 行为

审批模式决定如何处理 Policy 的 `require_approval`，不替代 Policy 的动作分类或身份授权。
write 只是一个例子，所有要求审批的操作都经过同一个 `ApprovalModeResolver`。

| 模式 | API 值 | require_approval 的处理 |
| --- | --- | --- |
| 请求批准 | `request_approval` | 现有人工审批、暂停和恢复闭环 |
| 帮我批准 | `auto_review` | 独立审核模型确认安全才继续；风险、不确定、失败、超时转人工 |
| 完全访问权限 | `full_access` | 记录模式放行证据后继续，无逐次人工审批 |

`deny` 始终拒绝；`allow` / `allow_with_constraints` 保持原规则。取消 Tool Gateway 原有的
“只读 remote-mcp 即使 Policy 要求审批也放行”例外，只读默认仍由基础 Policy 判为 allow。

## API

`GET /v1/approval-modes` 返回版本 1、三档值和默认映射，供客户端做能力协商。

- `POST /v1/tasks` 接受 `interaction_mode: streaming | non_streaming` 和可选 `approval_mode`。
  chat 旧入口省略 interaction_mode 时为 streaming；schedule 为 non_streaming。
- `POST /v1/tasks/sync` 固定 non_streaming，接受可选 approval_mode。
- 流式默认 request_approval；非流式默认 full_access。异步提交并等待结果的客户端须显式传
  non_streaming。HTTP 是否返回 202、是否订阅 SSE、模型内部是否 stream 均不改变审批模式。
- TaskAccepted、TaskView、Transcript、Result 返回 effective_approval_mode、interaction_mode、
  approval_mode_source、approval_mode_revision。
- `POST /v1/sessions/{id}/runs` 接受可选 `{ "approval_mode": "auto_review" }`；空 body 兼容原调用。
  仅 created/ready/paused 可启动新 Run。模式变更与 run.requested 原子提交，后续 Run 默认继承；
  resume/断线/后台观察不改变配置。
- 同步显式人工模式可能返回 `409 needs_human`，客户端保存 session_id 并读取 Transcript 的待审批项。
  不自动改成 full_access 重试。202 timeout 继续使用原结果等待协议。
- 同一 command id 不可换参数/模式；Run 命令重试在当前状态变化后仍返回原 Run。

## 权威状态与审核

`session.created`、`run.requested`、`session.approval_mode_changed` 中的 approval 对象保存模式与修订。
Snapshot 保存同一配置，子 Session 从服务端加载父 Session 配置继承。
旧事件没有字段时 effective_approval_mode=null / source=legacy，继续执行原有审批规则。
Task 投影的 approval JSON 仅用于显示，不是执行授权依据。

Policy 服务通过已鉴权的 Session Event Store 读取当前 Run，验证 Session/Run/修订，不信任
Runtime 或 Tool 参数自报的模式。Runtime 禁止写入 approval 配置或批准/拒绝事实。

自动审核经 Model Gateway、使用独立 POLICY workload；只能无工具、最多 512 output tokens。
与普通模型调用共用配额和持久 Model Call claim，Runtime 不允许声明 approval_review purpose。
审核输入来自 Canonical 用户目标/消息及标准化、脱敏动作；系统指令明确将其中的指令视为数据。

审核预算：单次最多 20 秒，每 Run 最多 32 次审核机会；批准有效期 5 分钟，过期转人工。
Policy HTTP client 使用 30 秒超时，容纳审核及 Canonical 结果落库。

`policy.review.requested` / `policy.review.completed` 为独立审核事实；自动通过不伪造 human response，
也不使用会把正在执行的 Run 重新标为 runnable 的 approval.approved 事件。
人工批准和待审批缓存按 Run 隔离；新 Run 不复用上一轮批准，避免切换模式后沿用旧授权。
`policy.mode.resolved` 记录 full_access 来源。三者均进入执行轨迹。
审核完成采用 Canonical command 去重和 expected version 仲裁；跨副本复用结果，先转人工后到达的
自动通过不能覆盖既有结论。审核调用 Model ID 由动作摘要、Session/Run、模式修订、策略版本确定。

当前实际产生人工审批的执行点是 Tool Gateway/Hands。基础 Policy 在 Model、Admission、Skill、
Artifact 等其他现有执行点尚未配置 require_approval，它们原有 allow/deny 规则不变；共享 resolver
对动作类型不设 write 限制。新的执行点引入人工暂停时还须提供自己的持久暂停/恢复适配，不能把
Policy 的 require_approval 当成已执行或改为 allow。无 Session 的管理操作不继承任意任务模式。

## 发布、迁移与回退

1. 维护窗口停止旧服务，执行 `0059_approval_modes.sql`；该迁移仅增加可重建的 task_view.approval。
2. 同批更新 Session、Policy、Model Gateway、Task API、Hands、Projection；避免新旧模式逻辑混跑。
3. 确认 Policy→Session 和 Policy→Model Gateway 的既有 POLICY workload token 配置可用。
   不需要向 Runtime 提供模型密钥，也不增加明文密钥配置。
4. 校验 migration ledger / readiness，重建旧投影按需执行，再更新 AuraX/SDK。
5. 对话默认人工、任务两种入口默认全权限；用显式 auto_review 验证审核及转人工路径。

0059 down/up 已在临时 PostgreSQL 验证；再次 up 后可以从 Canonical Events 重建模式。
已产生新 policy.* / mode_changed 事件后，不能直接切回无法识别这些事件的旧 Projection/Session
二进制；优先修复并前滚，或先准备支持新事件的兼容版本。不要删除 Canonical 审核/审批历史来回滚。
本次开发验证不代表测试/生产环境已部署。
