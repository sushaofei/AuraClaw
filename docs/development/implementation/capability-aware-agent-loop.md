# M11 Capability-Aware Agent Loop 实施与运维

## 1. 交付边界

M11 将 M9 的 Capability Plane 接入普通 Agent 任务。生产 Runtime 仍只连接 Model Gateway、
Session、Control 和内部 Action Hands Contract，不增加服务或外部凭证路径。

核心组件：

- `runtime/harness.py`：可恢复的 bounded multi-turn loop；
- `runtime/capability_controller.py`：bootstrap control tools、候选和固定 binding；
- `action/capability_catalog.py`：按 `capability_id` 加载权威契约和内部 Skill resolve；
- `runtime/hands_adapter.py`：Capability、Resource 和 Skill 的单一受控 Hands 客户端；
- `session/internal_service.py`：Runtime 写入 Skill 和 Resource 证据的身份边界。

未配置 Capability Controller 的旧测试 Harness 保持 one-step 兼容；生产 `agent-runtime`
入口启用 Capability-Aware Loop。

## 2. 运行状态

Checkpoint phase：

```text
capability.model_pending
capability.model_completed
capability.call_completed
capability.approval_waiting
capability.completed
```

Checkpoint 是恢复控制状态，不替代：

- Canonical Event：模型终态、Tool、Skill、Resource 采用和 Run 终态；
- Hands Invocation Store：Tool 幂等、副作用和未知终态；
- Artifact Store：大型 Resource、Tool Result 和 Skill Package。

## 3. 安全控制

- 模型初始只能看到固定 bootstrap tools，不接收全量 Tool Schema。
- `capabilities.load` 只接受当前 Run 搜索结果中的 `capability_id`，每 Run 有数量上限。
- 未加载业务 Tool 即使由模型伪造 Tool Call，也以 `capability_not_loaded` 拒绝。
- Skill 激活参数只有 `capability_id + inputs`；Role、tenant、Policy 和 publisher/version
  来自已加载契约与 Runtime Assignment。
- Resource 读取只展开已加载 URI/template，参数集合必须与模板字段完全一致。
- Prompt Injection finding 的 Resource 正文不进入模型；大文本在 Runtime 再次截断。
- `auraclaw.skills.resolve` 是 Runtime 内部调用，不进入模型 bootstrap tools。

## 4. 预算与故障恢复

模型轮次和所有 capability calls 共同消耗 `RuntimeBudget.max_steps`；输出 Token 与 Cost 在整个
Run 累计。搜索、加载、候选和已加载契约另有硬上限。相同名称和参数的调用连续重复超过三次视为
无进展循环。

进程在 Tool 执行完成后死亡时，`capability.call_completed` Checkpoint 保存结果、更新后的 binding
和待写 Side Events。新 Runtime 先补写 Canonical Event，再进入下一轮，不重新执行调用。

审批需要时进入 `capability.approval_waiting`。恢复继续使用原 Tool Call 和幂等键；拒绝或失败结果
作为 Tool Message 反馈下一轮模型。

## 5. 观测与排障

排障时按以下顺序检查：

1. Session 中的 `model.turn.completed` 与 `tool.call.requested/completed` 是否按 invocation id 配对；
2. Control Checkpoint 的 phase、turn、call index、steps 和 capability state；
3. Hands Invocation Store 的副作用状态；
4. `skill.activated/completed/failed/cancelled` 与 package digest；
5. `context.resource.used` 的 digest、revision、Policy decision 和 Artifact Ref。

不得在日志中记录完整 Skill 正文、Resource 正文、未经脱敏的 Tool 参数/结果或 Secret。

## 6. 验证与回滚

门禁：

```bash
uv run ruff check .
uv run mypy src/auraclaw
uv run pytest
uv run lint-imports
```

回滚时先回滚 `agent-runtime`，使其恢复旧 one-step Harness，再回滚 Action Hands 的 load/resolve
控制 Tool。新增事件属于向前兼容的 Canonical Event；回滚不删除 Session Event、Checkpoint、
Invocation 或 Artifact。
