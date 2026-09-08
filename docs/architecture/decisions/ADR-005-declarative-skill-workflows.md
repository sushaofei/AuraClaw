# ADR-005：Skill scripts 使用声明式 Workflow，不执行包内任意代码

- 状态：Accepted（Issue #75）
- 日期：2026-09-01
- 适用范围：Skill package、发布准入、Runtime、Hands Capability 调用与恢复
- 架构真源：`docs/architecture/system/23 MCP Runtime 能力平面.md`

## 背景

仅让模型读取 `SKILL.md` 再自行决定 Tool/Resource 调用顺序、参数映射和重试，会把本可确定的流程变成
概率行为。AuraClaw 已能固定 package digest、Tool schema、Resource binding 和 Policy decision，也已有
Hands Invocation Store 与 Runtime checkpoint，因此可以在不扩大代码执行边界的前提下提供确定性工作流。

## 决策

- Skill 包可包含 `scripts/*.workflow.json`；Manifest 用可选 `workflow` 指定唯一 entrypoint。
- `scripts` 在产品语义上称为 Workflow，只接受 `skills.auraclaw.io/v1alpha1` 的 UTF-8 JSON。
- v1alpha1 只支持顺序 `tool.call`、`resource.read`、结构化 `literal/from` selector、有界 timeout/retry
  和输出映射；不支持循环、递归、动态代码、通用表达式或任意并发。
- Python、Shell、JavaScript、Wasm、二进制和 executable magic 继续被拒绝。未来任意代码执行必须是独立、
  沙箱化且经 Hands/Policy 管理的 Capability，不能进入 Skill Runner 进程。
- Workflow 使用的 Tool/Resource 必须是 Manifest 声明依赖的子集。Resolver 把 workflow digest 与
  Tool/Resource binding 一并固定，Runtime 不在执行中重新搜索或升级。
- 所有调用继续走 Runtime port → Hands → Policy/Approval/Invocation Store → Managed Connector；Runtime
  不感知或直连下游 MCP，也不能读取 credential 或覆盖可信身份。
- 每个逻辑步骤由 activation、workflow digest 和 step id 生成稳定 invocation/idempotency key。恢复、重试和
  审批续跑复用同一标识，结果未知的写操作不得换 id 盲目重试。
- `required_references` 显式声明路径、媒体类型和大小。Executor reference 首版只使用 JSON；模型 reference
  仅在 `preload=true` 时进入有界 trusted prompt，其他内容通过 `skill://` 按需加载。
- Workflow 中间结果最多 1 MiB；更大或受限结果应由 Tool/Resource Gateway Artifact 化，Runtime 不保存
  credential、Authorization 或无限增长的原始正文。

## 备选方案

1. **执行包内 Python/Shell。** 否决：扩大供应链、进程、网络、文件系统和 Secret 边界，且绕过 Hands 治理。
2. **继续只使用 SKILL.md。** 保留兼容，但不能给固定流程提供确定性和恢复幂等保证。
3. **让 Workflow 直接调用 MCP。** 否决：破坏 Runtime 与协议/Connector 解耦以及 Credential Proxy 边界。
4. **首版支持完整 DAG/循环。** 延后：先以顺序、有限 DSL 验证需求，避免形成第二种通用编程语言。

## 后果与回滚

- 旧 Manifest 字段均保持兼容；无 `workflow` 的 Skill 沿用模型驱动路径。
- 不新增数据库表；Workflow 随不可变包存储，binding/checkpoint 只保存 digest、状态和有界输出。
- 回滚时停止发布含 `workflow` 的新包并回滚 Runtime/准入代码；已发布包保留在 Artifact，但旧版本服务会
  fail closed，不能把未知 `scripts/` 当普通内容执行。
- Skill 内容缓存 Issue #74 只优化读取，不改变本 ADR 的 digest、Policy、revocation 或 reference 语义。
