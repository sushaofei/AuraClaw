# M13 Role-aware Agent Runtime 实施与运维

## 交付结果

Issue #55 将 M4 已有的 Collaboration Domain 闭环接入生产 Agent Runtime。Coordinator、Worker、
Reviewer 和 Repair 现在共用同一 `agent` Runtime Pool，同时由 Assignment 的语义角色决定 Context、
协作工具白名单和终态合同。

## 执行链路

```text
AgentHarness
  -> RuntimeCollaborationController
  -> RemoteCollaborationClient
  -> /internal/v1/collaboration/command
  -> CollaborationInternalService
  -> CollaborationService
  -> Canonical Event Store + Outbox
  -> RunnableFeedConsumer
```

内部请求携带服务身份和签名 Lease Assertion。Session Service 验证 tenant、Root Session、当前
Session、Run、Runtime、角色和 fencing token，并从签名角色派生 actor。模型参数不能覆盖这些字段。

## 等待与恢复

Coordinator 调用 `auraclaw.collaboration.await_children` 后：

1. Harness 保存 `agent.waiting_children` checkpoint 和等待的 Child Session IDs。
2. Control Store 将 Assignment 标为 `waiting_children`，释放 Assignment Lease。
3. Harness 不写 Root `run.completed`。
4. Child 发布 Result 或 Review 时，Canonical Outbox 唤醒 Orchestrator。
5. Runnable Feed 回放同一 Root 的协作事件；解锁新的依赖节点，并在全部等待目标终态后重新排队
   原 Root Run。
6. 新 Runtime 实例加载 checkpoint，读取权威 Graph，继续下一轮模型决策。

这一流程不以 Collaboration Projection 的即时一致性作为调度正确性的前提。

## 角色工具矩阵

| Assignment role | 模型可见的协作工具 | 成功终态 |
|---|---|---|
| root / coordinator | get_graph、create_child、set_dependencies、request_review、cancel_child、await_children、join | join，或没有 Child 时直接完成 |
| worker / repair | publish_result | Child Result + Run completed |
| reviewer | publish_review | Review decision + Run completed |

V1 不向模型开放 `delegate` / `handoff`。Service API 和 Canonical owner 事件保持兼容，但执行归属只由
Control Plane Lease 决定。若未来开放，需要先定义 owner 与 Lease 的一致性、冲突、恢复和审计规则。

## 失败与排障

- Coordinator 有活动 Child 却未 await：Harness 自动转为 waiting，避免忙轮询。
- Coordinator 的 Child 已终态但未 join：Harness 以合同错误失败，不把普通文本当作 Root Result。
- Worker / Reviewer 未调用发布工具：Harness fail closed。
- 签名角色、Session 或 fencing token 被改写：内部服务拒绝请求。
- Child 工具权限超过 Root grant：Runtime 在提交 create_child 前拒绝。
- Root 长时间处于 waiting_children：检查 checkpoint 中 `waiting_child_ids`、对应 Child 终态事件、
  Control queue 状态和 Session Outbox consumer lag。

## 验证范围

- M4 串行、并行、树形、混合 DAG 与 Reviewer lineage 回归。
- M13 shared pool、串行依赖解锁、Root suspend/wake、内部命令幂等、签名角色防伪、runtime budget
  持久化和角色 Harness fail-closed。
- memory 路径执行；PostgreSQL/KingBase suspend/wake 与 Root Feed SQL 通过兼容性测试覆盖。
