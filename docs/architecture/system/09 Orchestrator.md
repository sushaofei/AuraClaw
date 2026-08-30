# Orchestrator

## 定位

Orchestrator 是外部运行时控制平面，回答“哪个 Session 应由哪个 Runtime 在什么资源上运行”。它不参与 Prompt 构造、不判断任务语义、不拆分 Task DAG，也不转发每一次工具调用。

## 输入与输出

读取：

- Read Model Store 的 Control Projection。
- Control State Store 的 Lease、Assignment、Capacity 和 Heartbeat。
- Policy 决策和运行资源约束。

写入：

- Control State Store 的运行态控制数据。
- Session Service 的重要生命周期事件。
- Agent / Hands Runtime 的控制命令。

## 核心模块

| 模块 | 功能 |
|---|---|
| Runnable Watcher | 观察达到目标版本的 runnable 状态 |
| Scheduler | 优先级、Deadline、租户公平和队列分片 |
| Capability Matcher | 匹配 Agent、模型、工具、Sandbox 和资源 |
| Lease Controller | 获取、续租、释放并使用 Fencing Token |
| Runtime Provisioner | 创建、恢复、迁移 Agent / Hands Runtime |
| Assignment Controller | 维护 Session 与 Runtime 的绑定 |
| Health Monitor | 心跳、超时、僵尸实例和资源健康 |
| Recovery Controller | 重建、重新调度、handoff 和回收 |
| Cancellation Controller | 传播取消、Deadline 和强制终止 |
| Reconciler | 比较 desired state 与 actual state |

## 控制接口

```text
watchRunnable()
schedule(sessionId, runtimeSpec)
provision(runtimeSpec)
wake(sessionId)
pause(sessionId)
resume(sessionId)
handoff(sessionId, targetProfile)
cancel(sessionId)
shutdown(runtimeId)
reconcile(scope)
```

## 生命周期事件

进入 Canonical Session Log：

```text
run.scheduled
run.started
run.paused / resumed
runtime.failed / reprovisioned
session.handed_off
run.terminated
```

心跳、CPU、内存、Lease 续期和短期重试不进入 Canonical Log。

## 调度流程

```text
Control Projection runnable@version
 -> Admission / Capacity Check
 -> 原子 Claim Runnable Item
 -> 获取 Session Lease + Fencing Token
 -> Provision Agent / Hands
 -> 写 Assignment
 -> append run.scheduled / started
 -> 持续 Heartbeat 和 Reconcile
```

## 故障恢复

- Runtime 心跳超时：标记 suspect，确认后回收 Lease。
- Runtime execution claim 到期：同时失效 Session lease，旧尝试由 fencing 拒绝，新实例从 checkpoint 接管。
- Sandbox 故障：重建环境；是否重放有副作用工具由 Agent 判断。
- Orchestrator 实例故障：其他实例在 Lease 到期后接管。
- Projection 暂时落后：等待 `min_version`，不基于旧状态调度。
- Handoff：先持久化事实，再迁移执行所有权。

## 与 Coordinator 的边界

```text
Coordinator：拆分、依赖、角色、汇总和动态计划
Orchestrator：启动、放置、监测、恢复和回收 Runtime
```

Orchestrator 不读取自然语言任务后自行创建 Child Session。

## 安全与观测

- 资源操作使用服务身份和最小权限。
- 每次控制动作记录 session、runtime、fencing token、原因和结果。
- 容量饱和：保留原 priority/tenant partition，带 jitter 延迟重排；这是正常背压而非调度异常。
- 指标：queue wait、schedule latency、lease/claim conflict、duplicate attempt prevented、lease renewal
  failure、capacity saturation、runtime startup、recovery count、orphan assignment。

## 验收条件

- 多个 Orchestrator 不会同时成功控制同一 Session。
- 旧 Runtime 使用过期 Fencing Token 时被执行端拒绝。
- Orchestrator 重启后能够通过 Reconciliation 恢复。
- 调度不需要扫描完整 Session 对话历史。
