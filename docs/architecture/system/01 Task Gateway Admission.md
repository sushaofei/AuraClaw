# Task Gateway / Admission

## 定位

Task Gateway 是所有任务写命令的外部边界。它负责把 Web、API、Webhook 和 Timer 请求转换为确定性的 Session Command，但不理解自然语言任务拓扑，也不直接启动 Agent Runtime。

## 职责与非职责

负责：

- 身份认证、租户识别和命令级授权。
- 请求 Schema 校验、标准化和大小限制。
- `idempotency_key` 去重。
- Admission、配额、优先级和 Deadline 校验。
- 创建通用 Root Session，或向已有 Session 追加继续、取消、反馈命令。
- 接收 Human Approval Response。
- 返回 `session_id`、查询地址、结果地址和 Stream 地址。

不负责：

- 判断任务是单 Agent、串行、并行还是树形。
- 创建 Child Session 或直接修改 Task DAG。
- 调度 Brain、Sandbox 或 Hands。
- 承载 SSE / WebSocket 长连接。
- 读取完整 Session 历史并做语义推理。

## 核心模块

| 模块 | 功能 |
|---|---|
| Authentication | 验证用户、服务账户和签名调用方 |
| Authorization | 校验 tenant、project、session 和 command 权限 |
| Request Validation | 校验输入、附件引用、Callback、Deadline |
| Idempotency | 以 tenant + operation + key 防止重复创建和重复响应 |
| Admission / Quota | 并发、成本、优先级和速率限制 |
| Command Normalizer | 转换为统一 Session Command |
| Root Session Admission | 创建 Root Session 和初始资源声明 |
| Human Response Handler | 校验 approval_id、action_digest 和响应人 |
| Response Builder | 返回 status/result/stream URL 和版本 |

## 对外接口

```http
POST /v1/tasks
POST /v1/sessions/{session_id}/resume
POST /v1/sessions/{session_id}/cancel
POST /v1/approvals/{approval_id}/responses
```

创建任务返回：

```json
{
  "session_id": "ses_123",
  "run_id": "run_1",
  "status": "pending",
  "status_url": "/v1/tasks/ses_123",
  "result_url": "/v1/tasks/ses_123/result",
  "stream_url": "/v1/streams/ses_123"
}
```

## 下游命令

```text
create_root_session
append_user_message
request_run
resume_session
request_cancellation
record_human_response
configure_result_delivery
```

所有命令必须携带 `command_id`、`tenant_id`、`actor`、`expected_version` 或明确的并发策略。

## 关键流程

```text
请求
 -> 鉴权与配额
 -> 幂等检查
 -> 标准化 Command
 -> Session Service 原子追加事件
 -> 返回 202 + 任务地址
```

简单任务和复杂任务都创建相同的 Root Session。复杂度判断由初始 Agent 或 Coordinator 完成。

## 一致性与失败处理

- 幂等结果需要保存原始响应摘要，重复请求返回同一 `session_id`。
- Gateway 超时但 Session 已提交时，客户端重试不得创建第二个任务。
- Session 版本冲突返回 `409 Conflict`，调用方刷新状态后重试。
- Admission 拒绝不创建 Session；任务创建后的策略拒绝必须成为 Session Event。
- Gateway 不在本地保存不可恢复的任务状态。

## 安全与观测

- 记录 actor、tenant、command、idempotency hit、拒绝原因和延迟。
- Approval Response 必须验证响应人和有效期。
- 日志不得包含原始 Secret、完整敏感 Prompt 或未脱敏附件。
- 核心指标：接纳率、拒绝率、幂等命中率、Session 创建延迟、版本冲突率。

## 验收条件

- 相同幂等键并发提交只创建一个 Root Session。
- Gateway 重启不影响已接纳任务。
- Human Response 不经过 Streaming Gateway。
- Task Gateway 无法直接创建 Child Session 或启动 Runtime。

## 当前实现对照

- 归属：`api/routes/tasks.py`、`api/dependencies.py`、`gateways/task/`，部署在 `task-api`。
- 已实现创建、同步调用、追加消息、请求新 Run、取消、恢复、关闭 Session 和审批响应；写入通过
  `TaskCommandGateway`/远端 Session Client，不直接写 Session 表。
- 生产身份支持签名 Agent Context、workload bearer、租户/声明一致性校验和 replay guard；命令上下文携带
  command、actor、correlation 与 causation。

## 现有缺陷与待完善

- `AllowAllAdmissionController` 仍是开发参考实现；生产接纳依赖远端 Policy，但尚无完整的租户并发、成本、
  Deadline 与优先级联合 admission 算法。
- 当前公开入口以任务 HTTP API 为主，通用 Webhook/Timer 适配器和附件预检没有形成独立受管入口。
- 同步调用只是异步任务之上的有界等待，容量限制为进程级；需要跨副本总量治理与更明确的超时预算传播。
- 待补：外部契约兼容测试、请求体/附件配额矩阵、限流拒绝审计以及真实身份提供方的轮换/撤销演练。
