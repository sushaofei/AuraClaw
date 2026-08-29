# AuraClaw 公开 API 与身份接入手册

> 面向：chaintower 后端、工作台 / BFF、定时调度接入方。  
> 代码基线：当前仓库公开路由 `src/auraclaw/api/routes/`，身份契约 [ADR-003](../architecture/decisions/ADR-003-trusted-identity-context.md)。  
> 配套文档：[chaintower 身份联调任务](./chaintower-identity-integration.md)、[Task Gateway](../architecture/system/01%20Task%20Gateway%20Admission.md)、[Policy Approval](../architecture/system/18%20Policy%20Approval%20Service.md)、[External Integration Contracts](../architecture/system/21%20External%20Integration%20Contracts.md)。

本文回答四件事：

1. 租户、用户、触发方分别是什么，谁负责签发身份。
2. AuraClaw 对外到底开放哪些 HTTP/SSE 接口。
3. chaintower 或定时任务应如何调用，以及不要调用什么。
4. 工具需要人工审批（Human-in-the-Loop）时，接入方如何检测、展示与响应。

---

## 1. 先给结论

- **租户不是用户。** `tenant_id` 是企业/组织数据隔离边界；`user_id`（命令里的 `actor`）是这次动作的操作者。
- **AuraClaw 不会自己跑起来。** 合法触发只有两类：已登录用户，或 chaintower 侧的定时/调度任务。两者都走同一套 `POST /v1/tasks`。
- **chaintower 是用户身份与业务权限的唯一权威。** AuraClaw 不实现 SSO，不管理用户 access token，不查询菜单/部门 RBAC。
- **AuraClaw 必须自己验签。** 它验证 chaintower workload 与短期 Agent Context，再按租户隔离 Session。不能因为“请求来自内网”就信任裸 `X-Tenant-ID`。
- **对外是 Task API + Streaming Gateway + 工作台 Admin + 健康检查。** `/internal/v1/*`、Hands、AuraMCP 都不是给业务调用方或 AuraX 用的。
- **人审是公开写命令，不是 SSE 上行。** 高风险工具进入 `waiting_for_human` 后，必须走 `POST .../approvals/{approval_id}/responses`；Streaming 只通知，不接收批准/拒绝。

```text
用户或定时任务
  -> chaintower：登录 / 调度授权 + 签发短期 Agent Context
  -> AuraClaw Task API：验签 workload + Assertion
  -> 编排 / 策略 / 租约 / 幂等
  ->（需要人审时）waiting_for_human → 审批响应 → 恢复同一工具调用
  -> Hands -> chaintower MCP：最终业务鉴权
```

---

## 2. 租户、用户与触发方

### 2.1 租户 ≠ 用户

| 概念 | AuraClaw 字段 | chaintower | 含义 |
|---|---|---|---|
| 租户 | `tenant_id` | `LoginUser.tenantId` | 哪家企业的数据，查哪套表、哪套数据权限 |
| 用户 | `user_id` / `actor.id` | `LoginUser.id` | 谁点的按钮，谁为这次任务负责 |
| 部门（可选） | `dept_id` | `deptId` | 行级 / 部门数据权限 |
| 会话绑定 | `session_id` | 创建后回填 | 后续命令必须绑到已有 Session |

一个租户下有多个用户。Session、Canonical Event、投影、工具结果都按 **租户** 隔离；审计、审批、人审卡片按 **用户** 记录。跨租户读取别人的 `session_id` 返回 **404**，不泄漏任务是否存在。

开发环境里的 `X-Tenant-ID` 与 `X-Actor-ID` 也是两套 Header，不是同一个值。

### 2.2 谁触发 AuraClaw

AuraClaw 公开入口只有 Task Gateway。外面只有两类合法触发：

```text
用户（页面 / API）
  -> chaintower 登录 + 业务授权
  -> 签发 Agent Context（同时包含 tenant_id + user_id）
  -> POST /v1/tasks

定时任务 / Timer
  -> chaintower 调度器按计划触发
  -> 同样走 Task Gateway（同一套 POST /v1/tasks）
  -> 必须带租户；actor 可以是系统账号，但不能没有租户
```

Timer 创建完即结束，**不轮询结果**。结果靠 Query API 或 Result Delivery（Webhook 等）回来。架构图中的 `Web / API / Timer → Task Gateway` 指的就是这两条路径。

定时任务也不是“没有用户”。它至少要有：

- **租户**：跑哪家企业的数据。
- **actor**：系统主体，例如 `type=system, id=price-insight-daily`，便于审计。

不能用“定时任务”省略租户，否则会串数据。

### 2.3 身份字段怎么填

- 人触发：`tenant_id` = 当前登录租户，`user_id` = 当前登录用户，`dept_id` = 当前登录部门（写入 Assertion，AuraClaw 固化到 Root Session）。
- 定时触发：`tenant_id` = 任务所属租户，`user_id` / `actor` 与 `dept_id` = 调度主体（仍由 chaintower 签发）。

AuraClaw 不判断“该不该这个人点、这个定时任务属不属于这家企业”——那是 chaintower 的事。它把验过的租户、用户、部门写入 `session.created`，并透传到 Hands → MCP 的 `X-CT-Tenant-ID` / `X-CT-User-ID` / `X-CT-Dept-ID` 与 `_meta.io.auraclaw/*`。

不要把 `dept_id` 写进 `POST /v1/tasks` body。body / query 若声明部门且与 Assertion 冲突 → 403。

---

## 3. 身份职责怎么拆

把“看起来像鉴权”的逻辑全部搬出 AuraClaw **不合适**。用户身份权威外置是对的；验签和内部控制面必须留在 AuraClaw。

| 类别 | 典型位置 | 做什么 | 是否留在 AuraClaw |
|---|---|---|---|
| 用户身份消费 | `api/dependencies.py`、`infrastructure/identity/` | 校验 chaintower workload + `X-CT-Agent-Context` | **必须留**：消费身份，不是颁发身份 |
| 内部服务互认 | `ServiceIdentity`、workload token、Hands lease | 12 个内部入口互证“我是哪个服务、持有哪次租约” | **必须留** |
| Agent 策略 / 审批 | Policy、Tool permission、配额 | 这次工具能不能做、要不要人审 | **必须留**：Agent 控制面，不是用户 RBAC |
| 下游凭证代持 | Credential Proxy / Vault | Agent 碰不到真实 Secret | **必须留** |
| 开发期例外 | `X-Tenant-ID` / `X-Actor-ID` | 本地和单测 | 仅 development；生产 fail closed |

判断标准：

- 问“这个人是谁、能不能看这张表” → chaintower。
- 问“这个调用是不是可信的 chaintower、这次 run 有没有租约、Secret 能不能进模型” → AuraClaw。

否决过的方案：AuraClaw 自建 OAuth/SSO；转发并持久化浏览器 access token；只靠内网或 IP allowlist；生产继续信任裸租户 Header。

---

## 4. 对外入口范围

生产 ingress 默认 `:8080`：`/v1/streams/*` 路由到 `streaming-gateway`，其余路由到 `task-api`。本地可用 `uv run uvicorn auraclaw.main:app --reload`（默认 `:8000`，命令和 SSE 在同一进程）。机器可读契约见 `/docs`。

| 入口 | 调用方 | 用途 |
|---|---|---|
| `/v1/tasks*`、`/v1/sessions*`、`/v1/operations*` | chaintower / 前端 BFF | 创建任务、续聊、取消、审批、查状态和结果 |
| `/v1/streams/{session_id}` | 同上 | SSE 实时增量，**不保证结果** |
| `/health/live`、`/health/ready` | 探活、ingress | 无业务身份 |
| `/internal/v1/*` | AuraClaw 内部 12 服务 | **不要调** |
| Hands `/internal/v1/hands/*` | Agent Runtime | **不要调** |

关闭 SSE 不会取消任务。最终答案以 Result API 为准。

---

## 5. 认证与公共 Header

### 5.1 生产（chaintower → AuraClaw）

```http
Authorization: Bearer <chaintower-workload-credential>
X-CT-Agent-Context: <base64url(canonical-json)>.<base64url(hmac-sha256)>
X-Correlation-ID: <可选>
```

Assertion 至少包含：`iss=chaintower`、`aud=auraclaw-task-api`、`tenant_id`、`user_id`、`scopes`（含 `agent.task.invoke`）、`iat` / `exp`（默认不超过 5 分钟）、`jti`、`kid`。可选 `dept_id`、`session_id`、`permission_version`。

约束：

- 创建之后的调用，Assertion 必须携带与路径一致的 `session_id`。
- body / query / Header 里声明的 tenant、user 若与 Assertion 冲突 → 403。
- 密钥与 AuraClaw `AURACLAW_AGENT_CONTEXT_SIGNING_KEYS_JSON` 的 N/N-1 `kid` 对齐。
- Assertion 原文不得进入 Event、Projection、日志或 `model_dump`。
- 创建 Root Session 时把 Assertion 的 `dept_id` 写入 `session.created` 并冻结；后续 Run / MCP 使用这份快照，不再现查用户部门。

chaintower 签发入口：`POST /rpc-api/agent-runtime/auth/agent-context`（管理端适配路径见 agent-runtime README）。请求体只能带可选 `sessionId`，tenant / user / dept 一律从当前 `LoginUser` 恢复。

### 5.2 开发例外

`deployment_profile=development` 默认允许：

```http
X-Tenant-ID: local
X-Actor-ID: local-user
```

缺省分别为 `local` / `local-user`。生产打开 `allow_insecure_identity_headers` 会在 Settings 校验期失败。

### 5.3 写查询公共头

| Header | 何时 | 作用 |
|---|---|---|
| `Idempotency-Key` | 所有 POST | 幂等键；相同键返回同一结果 |
| `X-Expected-Version` | 除创建外的写 | 乐观并发，等于当前 Task 的 `projection_version` |
| `If-None-Match` | GET Task / Result | 条件查询，命中返回 304 |
| `Last-Event-ID` | SSE | 游标 `session_id:sequence` |

写命令超时但可能已提交时：**用同一个 `Idempotency-Key` 重试**，不要换键。

---

## 6. 接口总表

### 健康检查（无需身份）

| 方法 | 路径 | 成功 |
|---|---|---|
| `GET` | `/health/live` | `{"status":"ok"}` |
| `GET` | `/health/ready` | `200` 就绪 / `503` 降级 |

### 写命令（一律 `202 Accepted`）

| 方法 | 路径 | 作用 |
|---|---|---|
| `POST` | `/v1/tasks` | 创建 Root Session + 第一轮 Run |
| `POST` | `/v1/sessions/{session_id}/messages` | 同一 Session 追加用户消息 |
| `POST` | `/v1/sessions/{session_id}/runs` | 对已有消息发起新 Run |
| `POST` | `/v1/sessions/{session_id}/cancel` | 取消当前 Run；Root Session 回到 `ready` |
| `POST` | `/v1/sessions/{session_id}/resume` | 从暂停 / 等待人审等恢复，生成新 `run_id` |
| `POST` | `/v1/sessions/{session_id}/close` | 关闭会话，之后拒绝新消息和 Run |
| `POST` | `/v1/sessions/{session_id}/approvals/{approval_id}/responses` | 人审批准 / 拒绝 |

### 同步外观（Query 等待，不是第二条写路径）

接纳语义与 `POST /v1/tasks` 相同（Canonical Event + `202` 意义的接纳）。HTTP 连接额外阻塞到当前 Run 终态或超时。AuraX 和 Timer **不要**用这条路径。

| 方法 | 路径 | 作用 |
|---|---|---|
| `POST` | `/v1/tasks/sync` | 创建任务并等待权威结果 |

### 查询

| 方法 | 路径 | 作用 |
|---|---|---|
| `GET` | `/v1/tasks` | Root Session 列表（cursor 分页，按租户隔离） |
| `GET` | `/v1/tasks/{session_id}` | Task View（Session + 最新 Run 投影） |
| `GET` | `/v1/tasks/{session_id}/result` | 权威结果；未就绪返回 `202`。`wait=true` 时可阻塞到终态 |
| `GET` | `/v1/tasks/{session_id}/transcript` | 对话恢复（用户 / 助手消息 + 待审批） |
| `GET` | `/v1/tasks/{session_id}/activity` | 产品级执行轨迹（模型 / MCP / Tool / Skill / 状态） |
| `GET` | `/v1/tasks/{session_id}/children` | 子 Session 图 |
| `GET` | `/v1/operations/sessions/{session_id}/timeline` | 运维时间线 |
| `GET` | `/v1/operations/metrics` | 当前租户可见指标 |

### 实时流

| 方法 | 路径 | 作用 |
|---|---|---|
| `GET` | `/v1/streams/{session_id}` | SSE；`Accept: text/event-stream` |

### 工作台 Admin（AuraX / 运维）

Secret 只允许引用 `credential_ref`，响应里不会出现明文。写命令仍要 `Idempotency-Key`。

| 方法 | 路径 | 作用 |
|---|---|---|
| `GET` / `POST` | `/v1/admin/mcp-servers` | 列出 / 创建受管 MCP Server |
| `GET` / `PUT` | `/v1/admin/mcp-servers/{server_id}` | 详情 / 更新配置 |
| `GET` | `/v1/admin/mcp-servers/{server_id}/tools` | 列出该 Server 对账后的 Catalog tools |
| `POST` | `/v1/admin/mcp-servers/{server_id}:test` | 连通性探测 |
| `POST` | `/v1/admin/mcp-servers/{server_id}:enable` | 启用 |
| `POST` | `/v1/admin/mcp-servers/{server_id}:disable` | 停用 |
| `POST` | `/v1/admin/mcp-servers/{server_id}:reconcile` | 对账 |
| `POST` | `/v1/admin/mcp-servers/{server_id}:retire` | 退役 |
| `GET` | `/v1/admin/mcp-operations/{operation_id}` | 异步操作状态 |
| `GET` | `/v1/admin/skills` | 租户可见 Skill 目录（每个 name 一条最新版本） |
| `GET` | `/v1/admin/skills/{publisher}/{name}` | Skill 详情 + `SKILL.md` + 版本列表 |
| `GET` | `/v1/admin/skills/{publisher}/{name}/versions/{version}` | 指定版本详情 |
| `POST` | `/v1/admin/skills/{publisher}/{name}:enable` | 租户级启用（不改进行中 Run 的 binding） |
| `POST` | `/v1/admin/skills/{publisher}/{name}:disable` | 租户级停用 |
| `POST` | `/v1/admin/skill-publications/{publisher}/{name}/versions/{version}:restore` | 审核后恢复普通退役版本；要求 revision、reason 与幂等键 |
| `POST` | `/v1/admin/skill-publications/{publisher}/{name}/versions/{version}:revoke` | 安全撤销版本；`X-Skill-Revocation-Action: continue|pause|cancel` 明确处理活动 binding，默认 cancel；不可通过 restore 复活 |
| `GET` | `/v1/admin/skill-publishers/{publisher}` | Publisher 与公钥状态 |
| `POST` | `/v1/admin/skill-publishers/{publisher}` | 注册 tenant Publisher |
| `POST` | `/v1/admin/skill-publishers/{publisher}/keys:rotate` | 原子轮换 Ed25519 公钥 |
| `POST` | `/v1/admin/skill-publishers/{publisher}/keys/{key_id}:revoke` | 撤销泄露或不再可信的公钥 |
| `POST` | `/v1/admin/skill-publishers/{publisher}/status:suspend` | 暂停 Publisher 信任并移除其可发现 Skill |
| `POST` | `/v1/admin/skill-publishers/{publisher}/status:resume` | 恢复 Publisher；已撤销 key 不会恢复 |

---

## 7. 推荐调用链

人机问答：

```text
1. POST /v1/tasks                    → 202，拿到 session_id / stream_url / result_url
2. GET  /v1/streams/{session_id}     → 展示 model.output.delta（可选）
3. GET  /v1/tasks/{session_id}       → 轮询 status / run_status，遵循 Retry-After
4. GET  /v1/tasks/{session_id}/result
     未完成 → 202 + Retry-After
     完成   → 200，result_summary 为权威答案
5. 追问：POST .../messages（带 X-Expected-Version）
   需要新一轮推理时：POST .../runs
6. 结束：POST .../close
```

定时任务：

```text
1. chaintower 调度器按租户签发 Agent Context（actor 为系统主体）
2. POST /v1/tasks，Idempotency-Key 用计划实例稳定键（例如 jobId + 业务日）
3. 不要保持 SSE；配置 Result Delivery，或事后用 Query 核对
4. Timer 在 202 Accepted 后结束本次触发
```

Java / 脚本一次性调用（同步外观）：

```text
1. POST /v1/tasks/sync
     终态     → 200，wait_outcome=completed|failed|cancelled，result_summary 为权威答案
     超时     → 202，任务继续，可用 GET .../result?wait=true 再等
     人审/暂停 → 409 needs_human / needs_resume，走既有审批或 resume
2. 客户端 HTTP 读超时必须大于 timeout_seconds（默认 60，最大 120）
3. 断线不会取消已接纳任务
```

不要对 Timer 使用 `/v1/tasks/sync`。`source=schedule` 及多余字段会被 422 拒绝。

人审完整处理见 [§8 Human-in-the-Loop](#8-human-in-the-loop人审)。

---

## 8. Human-in-the-Loop（人审）

AuraClaw 把 HITL 建模为 **Policy / Approval** 决策，而不是 Prompt 约束。  
`write-with-approval`、`destructive/admin` 等高风险动作在无有效批准时 **fail closed**：Runtime 写入 `approval.requested`，Session / Run 进入 `waiting_for_human`，原工具调用与幂等键被保留，等待人响应后再恢复。

### 8.1 主链路

```text
Runtime 调工具
  -> Tool Gateway / Policy: evaluate
  -> require_approval
  -> Session: approval.requested（visibility=user）
  -> Session/Run = waiting_for_human；Task View current_stage = waiting_for_human
  -> 接入方：SSE 通知 或 Query/transcript 发现待审
  -> 人：POST /v1/sessions/{session_id}/approvals/{approval_id}/responses
  -> Session: human.response.recorded + approval.approved | approval.rejected
  -> Session/Run = runnable；Orchestrator 重新调度
  -> 同一 tool_invocation_id / idempotency_key 继续执行
  -> Policy: validateApproval（绑定 digest + policy_version）后真正副作用
```

Streaming Gateway **只推送**，不接收审批结果。Human Response 必须经过 Task Gateway 的身份验签、幂等与乐观并发。

### 8.2 接入方如何发现待审

推荐按权威程度降序使用：

| 来源 | 怎么判断 | 用途 |
|---|---|---|
| `GET /v1/tasks/{session_id}` | `status` 或 `run_status` = `waiting_for_human`；`current_stage` = `waiting_for_human` | 轮询权威状态；拿 `projection_version` 供写命令 |
| `GET /v1/tasks/{session_id}/transcript` | `pending_approval != null` | 恢复聊天 UI；拿到卡片字段 |
| `GET /v1/streams/{session_id}` | `event: approval.requested`（`visibility=user`） | 实时弹出审批卡；断线后仍要以 Query 为准 |
| `GET /v1/operations/.../timeline` | 运维排障 | **不要**当主聊天路径 |

`pending_approval` 示例：

```json
{
  "approval_id": "apr_...",
  "tool_name": "controlled-write",
  "reason": "write requires approval",
  "risk": "high",
  "redacted_arguments": {"target": "release"},
  "expected_effect": "write",
  "status": "waiting"
}
```

刷新 / 重连时：先看 Task 是否 `waiting_for_human`，再读 transcript 的 `pending_approval` 渲染卡片。不要假设 SSE 一定还连着。

### 8.3 如何批准或拒绝

```http
POST /v1/sessions/{session_id}/approvals/{approval_id}/responses
Idempotency-Key: <必填，稳定键>
X-Expected-Version: <当前 Task.projection_version>
Content-Type: application/json

{"decision":"approved","feedback":"可以执行"}
```

| 字段 | 约束 |
|---|---|
| `decision` | 只能是 `approved` 或 `rejected` |
| `feedback` | 可选，最长 10000 |
| 审批人 | Assertion 的 `user_id`（开发环境 `X-Actor-ID`）；若审批单有 `assigned_approvers`，actor 必须在名单内 |
| 并发 | 必带 `X-Expected-Version`；过期 → 409 `version_conflict`，刷新 Task 再试 |
| 幂等 | 同一 `Idempotency-Key` 重试返回同一结果，不要换键 |

成功 **202**：

```json
{
  "session_id": "ses_...",
  "run_id": "run_...",
  "status": "runnable",
  "run_status": "runnable",
  "approval_id": "apr_...",
  "decision": "approved"
}
```

批准后 Session/Run 变为 `runnable`，Runtime 用**原调用身份**继续工具执行。  
拒绝后同样回到 `runnable`（`current_stage` 倾向 replanning），Agent 可改计划，**不必**整段对话终止。

### 8.4 推荐接入流程

```text
1. 轮询 GET /v1/tasks/{session_id}，或监听 SSE
2. 发现 waiting_for_human
3. GET .../transcript → pending_approval（或从 SSE payload 取 approval_id）
4. UI 展示工具名、原因、风险、脱敏参数；禁止在等待中追问新目标
5. 用户确认后 POST .../approvals/{approval_id}/responses
6. 继续轮询 Task / Result；可重新订 SSE
7. transcript.pending_approval 变为 null 后再允许追问
```

开发示例：

```bash
# 1) 确认等待人审
curl -sS "http://127.0.0.1:8000/v1/tasks/$SESSION_ID" \
  -H 'X-Tenant-ID: local' -H 'X-Actor-ID: local-user'

# 2) 取待审卡片
curl -sS "http://127.0.0.1:8000/v1/tasks/$SESSION_ID/transcript" \
  -H 'X-Tenant-ID: local' -H 'X-Actor-ID: local-user'

# 3) 批准（VERSION 用上一步 Task.projection_version）
curl -sS -X POST \
  "http://127.0.0.1:8000/v1/sessions/$SESSION_ID/approvals/$APPROVAL_ID/responses" \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: approval-1' \
  -H 'X-Expected-Version: 3' \
  -H 'X-Tenant-ID: local' \
  -H 'X-Actor-ID: local-user' \
  -d '{"decision":"approved","feedback":"可以执行"}'
```

### 8.5 绑定规则与失效条件

批准不是一句模糊的“允许写”，而是绑定不可变动作摘要：

```text
approval_id + tenant_id + session_id + action_digest(tool + version + args) + policy_version
```

以下任一变化都会使旧批准在 `validateApproval` 时失败，必须重新人审：

- 工具参数 / 目标资源变化（digest 变）
- 换了 Session
- Policy 版本变化
- 审批已过期（默认 TTL 约 1 小时；过期产生 Canonical Event，**不**自动当成拒绝或失败）
- 审批已是 `approved` / `rejected` / `expired` / `cancelled`（再响应 → 409 `approval_invalid`）

跨租户或错误 `session_id` 查审批 → **404**，不泄漏是否存在。

### 8.6 接入方不要做的事

- **不要**用 SSE 发送批准/拒绝。
- **不要**把 `POST .../resume` 当人审主路径。`resume` 可从 `waiting_for_human` 拉起新 `run_id`，但正规恢复是审批接口；人审等待不是新对话，也不能丢掉原 `tool_invocation_id`。
- **不要**在 `waiting_for_human` 时当普通追问继续 `POST .../messages` / `.../runs`（工作台应拦截并提示先审）。`runs` 在该状态下会 409；消息追加虽未在领域层硬拦，业务上应先结束待审。
- **不要**在等待人审时把 Result 的 202 当成最终失败。
- **不要**调用 `/internal/v1/policy/approvals/*`；那是内部 Policy 服务口。
- 定时任务若触发了需人审的工具：Timer 本身结束后，由业务侧通知有权审批人，或走 Result Delivery；**不要**让调度器代人点击批准，除非 chaintower 明确把系统 actor 配进 `assigned_approvers`。

### 8.7 与身份、审计的关系

- 人审属于 **Agent 控制面**（§3），不是 chaintower 菜单 RBAC。chaintower 仍负责“谁能打开工作台”；AuraClaw 负责“这次工具副作用能不能做”。
- 审批 actor 写入 Canonical Event（`human.response.recorded`），可审计。
- 参数以 `redacted_arguments` 展示，真实 Secret 不进审批卡、不进模型。

架构真源：[Policy Approval Service](../architecture/system/18%20Policy%20Approval%20Service.md)。

---

## 9. 各接口契约

### `POST /v1/tasks`

创建 Root Session，并立刻请求第一轮 Run。`Idempotency-Key` 必填；**不要**带 `X-Expected-Version`。

```json
{ "goal": "分析华东区本周猪肉价格波动", "source": "chat" }
```

`goal`：1～100000 字符。  
`source`：`chat`（默认）或 `schedule`。对话工作台固定传 `chat`。  
`schedule` 必须同时带 `schedule_id` 与 `occurrence_id`；`chat` 会忽略这两个字段。它们写入 `session.created`，并出现在 Task 投影 / 列表里。v1 AuraX 只用 `chat`；`schedule*` 先落契约，等 AuraAPI Timer。

```json
{
  "session_id": "ses_...",
  "run_id": "run_...",
  "status": "pending",
  "status_url": "/v1/tasks/ses_...",
  "result_url": "/v1/tasks/ses_.../result",
  "stream_url": "/v1/streams/ses_..."
}
```

同一 `Idempotency-Key` 重试返回同一 body，不会创建第二个 Session。

### `POST /v1/tasks/sync`

给 Java / 脚本等无法轮询的调用方。内部仍调用 `create_task` 写入 Canonical Event，然后在 Result 投影上等待当前 Run 终态。**不是** Gateway 同步调度 Runtime，也 **不是** 订 SSE。

```json
{ "goal": "分析华东区本周猪肉价格波动", "timeout_seconds": 60 }
```

只接受 `goal` 与可选 `timeout_seconds`。禁止 `source=schedule` 及其他多余字段（422）。身份头、`Idempotency-Key` 与 `POST /v1/tasks` 相同。

| 条件 | HTTP | `wait_outcome` |
|---|---|---|
| Run `completed` / `failed` / `cancelled` | 200 | 同 Run 状态。业务失败仍是 200 + `status=failed` |
| 超时且仍在跑（含 `retry_wait`） | 202 + `Retry-After` | `timeout`；任务继续，不取消 |
| `waiting_for_human` / `paused` | 409 | `needs_human` / `needs_resume`，并带 `code` |
| 同步等待并发超额 | 429 + `Retry-After` | `code=sync_invoke_busy` |

响应在 Result 形状上增加 `wait_outcome`、`status_url` / `result_url` / `stream_url`。人审之后用 `GET /v1/tasks/{session_id}/result?wait=true` 继续等，不要把批准发到这条连接上。

配置：`AURACLAW_SYNC_INVOKE_DEFAULT_TIMEOUT_SECONDS`（60）、`MAX_TIMEOUT_SECONDS`（120）、`POLL_INTERVAL_SECONDS`（0.25）、`MAX_CONCURRENT`（32）。

### `GET /v1/tasks`

只列出当前租户的 **Root Session**（`role=root`），按 `projected_at` 倒序，cursor 分页。不要扫 Canonical Event Log。

查询参数：

| 参数 | 说明 |
|---|---|
| `kind` | 可选。`chat` → `source=chat`；`scheduled` → `source=schedule`。省略则两种都返回 |
| `status` | 可选。按 Session `status` 过滤 |
| `cursor` | 上一页返回的 `next_cursor` |
| `limit` | 默认 20，最大 100 |

```json
{
  "tasks": [
    {
      "session_id": "ses_...",
      "goal": "分析华东区本周猪肉价格波动",
      "source": "chat",
      "schedule_id": null,
      "occurrence_id": null,
      "status": "ready",
      "run_status": "completed",
      "projection_version": 8,
      "projected_at": "2026-08-24T06:00:00+00:00"
    }
  ],
  "next_cursor": null
}
```

AuraX 历史页 v1 只请求 `kind=chat`。多进程 memory 投影暂不提供跨进程列表（返回空页）；开发与生产用 SQL 投影。

开发示例：

```bash
curl -sS 'http://127.0.0.1:8000/v1/tasks?kind=chat&limit=20' \
  -H 'X-Tenant-ID: local' \
  -H 'X-Actor-ID: local-user'
```

```bash
curl -sS -X POST http://127.0.0.1:8000/v1/tasks \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: cmd-create-1' \
  -H 'X-Tenant-ID: local' \
  -H 'X-Actor-ID: local-user' \
  -d '{"goal":"分析华东区本周猪肉价格波动","source":"chat"}'
```

### `GET /v1/tasks/{session_id}`

权威 **Session 投影**，不是实时流。

查询参数：`min_version`（可选）。投影版本不够时 **202**，并带 `Retry-After: 1`。  
响应头：`ETag: W/"{projection_version}"`。Run 未终态时还会给 `Retry-After: 2`。`If-None-Match` 命中且版本够则 **304**。

```json
{
  "tenant_id": "local",
  "session_id": "ses_...",
  "root_session_id": "ses_...",
  "run_id": "run_...",
  "status": "pending",
  "run_status": "pending",
  "goal": "...",
  "source": "chat",
  "schedule_id": null,
  "occurrence_id": null,
  "progress": 0.0,
  "current_stage": "pending",
  "result_summary": null,
  "result_ref": null,
  "artifact_refs": [],
  "error": null,
  "delivery_status": null,
  "delivery_id": null,
  "delivery_attempt_count": 0,
  "delivery_response_summary": null,
  "projection_version": 2,
  "projected_at": "2026-08-20T06:00:00+00:00"
}
```

两个状态不要混用：

- `status`：Session。Root 一轮结束后回到 `ready`，只有 `/close` 才到 `closed`。
- `run_status`：当前或最近一轮 Run。

Session：`created` → `pending` → `runnable` → `running` →（`waiting_for_human` / `paused` / `retry_wait`）→ `ready` → `closed`。  
子 Session 终态可以是 `completed` / `failed` / `cancelled`。  
Run：`pending` / `runnable` / `running` / `waiting_for_human` / `paused` / `retry_wait` / `completed` / `failed` / `cancelled`。

写后续命令时，把这里的 `projection_version` 填进 `X-Expected-Version`。版本落后会 **409** `version_conflict`。

### `GET /v1/tasks/{session_id}/result`

**结果权威源。** 投影未追上或 Run 未终态 → **202** + `Retry-After: 2`，body 仍返回当前快照。

```json
{
  "session_id": "ses_...",
  "run_id": "run_...",
  "status": "completed",
  "session_status": "ready",
  "result_summary": "最终回答……",
  "result_ref": null,
  "artifact_refs": [],
  "error": null,
  "delivery_status": null,
  "delivery_id": null,
  "delivery_attempt_count": 0,
  "delivery_response_summary": null,
  "projection_version": 8
}
```

这里的 `status` 是 **Run**，`session_status` 才是 Session。流式内容与这里不一致时，以这里为准。

可选 `wait=true`（及 `timeout_seconds`）会在 Query 侧受控等待到 Run 终态、人审/暂停或超时，响应形状与 `POST /v1/tasks/sync` 相同。未传 `wait` 时行为不变：立即返回当前快照，未就绪为 202。`wait=true` 时不使用 `If-None-Match` / `304`。

### `GET /v1/tasks/{session_id}/transcript`

刷新、重连时恢复对话，不拉完整运维 Timeline。

```json
{
  "session_id": "ses_...",
  "projection_version": 8,
  "status": "ready",
  "run_status": "completed",
  "messages": [
    {
      "role": "user",
      "content": "分析华东区本周猪肉价格波动",
      "event_id": "evt_...",
      "occurred_at": "2026-08-20T06:00:00+00:00"
    },
    {
      "role": "assistant",
      "content": "最终回答……",
      "event_id": "evt_...",
      "occurred_at": "2026-08-20T06:00:12+00:00",
      "run_id": "run_..."
    }
  ],
  "pending_approval": null
}
```

`messages` 来自 `session.created`（goal）、`user.message.appended`、`model.output.completed`。  
有待审时 `pending_approval` 含 `approval_id`、`tool_name`、`reason`、`risk`、`redacted_arguments`、`expected_effect`、`status`。人审发现与响应见 [§8.2](#82-接入方如何发现待审) / [§8.3](#83-如何批准或拒绝)。

### `GET /v1/tasks/{session_id}/activity`

恢复面向用户的产品执行轨迹，不返回 trace span、audit、alert 或完整内部 Prompt。参数：

- `after_version`：默认 `0`；只返回最后变化版本大于该 Session aggregate version 的节点；
- `limit`：默认/最大 `200`。

```json
{
  "session_id": "ses_...",
  "projection_version": 31,
  "source_version": 34,
  "nodes": [
    {
      "id": "tool:run_...:tci_...",
      "type": "tool",
      "status": "completed",
      "title": "procurement.price.profile",
      "summary": "success",
      "sequence": 18,
      "updated_version": 20,
      "run_id": "run_...",
      "started_at": "2026-08-20T06:00:04+00:00",
      "completed_at": "2026-08-20T06:00:06+00:00",
      "duration_ms": 2000,
      "detail": {
        "request": {"activity": {"source": "mcp", "server_id": "price-data"}},
        "result": {"status": "success"}
      },
      "correlation": {
        "event_ids": ["evt_request", "evt_completed"],
        "tool_invocation_id": "tci_..."
      }
    }
  ],
  "next_after_version": 34,
  "has_more": false
}
```

Tool、Skill、Approval 与 Model 生命周期按稳定 ID 折叠；同一节点在增量读取时可能再次返回，
消费者应以更大的 `updated_version` 覆盖旧节点。`next_after_version` 是下次读取游标，
`source_version` 是本次 Canonical Event 读取的最高版本。响应带 `Cache-Control: private, no-store`
与 `X-Activity-Version`。

`model_input` 只含消息计数、用户输入预览、Tool 名称、模型策略和 digest，不包含完整 system
prompt、Skill/Resource 正文或 Chain-of-Thought。Tool 参数/结果递归脱敏并限制大小。SSE 只用于
通知客户端刷新 Activity；刷新、重连和游标过期均以本接口为准。

### `GET /v1/tasks/{session_id}/children`

返回子 Session 图。单 Agent 问答通常 `children` 为空，不要用这个接口轮询主对话。

```json
{
  "root_session_id": "ses_...",
  "children": [
    {
      "tenant_id": "local",
      "root_session_id": "ses_...",
      "session_id": "ses_child_...",
      "parent_session_id": "ses_...",
      "role": "worker",
      "goal": "...",
      "status": "runnable",
      "runnable": true,
      "run_id": "run_..."
    }
  ]
}
```

### `POST /v1/sessions/{session_id}/messages`

```json
{ "message": "再对比一下华南" }
```

需要 `Idempotency-Key` + `X-Expected-Version`。Session 已 `closed` 或当前状态不允许追加 → **409** `invalid_transition`。

```json
{
  "session_id": "ses_...",
  "run_id": "run_...",
  "status": "ready",
  "run_status": "completed"
}
```

### `POST /v1/sessions/{session_id}/runs`

无 body。生成新 `run_id`，Session 进入 `pending`。仅当 Session 为 `created` / `ready` / `paused`。

```json
{
  "session_id": "ses_...",
  "run_id": "run_new_...",
  "status": "pending",
  "run_status": "pending"
}
```

### `POST /v1/sessions/{session_id}/cancel`

```json
{ "reason": "用户停止生成" }
```

`reason` 默认 `cancelled by user`，最长 2000。取消的是 **当前 Run**，Root Session 回到 `ready`，仍可继续发消息。取消 ≠ 关闭。

### `POST /v1/sessions/{session_id}/resume`

无 body。用于 `paused` / `retry_wait` / `waiting_for_human` 等可恢复状态，生成新 `run_id`。**人审的正规路径是审批接口，不是 resume**；见 [§8.6](#86-接入方不要做的事)。

### `POST /v1/sessions/{session_id}/close`

```json
{ "reason": "对话结束" }
```

之后 messages / runs 都会 409。这是 Root Session 的真正终态。关窗口 ≠ 关闭 Session。

### Skill Admin 与 Publisher Registry

Skill 启停是租户级目录状态，**不改**进行中 Run 的 Skill binding。Publisher Registry 只保存
Ed25519 公钥，不接收或生成私钥；新发布只接受 active key，retiring key 仅用于恢复历史包，revoked key
立即 fail closed。相关入口挂在 task-api 的 `/v1/admin/skills*` 与 `/v1/admin/skill-publishers*`。
普通来源下架产生的 `retired` Publication 只能经显式 restore 恢复。restore 先持久化 reviewer、reason、
correlation/causation 和 revision 证据，再复验原 Artifact 与当前 Source/Publisher/key 信任；失败保持
`restoring` 且不进入新发现，同一 Idempotency-Key 可在修复信任条件后重试。

安全 revoke 与普通停用不同。Publication 会持久化 reason、`continue|pause|cancel`、policy version 和
可选 decision id；Runtime 在每个模型轮次及 Skill step 前通过内部 Hands 能力校验固定
publisher/name/version/digest。`continue` 只保留已固定 digest 的 `skill://` 正文读取，不恢复 Catalog
可发现性；`pause` 保存 checkpoint 并挂起 assignment；`cancel` 写 `skill.revocation.applied`、
`skill.cancelled` 与 `run.cancelled` Canonical Event 后结束 assignment。状态查询会返回完整撤销策略证据。

外部 Publisher 可先用 `auraclaw skills sign <directory> --publisher <name> --key-id <id>` 离线签名，
private key 只从 `AURACLAW_SKILL_SIGNING_KEY`（或指定 Secret 环境变量）读取。命令输出的 Ed25519 公钥
需通过 Publisher key rotation API 登记；后续 validate/test/publish 用显式公钥本地验签，而服务端仍以
tenant Registry 的 active key 独立验签。CLI 不接受命令行私钥，也不允许外部签名声明 `platform`。

每次 Skill publish/publish-artifact 的成功、拒绝或隔离都会进入 tenant 审计账本。账本包含可信身份、命令
上下文、验证阶段、稳定错误码、内容策略版本和耗时，不包含 Skill 正文或异常消息。安全运维角色可通过
`GET /v1/admin/skill-admissions` 按 `outcome`、`stage`、`content_policy_version` 查询当前 tenant，或通过
`GET /v1/admin/skill-admissions/metrics` 获取按 outcome/策略版本聚合的 count 与平均延迟。产品端不得把
这些管理详情或原始拒绝原因返回给普通终端用户。

列表还接受 timezone-aware `since`、`cursor` 和 `limit`（1–500），响应返回 `next_cursor`。继续翻页时
必须保持相同 filters 与 since。指标端点接受 `window_hours`（1–2160），同时返回 quarantine ratio 和
`ok|firing|insufficient_data` 告警状态；低于最小样本数不会告警。

签名通过后，服务端还会扫描可执行扩展与 magic、高置信凭据、Secret 赋值和 Prompt Injection 模式。
命中后 API 以受控 `skill_content_*` 策略错误拒绝，本次 admission 在内部记为 `quarantined`，但不会把
不可信内容创建为 Package/Publication/Installation。客户端不得根据具体 finding 自动修改或重发正文；
应由 Publisher 在本地清除风险、重新签名并以新命令提交。

```bash
curl -sS 'http://127.0.0.1:8000/v1/admin/skill-admissions?outcome=quarantined&content_policy_version=skill-content-v1' \
  -H 'X-Tenant-ID: local' \
  -H 'X-Actor-ID: security-operator'
```

策略版本用于解释历史 finding 和灰度期间的结果差异，不表示隔离内容可以恢复或启用。修复后的内容仍须
重新签名并以新 Idempotency-Key 发布。

```bash
curl -sS 'http://127.0.0.1:8000/v1/admin/skill-admissions/metrics?window_hours=24' \
  -H 'X-Tenant-ID: local' \
  -H 'X-Actor-ID: security-operator'
```

```bash
curl -sS http://127.0.0.1:8000/v1/admin/skills \
  -H 'X-Tenant-ID: local' \
  -H 'X-Actor-ID: local-user'
```

```json
{
  "skills": [
    {
      "publisher": "platform",
      "name": "release.prepare",
      "version": "1.4.0",
      "status": "active",
      "description": "Prepare an auditable release",
      "risk_level": "medium",
      "package_digest": "sha256:...",
      "required_tools": [{"name": "github.pull_request.get", "version": ">=2,<3"}],
      "required_resources": [{"uri_template": "repo://{repo}/release-policy"}],
      "required_skills": []
    }
  ]
}
```

详情含 `skill_markdown` 与 `versions`。`POST ...:disable` / `:enable` 返回 `202`，需要 `Idempotency-Key`。

### `POST /v1/sessions/{session_id}/approvals/{approval_id}/responses`

人审主路径；完整流程见 [§8](#8-human-in-the-loop人审)。仅当 Session 为 `waiting_for_human`。

```json
{ "decision": "approved", "feedback": "可以执行" }
```

`decision` 只能是 `approved` 或 `rejected`。`feedback` 可选，最长 10000。审批人必须是 Assertion 里的 `user_id`（开发环境是 `X-Actor-ID`）；若审批单指定了 `assigned_approvers`，actor 还必须在名单内。需要 `Idempotency-Key` + `X-Expected-Version`。

```json
{
  "session_id": "ses_...",
  "run_id": "run_...",
  "status": "runnable",
  "run_status": "runnable",
  "approval_id": "apr-...",
  "decision": "approved"
}
```

批准 / 拒绝后 Session 与 Run 都回到 `runnable`。批准后 Runtime 恢复同一工具调用；拒绝后可改计划，不必关闭 Session。
### `GET /v1/streams/{session_id}`

```http
GET /v1/streams/ses_... HTTP/1.1
Accept: text/event-stream
Last-Event-ID: ses_...:12
```

响应：`text/event-stream`，`Cache-Control: no-cache`，`X-Accel-Buffering: no`（反代不要缓冲）。

```text
id: ses_abc:13
event: model.output.delta
data: {"event_id":"rte_...","session_id":"ses_abc","run_id":"run_...","sequence":13,"type":"model.output.delta","payload":{"delta":"正在分析"},"visibility":"user"}
```

规则：

- 只推 `visibility=user` 的 Runtime / Canonical 事件；聊天增量主事件是 `model.output.delta`（`payload.delta`）。
- 人审时可能收到 `approval.requested`（含 `approval_id` 等）；仍只作通知，响应必须走审批写接口。
- 游标 `session_id:sequence`，同一 Session 多轮 Run 单调递增。
- 无 `Last-Event-ID` 会回放保留窗口。
- 游标过期时收到 `event: stream.reset`，`data.reason=cursor_expired`，应回退 Task / Result API。
- Kafka offset 不对外。SSE 不是结果交付保证。

### `GET /v1/operations/sessions/{session_id}/timeline`

运维 / 排障。聊天恢复请用 transcript，不要把 Timeline 当主路径。

`kind`：`canonical_event` / `trace_span` / `audit_event` / `alert`。敏感字段已脱敏。

### `GET /v1/operations/metrics`

返回无租户标签或当前租户的指标快照，例如 `http.request.duration_ms`。

---

## 10. 错误体

统一：

```json
{ "code": "version_conflict", "message": "...", "detail": null }
```

| HTTP | code | 含义 |
|---|---|---|
| 401 | `missing_credential` / `invalid_signature` / `expired` / `workload_mismatch` / `replayed` / … | 未认证或 Assertion 无效 |
| 403 | `scope_denied` / `tenant_session_mismatch` / `policy_denied` | 已认证但无权限或身份冲突 |
| 404 | `not_found` | Session / 审批不存在，或跨租户 |
| 409 | `version_conflict` | `X-Expected-Version` 过期，刷新 Task 再试 |
| 409 | `invalid_transition` | 当前状态不允许该命令（例如 closed 后再发消息；非 `waiting_for_human` 时提交审批） |
| 409 | `approval_invalid` | 审批已处理、已过期、digest/策略不匹配，或 actor 不在 `assigned_approvers` |
| 422 | 请求校验失败 | 如 `decision` 不是 `approved` / `rejected` |

---

## 11. chaintower 接入约定

1. 只由已登录用户或已授权的调度器在 chaintower 签发 Assertion，再调 AuraClaw。生产前端不要直连 Task API（仓库内测试台除外）。
2. 创建后把 `session_id` 写进后续 Assertion。
3. 轮询尊重 `Retry-After` 和 `ETag`。
4. UI 可以订 SSE，但完成态、最终答案、是否可追问，只信 Task / Result。
5. 取消停当前生成，关闭结束整段对话。
6. 人审：发现 `waiting_for_human` 后展示 transcript/`pending_approval`，经 Task Gateway 提交 `approved`/`rejected`；不要用 SSE 上行，也不要把 resume 当主路径。详见 [§8](#8-human-in-the-loop人审)。
7. 不要调用 `/internal/v1/*`，不要把用户 access token 转给 AuraClaw。
8. 不要再往 AuraClaw 加用户 / 部门 / 菜单查询。用户禁用、权限版本变化，由 chaintower 在签发时和 MCP 执行时 fail closed。
9. chaintower MCP 走 `workload_trusted_context`，不要给平台 MCP 配“固定租户的 OAuth client_credentials”。

---

## 12. 相关真源

- 身份决策：[ADR-003 用户身份归属与可信上下文](../architecture/decisions/ADR-003-trusted-identity-context.md)
- 跨仓签发任务：[chaintower 身份联调任务](./chaintower-identity-integration.md)
- 人审 / Policy：[Policy Approval Service](../architecture/system/18%20Policy%20Approval%20Service.md)
- 部署边界：[ADR-001](../architecture/decisions/ADR-001-production-service-boundaries.md)
- 代码：`src/auraclaw/api/routes/tasks.py`、`streams.py`、`operations.py`、`health.py`；`src/auraclaw/api/models.py`；`src/auraclaw/domain/approval.py`；`src/auraclaw/infrastructure/identity/verifier.py`
