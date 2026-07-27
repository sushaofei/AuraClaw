# AuraClaw Protocol Test Console

AuraClaw 的独立纯前端测试与监控工作台。它只调用公开 HTTP/SSE API，不读取数据库、Kafka、后端内部模块，也不把 Runtime Event 当作业务事实。

## 协议测试页面

- **智能问答**：首次提问创建 Session 并自动连接 SSE，将 `model.output.delta` 按事件游标去重合并；断线携带 `Last-Event-ID` 重连，每轮 Run 终态后使用 Task / Result API 核对最终回答。后续追问在同一 Session 追加消息并创建新 Run；只有显式关闭的 Session 才会为新问题创建新的 Session。
- **打断与恢复**：生成中可「停止生成」调用 cancel；左侧「历史会话」按 tenant 保存在浏览器本地的 Session 索引（`session_id` / 标题摘要 / 状态 / 时间），不存问答正文。点击可恢复并重连；也支持粘贴 Session ID。恢复时从 Session Timeline 的 Canonical Event（`session.created` / `user.message.appended` / `model.output.completed`）按时间序重建完整多轮对话，Timeline 不可用时回退到 Goal / Result。
- **Human-in-the-loop**：当 Task 进入 `waiting_for_human` 时，从 SSE 或 Timeline 解析 `approval_id`，在对话内展示审批卡片并支持批准/拒绝，随后继续同一 Session 的 Runtime。
- **创建任务**：并列展示脱敏后的 Query 请求、权威 Task View 和 Result；轮询遵循 `Retry-After`，条件查询使用 `ETag / If-None-Match`，支持 Session ID 恢复、手动刷新、停止自动轮询和复制脱敏 JSON。

两个入口使用 `#chat` 与 `#create` 页面锚点，刷新或直接访问时会恢复当前功能页；不会把问答正文写入浏览器持久化存储。

## 本地运行

需要 Node.js 22.13 或更高版本。

### 仅前端联调远程生产栈（推荐 / 默认）

后端调试统一使用 `.host.env` 中的 `AURACLAW_HOST`（当前为 `10.244.16.131`）上的 Compose 容器，不要再本地起 Python 后端服务。

读取仓库根目录 `.host.env` 的 `AURACLAW_HOST`，代理到 `http://<host>:8080`（无需 SSH 隧道；会自动设置 `NO_PROXY` 绕过本机 HTTP 代理）：

```bash
npm ci
npm run dev:remote
```

Cursor / VS Code：

- Debug：**AuraClaw: Frontend → remote containers (10.244.16.131)**（或 compound **AuraClaw: Debug against remote containers**）
- Tasks：`Remote containers: ps` / `logs` / `restart` / `health`

打开 http://localhost:3000 ，页面顶部 API endpoint 默认为：

`http://localhost:3000/auraclaw-api`

Tenant / Actor 默认 `local` / `local-user`，点「检查连接」即可。

### 前端 + 本地后端（已弃用，仅特殊需要）

本地后端 launch 配置已隐藏。若必须本地联调：

```bash
AURACLAW_DEV_API_TARGET=http://127.0.0.1:8000 npm run dev
uv run uvicorn auraclaw.main:app --reload
```

`/auraclaw-api` 只在设置了 `AURACLAW_DEV_API_TARGET`（或 `npm run dev:remote`）时由开发服务器代理到后端。

后端始终使用统一 Runtime Worker、Model Gateway 与 Runtime Event 发布链；本地和部署环境
只通过各自 `.env.development` / `.env.production` 文件选择资源，文件内容不含环境标签。
已部署外部 Runtime 时设置 `AURACLAW_RUNTIME_ENABLED=false`。

## 构建与测试

```bash
npm run build
npm test
npm run lint
```

## 部署

应用构建为静态前端资源，可与 AuraClaw API 同源部署，也可通过反向代理转发 `/v1` 和 `/health`。API Base URL 可在页面运行时配置，非敏感配置和最近 Session ID 保存在当前浏览器。

跨域直连需要 AuraClaw 服务允许页面 Origin，以及 `GET`、`POST`、`Content-Type`、`X-Tenant-ID`、`X-Actor-ID`、`X-Correlation-ID`、`Idempotency-Key`、`X-Expected-Version`、`If-None-Match` 和 `Last-Event-ID` 等请求头。本地 `3000` 端口来源默认允许；额外来源通过逗号分隔的 `AURACLAW_CORS_ALLOW_ORIGINS` 配置。生产仍推荐同源反向代理，禁止通过关闭浏览器安全策略绕过。

## SSE 排障

- 页面使用 `fetch` 读取 SSE，因此可以携带 tenant/actor 和 `Last-Event-ID`。
- 无游标首次连接会回放当前保留窗口，避免任务在 SSE 建连前已输出时丢失开头内容。
- 状态为 `reconnecting` 时会指数退避重连，最长等待 10 秒。
- 收到 `stream.reset` 表示回放游标已过期；应刷新 Task View，以 Task/Result API 的最终状态为准。
- Task View 的 `status` 是 Session 状态，`run_status` 是当前或最近一次 Run 状态；Result 的 `status` 对应其 `run_id`，`session_status` 表示 Session 是否仍可继续。
- 代理层必须关闭响应缓冲，并保持 `text/event-stream` 长连接。

## 安全边界

- 不在 localStorage、URL、控制台或请求历史中保存 Secret。
- 复制 curl 时会移除敏感 Header，并递归脱敏常见敏感字段。
- 取消、恢复和审批操作会展示 tenant/session 并要求二次确认。
- 智能问答的历史会话列表只保存 Session 索引元数据，不保存消息正文或 Result 全文。
- 前端不提供 Projection 重建、DLQ 重投、Retention 或 GC 操作。
