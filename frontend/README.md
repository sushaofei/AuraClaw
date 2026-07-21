# AuraClaw Protocol Test Console

AuraClaw 的独立纯前端测试与监控工作台。它只调用公开 HTTP/SSE API，不读取数据库、Kafka、后端内部模块，也不把 Runtime Event 当作业务事实。

## 协议测试页面

- **智能问答**：首次提问创建 Session 并自动连接 SSE，将 `model.output.delta` 按事件游标去重合并；断线携带 `Last-Event-ID` 重连，每轮 Run 终态后使用 Task / Result API 核对最终回答。后续追问在同一 Session 追加消息并创建新 Run；只有显式关闭的 Session 才会为新问题创建新的 Session。
- **创建任务**：并列展示脱敏后的 Query 请求、权威 Task View 和 Result；轮询遵循 `Retry-After`，条件查询使用 `ETag / If-None-Match`，支持 Session ID 恢复、手动刷新、停止自动轮询和复制脱敏 JSON。

两个入口使用 `#chat` 与 `#create` 页面锚点，刷新或直接访问时会恢复当前功能页；不会把问答正文写入浏览器持久化存储。

## 本地运行

需要 Node.js 22.13 或更高版本。

```bash
npm ci
AURACLAW_DEV_API_TARGET=http://127.0.0.1:8000 npm run dev
```

打开开发服务器输出的地址，在页面顶部将 API 地址设为当前站点 Origin 加 `/auraclaw-api`，再配置 tenant 和 actor。该路径只在设置了 `AURACLAW_DEV_API_TARGET` 的本地开发服务器中代理到后端。后端用于真实 Streaming 联调时通过以下命令启动：

```bash
uv run uvicorn auraclaw.main:app --reload
```

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
- 前端不提供 Projection 重建、DLQ 重投、Retention 或 GC 操作。
