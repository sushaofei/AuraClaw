# AuraClaw Operations Console

AuraClaw 的独立纯前端测试与监控工作台。它只调用公开 HTTP/SSE API，不读取数据库、Kafka、后端内部模块，也不把 Runtime Event 当作业务事实。

## 本地运行

需要 Node.js 22.13 或更高版本。

```bash
npm ci
AURACLAW_DEV_API_TARGET=http://127.0.0.1:8000 npm run dev
```

打开开发服务器输出的地址，在页面顶部将 API 地址设为当前站点 Origin 加 `/auraclaw-api`，再配置 tenant 和 actor。该路径只在设置了 `AURACLAW_DEV_API_TARGET` 的本地开发服务器中代理到后端。后端默认可通过以下命令启动：

```bash
uv run uvicorn auraclaw.main:app --reload
```

## 构建与测试

```bash
npm run build
npm test
npm run lint
```

## 部署

应用构建为静态前端资源，可与 AuraClaw API 同源部署，也可通过反向代理转发 `/v1` 和 `/health`。API Base URL 可在页面运行时配置，非敏感配置和最近 Session ID 保存在当前浏览器。

跨域直连需要 AuraClaw 服务预先允许页面 Origin，以及 `GET`、`POST`、`Content-Type`、`X-Tenant-ID`、`X-Actor-ID`、`X-Correlation-ID`、`Idempotency-Key`、`X-Expected-Version`、`If-None-Match` 和 `Last-Event-ID` 等请求头。当前后端没有默认启用 CORS；推荐本地开发代理或同源反向代理，禁止通过关闭浏览器安全策略绕过。

## SSE 排障

- 页面使用 `fetch` 读取 SSE，因此可以携带 tenant/actor 和 `Last-Event-ID`。
- 状态为 `reconnecting` 时会指数退避重连，最长等待 10 秒。
- 收到 `stream.reset` 表示回放游标已过期；应刷新 Task View，以 Task/Result API 的最终状态为准。
- 代理层必须关闭响应缓冲，并保持 `text/event-stream` 长连接。

## 安全边界

- 不在 localStorage、URL、控制台或请求历史中保存 Secret。
- 复制 curl 时会移除敏感 Header，并递归脱敏常见敏感字段。
- 取消、恢复和审批操作会展示 tenant/session 并要求二次确认。
- 前端不提供 Projection 重建、DLQ 重投、Retention 或 GC 操作。
