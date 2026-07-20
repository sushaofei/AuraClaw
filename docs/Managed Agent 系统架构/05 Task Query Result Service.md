# Task Query / Result Service

## 定位

Task Query / Result Service 是任务状态、运行记录、结果、Child Session 和 Artifact 的只读 API。它读取 Read Model Store，不扫描 Canonical Event Log，也不修改 Session。

## 核心模块

```text
Authentication / Query Authorization
Task View Assembler
Result View Assembler
Child Session Query
Artifact Reference Resolver
Pagination / Filtering
ETag / Conditional GET
Projection Freshness Guard
Response Redaction
```

## API

```http
GET /v1/tasks/{root_session_id}
GET /v1/tasks/{session_id}/runs/{run_id}
GET /v1/tasks/{session_id}/runs/{run_id}/result
GET /v1/tasks/{session_id}/children
GET /v1/tasks/{session_id}/artifacts
GET /v1/approvals/{approval_id}
```

任务响应包含：

```json
{
  "session_id": "ses_123",
  "status": "running",
  "run_id": "run_456",
  "run_status": "running",
  "progress": 0.6,
  "current_stage": "review",
  "projection_version": 42,
  "result": null,
  "links": {
    "stream": "/v1/streams/ses_123",
    "children": "/v1/tasks/ses_123/children"
  }
}
```

`status` 描述 Session 生命周期，`run_status` 描述当前或最近一次 Run。兼容的
`GET /v1/tasks/{session_id}/result` 返回最新 Run 的结果，响应中的 `run_id` 明确关联该结果，
`status` 表示 Run 状态，`session_status` 表示 Session 状态。新 Run 请求后，最新结果字段在
投影中清空，避免把上一轮结果误认为当前轮结果。

## 轮询治理

- 支持 `ETag / If-None-Match`，无变化返回 `304`。
- 返回 `Retry-After`，避免客户端固定高频轮询。
- 支持 `min_version`；投影尚未追上时返回 `202` 或受控等待。
- 大列表使用 Cursor Pagination，不使用不稳定 Offset Pagination。
- Root 查询默认返回聚合结果，Child 详情显式查询。

## Artifact 访问

Query Service 只返回：

- Artifact 元数据。
- 权限校验后的短期下载链接。
- 内容类型、大小、Hash 和版本。

它不代理无限大小的文件流，也不暴露底层 Bucket 路径或永久 URL。

## 一致性与失败

- Read Model 不可用时返回明确的可重试错误，不回退扫描 Session Log。
- Projection 落后必须在响应中体现 `projection_version` 和新鲜度。
- 查询操作无业务副作用。
- 结果脱敏策略与 Streaming、Delivery 保持一致。

## 观测指标

```text
query_latency
status_poll_rate
conditional_get_hit_rate
projection_staleness
artifact_link_issued
authorization_denied
large_response_total
```

## 验收条件

- API Client 可以只通过查询接口获取最终结果。
- Timer 不需要轮询；历史任务仍可通过本服务查询。
- 无权限用户无法通过 Child 或 Artifact 接口绕过 Root 权限。
- 查询接口不会改变任务状态。
