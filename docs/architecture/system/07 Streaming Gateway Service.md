# Streaming Gateway Service

## 定位

Streaming Gateway 将 Runtime Event Bus 的消费流桥接为浏览器或 API Client 的 SSE / WebSocket 连接。它负责实时展示，不负责创建、取消、审批或调度任务。

## 技术链路

```text
Runtime Event Bus
  -- Kafka Consumer Fetch / Offset -->
Streaming Gateway
  -- SSE / WebSocket -->
Web / API Client
```

Kafka 到 Gateway 不是 SSE，而是持续 Consumer Poll/Fetch；SSE 或 WebSocket 只用于 Gateway 到客户端。

## 核心模块

| 模块 | 功能 |
|---|---|
| Consumer Adapter | 消费 Runtime Topic，提交 Offset |
| Subscription Authorization | 校验调用方可订阅的 Root/Child Session |
| Connection Registry | 保存 connection、user、session 和所在实例 |
| Event Router | 按 root/session/run 分发到目标连接 |
| Ordering / Dedupe | 按 sequence 排序并用 event_id 去重 |
| Replay Manager | 处理 Last-Event-ID 和短期重放 |
| Token Coalescer | 合并过细模型增量 |
| Backpressure Manager | 有界队列、降级、采样和慢连接断开 |
| Redaction / Visibility | 过滤 internal、secret 和敏感 payload |
| SSE / WebSocket Writer | 心跳、Flush、关闭和错误帧 |

## 接口

```http
GET /v1/streams/{root_session_id}
GET /v1/streams/{session_id}?run_id=...
GET /v1/streams/{session_id}?after=cursor
WebSocket /v1/streams
```

SSE 示例：

```text
id: ses_123:128
event: model.output.delta
data: {"delta":"正在分析"}
```

## Root 与 Child Stream

- Root Stream：任务级进度、Child 生命周期、审批和最终结果。
- Child Stream：特定 Agent 的 Token、工具过程和详细状态。
- 默认不把多个 Child 的 Token 混在 Root Stream。

## 集群路由

生产集群需要解决“事件由 Gateway A 消费，但连接位于 Gateway B”：

```text
Kafka Consumer Group
  -> Stream Ingestor
  -> Connection Router / Internal PubSub
  -> Connection Owner Gateway
```

Connection Registry 保存实例所有权和过期时间。不能为每个浏览器建立 Kafka Consumer，也不建议每个 Gateway 独立消费全部 Topic。

每个 Gateway 启动时以 `owner_id + generation` 取得实例所有权。相同 owner 的未过期 generation 禁止被
覆盖；只有 TTL 过期后新 generation 才能接管。实例心跳与连接心跳分开持久化，连接更新必须同时匹配
owner generation，旧进程失去所有权后 readiness 立即降级并拒绝新订阅。

## 断线重连

1. 客户端携带 `Last-Event-ID`。
2. Gateway 从 Replay Buffer 重放缺失事件。
3. 如果游标超过保留期，返回最近持久化消息或要求客户端查询当前 Projection。
4. 继续实时消费。

关闭连接不等于取消任务；取消必须调用 Task Gateway。

## Drain、接管与孤儿回收

1. LB 先依据 readiness 摘除目标实例；Gateway 进入 `draining` 后以 503 + Retry-After 拒绝新 SSE。
2. 进程终止钩子关闭本地订阅，客户端使用最后收到的 `Last-Event-ID` 连接其他 active Gateway；TCP/SSE
   本身不跨进程迁移。
3. 新 Gateway 从共享 replay store 接续 sequence；Runtime Event 不用于补写 Canonical Result。
4. 后台实例心跳定期删除 TTL 到期、owner 不存在或 generation 不匹配的 connection_registry 行。
5. 正常关闭只删除当前 generation 的记录，不能误删同 owner 已接管的新 generation。

`AURACLAW_STREAMING_CONNECTION_TTL_SECONDS` 必须大于两倍实例心跳间隔；drain timeout 控制旧实例注册
最长保留时间，Retry-After 引导客户端退避。Compose 为 Streaming Gateway 单独保留 90 秒退出窗口。

## Offset 与交付语义

- Kafka Offset 是 Gateway 内部消费位置。
- `session_seq` 是业务显示顺序。
- Last-Event-ID 是公开重连游标。
- Gateway 在事件进入可恢复的内部路由或缓冲后提交 Offset，不能等待所有浏览器 ACK 阻塞整个分区。
- 采用 at-least-once + event_id 去重。

## 观测指标

```text
active_connections
subscribe_denied
event_to_client_latency
connection_queue_depth
slow_consumer_disconnect
replay_success / replay_miss
sequence_gap
token_coalesce_ratio
```

## 验收条件

- Gateway 发布或重启不会取消任务。
- 未授权连接无法猜测 session_id 订阅。
- 慢客户端不会拖慢 Kafka 分区和其他用户。
- Human Response 不通过本服务写入 Session。

## 当前实现对照

- 归属：`gateways/streaming/gateway.py`、`api/routes/streams.py` 和 Kafka Streaming Ingestor，部署在
  `streaming-gateway`。
- 已实现 SSE、公开 cursor/Last-Event-ID replay、tenant/session 过滤、连接队列上限、delta 合并节流和
  PostgreSQL connection registry。
- 已实现 Gateway generation fencing、动态 readiness、显式 drain、SSE 断开重连、过期接管和孤儿连接
  sweep；共享 replay 是跨副本接续事实，连接注册表不承担业务结果交付。
- 网关没有 Session 写依赖；客户端上行命令仍回到 Task API。

## 现有缺陷与待完善

- 当前公开协议只有 SSE，没有 WebSocket；连接迁移、跨实例定向路由主要依赖共享 ingest/replay，不是专用 router。
- 慢消费者处理以有界队列和断连为主，缺少面向客户端的细粒度 backpressure/优先级协议。
- 待补：代理/LB 缓冲配置验证、百万级连接容量基线、断线风暴演练和 cursor 兼容策略。
