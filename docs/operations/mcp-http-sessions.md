# 受管 MCP HTTP 会话

Issue #93 的标准协议阶段：2025-06-18 / 2025-11-25 在 Credential Proxy 的
ManagedMcpEgressAdapter 内完成 initialize、无 id 的 notifications/initialized、后续请求和 DELETE。
2026-07-28 保持独立 server/discover 分支；官方 SDK 1.29.1 不能协商此版本，
本阶段未整体替换 SDK，也不引入 LangChain。兼容性 spike 见 mcp-arguments-and-errors.md。

## 生命周期和隔离

每个 adapter 固定 Server/config revision；会话键再绑定可信 tenant/root session/chat session/user/dept
与有效 Bearer 的摘要。Catalog 探测和业务身份不会共用会话；凭据轮换会建立新会话。
Session ID 只存于 Credential Proxy 内存，不进入 Runtime、参数、公开 DTO 或常规日志。
服务端未返回 Session ID 时支持无状态初始化。初始化结果版本必须与配置一致，不静默降级。

同身份的初始化和请求串行执行；最多 128 个身份条目，满时先终止无在途请求的旧条目。
全部繁忙时拒绝新身份请求。通知发送真正无 id 报文，接受 202/204 空响应。
任何 404 会话失效都会丢弃旧会话并初始化新会话，但不重放刚才的业务请求，包括只读请求。
原调用返回 mcp_session_expired / unknown，写操作按 invocation 状态查询恢复。
取消已发送请求时仅尽力发出 notifications/cancelled（最多一秒）；取消不是副作用已回滚的证明。

撤销先阻止入口；关闭等待在途请求退出，逐个 DELETE。200/202/204/404/405 均可完成清理。
关闭失败保留条目，由 #98 重试；迟到 initialize 不能重新开放入口，返回的会话仍会被清理。
所有 POST/DELETE/OAuth 路径仍使用 DNS/IP 固定、网络策略和受管凭据。

SSE 按事件边界合并多行 data，支持空 priming event 和附带通知；单次响应必须包含一个 JSON-RPC
结果/错误且 id 匹配。HTTPX 分块读取受 max_response_bytes 限制，不再读取无限体后才校验。
当前不提供独立 GET 监听流、断线 Last-Event-ID 恢复或客户端 sampling/elicitation；断线结果未知，
不会重新 POST 工具来模拟恢复。需要这些服务端功能时先扩展兼容矩阵，不能宣称已完整支持。

规范参考：[HTTP 会话管理与通知](https://modelcontextprotocol.io/specification/2025-11-25/basic/transports)。

## 发布和验证

无 DDL/外部 DTO 变化。先升级 Credential Proxy，再升级 Hands；会话不跨进程迁移，
切换时按 #98 撤销/drain 原 adapter。不要对未知写调用自动重试。

严格 loopback HTTP 回归覆盖有/无 Session、通知顺序、并发、身份隔离、404 不重放、DELETE/405、
关闭失败重试；独立用例覆盖迟到初始化撤销、取消和多行 SSE。临时 PostgreSQL、真实 HTTP MCP、
AuraMCP、热配置和 invocation 联合 75 passed；全量 669 passed / 56 skipped（取消专项另通过）。
实际测试环境发布与库存工具业务成功验收尚待后续阶段完成。
