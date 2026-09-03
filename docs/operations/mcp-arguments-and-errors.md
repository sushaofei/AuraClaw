# MCP 入参和错误契约

Issue #93 的入参/错误阶段将远端 inputSchema 原样保存在目录并提供给模型。
模型根据 schema 与任务上下文生成 arguments；声明 input 对象时必须传相同结构。
Connector 只翻译显式配置的工具名称，不再根据 required=[input] 猜测包装，不补业务值、不注入 default、不转换类型。
管理测试与 ToolGateway 共用校验入口。旧扁平直调调用者应改为公开 schema 的形状；已加载的旧会话不会被悄悄改写。
缺失、非法 JSON、数组或双重编码字符串的模型 arguments 明确失败，不转换为 {}。
Schema 本身不包含业务字段值；允许的空 input 与业务能否采用默认范围必须由下游工具契约说明。

## 校验实现

采用 jsonschema 4.26.0（兼容范围 >=4.26,<5），正则采用 regex（锁文件固定版本），默认方言 2020-12，支持 2019-09、draft-07。
支持联合 type、组合约束、本地 $ref/$defs/definitions、布尔子 schema、additionalProperties 的 schema 形式及字符串/数值/数组约束。
外部引用和未知方言明确拒绝；format 默认作为 annotation。无隐式联网、类型转换或 default 注入。
Schema 限 256 KiB/64 层/10000 节点，实例限 64 层/50000 节点，验证限 10000 工作单元，最多返回 8 个错误，validator LRU 最多 256 项。
正则限 4096 字符、单次匹配 20ms；patternProperties 与 unevaluatedProperties 同时出现的 schema 暂不支持，明确拒绝以维持限时保证。
无效 schema 在目录接纳/Registry 更新阶段拒绝；tool_schema_definition_invalid 与 tool_schema_invalid 分开。

## 错误

| code | 阶段 | 副作用含义 |
| --- | --- | --- |
| tool_schema_invalid | argument_validation | 调用未发出，not_started |
| tool_schema_definition_invalid | schema admission/validation | 发布者修正 schema |
| mcp_jsonrpc_error | protocol | 保留 remote_code，unknown |
| mcp_tool_error | remote_tool | 远端 isError，unknown |
| mcp_protocol_error | protocol | 畸形 JSON/结果/ID，unknown |
| mcp_http_error / mcp_network_error | transport | HTTP 状态/网络失败，unknown |
| tool_output_schema_invalid | output_validation | 调用已发出，unknown |
| tool_adapter_error | dispatch | 未分类程序异常，以 invocation ID 关联诊断 |

分类和字段路径放在现有 metadata.error_details，通过 Credential HTTP、Hands、invocation store、Canonical tool.call.completed 和下一轮模型消息保留。
retryable 不代表可以重新执行写操作。未知结果不推断为未执行；原有审批、幂等和 fencing 不变。
错误文本先移除凭据、URL、SQL/堆栈片段并限制长度；JSON Schema 错误只返回字段路径/规则，不回显实例值。
未知异常的普通日志不记录异常 payload/完整堆栈。不要根据历史通用 tool_adapter_error 猜写历史根因。

默认把 structuredContent 当业务对象，即使它恰好带 status 字段。需要旧 AuraClaw 结果信封的明确适配器必须配置
server metadata.result_envelope=auraclaw-v1；普通 MCP 不根据 status 猜内部信封。

## SDK 决策与后续协议阶段

官方 Python SDK 1.29.1 的 ClientSession 内存通道 spike 接受 2025-06-18、2025-11-25 并发送 notifications/initialized，
拒绝 2026-07-28。复现实验：独立安装 mcp==1.29.1 后运行 scripts/mcp_sdk_compatibility_spike.py。
本阶段不整体替换 ClientSession：项目还需兼容 server/discover 分支及 Credential Proxy 的逐请求身份与固定 IP 出口。
默认 SDK HTTP transport 不能直接绕过这些边界；完整迁移须通过后续会话/安全出口矩阵。
协议握手、session ID、404 恢复、DELETE/405 和 SSE 继续在 #93 的协议阶段实施，本阶段不宣称已完成。

LangChain 不作为本次修复前置依赖。它不会从 optional properties 推导业务值，也不会修正服务端 schema 或恢复上层已丢弃的错误。
业务编排仍归 Runtime/Coordinator，成熟库用于标准协议和 schema 等边界。
依据：[jsonschema 验证文档](https://python-jsonschema.readthedocs.io/en/stable/validate/)、
[官方 MCP Python SDK](https://github.com/modelcontextprotocol/python-sdk)。

## 升级与验证

无 DDL；metadata 向后兼容。先升级 Credential Proxy 对受控 MCP 错误的响应，再协调升级 Hands/Runtime；
调用者迁移嵌套参数后切换，避免混合版本随机包装。保留历史 invocation 去重记录，不重新执行旧 Run。
全量 665 passed / 56 skipped；最终真实模型 HTTP schema/分片参数 → 冷副本 → HTTP Hands → MCP 及 PostgreSQL/AuraMCP 联合 12 passed。
Canonical 错误 → 下一轮模型纠错 → 成功回归通过。测试数据库 URL 必须显式指定，不依赖单测夹具重置后的默认数据库。
原测试环境业务 Session 的成功查询及协议生命周期验收仍待完成，issue 保持开放。
