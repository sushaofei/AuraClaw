# MCP 完整目标路由（#97）

第三方 Tool 名称保持不变。目录 load 额外返回 CapabilityInvocationRef，包含 tenant、server、capability ID、version、content digest 和 config revision；模型 function name 使用该引用 SHA-256 的稳定别名（mcp_ + 48 hex，52 字符）。完整引用保存在已加载目录/checkpoint，别名通过现有 ToolCall/Hands name 字段传输；不新增严格 HTTP DTO 字段。Hands 用当前注册表解析别名，校验可信 tenant，再向 Connector 发送原 canonical name。

Registry 与 Router 使用同一别名键。Legacy name+version 仅在可信租户/平台可见范围唯一时解析；同名多 Server 返回 ambiguous_capability。内建已注册的直接名称保持独立路由；同名远端通过别名访问。旧版本或旧配置绑定被当前注册表移除，返回 stale_capability，不调用远端新契约。标准 MCP tools/call 不支持选择历史版本，AuraClaw 不模拟这一能力。

有效目标加入 invocation action digest，保护本地/持久执行结果与人工、自动审批。同 ID 不同 Server 返回 idempotency_conflict；保留旧执行证据，不更换写 ID 重发。Workflow 核对加载引用与已绑定 capability/version/schema digest，防止固定工作流静默换契约。无新增数据库列，持久 argument_digest 负责复核恢复请求。

发布先构造和校验候选 Registry，随后使用既有目录 lease/generation CAS 提交。Registry/Router 在同一同步段切换，无 await 暴露半套键；本地安装失败则撤销本机路由、清除 ready snapshot 并保留 dirty 重建请求，不将旧契约接到新 Server。共享数据库与内存不是同一事务，冷副本重建和撤销收敛继续由 #99/#98 联合验收。

部署顺序：先 Hands 与其目录发布/路由组件，后 Runtime；旧 Runtime 可使用唯一裸名称，歧义必须重新 load 取得别名。旧目录缺配置修订元数据应重新发现后再发布。含 MCP 目标的新摘要使旧审批/旧结果不能用于新引用：需要按原调用证据恢复或重新授权，不能清理幂等记录。执行活动同时提供 canonical name 和完整引用，展示不必暴露 opaque alias。
