# 实时能力查询升级（#95）

能力 search/load、Skill resolve/binding-status 是服务端登记的只读权威查询，
其 ToolCapability 设置 `cache_result=False`。该标记不属于 MCP 描述符或模型调用参数，
只有只读能力可以使用。查询仍经过 Schema、容量、Policy、必要审批和受管适配器。

每次查询不读取或写入 ToolGateway 的执行结果缓存，也不进入持久 invocation claim/result
回放路径。普通业务工具默认保留 `cache_result=True`，业务幂等、未知结果恢复和审批不改变。
Canonical Session Events 继续记录 Runtime 实際发生的工具调用；查询响应不是执行授权缓存。

Runtime 主循环与 Skill Runner 为每次逻辑读取生成新的 run-scoped request ID；
内部 HTTP 重传沿用已经构造的请求 ID。恢复后的新检查新建 ID，不从 checkpoint 的旧查询结果
推断可继续执行。失败或非法 disposition 映射为可恢复暂停，不使用旧 continue，也不伪称
权威撤销已经取消了任务。

先部署全部 Action Hands 查询语义，再部署 Runtime；旧 Runtime 固定 ID 在新 Hands 上也读取
当前权威状态。混合 Hands 版本时旧副本仍可能返回缓存，不能宣称撤销链路已完成升级。
本次没有数据库列/事件结构迁移，不删除旧 invocation 结果或业务幂等证据。
回退旧 Hands 会恢复有缺陷的缓存行为，涉及强制撤销的运行必须先暂停，再协调回退。

验收应在真实会话先取得 continue，再修改安装/发布者撤销状态，并检查下一轮 pause/cancel、
引用释放以及事件。独立 SQL 回归会预置旧 continue，重建两个 Gateway 并验证其分别取得
当前 pause/cancel，同时证明旧数据库结果未被清除。查询失败的暂停需要通过既有恢复入口
重试，不能以读取失败作为旧包已无引用的证明。


## 可信 MCP 身份（#100）

Hands 将已验证 lease 的 user_id / dept_id 写入 ToolInvocation，Connector 重建可信上下文时保持这两个值；模型 arguments 不是身份来源。开发用 GatewayToolClient 同步透传。
已有 PostgresControlStore 的 resource_profile JSON 保存可信身份并在分配恢复时解码，signed lease 承载到 HTTP Hands；invocation store 不反序列化运行上下文，而是校验新请求及持久 argument_digest。因此本变更不新增数据库列或迁移。

有用户或部门的调用使用 v2 身份绑定摘要（tenant、root/session、user、dept、role 与原 action digest），同时保护内存/数据库结果重放和审批。Runtime ID 与 run ID 不进入该摘要，允许同一可信业务身份在恢复分配后使用原幂等键；审批仍有独立 run 范围校验。无用户及部门的旧服务调用保留旧摘要。

升级时先更新 Hands，随后 Runtime。旧用户调用的摘要不兼容时返回 idempotency_conflict；保留其执行证据，不能清空记录、换幂等键重发未知写入或从模型参数补填部门。需查明原调用结果并重新授权后才能发起新业务操作。合法无部门保持 None。该策略不改变 #92 的审批模式。
