# Hands Service

## 定位

Hands Service 承载工具的实际执行环境和适配器。它执行 Tool Gateway 下发的标准化调用，可以是 Sandbox、Browser、File Runtime、MCP Adapter 或企业系统 Connector。

## 核心模块

| 模块 | 功能 |
|---|---|
| Tool Executor | 调用具体执行器并返回标准结果 |
| Sandbox Manager | 隔离进程、文件系统、CPU、内存和时间 |
| Browser / File Runtime | 浏览器、文件和桌面能力 |
| MCP Adapter | 下游 ManagedMcpConnector，协议转换后进入 Hands DTO |
| Java API Connector | 已注册 operation 的受管 REST 调用 |
| Connector Adapter | 企业 API、数据库和消息系统适配 |
| Resource Limit | 配额、超时、并发和输出大小 |
| Network Egress Policy | 域名、地址、协议和数据外发限制 |
| Cancellation | 传播取消并终止可停止动作 |
| Artifact I/O | 输入挂载、输出收集和受控引用 |
| Runtime Health | 心跳、探针和故障分类 |

`External Systems` 不应作为 Hands 内部模块；它是 Hands 调用的外部依赖。

## 执行契约

```text
execute(invocation, runtimeContext)
  -> accepted
  -> progress events
  -> result | error | timeout | cancelled | unknown
```

Runtime Context 包含：Sandbox Spec、网络策略、Tool Permission、Artifact Mount、Deadline、Fencing Token 和受控 Credential Reference。

## Sandbox 恢复

- Sandbox 是可替换资源，不是任务事实源。
- 关键文件进入 Artifact Store，环境规格进入 Provisioning Spec。
- Sandbox 死亡产生 tool/runtime failure event。
- Orchestrator 重建环境；Agent 根据副作用状态决定是否重试动作。
- 临时缓存允许丢失，Secret 不能进入环境变量或可读文件。

## 外部系统调用

```text
Hands / Tool Adapter
 -> Credential Proxy
 -> Vault 获取受控凭证
 -> External System
 -> 脱敏标准结果
```

对于数据库、GitHub、SaaS 和通知系统，应显式区分读、写、删除和管理员能力。

## Artifact

- 输入 Artifact 以只读或 Copy-on-write 方式挂载。
- 输出文件收集后计算 Hash、扫描并写入 Artifact Store。
- Tool Result 只返回 `artifact_ref` 和摘要。
- 外部系统写入结果记录 resource id、version 和 side-effect status。

## 观测指标

```text
sandbox_startup
execution_latency
resource_exhausted
network_denied
runtime_crash
cancel_latency
artifact_bytes
external_error_rate
```

## 验收条件

- Sandbox 被攻破时仍无法读取真实凭证。
- Sandbox 死亡后 Session 和 Artifact 不丢失。
- Tool Gateway 可以取消长时间执行。
- 外部系统响应通过 Hands → Tool Gateway → Agent Runtime 返回。
