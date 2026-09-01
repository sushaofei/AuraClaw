# Model Gateway / Inference Service

## 定位

Model Gateway 是 Agent Runtime 与外部或本地模型之间的统一访问边界。Agent Runtime 中的 `Model` 应理解为 Model Client，真实 Provider、鉴权、路由和配额由 Gateway 管理。

## 核心模块

| 模块 | 功能 |
|---|---|
| Model Registry | 模型、版本、能力、上下文和价格元数据 |
| Routing Policy | 按任务、租户、数据级别和可用性选模型 |
| Provider Adapter | 统一不同 Provider 请求、响应和错误 |
| Streaming Adapter | 统一 Token Delta、Usage 和结束原因 |
| Quota / Rate Limit | 租户、Session、模型和预算限制 |
| Retry / Fallback | 只对安全错误重试或切换模型 |
| Safety Filter | 输入输出策略和敏感数据约束 |
| Usage Accounting | Token、延迟、成本和缓存命中 |
| Request Correlation | model_call_id、run_id、trace_id |
| Prompt Cache Policy | 安全的缓存键、隔离和失效 |

## 接口

```text
generate(modelPolicy, messages, tools, budget)
stream(modelPolicy, messages, tools, budget)
embed(modelPolicy, inputs)
cancel(modelCallId)
getUsage(modelCallId)
```

统一响应：

```text
model_call_id
provider / model / version
output_delta | completed_output
tool_calls
finish_reason
usage
latency
error
```

## 路由与回退

- Model Policy 描述能力和限制，不让 Harness 硬编码 Provider。
- Fallback 必须评估上下文兼容、工具协议和数据驻留。
- 已产生部分外部副作用后，不能简单重跑整个 Agent Step。
- Provider 超时重试使用稳定 `model_call_id`，避免 Usage 重复计算。

## Streaming

Model Gateway 将 Provider Token Stream 返回 Agent Runtime；Agent Runtime 负责：

- 解析和累计完整输出。
- 向 Runtime Event Bus 发布用户可见 Delta。
- 完成后向 Session 写入完整模型输出。

Model Gateway 不直接连接 Web。

Terminal `completed` 是持久事实的交付结果，不是 Provider stream 的临时信号。Gateway 必须先在同一
Model Call completion 事务中提交 response、final usage 并结算 reservation，成功后才能向 Runtime
发送 terminal event。若 Provider 已完成但持久提交失败，stream 以错误结束且不得声称 completed；
已发送 delta 不构成最终结果，调用进入后续持久 lifecycle/reconciliation 处理。

每个执行中的 Model Call 由 PostgreSQL 中的 `execution_owner + claim_token` 唯一约束，owner 定期续租
`heartbeat_at/claim_expires_at`。任意 Gateway 副本收到认证后的 cancel 都只先写共享
`cancel_requested`；owner 从心跳观察到请求后调用 Provider Adapter 的协作取消，确认本地 Provider
task 已停止且拿到权威 final usage 后才提交 `cancelled`、结算实际 usage 并释放剩余 token reservation。Provider 不支持取消时响应明确标记
`provider_cancellable=false`，不得把“已请求”谎报为“已取消”；若 Provider 已自然完成，durable
`completed` 可以赢得竞态。

OpenAI-compatible HTTP stream 可协作停止本地 task，但中途断流通常没有权威 partial usage，因此进入
`reconciling(cancel_usage_unknown)` 并保留 reservation，而不是把已消耗成本当作零。客户端断开不等于
业务取消。断开、owner/claim 丢失或 Provider 已返回但结果能否生效不确定时，调用进入
`reconciling`，保留额度 reservation 且禁止自动重放，等待 Provider 查询能力或人工核对。`completed` 与
`cancelled` 都是单调终态，迟到的旧 claim 不能覆盖终态；tenant、run 和 request digest 不匹配均
fail closed。Model Call 保存 actor/service identity、correlation/causation、Provider correlation ref、
安全 error code 与各阶段时间；运维指标从状态、heartbeat age、cancel latency、reconciliation backlog
和 usage reservation/settlement 聚合，日志不作为唯一审计事实。

## 安全

- Provider Secret 只存在 Gateway/Vault 信任域。
- 按数据分类限制可用 Provider、区域和日志策略。
- Prompt/Response 日志默认保存摘要或受控 Artifact，不无条件明文记录。
- Tool Schema 和 Model 输出都需大小、格式和注入风险检查。

## 观测指标

```text
model_latency_ttft / total
tokens_in / tokens_out
cost
provider_error_rate
fallback_rate
rate_limit
stream_disconnect
cache_hit
```

OpenAI-compatible Chat Completions Adapter 默认只使用 Provider 的隐式 prefix cache；只有显式声明
`AURACLAW_MODEL_PROMPT_CACHE_KEY_ENABLED=true` 才发送 Runtime 提供的 tenant 隔离稳定 key，避免未知
字段破坏第三方兼容服务。Gateway 将 `prompt_tokens_details.cached_tokens` 和 `cache_write_tokens` 归一化为
`cached_input_tokens`/`cache_write_input_tokens`，并记录：

```text
model.prompt_cache.cached_input_tokens
model.prompt_cache.write_input_tokens
model.prompt_cache.hit_ratio
model.ttft.seconds
```

cache key 和 Runtime 旁路指标不参与 Model Call request digest；它们只改变性能与观测，不能让同一业务请求
因重试时的耗时或 cache 状态变化产生幂等冲突。不能用配置开关代替实际 usage：只有 Provider 返回的 cached
tokens 才算命中。

## 验收条件

- 更换 Provider 不需要修改 Session、Tool Gateway 或 Orchestrator。
- Agent Runtime 无法读取 Provider Secret。
- 完整模型输出最终进入 Session，Token Delta 只进入 Runtime Bus。
- 租户预算耗尽时产生可解释的 Policy/Failure Event。
- 跨副本取消由持久 owner/heartbeat 协作完成；断线和未知 Provider 结果不会被误记为 cancelled 或自动重放。
