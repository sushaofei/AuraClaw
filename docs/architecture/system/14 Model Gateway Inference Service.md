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

## 验收条件

- 更换 Provider 不需要修改 Session、Tool Gateway 或 Orchestrator。
- Agent Runtime 无法读取 Provider Secret。
- 完整模型输出最终进入 Session，Token Delta 只进入 Runtime Bus。
- 租户预算耗尽时产生可解释的 Policy/Failure Event。
