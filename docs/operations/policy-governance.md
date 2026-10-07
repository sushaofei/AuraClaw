# Policy 约束治理

AuraClaw 的 Budget、模型选择、数据驻留和 Artifact 分享由 Policy Service 统一裁决。调用方提供请求值，
执行服务只消费 Policy 返回的约束；不能用本地默认值覆盖策略结果。

## 配置与执行点

- Runtime Budget：Policy 从 `AURACLAW_RUNTIME_*` 生成版本化预算快照。Task API 在每次创建 Task 和请求
  新 Run 时重新取值，并把该快照写入 Canonical `session.created` / `run.requested` 事实。
- Model：Policy 固定 `AURACLAW_MODEL_PROVIDER`、`AURACLAW_MODEL_NAME` 和
  `AURACLAW_MODEL_DATA_REGION`。区域必须出现在 `AURACLAW_POLICY_ALLOWED_DATA_REGIONS` 中。Model
  Gateway 在 token/cost 预留和 Provider 调用前应用约束；冲突请求 fail closed。
- Artifact 分享：`POST /v1/artifacts/{artifact_id}/shares` 只接受已认证用户身份。Artifact Service 使用
  自己持久化的 classification 进行裁决，不信任客户端声明；Policy 按
  `AURACLAW_ARTIFACT_SHARE_CLASSIFICATIONS` 拒绝敏感级别，并用
  `AURACLAW_ARTIFACT_SHARE_MAX_TTL_SECONDS` 缩短预签名 URL 有效期。

生产变更必须同步更新 Policy 与执行服务，先验证拒绝路径，再验证约束值确实出现在 Canonical Event、
Model Provider 请求或预签名 URL 中。Policy 不可用、约束缺失、区域不匹配、空 audience 或非法 TTL
全部拒绝，不降级为本地放行。

分享 URL 是租户内 Artifact 的短期 bearer URL；audience 参与 Policy 决策和审计，但对象存储不会在下载时
二次认证该 audience。调用方必须通过目标渠道安全投递，且不得把 URL 写入日志或 Canonical Event；过期后
需要重新走 Policy，不提供永久公开链接。
