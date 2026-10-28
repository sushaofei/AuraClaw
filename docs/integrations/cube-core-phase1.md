# Cube Core 语义问数一期

关联 [AuraClaw #108](https://github.com/sushaofei/AuraClaw/issues/108) 与 [AuraMCP #3](https://github.com/sushaofei/AuraMCP/issues/3)。本期复用现有 Runtime、Skill 发布和发现、Hands、Credential Proxy、MCP 目录及 Session Events；只新增可签名的业务查询 Skill 包，不改 Runtime 主循环或业务状态事实源。

## 模块边界

| 模块 | 职责 |
|---|---|
| Runtime + `semantic-query` Skill | 理解自然语言、澄清口径、选择指标和筛选、解释结果 |
| Skill Registry | 发布者 Ed25519 校验、安装启停、锁定包 digest 与工具依赖 |
| Capability Registry / Hands | 发现并准入三个 read-only 工具，传递受信身份，执行与审计 |
| Credential Proxy | 使用既有 AuraMCP workload 凭据；Runtime 不接触密钥 |
| AuraMCP semantic 扩展 | 身份与参数校验、模型版本校验、RS256 身份转发、结果和错误归一化 |
| Cube Core | 独立部署的语义模型、SQL 生成、受控只读源连接和缓存 |
| 既有业务数据源 | 复用原授权连接、关系、字段及账户；与平台治理库隔离 |

模型版本与 Skill 包版本不同。Skill 通过 describe 获取当前模型版本，不硬编码源表名、指标或业务源连接。自然语言到结构化指标查询由 LLM 完成，结构化查询到 SQL 由 Cube Core 完成。

## 发布 Skill

源包在 `examples/skills/semantic-query/`，manifest 为待签名模板，不能直接发布。将目录复制到私有发布目录；用专用 Ed25519 私钥签名，注册同一 publisher 的公钥，再调用既有 Skill publication API 激活。

```bash
# 签名密钥由私有 secret 注入，不写入命令参数、仓库或日志。
uv run auraclaw skills sign <private-skill-directory> \
  --publisher aura-semantic --key-id <registered-key-id>
```

签名命令使用 `AURACLAW_SKILL_SIGNING_KEY`，生成 v2 签名并输出公钥与 digest。发布者和公钥通过 `/v1/admin/skill-publishers` 注册，包通过 `/v1/admin/skill-publications` 发布；操作均使用租户身份、Idempotency-Key 和 expected revision。回读 installation 必须为 active、固定版本且 digest 匹配。

`publisher/name` 是管理路径；当前能力目录的 canonical_name 为 `semantic-query`。Runtime 应依据搜索结果的 capability ID 或 canonical_name 激活，而非假定斜杠限定名可用。不要绕过签名直接装入 Runtime 提示词。

三个依赖为 `auramcp.semantic.discover`、`describe`、`query`，版本范围 `[1.0.0,2.0.0)`。发布前在 Hands 确认已 active/read-only。旧部署若仍有精确 schema digest 策略，只追加这三个工具的已审核准入，保留原 database 工具策略；新版本按自身目录合同准入，不回加已废弃配置。

## 查询行为

先 discover 再 describe，明确单位、指标与记录粒度；有歧义先澄清。时间采用模型当地时间与左闭右开范围；公告时间不等于入库时间。记录子集用 filter 约束，结果 limit 不能作为聚合输入限制。保留 null、原金额单位、结果截断和未知业务水位。

模型变更后重新 describe；越权停止，不能改用 database 绕过；超时仅在确实缩小范围后重试一次。服务端负责强制身份与参数约束，Skill 的文字规则不代替安全边界。

## 一期验收和限制

在现有开发验证环境完成：签名发布与安装回读、三个工具真实 Hands 测试、固定记录范围的总计/地区分组/日期范围与数据库基准比对，以及自然语言任务的 Skill 激活、discover → describe → query 和结果交付。Session Events 的 activity/transcript 与数据库基准作为证据，不能仅看模型声称成功。

无数据库迁移或 Runtime 密钥配置变更。回滚先停用 Skill 安装并停止相关新任务，再回滚 AuraMCP 目录与镜像、停用 Cube；不删除历史 Session Events。既有 AuraMCP 凭据代理挂载必须保留。

真实模型、源配置、金额与查询结果、签名私钥、事件轨迹及部署回执均只存私有运维目录。提交的模板不含业务源信息。当前仅验收已授权单源、单实例和有限指标，未宣称任意 SQL、自动模型发布、共享表租户行隔离或大规模并发容量。
