# M12 价格洞察业务 Skill 实施与运维

## 1. 交付目标

M12 以“全场景中心 → 成本 → 价格管理控制塔 → 价格洞察智能体”为首个业务样板，
验证新增场景可以通过版本化 Skill、受治理 Resource、确定性 Tool 与标准 Agent Loop
完成，而不在 Runtime 增加场景分支。

生产形态下，Skill 的发布事实来自
`10.244.16.133/chaintower_db.ct_model_*`。仓库内 Skill 包只作为未启用 Model Skill Source
时的开发回退；启用 `MYSQL_DB_*` 源后，由 `ModelSkillPublisher` 编译并发布
`ct-model/procurement.price-insight.generate@4.0.0`。仓库内平台回退包仍为 3.0.0，
两者不会在同一配置下同时发布。

端到端流程：

```text
用户问题
  -> capabilities.search / load
  -> procurement.price-insight.generate@4.0.0
  -> price-data.validate + price-metrics.analyze 两个子 Skill
  -> Skill Resolver 递归解析并固定 11 个原子 Tool + 3 Resource
  -> Runtime 分批自动 hydration，并注入父/子 Skill 签名 SOP
  -> dataset.profile -> dataset.quality.check
  -> 八个固定指标 Tool -> metric.evidence.list（按需）
  -> 证据化业务结论
```

## 2. 关键指标

页面的首版权威输出固定为八项：

1. 历史维偏离；
2. 区域维价差；
3. 市场维偏离；
4. 正偏移金额；
5. 负偏移金额；
6. 正偏移占采购额；
7. 负偏移占采购额；
8. 市场偏离行数。

历史、区域、市场三类锚点分别计算影响金额。正负影响分开聚合，不做净额抵消。完整公式和
可比键位于 Skill 的 `references/metric-definitions.md` 与
`references/comparability-rules.md`。

## 3. 组件与职责

- `contracts/price_insight.py`：筛选、DWD 行、数据集和快照契约；
- `action/price_insight.py`：确定性 KPI、范围画像、质量门禁、单指标计算与证据下钻；
- `infrastructure/price_insight.py`：黄金数据和固定 SQL 的租户隔离只读适配器；
- `skills/procurement-price-insight/`：场景编排 Skill、业务规则、输出契约和黄金样本；
- `skills/procurement-price-data-validation/`：可复用的数据范围和质量子 Skill；
- `skills/procurement-price-metrics/`：可复用的八项价格指标和证据子 Skill；
- `composition/business_skills.py`：平台签名、Resource 与 Capability 描述符；
- `runtime/capability_controller.py`：所有 Skill 共用的依赖自动装载。
- `config/model-skills/procurement-price-insight.json`：可审计的 ct_model 配置发布事实；
- `scripts/configure_price_insight_model_skill.py`：显式 validate/plan/apply 的配置引导工具。

Skill 负责“何时使用、调用顺序、解释限制”；Tool 负责可复现计算；数据适配器负责租户隔离和
固定查询；Agent 负责理解问题与组织结果。

原子 Tool 的职责边界：

- `procurement.price.dataset.profile`：只确认数据覆盖和修订版本；
- `procurement.price.dataset.quality.check`：只执行质量规则；
- `procurement.price.metric.<固定指标>.compute`：每个 Tool 只计算一个固定指标，不接受
  `metric_key` 选择器；
- `procurement.price.metric.evidence.list`：每次只返回一个指标的有界证据。

旧 `price_insight.*`、`snapshot`、`data_quality` 和 `drilldown` 暂留作兼容接口，不再是
3.0 Skill 的依赖。

## 4. 数据契约

MySQL 使用：

- `dwd_pr_price_event_detail_di`：最终成交价格行；
- `dwd_pr_price_compare_pair_di`：成交价与行业基准的匹配证据；
- `dwd_pr_industry_price_benchmark_di`：版本化行业基准；
- `dwd_pr_price_insight_rule_di`：版本化阈值与可比规则。

所有查询强制 `tenant_id` 和月份区间。筛选值只作为参数绑定；Agent 无权传入 SQL 或表名。
稳定键 `price_line_id` / `compare_pair_id` 用于证据、去重和钻取。行业基准必须声明统计类型，
避免把均值、P50 或演示口径混用。

TODO（后续权限阶段）：为远程 DWD 建立价格洞察专用只读账号，仅授予上述四张表的
`SELECT`，替换当前联调凭据，并增加启动时授权自检。当前 Tool 仍只执行只读事务和固定 SQL。

## 4.1 ct_model 配置映射

`PRICE_IMPACT@4.0.0` 使用以下配置：

- `ct_model_definition/version`：模型身份、SemVer、发布状态和不可变配置快照；
- `ct_model_input_source`：四张 DWD 逻辑表，只记录 adapter、只读模式和租户约束，不记录连接信息；
- `ct_model_input_feature`：月份、锚点和偏离阈值请求字段；
- `ct_model_output_schema`：八项 KPI、数据修订版本和质量状态；
- `ct_model_weight_config`：八项 KPI 的解释优先级，合计为 1；只控制 Agent 的解读与
  呈现顺序，不参与加权、归一化、总分或 Tool 数值计算；
- `ct_model_tag`：控制塔、行业均价、价格影响和价格异常四组受控发现标签；标签只扩展
  Capability 的 `applies_when`，不执行标签规则文本；
- `ct_model_switch_config`：`PRICE_MANAGEMENT_CONTROL_TOWER` 场景开关；
- `config_snapshot_json.auraclaw_skill`：受校验的 Skill 名称、执行模板、子 Skill 依赖、
  表边界、指标顺序和输入输出 Schema。

编译器只允许代码注册的 `procurement-price-insight-atomic-v1` 模板。v2 配置必须依赖
`procurement.price-data.validate` 和 `procurement.price-metrics.analyze` 两个精确版本的
平台 Skill，不能直接扩张 Tool。子 Skill 缺失、循环依赖、越界表或指标集合不一致时 fail
closed，不允许数据库中的任意文本生成 Agent 控制指令。

DWD 规则与模型管理配置职责分离：`dwd_pr_price_insight_rule_di` 是偏离阈值、行业基准
最小样本量和物料匹配最低分的权威来源。请求未传阈值时 Tool 使用与锚点匹配的唯一启用
DWD 规则；显式请求值可以覆盖并记录来源。多个启用规则同时匹配时质量状态为 blocked，
原子指标 Tool 拒绝计算。低于规则最小值的市场证据被排除并产生质量告警。

2026-07-31 已在 tenant 1 发布 model id 3 / version id 7 / `4.0.0`。远端回读得到
8 条权重、4 条标签和 1 条场景开关，并成功编译签名 Skill。

### 远端 DWD Schema 前置条件

2026-07-31 对 `10.244.16.133/chaintower_db` 的只读审计发现：三个同名 DWD 表仍是旧版
Schema，缺少当前 DDL 要求的 `tenant_id`、稳定行 ID、基准统计类型和物料匹配分等字段，
且规则表尚不存在。真实 MySQL Tool/模型 Provider/前端验收因此仍被 Schema migration
阻塞。不得用 `CREATE TABLE IF NOT EXISTS`、删除重建或模拟结果掩盖该差异；需由数据侧按
`docs/ddl/行业均价智能横向比对-DWD-MySQL-DDL.sql` 完成兼容迁移并保留现有数据后再验收。

配置命令：

```bash
PYTHONPATH=src uv run python scripts/configure_price_insight_model_skill.py
PYTHONPATH=src uv run python scripts/configure_price_insight_model_skill.py --plan
PYTHONPATH=src uv run python scripts/configure_price_insight_model_skill.py --apply
```

默认只做本地契约校验，`--plan` 只读远程状态，只有 `--apply` 才在显式事务中创建或对账目标版本。
已发布的同版本配置不可变；内容不同必须提升版本号。

## 5. 配置

```bash
AURACLAW_PRICE_INSIGHT_SOURCE=auto|disabled|fixture|mysql
AURACLAW_PRICE_INSIGHT_TARGET_TENANT_ID=development
AURACLAW_PRICE_INSIGHT_MYSQL_HOST=
AURACLAW_PRICE_INSIGHT_MYSQL_PORT=3306
AURACLAW_PRICE_INSIGHT_MYSQL_USER=
AURACLAW_PRICE_INSIGHT_MYSQL_PASSWORD=
AURACLAW_PRICE_INSIGHT_MYSQL_DATABASE=
AURACLAW_DEVELOPMENT_MODEL_MODE=provider|price-insight-scripted
```

`auto` 在 development 使用黄金数据，在 production 关闭。生产启用 `mysql` 时必须提供完整
只读连接配置；密码可通过 `AURACLAW_PRICE_INSIGHT_MYSQL_PASSWORD_FILE` 注入。

### 本地真实 DWD 与前端联调

本机 MySQL 可重复应用 DDL 并写入隔离的黄金验证行：

```bash
PYTHONPATH=src uv run python scripts/seed_price_insight_mysql.py \
  --tenant-id development
```

本地 `.env` 设置 `AURACLAW_PRICE_INSIGHT_SOURCE=mysql` 和完整的
`AURACLAW_PRICE_INSIGHT_MYSQL_*` 后，执行 VS Code 的
`AuraClaw: Debug local frontend + backend`，访问
`http://localhost:3000/price-insight`。该页面通过标准 `/v1/tasks` 创建任务，并从
Canonical Timeline 展示 Capability Search/Load、父子 Skill Activate、数据质量、
八项 KPI 和 `mysql-price-insight:*` 数据修订证据。

当外部模型端点不可用、只需做确定性框架回归时，可在 development 设置
`AURACLAW_DEVELOPMENT_MODEL_MODE=price-insight-scripted`。这个模式只固定模型决策序列；
Agent Harness、MCP Capability、签名 Skill、Tool Gateway 和 MySQL DWD 都仍走真实实现。
production 始终使用配置的模型 Provider。

## 6. 扩展新业务场景

新增业务场景时复用相同形态：

1. 定义场景数据与输出契约；
2. 实现受治理 Source 和确定性只读/写入 Tool；
3. 优先把可复用的一步计算实现为固定输入输出的原子 Tool；
4. 用 `skill-creator` 创建领域子 Skill，将相关 Tool 组合成短 SOP；
5. 场景 Skill 在 manifest 声明 `required_skills`，只有无法复用时才直接声明 Tool；
6. 在 Composition 注册能力并签名发布；
7. 添加黄金数据、真实适配器测试和第二场景框架回归。

Runtime 不应按 Skill 名称、业务域或页面路径分支。激活后依赖解析失败、超过已加载能力数或
Schema 字节预算时，激活必须 fail closed。

## 7. 验证与回滚

```bash
uv run ruff check .
uv run mypy src/auraclaw
uv run pytest
uv run lint-imports
npm --prefix frontend run lint
npm --prefix frontend run build
python /Users/tong/.codex/skills/.system/skill-creator/scripts/quick_validate.py \
  src/auraclaw/skills/procurement-price-insight
```

回滚时先设置 `AURACLAW_PRICE_INSIGHT_SOURCE=disabled` 并重启 Action Hands，业务 Skill、
Tool 和 Resource 将不再发布。代码回滚不会删除 DWD 数据；签名包和历史 Session 证据保留。
