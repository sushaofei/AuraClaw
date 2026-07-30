# M12 价格洞察业务 Skill 实施与运维

## 1. 交付目标

M12 以“全场景中心 → 成本 → 价格管理控制塔 → 价格洞察智能体”为首个业务样板，
验证新增场景可以通过版本化 Skill、受治理 Resource、确定性 Tool 与标准 Agent Loop
完成，而不在 Runtime 增加场景分支。

端到端流程：

```text
用户问题
  -> capabilities.search / load
  -> procurement.price-insight.generate@1.0.0
  -> Skill Resolver 固定 3 Tool + 3 Resource
  -> Runtime 自动 hydration
  -> data_quality -> snapshot -> drilldown
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
- `action/price_insight.py`：确定性 KPI、质量检查、钻取与三个只读 Tool；
- `infrastructure/price_insight.py`：黄金数据和固定 SQL 的租户隔离只读适配器；
- `skills/procurement-price-insight/`：签名 Skill、业务规则、输出契约和黄金样本；
- `composition/business_skills.py`：平台签名、Resource 与 Capability 描述符；
- `runtime/capability_controller.py`：所有 Skill 共用的依赖自动装载。

Skill 负责“何时使用、调用顺序、解释限制”；Tool 负责可复现计算；数据适配器负责租户隔离和
固定查询；Agent 负责理解问题与组织结果。

## 4. 数据契约

MySQL 使用：

- `dwd_pr_price_event_detail_di`：最终成交价格行；
- `dwd_pr_price_compare_pair_di`：成交价与行业基准的匹配证据；
- `dwd_pr_industry_price_benchmark_di`：版本化行业基准；
- `dwd_pr_price_insight_rule_di`：版本化阈值与可比规则。

所有查询强制 `tenant_id` 和月份区间。筛选值只作为参数绑定；Agent 无权传入 SQL 或表名。
稳定键 `price_line_id` / `compare_pair_id` 用于证据、去重和钻取。行业基准必须声明统计类型，
避免把均值、P50 或演示口径混用。

## 5. 配置

```bash
AURACLAW_PRICE_INSIGHT_SOURCE=auto|disabled|fixture|mysql
AURACLAW_PRICE_INSIGHT_TARGET_TENANT_ID=development
AURACLAW_PRICE_INSIGHT_MYSQL_HOST=
AURACLAW_PRICE_INSIGHT_MYSQL_PORT=3306
AURACLAW_PRICE_INSIGHT_MYSQL_USER=
AURACLAW_PRICE_INSIGHT_MYSQL_PASSWORD=
AURACLAW_PRICE_INSIGHT_MYSQL_DATABASE=
```

`auto` 在 development 使用黄金数据，在 production 关闭。生产启用 `mysql` 时必须提供完整
只读连接配置；密码可通过 `AURACLAW_PRICE_INSIGHT_MYSQL_PASSWORD_FILE` 注入。

## 6. 扩展新业务场景

新增业务场景时复用相同形态：

1. 定义场景数据与输出契约；
2. 实现受治理 Source 和确定性只读/写入 Tool；
3. 用 `skill-creator` 创建并校验 Skill 包；
4. 在 manifest 声明 Tool/Resource 依赖；
5. 在 Composition 注册能力并签名发布；
6. 添加黄金数据、真实适配器测试和第二场景框架回归。

Runtime 不应按 Skill 名称、业务域或页面路径分支。激活后依赖解析失败、超过已加载能力数或
Schema 字节预算时，激活必须 fail closed。

## 7. 验证与回滚

```bash
uv run ruff check .
uv run mypy src/auraclaw
uv run pytest
uv run lint-imports
python /Users/tong/.codex/skills/.system/skill-creator/scripts/quick_validate.py \
  src/auraclaw/skills/procurement-price-insight
```

回滚时先设置 `AURACLAW_PRICE_INSIGHT_SOURCE=disabled` 并重启 Action Hands，业务 Skill、
Tool 和 Resource 将不再发布。代码回滚不会删除 DWD 数据；签名包和历史 Session 证据保留。
