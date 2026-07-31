---
name: procurement-price-insight
description: 以受治理 DWD 数据生成采购价格洞察，逐项计算历史价格偏离、区域价差、行业均价偏离、正负价格影响金额及占比，并提供数据质量和明细证据。当用户询问采购价格、价格管理控制塔、历史或区域价差、行业均价横向对标、价格影响或异常采购明细时使用。
---

# 采购价格洞察

先读取 `metric-definitions`、`comparability-rules` 和 `output-contract` 资源。只使用本 Skill
声明的 Tool；不要生成 SQL、表名或连接参数。

## SOP

1. 规范化筛选条件。
   - 提取 `period_from`、`period_to`、组织、区域、品类、物料、基准版本和规则版本。
   - 月份缺失时要求用户补充，不猜测。
   - 默认锚点为 `history`；跨区域问题用 `region`；行业均价或市场横向对标用 `market`。
   - 后续所有调用复用完全相同的 `filter`。
2. 遵循子 Skill `procurement.price-data.validate`，调用
   `procurement.price.dataset.profile`。
   - 记录 `source_revision`、原始行数、可比行数和实际读取表。
   - `records=0` 时停止，不输出零值指标。
3. 调用 `procurement.price.dataset.quality.check`。
   - 返回的 `source_revision` 必须与范围画像一致。
   - `blocked` 时停止，只输出阻断项与修复建议。
   - `warning` 时继续，但在结论中披露排除行和影响范围。
4. 遵循子 Skill `procurement.price-metrics.analyze`，对页面全量分析按下列顺序调用八个
   独立 Tool：
   1. `history_dev_pct`
   2. `region_gap_max`
   3. `market_dev_pct`
   4. `impact_amount`
   5. `impact_neg_amount`
   6. `impact_share_pct`
   7. `impact_neg_share_pct`
   8. `deviation_cnt`
   用户只问单一指标时，只计算该指标及解释它所需的指标。
5. 每次计算后核对 `source_revision`。
   - 任一版本不同，丢弃本轮全部指标，从步骤 2 完整重试一次。
   - 第二次仍不一致时停止，报告数据在分析期间变化，不拼接跨版本结论。
6. 需要解释异常、给出 Top 明细或用户明确要求证据时，对目标 `metric_key` 调用
   `procurement.price.metric.evidence.list`。使用 `limit<=50`，需要更多数据时分页。
7. 校验结果，不自行重新计算 Tool 数值。
   - 八个 `metric_key` 不得缺失或重复。
   - 正负影响金额分别展示，不计算净额。
   - 占比与金额必须使用同一锚点。
   - `deviation_cnt` 必须解释当前阈值。
   - 均值、P50 等名称必须服从返回的统计口径。
8. 按输出契约组装答案：筛选与数据版本、数据质量、八项指标、主要发现、证据和限制。

禁止跨标准单位、币种、税价口径或规格直接比较。所有结论必须引用共同的
`source_revision`；有版本筛选时同时引用 `benchmark_version` 和 `rule_version`。
