---
name: procurement-price-metrics
description: 使用独立原子 Tool 分析采购价格历史偏离、区域最大价差、市场偏离、正负影响金额和占比，并查询指标证据。当采购价格场景已通过数据质量门禁且需要一个或多个标准价格指标时使用。
---

# 采购价格原子指标

每个 Tool 只计算一个固定指标，不接受指标选择器：

- `history_dev_pct` → `procurement.price.metric.history-deviation.compute`
- `region_gap_max` → `procurement.price.metric.region-max-gap.compute`
- `market_dev_pct` → `procurement.price.metric.market-deviation.compute`
- `impact_amount` → `procurement.price.metric.positive-impact-amount.compute`
- `impact_neg_amount` → `procurement.price.metric.negative-impact-amount.compute`
- `impact_share_pct` → `procurement.price.metric.positive-impact-share.compute`
- `impact_neg_share_pct` → `procurement.price.metric.negative-impact-share.compute`
- `deviation_cnt` → `procurement.price.metric.market-deviation-count.compute`

只调用用户问题或父 Skill 所需的指标。所有调用复用相同 `filter`，并核对共同
`source_revision`。需要解释时调用 `procurement.price.metric.evidence.list`，一次只查询一个
`metric_key` 且 `limit<=50`。禁止自行重算 Tool 数值或合并正负影响为净额。
