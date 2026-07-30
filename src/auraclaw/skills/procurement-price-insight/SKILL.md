---
name: procurement-price-insight
description: 生成采购价格洞察。当用户询问历史价格变化、区域价差、行业均价偏离、价格影响金额或异常采购明细时使用。
---

# 采购价格洞察

先读取指标定义、可比规则和输出契约资源，再执行分析。

1. 从用户问题提取月份区间和组织、区域、品类、物料筛选条件。月份区间缺失时要求用户补充，不猜测。
2. 默认对标锚点为 `history`；明确提到跨区域时用 `region`，提到市场、行业均价或横向对标时用 `market`。
3. 调用 `procurement.price_insight.data_quality`。状态为 `blocked` 时停止结论输出，只说明阻塞原因和修复建议。
4. 调用 `procurement.price_insight.snapshot` 生成八项关键指标及三维分析。
5. 需要解释异常来源时，使用相同筛选条件调用 `procurement.price_insight.drilldown`，不要自行拼 SQL。
6. 严格按输出契约给出筛选口径、指标、证据、数据质量和建议。金额正负影响不得相互抵消。

所有结论都必须引用返回结果中的 `source_revision`、规则版本和基准版本。不要把均值描述成 P50，也不要跨币种、税价口径、单位或规格直接比较。
