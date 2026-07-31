---
name: procurement-price-data-validation
description: 验证受治理采购价格数据的范围、覆盖度、可比性和质量门禁。当任何采购价格场景需要在计算指标前确认数据是否可用、是否为空或是否存在严重质量问题时使用。
---

# 采购价格数据校验

1. 对完整且固定的 `filter` 调用 `procurement.price.dataset.profile`。
2. 保存 `source_revision`；`records=0` 时停止，不把无数据解释为指标为零。
3. 使用相同 `filter` 调用 `procurement.price.dataset.quality.check`。
4. 两次调用的 `source_revision` 必须一致；不一致时完整重试一次。
5. `blocked` 时停止指标计算；`warning` 时披露排除范围；`pass` 时继续。

不得计算业务指标、生成 SQL、修改筛选范围或绕过质量门禁。
