# 开发治理

- [开发阶段校验清单](./stage-gates.md)：所有阶段共用的完成门禁和验收记录。
- `implementation/`：仍有助于理解当前能力落地方式的实施说明。
- [Policy 约束治理](../operations/policy-governance.md)：Budget、模型/区域与 Artifact 分享的生产配置和
  fail-closed 执行契约。

阶段测试报告已合并到校验清单这一唯一验收入口。测试用例与当前结果以 `tests/`、CI 和 Git 历史为准，避免长期维护会迅速失真的“通过数量”快照。
