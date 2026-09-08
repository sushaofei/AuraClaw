# 架构文档

## 推荐阅读顺序

1. [系统架构总览](./system/00%20Managed%20Agent%20系统架构总览.md)
2. [代码组织与部署映射](./code-organization.md)
3. [核心设计讲解](./core/README.md)
4. 按需阅读 `system/` 中的组件文档和 `decisions/` 中的 ADR

## 内容边界

- `system/`：架构真源，描述组件职责、状态与通信契约。
- `decisions/`：已经接受的跨组件决策，解释为什么采用当前边界。
- `core/`：面向开发者的机制讲解，不新增与架构真源冲突的约束。
- `code-organization.md`：架构组件、Python 包和生产进程之间的当前映射。
- `system-overview.png`：系统总图。

完成态 RFC、早期路线图和按日期生成的代码梳理已移除；稳定结论已收敛到本目录。

