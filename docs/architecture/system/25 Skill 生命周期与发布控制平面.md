# Skill 生命周期与发布控制平面

## 定位

本模块描述通用 Skill 包从发布者信任、校验、发布、安装、运行时绑定到退役与清理的控制面。
它由 `action-hands` 进程承载，但不是 Runtime 内部状态，也不同于
[Model Skill 转换服务](./24%20Model%20Skill%20转换服务.md)：后者是特定源模型到 Skill 的编译适配器，
本模块是所有 Skill 的权威生命周期。

## 实现归属

| 能力 | 当前代码/存储 |
|---|---|
| 包格式、摘要、签名、内容扫描 | `action/skill_packages.py` |
| 发布与幂等恢复 | `action/skill_publication.py`、`action/skill_reliability.py` |
| Package/Publication/Installation 状态 | `action/skill_lifecycle.py`、`hands.skill_*` |
| 发布者及密钥轮换/暂停 | `action/skill_publishers.py`、`hands.skill_publisher*` |
| 管理用例与查询 | `action/skill_management.py`、`action/skill_admin_catalog.py`、`api/routes/admin_skills.py` |
| Runtime 解析、缓存、注入和 Workflow | `runtime/capability_controller.py`、`runtime/skill_runner.py`、`runtime/skill_workflow.py` |
| Artifact 绑定与孤儿回收 | `artifact/internal_service.py`、`infrastructure/clients/artifact_reader.py` |
| 生命周期广播 | `hands.skill_lifecycle_broadcast_outbox`、Kafka `managed-agent.skill-lifecycle` |

## 权威状态与唯一写入方

`action-hands` 是 Package、Publication、Installation、Publisher 和生命周期广播记录的唯一
业务写入方。Artifact Service 只拥有对象和 Artifact 生命周期；Session Service 只记录某次 Run 实际绑定、
激活和完成的 Canonical Events；Runtime 只读取已发布且当前允许的不可变版本。

```text
Admin upload
  -> Package validated/quarantined
  -> Publication active/withdrawn
  -> Installation active/draining/disabled
  -> Runtime binding (immutable digest)
  -> activation/workflow events in Session
```

Package digest 是内容身份。Publication 和 Installation 是租户可变状态，不能通过覆盖包内容实现升级。
撤销、暂停、隔离和清理使用状态转换、命令去重、租约或 tombstone 留下证据。

## 当前实现对照

- 支持平台 HMAC 与外部 Ed25519 发布者校验、密钥轮换、暂停和运行时撤销传播。
- 支持 Package/Publication/Installation 的 PostgreSQL 权威状态、命令幂等和事务重试。
- 支持管理上传、draining、quarantine、restore、purge 与
  republish tombstone 约束。
- 发布过程与 Artifact 绑定跨服务运行，通过持久命令/outbox、claim 和 reconciliation 修复中断状态。
- Runtime 使用不可变 digest 解析 Skill，具有进程缓存、Run 内固定内容、Prompt 大小/token 上限和
  Capability 依赖检查。
- Skill 激活、完成、失败、取消和依赖调用写入 Session Canonical Events；生命周期变更另有广播 outbox。

## 安全与一致性约束

- 上传内容先做结构、路径、大小、签名和内容扫描；隔离包不能发布或安装。
- Runtime 不信任管理 API 参数直接指定的正文，只读取已绑定 Artifact 并复核 digest。
- Publisher、Package、Publication、Installation 的租户边界在查询和写入两端校验。
- 对多副本写入使用数据库 CAS、行锁、租约与持久 fencing；进程内锁不是正确性边界。
- 删除必须先证明不存在有效 Publication/Installation/Runtime 依赖，并保留必要 tombstone 防止身份复用。

## 现有缺陷与待完善

- 管理路由体量过大，HTTP 映射、查询编排和部分兼容逻辑仍集中在 `api/routes/admin_skills.py`，维护成本高。
- Skill 控制面与 Tool/MCP 执行面共用 `action-hands`，高负载发布可能与在线调用争用进程资源。
- 生命周期广播具备持久 outbox 与 Kafka 适配，但消费者覆盖仍有限；部分缓存失效继续依赖 TTL 和主动复核。
- 内容扫描以规则和结构校验为主，尚未形成可插拔恶意代码分析、SBOM、许可证与供应链信誉闭环。
- 管理能力已有大量单元/集成测试，但缺少跨版本兼容矩阵、海量租户目录压测和长期 retention 演练证据。

### 演进项

1. 将 Admin route 收敛为薄适配层，稳定 Package、Publication、Installation、Publisher 用例边界。
2. 建立生命周期事件消费者清单、lag/SLO、DLQ 与全量快照恢复流程，验证多副本缓存一致性。
3. 引入可版本化扫描策略、SBOM/许可证/恶意内容检查和人工解除隔离审计。
4. 补充大目录分页、跨租户同名发布、发布风暴和 Artifact 故障下的容量/恢复测试。
5. 当在线 Invocation 与控制面负载出现独立扩缩容需求时，再拆分 Skill Control Plane 进程；拆分不得改变
   当前唯一写入与端口契约。

## 验收条件

- 同一命令重放只产生一个业务结果；多副本抢占不会绕过 revision、lease 或 fencing。
- Runtime 永远执行绑定 digest 的内容，撤销和 draining 语义在新 Run 上可预测。
- 跨服务发布在任一点崩溃后都能继续或进入可运维的 reconciliation 状态。
- Package、Publication、Installation 和 Publisher 的状态、审计与 Artifact 引用可追踪。
