# Skill 升级与旧版本清理

## 可用性判定（#94 A）

管理页和运行时目录重建共用 installation_availability。当前 publication 必须 active，installation
必须 active，版本约束及 pinned digest 必须匹配。管理页分别返回 installation_version_mismatch /
installation_digest_mismatch；不能再把“新版发布 active + 任意旧安装 active”显示为 available。
依赖仍由共享 SkillDependencyAvailability 判定，包恢复仍校验实际内容、签名和摘要。

搜索返回当前可执行能力，空结果并不能证明系统没有安装 Skill。列表查询使用 kinds=["skill"]、
query=""；已知名称使用 canonical_name。结果新增可选 truncated 标志，达到查询上限时不能宣称
返回全部。管理权限内的诊断仍通过现有 Skill 管理目录查看，不在模型搜索中泄露隐藏安装。

此阶段没有修改安装绑定、删除旧包或新增数据库结构。37 项管理目录、重建、正式 Agent 搜索与
能力目录回归通过。原子切换、旧版 drain 和物理删除将在后续阶段交付；不得仅凭 availability
修复宣称升级完成。

## 原子切换（#94 B）

正式发布更高版本时自动替换安装精确版本/digest，并提高 installation revision；旧 auto_upgrade=false
不再阻挡这次显式升级。已有 disabled/uninstalled 状态保持原状，不因发布包重新启用。
候选包在提交前只进入本地 staged 状态；签名、内容扫描、升级依赖及已安装依赖者的约束检查失败时，
当前版本仍可发现。声明式依赖缺失时拒绝升级，具体依赖版本在执行时仍重新校验。

publication、installation CAS、旧 active publication 撤销为 continue 和 skill_upgrade_current 在同一
事务中提交。新 PublishSkillCommand 可携带 expected_installation_revision；旧调用者省略时服务端
读取当前 revision 并 CAS。同命令重复请求沿用最小请求摘要幂等；迟到较低版本拒绝覆盖已发布新版。
PostgreSQL 对逻辑身份串行提交，内存 adapter 共用相同原子规则。SemVer 顺序用于防降级，精确 pin
支持 prerelease。SkillResolver 每次顶层 resolve 先刷新租户快照，避免一直使用已缓存旧 descriptor。

PublishedSkill 新增可选 upgrade：operation_id/current_version/package_digest/generation/phase。
phase=draining 表示新版已经切换，旧版本仍在等待清理，不能显示“升级完成”。当前阶段没有删除 worker，
也没有物理删除旧版本；这部分继续在 C/D 阶段交付。0061 仅存当前操作，不建立完整旧版本归档。

迁移先于 Hands/Runtime/API 协调发布；严格 DTO 消费者须同步升级。0061 down 在存在未完成操作时拒绝，
并且无论何时都不能恢复已删除包。临时 PostgreSQL 0061 up/down/up、双 store 竞争/幂等 4 passed；
全量 679 passed / 57 skipped。实际测试环境尚未升级。

## 对象物理清理基础（#94 C1）

内部 ArtifactDeleteRequest 新增 remove_history，限 Action Hands 的 skill_package_purge 用途，
仍校验删除策略、法律保留和删除租约。0062 保存持久化删除意图，普通 GC 不得恢复为 ready；
崩溃或不确定结果由同一物理清理请求重试。成功删除完整 Artifact 元数据，只保留 tenant 与不可恢复
包内容的幂等摘要。共享 storage_ref 的其他元数据仍有效时，仅删除本 Skill 的元数据。

S3 开启或暂停版本控制时，逐个删除精确 key 的所有对象版本和删除标记；不删除相邻前缀对象。
权限不足、列表异常或页数超限均报错并保持待重试状态。普通 DELETE 只增加删除标记，不能代表
历史字节已删除，依据 [S3 删除版本文档](https://docs.aws.amazon.com/AmazonS3/latest/userguide/DeletingObjectVersions.html)。
部署身份需要 GetBucketVersioning、ListBucketVersions 和 DeleteObjectVersion 权限；未开启版本控制
时使用普通删除并重新核验版本控制状态。清理期间包对象 key 不可复用或重新写入。

先迁移 0062，再协调发布 Artifact 与 Hands，最后启用生命周期清理 worker。down 在有待物理删除
对象时拒绝；任何回滚均不能恢复已删除字节。当前仅交付对象清理基础，旧 publication、安装引用
和生命周期 worker 的完整清理仍在后续阶段实施，不能将此阶段显示为升级完成。

全量 683 passed / 60 skipped；临时 PostgreSQL 双副本重试、旧 deleted 元数据、共享对象保护与
生命周期联合 11 passed。0062 up/down/up、Ruff、Mypy 和架构合同通过。

## 自动清理 worker（#94 C2）

Hands 启动恢复及周期任务自动处理 skill_upgrade_current。0063 的 generation/租约 token 隔离副本，
续租覆盖对象 I/O；失去租约的 worker 不能提交清理结果。先检查旧包法律保留和 Canonical 在途引用，
等待结束后关闭旧 publication 的继续执行入口，再次查询引用，随后调用物理清理接口。晚到旧 binding
必须在下一次权威检查停止，确认结束前不删除包。当前版本、未来 staged 候选和其他 Skill 不属于清理目标。

对象确认删除后，事务删除旧 publication/package、旧对象的 transient tombstone、旧 outbox 和 admission
记录，并清除 publish command 中的旧版本/digest 及内存完整重放结果。只保留不可恢复旧包的请求摘要，
旧命令重放不能重新生成旧包。同版本 purged 后重新发布也创建当前清理操作，旧 tombstone 仅作失败重试
期间的清理输入，完成后必须为空；不提供旧版本恢复历史。迟到 publication outbox 先验证当前 Artifact 引用，
不能重新绑定已删除的包。完成清理后重建并广播当前目录，移除本机旧资源和缓存。

只有全量旧包清除且目录重建后才能 phase=completed；在途为 draining，错误/法律保留为 blocked，周期任务
继续重试。blocked 不表示已经删除成功。先迁移 0063，再协调发布 Artifact、Session、Control、Hands 和 Runtime；
旧 Runtime 的在途写调用按恢复文档排空后启用本版本 Hands。0063 down 在仍有操作或已清除旧命令材料时拒绝，
回滚不可能恢复旧对象。历史 Canonical Session 事实与业务产物保持原有生命周期。

全量 694 passed / 62 skipped；最后 PostgreSQL/晚到激活/同版本清理/管理/可靠性联合通过。
独立数据库 0063 up/down/up、Ruff、Mypy（253 文件）、10 条架构合同通过。管理页操作状态展示与测试环境
实际升级验收仍在后续阶段完成。
