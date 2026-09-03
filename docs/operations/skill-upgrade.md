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
