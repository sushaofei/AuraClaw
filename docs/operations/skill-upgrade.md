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
