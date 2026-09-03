# Read Model Store

## 定位

Read Model Store 保存由 Projection Service 构建的查询优化视图。它可以使用 PostgreSQL、文档数据库、搜索引擎或缓存组合实现，但逻辑上始终是可重建的派生存储。

## 主要视图

### Control Projection

```text
session_id, desired_status, runnable, priority
resource_profile, agent_profile, tool_profile
checkpoint_ref, owner, source_version
```

### Collaboration Projection

```text
root_session_id, child_session_id, parent_session_id
role, dependencies, ownership, status, output_contract
```

### Result Projection

```text
session_id, run_id, status, progress, current_stage
result_summary, result_ref, artifact_refs, error
started_at, completed_at, source_version
```

### Approval Projection

```text
approval_id, session_id, action_digest, risk
status, requested_by, expires_at, decision, source_version
```

## 核心功能模块

| 模块 | 功能 |
|---|---|
| Projection Schemas | 隔离不同读模型的数据结构 |
| Index Management | tenant、root、status、time 和 owner 索引 |
| Version / Watermark | 暴露投影新鲜度和来源版本 |
| Query DAO | 为内部消费者提供稳定读取接口 |
| Pagination | Cursor Pagination 和稳定排序 |
| Tenant Isolation | Schema、分区或行级隔离 |
| Cache Policy | 热查询缓存和失效策略 |
| Rebuild Switch | 新旧 Projection 原子切换 |

`Projection Writer` 和重放编排属于 Projection Service；外部 API 组装属于 Query Service，避免 Store 成为包含全部逻辑的“大组件”。

## 核心查询

```text
getControlState(sessionId, minVersion)
listRunnable(tenantId, shard, limit)
getTask(rootSessionId)
listChildren(rootSessionId, cursor)
getResult(sessionId, runId)
listArtifacts(sessionId)
getApproval(approvalId)
searchTimeline(filters, cursor)
```

## 数据库实现建议

MVP 可使用 PostgreSQL：

- 不同 Projection 使用独立表或 Schema。
- 不建立指向 Canonical Event 表的跨边界外键。
- 使用 JSONB 保存演进快的扩展字段，但关键过滤字段单列索引。
- 时间线和大租户数据按 tenant/time 分区。
- 使用只读副本承载历史查询，不用于 Orchestrator 的强时效读取。

## 一致性

- Store 只接受 Projection Service 写入。
- 每条记录保存 `source_version`。
- Query 返回 `projection_version` 和 `projected_at`。
- Orchestrator 和强 read-your-writes 查询可以要求 `min_version`。
- 缓存 Key 必须包含 Projection Version 或使用事件驱动失效。

## 安全与观测

- 所有查询强制 tenant scope 和字段级可见性。
- Result 和 Approval Projection 需要独立权限。
- Artifact 只返回受控引用，不直接泄漏存储地址。
- 指标：查询延迟、索引命中、Projection 新鲜度、缓存命中、慢查询和租户热点。

## 验收条件

- 删除 Store 后能由 Projection Service 重建。
- 外部调用方不能直接访问数据库。
- Query Service 与 Orchestrator 无需扫描 Canonical Event Log。
- 过期 Projection 可被调用方通过版本识别。

## 当前实现对照

- 当前实现固定使用 PostgreSQL/Kingbase 兼容 SQL：`projection.task_view`、
  `projection.collaboration_view`、`projection.approval_view`、checkpoint、processed event 与 poison event。
- `PostgresTaskProjection` 同时承担 Task/Result 查询视图；列表支持稳定 cursor、source/kind/status 等过滤。
- Query、Orchestrator 和管理接口分别使用受限读路径，不把投影视图作为 Canonical 写入口。

## 现有缺陷与待完善

- 文中的文档数据库、搜索引擎和缓存组合是可选目标，不是当前实现；尚无全文检索、向量检索或专用 Search View。
- Control 短期状态实际位于 `control.*`，不属于 Read Model；应避免继续把 lease/assignment 字段扩入投影表。
- 缺少投影 schema 版本切换、索引容量基线、冷数据分区和只读副本延迟治理。
- 待补：查询计划/索引回归测试、数据新鲜度 SLO、重建前后校验摘要和租户级容量/保留策略。
