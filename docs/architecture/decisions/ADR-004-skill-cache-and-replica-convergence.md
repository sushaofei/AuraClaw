# ADR-004：Skill 内容使用本地 L1，跨副本通过可靠事件收敛

- 状态：Accepted（Issue #74）
- 日期：2026-09-01
- 适用范围：Skill package 内容加载、Action Hands 多副本投影与缓存失效
- 架构真源：`docs/architecture/system/`

## 背景

Skill package 按 digest 不可变，但 Publication、Installation、Retention、Publisher Trust 和撤销状态会变化。
如果每个 Hands 副本周期性下载全部 archive，会把租户数、包体积和副本数相乘为持续对象存储流量；如果只依赖
单机缓存，又可能让负载均衡命中的冷副本短暂缺少本地投影。

## 决策

- PostgreSQL lifecycle 是状态事实源，Artifact Service/Object Store 是 package bytes 事实源。
- 每个 Hands 副本使用按 `(tenant_id, package_digest)` 隔离、容量和 TTL 有界的 L1；缓存命中不跳过
  lifecycle、Policy、Trust 或 digest 检查。
- lifecycle mutation 完成本地权威重建后，向 PostgreSQL broadcast outbox 写入每租户单调 revision、
  snapshot digest、change type 和 origin replica；事件不包含 `SKILL.md` 或 archive bytes。
- outbox relay 采用竞争领取，只发布一次 Kafka 元数据事件；Kafka producer 的重试可能形成重复消息，消费者以
  tenant revision 幂等处理。
- 每个 Hands 副本使用独立 consumer group。普通共享 group 只能让一个副本收到事件，不具备本地缓存广播语义。
- 消费者拒绝重复或旧 revision；包括 origin 在内的每个副本接受事件时都重新读取 PostgreSQL 并执行本地增量
  rebuild，不直接信任消息正文。origin 不能跳过，否则跨副本并发 mutation 可能留下较旧本地 snapshot。
- Kafka/outbox 只加速收敛。启动 snapshot、冷 miss read-through 和低频 reconciliation 继续承担扩容、漏消息、
  consumer 重启及 Kafka 暂时不可用时的正确性恢复。

## Redis 决策

当前不引入 Redis。不可变 digest + 本地 L1 + Artifact + PostgreSQL outbox/Kafka 已消除常态重复下载，Redis
既不能成为撤销或 purge 的正确性来源，也不应复制大型 archive。只有 1/2/4 副本基准证明扩容冷启动仍对
Artifact 形成不可接受的压力时，才重新评估可选 L2；届时必须提供 tenant quota、value size、TLS/ACL、
数据驻留、digest 校验和 `L1 + Artifact` 降级路径。

## 备选方案

1. **所有副本共享一个 Kafka consumer group。** 否决：一个事件只会到达一个副本，其余 L1 不会及时收敛。
2. **Redis Pub/Sub 做失效通知。** 否决：通知易失，不能承担 revoke/purge 的可靠传播。
3. **Redis 保存完整 archive。** 否决：形成第二个大对象存储，并扩大安全、容量和运维边界。
4. **仅保留每分钟全量 rebuild。** 否决：正确但延迟高，且副本数线性放大 PostgreSQL/Artifact 流量。

## 故障与回滚

- Kafka 发布失败：outbox 保留并重试；本地 mutation 已按 PostgreSQL 状态完成，其他副本由 reconciliation 恢复。
- consumer 不可用：Hands 继续启动 snapshot/read-through，并周期重试 consumer。
- 重复或乱序事件：tenant revision fence 丢弃，不发布旧状态。
- 回滚应用前先停用 lifecycle producer/consumer，再回滚 `0054`；回滚期间保留周期 reconciliation。

## 后果

- 新增 `managed-agent.skill-lifecycle` topic 和 `0054_skill_lifecycle_broadcast_outbox` migration。
- 每个副本的 group id 包含 replica identity，扩容不会与现有副本竞争事件。
- Redis 不是 AuraClaw Skill 加载路径的部署依赖。
