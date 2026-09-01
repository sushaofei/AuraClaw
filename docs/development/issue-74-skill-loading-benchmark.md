# Issue #74 Skill 加载容量与 Runtime 热路径基准

基准入口为 `benchmarks/issue_74_skill_loading.py`。容量矩阵覆盖 1/2/4 个 Hands 副本、
10/100/1000 个 Publication、10 KiB/100 KiB/1 MiB 包，并分别报告全冷启动、无变化对账和
单副本滚动重启。容量结果是确定性的对象存储请求/字节模型；Runtime 部分在执行机器上实测
1/4/16 个 active Skill 的冷下载次数与连续 20 轮热加载 p50/p95/p99。

运行方式：

```bash
PYTHONPATH=src .venv/bin/python benchmarks/issue_74_skill_loading.py
```

## 2026-09-01 本机结果

- 所有 Runtime 组合的热轮次下载数均为 0；冷轮次严格为 `replicas × active_skills`。
- 最大 Runtime 组合（4 副本、16 个 active Skill、每个正文 1 MiB）的热加载 p50/p95/p99 为
  11.684/17.294/19.223 ms。该值包含每轮重新组装完整 system message 的成本，不含 Model Provider
  网络 TTFT。
- 4 副本、1000 个 1 MiB Publication 同时全冷启动的理论 Artifact 流量是 4000 次/4000 MiB；单个
  替换副本的滚动重启单元是 1000 次/1000 MiB。无变化 reconciliation 为 0 次下载，因为重建复用
  Registry 中 digest 一致的已验证包。
- 4 副本、1000 个 100 KiB Publication 的对应值为全冷 4000 次/390.625 MiB、单副本重启
  1000 次/97.656 MiB、热对账 0。

本机延迟用于发现回归，不作为跨环境 SLO；CI/预发复跑时应保留机器规格、Artifact 后端、并发参数和原始
输出。容量模型不声称对象存储下载延迟，生产判断必须结合 `skill.package.download.*` 指标。

## Redis decision gate

本阶段不引入 Redis。热路径已经消除重复下载，多副本一致性由 PostgreSQL lifecycle、可靠 outbox/Kafka
广播、revision fence、启动 snapshot 和 read-through 保证，Redis 不参与正确性。最大冷启动流量提示应优先
使用滚动扩容、ready 前预热、对象存储/CDN 容量和受控并发来削峰。

只有生产或等价预发在目标副本数和真实包分布下同时满足以下条件，才重开可选 L2 ADR：滚动单副本预热持续
违反 readiness/SLO、Artifact 下载成为已证实瓶颈，且调整 L1/预热并发/部署节奏后仍无法达标。任何 L2 都
必须具备 tenant quota、value size 上限、TLS/ACL、数据驻留、digest 重验，以及 Redis 故障时回退
`L1 + Artifact` 的路径；不得保存 Publication/Installation/Trust/撤销权威状态。
