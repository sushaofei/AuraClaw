# Artifact Store

## 定位

Artifact Store 保存不适合进入 Session Event 的大型或二进制产物，包括文件、报告、数据集、补丁、模型输出快照和执行日志。Session 只保存受控 Artifact Reference。

## 核心模块

```text
Metadata Store
Object Storage Adapter
Version / Lineage Manager
Content Hash / Deduplication
Tenant / ACL
Upload / Download Authorization
Signed URL Issuer
Malware / Content Scanner
Encryption / Key Policy
Lifecycle / Retention
Garbage Collector
Audit Trail
```

## Artifact 元数据

```text
artifact_id
tenant_id
root_session_id / session_id
type / media_type
name
version
content_hash
size
storage_ref
producer
lineage_refs
classification
acl
created_at
retention_until
```

`storage_ref` 只在可信服务内部使用，外部返回短期受控 URL。

## 写入流程

```text
Hands / Agent / Delivery Producer
 -> 申请 Upload
 -> 写入临时对象
 -> Hash / Scan / Classification
 -> 提交不可变 Artifact Version
 -> Session 追加 artifact.attached(ref)
```

大型输出不能先写入 Session 再异步搬运，否则会污染 Event Log 并产生双事实源。

PostgreSQL metadata 是 ready、deleted、quarantined、retention、legal hold 与访问可见性的唯一权威。
对象上传/扫描完成后必须先提交 metadata，之后才能发布任何本地 ready cache；生产 download/delete
每次签发 URL 或执行对象操作前都重新读取 PostgreSQL，cache hit、cache miss、重启和数据库故障不能改变
授权结果。数据库不可用时敏感读 fail closed，不回退到旧 `_ready`。

## 读取流程

- Task Query：校验 Root/Child 权限后返回短期下载链接。
- Agent Runtime：根据 Context Policy 挂载或读取指定片段。
- Result Delivery：组装 Payload 或生成一次性链接。
- Reviewer：只读访问待审 Artifact 和 Lineage。

## 版本与所有权

- Artifact Version 默认不可变。
- 并行 Worker 写不同 Artifact 或 Patch，不覆盖共享文件。
- 合并由 Coordinator/Reviewer 产生新版本并记录 Lineage。
- 外部可变资源保存 resource id、etag/version 和证据引用。

## 生命周期

- 临时中间产物短期保留。
- 最终结果与审计证据按 Session 策略保留。
- 被 Session、Delivery 或 Review 引用的 Artifact 不得提前回收。
- 删除使用引用计数/标记清理和合规保留检查。
- Finalize、multipart complete、scan、delete、expired upload GC 与 Skill orphan resolution 使用可配置
  claim TTL，并以 owner token heartbeat 续租；每个对象存储副作用前后都验证 ownership。
- 对象副作用开始前先持久化 operation marker。owner 在副作用开始后失联时，过期 claim 不会被另一
  副本直接重放，而进入 `reconciling/object_state=unknown`；reconciler 通过 checksum/HEAD 对比对象与
  metadata，安全收敛为 ready、pending、quarantined 或 deleted。
- GC 与 orphan 使用逐条 just-in-time claim；批量上限只决定循环容量，不让排在批尾的记录提前过期。

## 安全与观测

- 上传下载全程 tenant scope、加密和审计。
- 下载、显式删除和 orphan 物理删除都必须复核权威 Policy decision；Policy 客户端缺失、超时、异常或返回无效决定时，在签发 URL、claim 删除或访问对象存储前 fail closed。
- 生产 Artifact Service 缺少调用方 workload identity、服务自身 Policy workload identity 或 Policy 地址时拒绝启动；空身份映射始终是 deny-all。
- 对可执行文件、压缩包和外部内容进行扫描。
- 敏感 Artifact 不生成可转发的永久链接。
- 指标：storage bytes、upload/download latency、scan failure、orphan artifact、GC reclaimed、signed URL use。
- 续租失败、lease lost、unknown object state、reconciliation backlog/latency 与 stale cache rejection 必须可观测。

## 验收条件

- Session Event 中只保存 Artifact Reference。
- 相同内容可以去重，但 ACL 和引用仍独立校验。
- Worker 无法覆盖不属于自己的不可变版本。
- Result Delivery 链接到期后不可继续访问。
