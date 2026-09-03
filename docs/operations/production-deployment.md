# S5 Docker Compose 生产部署与故障演练 Runbook

## 1. 适用范围

生产部署固定使用根目录的 `compose.prod.yml`。`compose.test.yml` 继续用于本地多进程
开发，不作为生产模板。生产拓扑包含 12 个 AuraClaw 服务、一个一次性 migration job 和一个
统一 Nginx ingress；除 ingress 外不发布宿主机端口。

Docker Compose 不提供 Kubernetes HPA、PDB 或 NetworkPolicy。本方案以显式副本数、资源
限额、内部网络、外部平台网络、服务身份/数据库角色标签、Secret 文件挂载和蓝绿切换提供
对应的生产控制。跨主机调度不是本方案目标；单个 Compose 集群应运行在同一故障域，跨故障
域由两套独立 Compose 集群和上游负载均衡承担。

## 2. 前置条件

- Docker Engine 与 Compose v2；
- 已推送且使用 digest 或不可变 Git SHA 标记的 AuraClaw 镜像；
- 测试与生产主存储固定为 KingBase V9 PostgreSQL 兼容模式，使用统一
  `postgresql+asyncpg://` DSN；`deploy/postgres/roles.sql` 为可选硬化参考；
- `.host.env` 保存 `KINGBASE_HOST/PORT/USER/PWD`，运行
  `scripts/sync_kingbase_env.py` 后再物化 Compose Secret；
- Compose `migrate` 使用 `/app/migrations`，当前目标 `0058`；
- Kafka/Replay Router、华为 OBS、Vault 和模型出口可从 `auraclaw-platform` 网络访问；
- 部署机存在被 `.gitignore` 排除的 `.env.prod`，从 `.env.prod.example` 复制后填真实密钥；
- Secret 不写入 Compose、镜像、命令参数或日志。
- 蓝绿窗口按两套完整集群预留 CPU、内存、数据库连接和外部配额；容量不足时不得开始切流。

创建共享平台网络，并把外部依赖或其受控代理接入该网络：

```bash
docker network inspect auraclaw-platform >/dev/null 2>&1 ||
  docker network create auraclaw-platform
```

最低必填配置包括不可变 `AURACLAW_IMAGE`、统一应用 DSN、migration admin DSN、工作负载
令牌、lease key、chaintower workload token、Agent Context 验签密钥、模型凭据、Vault 配置
及 OBS 配置。Agent Runtime 没有数据库、模型、Vault 或 OBS Secret；chaintower
身份密钥只挂到 Task API。只有对应 owner service 获得这些凭据。

Credential Proxy 的 production 入口要求外部 Vault 地址和 token，禁止 debug secret JSON。Artifact
Service 要求明确的 SeaweedFS/OBS backend 与对应 access/secret key；`local` 或 `auto` 回退到 local
会在启动时失败。服务启动完成前会持久 seed 受管 Connector reference；若已有定义冲突则停止启动，
若引用已撤销则保持撤销，需显式管理员恢复而不是靠重启。

## 3. 预检与迁移

```bash
uv run python scripts/sync_kingbase_env.py

uv run python scripts/materialize_compose_secrets.py \
  --env-file .env.prod --output-dir .runtime/compose-secrets

uv run python scripts/compose_preflight.py --env-file .env.prod

docker compose --env-file .env.prod \
  -f compose.prod.yml --profile migrate config --quiet

docker compose --env-file .env.prod \
  -f compose.prod.yml run --rm migrate migrate status \
  --directory /app/migrations

docker compose --env-file .env.prod -f compose.prod.yml stop

docker compose --env-file .env.prod \
  -f compose.prod.yml run --rm migrate migrate up \
  --target 0058 --directory /app/migrations

docker compose --env-file .env.prod -f compose.prod.yml \
  run --rm migrate migrate check --target 0058 --directory /app/migrations
```

迁移进程只挂载 migration admin DSN。KingBase 使用 PostgreSQL advisory lock 防止并发迁移，
checksum ledger 阻止已执行文件漂移；重复运行是幂等的。`0058` 删除 MCP Tool 前缀字段，必须
先停止所有旧实例，在维护窗口内迁移，再强制重建全部服务，不能滚动混跑。
运行迁移前应已准备好同一不可变镜像，并核对其 `migrate latest` 为 `0058`。
迁移或 `migrate check` 失败时禁止继续启动。

数据库服务在开始监听前会只读校验完整迁移账本，缺失、checksum 漂移、版本不匹配均拒绝启动。
使用分角色数据库账号时，在迁移后执行更新后的 `deploy/postgres/roles.sql`，
为应用角色授予 `auraclaw_meta.schema_migration` 只读权限；不授予 DDL 或账本写入权限。

Secret 生成目录必须与 `.env.prod` 的 `AURACLAW_SECRET_DIR` 一致；目录权限为 `0700`，
29 个文件权限为 `0600`，文件内容不会输出。既有数据库如果已由旧流程完整执行到目标版本、但
尚无 migration ledger，先确认 `status` 全部显示 pending，再且仅再执行一次：

```bash
docker compose --env-file .env.prod -f compose.prod.yml \
  --profile migrate run --rm migrate migrate baseline \
  --target 0058 --confirm-existing-schema --directory /app/migrations
```

全新库、未知来源库、部分迁移库或 checksum 不一致时禁止 baseline。

## 4. 首次部署

```bash
docker compose --env-file .env.prod \
  -f compose.prod.yml pull

docker compose --env-file .env.prod \
  -f compose.prod.yml up -d --force-recreate --wait --remove-orphans

docker compose --env-file .env.prod \
  -f compose.prod.yml ps

curl --fail http://127.0.0.1:8080/health/ready
```

Compose 的 `deploy.replicas`、resources、restart policy 会由 Compose v2 应用。模板中的
`update_config`/`rollback_config` 同时保留 Swarm 兼容语义，但普通 `docker compose up`
不被视为零停机滚动更新；零停机必须使用下一节蓝绿流程。
Ingress 使用 Docker DNS 动态重解析 Task API 与 Streaming Gateway；副本扩缩容或替换后
不需要重启 Nginx。

## 5. 蓝绿发布与回滚

当前启动检查要求镜像与数据库 schema 完全一致。本节仅用于相同 schema 的代码更新；
跨 schema（尤其 `0058`）采用第 3 节维护窗口发布及
[MCP Tool 前缀移除升级](mcp-tool-prefix-upgrade.md) 中的对账和回滚流程。

运行中的颜色假设为 blue。green 使用不同的 project、内部、edge、platform 网络和临时
ingress 端口。先创建 green platform 网络，并将 PostgreSQL、Kafka、OBS、Vault、模型
出口或其受控代理接入：

```bash
docker network create auraclaw-platform-green

AURACLAW_INTERNAL_NETWORK=auraclaw-green \
AURACLAW_EDGE_NETWORK=auraclaw-green-edge \
AURACLAW_PLATFORM_NETWORK=auraclaw-platform-green \
AURACLAW_INGRESS_PORT=18080 \
docker compose -p auraclaw-green --env-file .env.prod \
  -f compose.prod.yml up -d --wait

curl --fail http://127.0.0.1:18080/health/ready
```

随后执行一条真实的只读查询和一条隔离租户的 canary 任务，确认 Runnable、Assignment、
Model/MCP、Canonical Result、SSE 和 Delivery 均完成。上游负载均衡切到 green 后，先等待
最长请求时限和 60 秒优雅退出窗口，再停止 blue：

```bash
docker compose -p auraclaw-blue --env-file .env.prod \
  -f compose.prod.yml down
```

若 canary、错误率、队列延迟或外部依赖指标异常，不切流并删除 green。若切流后异常，立即
把上游切回仍在运行的 blue；此流程双方 schema 相同，不需要数据库 down migration。
确认恢复后再停止 green。

本地或预生产演练若没有两套完整副本的容量，只能显式 `--scale <service>=1` 缩小 green
用于契约兼容验证，并记录该限制；这种结果不能替代生产容量验收。

## 6. 扩缩容

先检查数据库连接预算、Kafka partition 数和外部配额，再显式覆盖副本数：

```bash
docker compose --env-file .env.prod -f compose.prod.yml \
  up -d --scale agent-runtime=6 --scale orchestrator=4 --scale delivery-worker=4
```

Session、Projection、Orchestrator、Runtime、Model、Hands、Policy、Credential、Artifact、
Streaming 和 Delivery 都支持多副本。缩容前观察 claim/lease、outbox、delivery job 和
multipart finalize/gc 是否排空；Hands 本地 workspace 是每个容器的临时文件系统，持久结果
必须进入 Artifact Service/OBS。

## 7. 故障演练矩阵

每次演练只注入一个故障，记录开始/恢复时间、队列深度、错误率、告警和重复副作用：

| 目标 | 注入 | 必须结果 |
|---|---|---|
| Session | stop/kill 一个副本 | ingress 继续服务；command id 与 expected version 防重复写 |
| Orchestrator/Runtime | kill 持有 claim/lease 的副本 | TTL 后重新 claim；旧 fencing token 写入失败 |
| Model/Hands | stop owner 副本或返回 5xx | 有界重试；idempotency key 不变；不绕过 owner |
| Skill publication | 两个租户发布相同 publisher/name/version | 两次发布均成功；Package、Publication、Installation 与 Catalog 保持租户隔离 |
| Skill uninstall | draining 时 kill 一个 Hands 副本、并发两个 drainer | 新发现保持关闭；活动 binding 继续；Run 终态后只推进一次 uninstalled revision |
| Policy/Credential | stop、Vault 断连或 deny | fail closed；不执行 tool；不泄露 credential |
| Artifact/OBS | S3 断连、multipart 中断 | metadata 保持 pending/failed；恢复后 finalize/gc 可重入 |
| Kafka/Streaming | broker 断连、消费者暂停 | 生产端背压；SSE 可 replay；Canonical Result 不依赖 SSE |
| Delivery | kill claim owner、sink 5xx | claim expiry 后重试；超限进入 DLQ；redrive 可审计 |
| PostgreSQL | 短暂断连 | readiness 失败、停止接流；恢复后 outbox/claim 继续 |
| Task API 身份 | 错误 kid、过期 Assertion、clock skew、缺 workload | 写与敏感读 401 fail closed；不信任裸 `X-Tenant-ID` |
| Assertion 验签密钥 | 清空/轮换 N-1 过早删除 | 写命令失败关闭；回滚不得重新开放公网裸 Header |

示例（选择一个副本，不要一次停止整个 owner service）：

```bash
docker kill "$(docker compose --env-file .env.prod \
  -f compose.prod.yml ps -q agent-runtime | head -n 1)"
docker compose --env-file .env.prod -f compose.prod.yml up -d agent-runtime
```

故障恢复后执行 `auraclaw operations status`，并按需使用 projection rebuild、projection/
delivery redrive。任何需要清空 DLQ、缩短 retention 或删除 Artifact 的操作都要记录 tenant、
actor、command id、correlation/causation 和审批依据。

Owner Admin 操作先持久 claim 再执行。重复 `operation_id` 在 active claim 期间返回 `running`，完成后返回
同一结果；同 ID 不同参数返回 conflict。若 owner 在结果落库前失联，claim 到期后状态变为
`unknown_side_effect`，禁止自动重放 rebuild/redrive/retention 等可能已产生副作用的操作。操作员应先核对
目标 owner schema 和外部系统实际状态，再使用新的 `operation_id` 执行补偿或恢复。

## 8. 停止条件

出现以下任一情况立即停止发布或扩缩容：迁移 checksum drift、N/N-1 契约不兼容、readiness
持续失败、Policy/Credential fail-open、生产身份 fail-open 或重新开放裸 tenant/user Header、
跨角色数据库写入成功、Canonical Result 丢失、
Delivery 重复副作用、OBS 对象与 metadata 无法收敛，或 Secret 出现在日志/config。
