# AuraClaw 发布手册（精简版）

> 目标：按顺序做完就能发版。蓝绿、扩缩容、故障演练见
> [S5 Docker Compose 生产部署与故障演练 Runbook](./production-deployment.md)。
>
> **当前开发环境部署机**：见 [.host.env](../../.host.env) 的 `DEV_SERVICE_HOST`（`10.244.16.131`）。
> 专用步骤：[DEV_SERVICE 部署手册](./dev-service-deployment.md)。  
> 开发一键部署：`./scripts/dev_service_deploy.sh`  
> `DEV_WEB`（`10.244.16.130`）只跑前端，不部署 AuraClaw 后端。

## 0. 先选模板

| 场景 | env 文件 | Compose 文件 | 主存储 | 说明 |
|------|----------|--------------|--------|------|
| 本地开发 | `.env.dev` | —（`auraclaw serve`） | 本机 PostgreSQL | 非 Docker；`AURACLAW_STORAGE_BACKEND=postgres` |
| DEV_SERVICE / 测试 | `.env.test` | `compose.test.yml` | 云 KingBase V9 | 部署在 `10.244.16.131`；数据库凭证源为 `.host.env` |
| 生产 | `.env.prod` | `compose.prod.yml` | 云 KingBase V9 | 数据库凭证源为 `.host.env`，镜像等单独维护 |

不要混用 env。本地开发用 `.env.dev`；服务器测试用 `.env.test`；生产用 `.env.prod`（均来自对应 `.example`，勿提交）。

---

## A. 服务器测试发布（`compose.test.yml`）

### A1. 前置

```bash
# 仓库根目录
cp .env.test.example .env.test
uv run python scripts/sync_kingbase_env.py
docker --version
docker compose version
```

关键变量（按需）：

- 主库：统一 `AURACLAW_DATABASE_URL`（Compose 共享 `database_url` secret）+ 可选 `AURACLAW_MIGRATION_DATABASE_URL`
- Runtime → Hands：`AURACLAW_HANDS_URL=http://action-hands:8006`（Compose 里已写死给 runtime）

### A2. 构建、迁移并启动

在本机执行 `./scripts/dev_service_deploy.sh`。脚本在构建后核对镜像要求的迁移版本，
再进入维护窗口，执行 stop → migrate up → migrate check → up --force-recreate。
默认目标为 `0063`，不再要求额外传入 `--migrate`。详情见 DEV_SERVICE 部署手册。

### A3. 验收

```bash
docker compose --env-file .env.test -f compose.test.yml ps

# 各服务 ready（示例）
curl -sf http://127.0.0.1:8000/health/ready   # task-api
curl -sf http://127.0.0.1:8006/health/ready   # action-hands（内部 Hands HTTP）
curl -sf http://127.0.0.1:8004/health/ready   # agent-runtime

# 有 ingress 时
curl -sf http://127.0.0.1:8080/health/ready
```

MCP 所在容器：

```bash
docker compose --env-file .env.test -f compose.test.yml logs --tail 100 action-hands
```

Runtime 应指向 `http://action-hands:8006`，不要把 Hands 入口改到 runtime 容器。

### A4. 更新单个服务（热修）

仅用于数据库版本未变化的代码热修。涉及 DDL 时必须走 A2 完整发布，重建所有副本连接池。

```bash
docker compose --env-file .env.test -f compose.test.yml up -d --build --no-deps action-hands
docker compose --env-file .env.test -f compose.test.yml up -d --build --no-deps agent-runtime
```

### A5. 回滚 / 停止

```bash
# 回到上一镜像 tag（若你打了 tag）
docker compose --env-file .env.test -f compose.test.yml up -d --no-build

# 停栈
docker compose --env-file .env.test -f compose.test.yml down
```

---

## B. 生产发布（`compose.prod.yml`）

### B1. 前置检查

```bash
docker --version          # Engine ≥ 20.10
docker compose version    # Compose v2

docker network inspect auraclaw-platform >/dev/null 2>&1 || \
  docker network create auraclaw-platform

# 镜像已就绪（digest 或不可变 tag）
docker image inspect "${AURACLAW_IMAGE:-auraclaw:s5}" >/dev/null
```

确认：

1. 若尚无 `.env.prod`：`cp .env.prod.example .env.prod`，填入不可变镜像与真实密钥（0600，不进 Git）
2. KingBase DB 角色已按 `deploy/postgres/roles.sql` 的权限意图授权
3. Kafka / OBS / Vault / 模型出口可从 `auraclaw-platform` 访问
4. Secret **不**写进 Compose、镜像、命令行

### B2. 物化 Secret + 预检

```bash
uv run python scripts/materialize_compose_secrets.py \
  --env-file .env.prod --output-dir .runtime/compose-secrets

uv run python scripts/compose_preflight.py --env-file .env.prod
```

Secret 目录权限：目录 `0700`，文件 `0600`。

### B3. 数据库迁移（先于应用）

先准备本次不可变镜像，核对 `migrate latest` 为 `0063`。已有集群升级时先停止所有旧实例；
`0058` 删除字段，不允许与旧实例混跑。升级后须按
[MCP Tool 前缀移除升级](mcp-tool-prefix-upgrade.md) 完成全量对账与各副本路由验证。

KingBase（PostgreSQL 兼容模式）：

```bash
docker compose --env-file .env.prod \
  -f compose.prod.yml --profile migrate run --rm migrate \
  migrate status --directory /app/migrations

docker compose --env-file .env.prod -f compose.prod.yml stop

docker compose --env-file .env.prod \
  -f compose.prod.yml --profile migrate run --rm migrate \
  migrate up --target 0063 --directory /app/migrations

docker compose --env-file .env.prod -f compose.prod.yml \
  --profile migrate run --rm migrate migrate check \
  --target 0063 --directory /app/migrations
```

迁移或校验失败时保持停服并排查，禁止跳过检查启动。进程监听前也会只读校验迁移账本的版本和 checksum；
应用账号使用分角色授权时，迁移完成后执行更新后的 `deploy/postgres/roles.sql`，授予账本只读权限。

### B4. 启动应用

```bash
docker compose --env-file .env.prod \
  -f compose.prod.yml pull

docker compose --env-file .env.prod \
  -f compose.prod.yml up -d --force-recreate --wait --remove-orphans

docker compose --env-file .env.prod \
  -f compose.prod.yml ps

curl --fail http://127.0.0.1:8080/health/ready
```

### B5. 发布后冒烟（最少集）

按顺序确认：

1. Ingress ready：`curl --fail http://127.0.0.1:8080/health/ready`
2. `action-hands` healthy（内部 Hands Gateway）
3. `agent-runtime` healthy，且能连 `http://action-hands:8006`
4. 一条只读任务 / 价格洞察 canary（隔离租户）
5. 看 Canonical Result 落库；SSE 失败不代表任务失败

常用远程命令（本机 `.host.env` 的 `AURACLAW_HOST` = DEV_SERVICE `10.244.16.131`）：

```bash
./scripts/remote_compose.sh ps
./scripts/remote_compose.sh logs action-hands
./scripts/remote_compose.sh restart action-hands
```

完整 DEV_SERVICE 步骤见 [DEV_SERVICE 部署手册](./dev-service-deployment.md)。

### B6. 简单回滚（非蓝绿）

仅相同 schema 版本的代码回滚可以直接切回旧镜像。跨 `0058` 回滚必须先停服并执行对应 down migration，
见 [MCP 升级与回滚](./mcp-annotation-upgrade.md)。启动检查拒绝账本与镜像不一致。

```bash
# 1. 把 .env.prod 里 AURACLAW_IMAGE 指回上一 digest/tag
# 2. 重新拉起
docker compose --env-file .env.prod \
  -f compose.prod.yml up -d --force-recreate --wait --remove-orphans

curl --fail http://127.0.0.1:8080/health/ready
```

相同 schema 版本的零停机切流可走 S5 蓝绿流程；当前跨 schema 升级采用维护窗口。

---

## C. 与 MCP / Skill 相关的发布注意

| 组件 | 容器 | 职责 |
|------|------|------|
| 内部 Hands Gateway | `action-hands` | Runtime 唯一 Hands 入口：`/internal/v1/hands/*` |
| Runtime | `agent-runtime` | 只持有 `HttpHandsClient`，不持远端 URL/Secret |

发版涉及 Skill / Tool 时：

1. 先保证 `action-hands` 起来且 ready
2. 改 Tool schema 必须 bump version，避免 Catalog 对账因 digest 漂移失败
3. 外部读模型只允许通过受管 PostgreSQL-compatible Source 配置，不向 Runtime 暴露数据库连接信息。
4. 执行 Skill 安全撤销前显式选择活动 binding 动作；不确定时使用默认 `cancel`。`pause` 需要确认恢复流程，
   `continue` 只适用于经 Policy 证明继续执行风险低于中断风险的场景，并核对 Publication 中的 policy evidence。
5. Publisher/key 级事件会批量影响其全部签名版本；永久 Publisher revoke 前核对 tenant、expected revision、
   `X-Revocation-Action`、policy version 与 decision id。Publisher suspend 可 resume，Publisher/key revoke 不可逆；
   多策略同时存在时 Runtime 采用 `cancel > pause > continue`。

---

## D. 停止发布的条件

出现任一情况立刻停发 / 回滚：

- 迁移 checksum drift 或 status 异常
- `/health/ready` 持续失败
- `action-hands` 起不来导致 Runtime 无 MCP
- Policy / Credential fail-open 或 Secret 出现在日志
- Canary 任务无 Canonical Result / 重复副作用

---

## E. 一页命令速查

测试环境完整发布：`./scripts/dev_service_deploy.sh`。
生产按 B1–B5 执行，不能省略 B3 的停服、迁移与校验步骤。

## Skill / MCP 联合修复发布（0063）

本次迁移目标为 0063；0058 至 0063 涉及 Tool 前缀、审批模式、本地目录 generation 和 Skill 升级清理。
协调发布全部服务，避免严格 DTO 及旧 Runtime 行为混跑。启用新 Hands 的自动清理之前，必须先确认旧 Runtime
的在途写调用已结束或人工核对其结果；没有 Canonical invocation 记录的旧调用不能自动推断已完成。
参见 [Skill 升级](skill-upgrade.md)、[工作流恢复](skill-workflow-recovery.md) 和各阶段门禁。
旧 Skill 清理包含对象所有版本及元数据物理删除，回滚二进制不会恢复旧包。测试环境业务验收须另行记录。
