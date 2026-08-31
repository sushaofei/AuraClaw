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

### A2. 构建并启动

```bash
docker compose --env-file .env.test -f compose.test.yml up --build -d
# 需要统一入口时：
docker compose --env-file .env.test -f compose.test.yml up --build -d
```

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
3. Kafka / SeaweedFS / Vault / 模型出口可从 `auraclaw-platform` 访问
4. Secret **不**写进 Compose、镜像、命令行

### B2. 物化 Secret + 预检

```bash
uv run python scripts/materialize_compose_secrets.py \
  --env-file .env.prod --output-dir .runtime/compose-secrets

uv run python scripts/compose_preflight.py --env-file .env.prod
```

Secret 目录权限：目录 `0700`，文件 `0600`。

### B3. 数据库迁移（先于应用）

KingBase（PostgreSQL 兼容模式）：

```bash
docker compose --env-file .env.prod \
  -f compose.prod.yml --profile migrate run --rm migrate \
  migrate status --directory /app/migrations

docker compose --env-file .env.prod \
  -f compose.prod.yml --profile migrate run --rm migrate \
  migrate up --target 0041 --directory /app/migrations
```

规则：只做 expand 迁移；发布窗口内不删 N-1 仍依赖的列/事件字段。

### B4. 启动应用

```bash
docker compose --env-file .env.prod \
  -f compose.prod.yml pull

docker compose --env-file .env.prod \
  -f compose.prod.yml up -d --wait --remove-orphans

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

数据库不 down migration。应用回滚到上一镜像：

```bash
# 1. 把 .env.prod 里 AURACLAW_IMAGE 指回上一 digest/tag
# 2. 重新拉起
docker compose --env-file .env.prod \
  -f compose.prod.yml up -d --wait --remove-orphans

curl --fail http://127.0.0.1:8080/health/ready
```

需要零停机切流时，走 S5 蓝绿流程（另起 `auraclaw-green` project → canary → 切流 → 停 blue）。

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

```bash
# --- 本地 ---
docker compose --env-file .env.test -f compose.test.yml up --build -d
curl -sf http://127.0.0.1:8006/health/ready

# --- 生产 ---
docker network create auraclaw-platform   # 已存在可忽略
uv run python scripts/sync_kingbase_env.py
uv run python scripts/materialize_compose_secrets.py \
  --env-file .env.prod --output-dir .runtime/compose-secrets
uv run python scripts/compose_preflight.py --env-file .env.prod
docker compose --env-file .env.prod -f compose.prod.yml \
  --profile migrate run --rm migrate migrate up \
  --target 0041 --directory /app/migrations
docker compose --env-file .env.prod -f compose.prod.yml \
  up -d --wait --remove-orphans
curl --fail http://127.0.0.1:8080/health/ready
```
