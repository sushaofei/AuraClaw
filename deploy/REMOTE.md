# AuraClaw 远程部署目录说明

本目录用于 `compose.production.yml` 生产部署。详细步骤见仓库内
`docs/S5 Docker Compose 生产部署与故障演练 Runbook.md`。

## 目录内容

| 路径 | 用途 |
|------|------|
| `compose.production.yml` | 生产 Compose 模板 |
| `.env.production.example` | 生产 env 模板（复制为 gitignored `.env.production`） |
| `.env.production` | 部署环境变量（0600，勿提交） |
| `.runtime/compose-secrets/` | Compose secrets 文件（0700/0600） |
| `deploy/nginx.conf` | Ingress 配置 |
| `deploy/mysql/roles.sql` | MySQL 角色授权 |
| `Dockerfile` / `src/` / `migrations/` | 本地构建镜像用 |
| `scripts/` | secrets 物化与 preflight |

## 部署前检查

> **硬性要求**：需要 **Docker Engine ≥ 20.10** 与 **Compose v2**（`docker compose`）。
> Docker 18.09 / `docker-compose` 1.22 无法解析顶层 `name:`、`--env-file`、
> `profiles`、`depends_on: service_healthy` 等语法，必须先升级再启动。

```bash
docker --version
docker compose version

# 外部平台网络
docker network inspect auraclaw-platform >/dev/null 2>&1 ||
  docker network create auraclaw-platform

# 确认镜像存在
docker image inspect auraclaw:s5 >/dev/null
```

## 常用命令

```bash
# 如需重新物化 secrets（需本机 Python + python-dotenv）
python3 scripts/materialize_compose_secrets.py \
  --env-file .env.production --output-dir .runtime/compose-secrets

python3 scripts/compose_preflight.py --env-file .env.production

# 迁移
docker compose --env-file .env.production \
  -f compose.production.yml --profile migrate run --rm migrate

# 启动
docker compose --env-file .env.production \
  -f compose.production.yml up -d --wait --remove-orphans

curl --fail http://127.0.0.1:8080/health/ready
```
