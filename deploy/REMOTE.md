# AuraClaw 远程部署目录说明

本目录用于 `compose.prod.yml` 与 `compose.test.yml` 部署。详细步骤见仓库内
`docs/operations/production-deployment.md`。

## 目录内容

| 路径 | 用途 |
|------|------|
| `compose.test.yml` | 服务器测试 Compose 模板 |
| `compose.prod.yml` | 生产 Compose 模板 |
| `.env.test.example` | 服务器测试 env 模板（复制为 gitignored `.env.test`） |
| `.env.prod.example` | 生产 env 模板（当前与 test 一致；复制为 `.env.prod`） |
| `.env.test` / `.env.prod` | 部署环境变量（0600，勿提交） |
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
  --env-file .env.test --output-dir .runtime/compose-secrets

python3 scripts/compose_preflight.py --env-file .env.test

# 迁移
docker compose --env-file .env.test \
  -f compose.test.yml --profile migrate run --rm migrate

# 启动（测试环境；生产将 .env.test 换为 .env.prod）
docker compose --env-file .env.test \
  -f compose.test.yml up -d --wait --remove-orphans

curl --fail http://127.0.0.1:8080/health/ready
```
