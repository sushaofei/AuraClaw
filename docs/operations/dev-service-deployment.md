# DEV_SERVICE 部署手册（开发用）

> 机器：`DEV_SERVICE` = `10.244.16.131`（见 `.host.env`）  
> 目录：`/home/jcroot/workspace/AuraClaw`  
> 入口：`http://10.244.16.131:8080`

## 日常怎么发

本机仓库根目录执行：

```bash
./scripts/dev_service_deploy.sh
```

等价于：上传代码 → 在 131 上 `docker build -t auraclaw:dev` → `compose.test.yml up`。

常用参数：

```bash
./scripts/dev_service_deploy.sh --migrate      # 顺带跑 DB 迁移
./scripts/dev_service_deploy.sh --skip-sync    # 不上传，只远程 build/up
./scripts/dev_service_deploy.sh --skip-build   # 不重建镜像，只重启
./scripts/dev_service_deploy.sh --help
```

运维查看：

```bash
./scripts/remote_compose.sh ps
./scripts/remote_compose.sh logs action-hands
```

---

## 一次性准备（只需做一次）

1. 本机有 `.host.env`，且能 SSH 免密到 `jcroot@10.244.16.131`
2. 服务器已有目录 `/home/jcroot/workspace/AuraClaw`
3. 服务器上配置 **`.env.test`**（不要从本机覆盖），至少：

```dotenv
AURACLAW_IMAGE=auraclaw:dev
KAFKA_HOST=10.244.16.132
AURACLAW_ARTIFACT_BACKEND=obs
OBS_ENDPOINT=obsv3.example.com
OBS_BUCKET=auraclaw-artifacts
OBS_REGION=replace-with-obs-region
# 其余 DSN / token / 模型 key 用服务器现网值（cp .env.test.example .env.test）
```

4. 确认脚本可执行：`chmod +x scripts/dev_service_deploy.sh`

---

## 脚本会做什么 / 不会做什么

| 会做 | 不会做 |
|------|--------|
| rsync 代码到 131 | 覆盖远程 `.env.test` |
| `docker build -t auraclaw:dev` | 覆盖 `.runtime/compose-secrets/` |
| `compose up -d --wait` | 上传 `.host.env` / 本机密码 |
| 检查 `http://131:8080/health/ready` | 默认跑 migrate（需加 `--migrate`） |

---

## 出问题：streaming-gateway unhealthy

日志里如果是 Kafka `bootstrap` / `TimeoutError`，说明 **131 连不上中间件 Kafka**：

```bash
# 在 DEV_SERVICE(131) 上测
timeout 3 bash -c 'echo >/dev/tcp/10.244.16.132/9092' && echo ok || echo fail
```

- `fail`：先修 `DEV_MIDDLEWARE`（`10.244.16.132`）的 Kafka 监听/防火墙/`advertised.listeners`，再：

```bash
./scripts/dev_service_deploy.sh --skip-sync --skip-build
```

Kafka `advertised.listeners` 已修复为真实可达地址；`compose.test.yml` 现直接连接 broker，不再使用任何 localhost 代理包装。

rsync 若曾报 `cannot delete ... Users/tong/...`，可在 131 删掉误传目录：

```bash
ssh jcroot@10.244.16.131 'rm -rf /home/jcroot/workspace/AuraClaw/Users'
```

## 一句话

改完代码后执行 `./scripts/dev_service_deploy.sh` 即可。

## TTFT / 调度间隔（与生产同构）

DEV_SERVICE 与生产共用 12 服务拓扑。缩短「创建任务 → 首 Token」时，只调配置，不改调度语义：

| 变量 | 默认 | 作用 |
|------|------|------|
| `AURACLAW_WORKER_WAKE_ENABLED` | `true` | Session append 后 HTTP 唤醒 projection/orchestrator/delivery |
| `AURACLAW_WORKER_IDLE_INTERVAL` | `0.25` | 有 wake 时的空闲 fallback 轮询（多副本兜底） |
| `AURACLAW_PROJECTION_WORKER_INTERVAL` | `0.1` | wake 关闭时的 Projection 轮询 |
| `AURACLAW_ORCHESTRATOR_WORKER_INTERVAL` | `0.1` | wake 关闭时的 Orchestrator 轮询 |
| `AURACLAW_RUNTIME_POLL_INTERVAL` | `0.05` | agent-runtime claim 轮询 |

性能结论请在多服务拓扑（本机 `compose.test.yml` 或 DEV_SERVICE）验证，不要以 `auraclaw serve` 合一体为依据。详见 #42。
