# 两机 Agent 评测部署（E2，Issue #106）

本手册对应开发评测环境，不是生产上线验收。MASTER2 运行 AuraClaw、Model Gateway 和评测客户端；
WORKER1 只运行隔离的 AppWorld Environment。业务 API/事件格式与生产依赖不变。
模型使用用户批准的现有 `qwen3.5-122b-a10b` 出口；该名称是服务别名，未验证不可变快照。

## 准入与目录

2026-10-21 检查两机均为 AlmaLinux 8.10、40 逻辑 CPU、约 125 GiB RAM，数据盘空间充足。
MASTER2 使用既有 Docker；WORKER1 使用新装 Podman，独立存储根 `/data4/appworld/container-storage`。
不恢复 WORKER1 的旧 Docker 数据目录和 cri-docker；没有重启、继续系统升级或迁移旧业务数据。
两机 `/data4` 的 fstab 项已改用核验过的 UUID，原配置先备份；AppWorld 启动前检查实际挂载 UUID。

WORKER1 原有升级恢复、Databuff 故障与外部 DNS/NTP 不通另行记录；不将评测就绪视为整机恢复。
WORKER1 已经 MASTER2 校时，NTP 同步正常。MASTER2 历史异常进程只做了当前状态与核心文件核验，
未做完整取证；准入限定在独立开发评测目录。原 Squid 等既有服务保持。

| 主机 | 路径 | 用途 |
|---|---|---|
| MASTER2 | `/data4/auraclaw-e2/source` | AuraClaw checkout、忽略的私有 Compose 配置 |
| MASTER2 | `/data4/auraclaw-e2/envs/{tau,appworld,ops}` | 两套 Python 3.11 SDK 与配置工具环境 |
| MASTER2 | `/data4/auraclaw-e2/upstream` | 固定 revision 的官方 SDK |
| MASTER2 | `/data4/auraclaw-e2/appworld` | AppWorld 客户端数据与安装收据 |
| MASTER2 | `/data4/auraclaw-e2/{manifests,results,verified-results}` | 私有配置与分轮结果 |
| WORKER1 | `/data4/appworld/{source,data,outputs,run}` | 固定 SDK、只读数据、任务输出与 Unix socket |
| WORKER1 | `/data4/appworld/{requirements.lock,provenance.json}` | 依赖与实际镜像/数据指纹 |

配置备份位于各机 `/root/maintenance/auraclaw-e2-20261021`。资产入口、SSH 主机密钥、登录信息
只保存在私有运维目录，不进入仓库和公开 issue。

## AuraClaw：统一应用数据库账号

按用户要求，11 个 SQL 服务共用 `auraclaw_app` 与同一个 `database_url` Secret。
Agent Runtime 维持内存进度与远程 Owner 调用，不直接访问 SQL。迁移使用独立管理账号
`auraclaw_migration`，不把它的 DSN 下发给业务服务。生产的分角色配置仍兼容，本环境不需要
`compose.roles.json` 或逐服务 DSN。

`postgres.eval.sql` 通过继承既有服务角色取得服务权限集合；应用账号不是超级用户，没有
CREATEDB/CREATEROLE、迁移账本写权限或 public schema CREATE 权限。先执行生产角色 bootstrap
是复用权限定义，不要求逐服务配置账号或密码。

在私有 Python 环境安装 `python-dotenv`，准备只读模型环境文件，使用以下流程；真实凭据
不写命令行。模型环境只导入四个模型字段，其余数据库、业务系统设置不会复用。

```bash
python benchmarks/deploy/prepare_master.py \
  --model-env /private/path/model.env --output-dir /private/path/eval-config \
  --image auraclaw:COMMIT --nginx-image nginx@sha256:VERIFIED_DIGEST
# 将生成的私有 deployment-config.tar 安全传至 MASTER2，解包到 source。
# 新目录 .runtime/compose-secrets 应为 0700；不要公开 tar 包或 credentials.json。
python scripts/materialize_compose_secrets.py \
  --env-file .env.test --output-dir .runtime/compose-secrets
```

Secret 文件 0600、属主 10001，入口脚本只读挂载且需 0644。`deployment-credentials.json` 必须
保留在私有输出目录以保证重复生成不会轮换已有数据库、模型入口和服务间凭据。此工具只生成
配置，不自动执行数据库操作。禁止向已有非评测数据库导入初始化 SQL。

```bash
docker network create auraclaw-e2-platform  # 仅首次创建
# 以下各条均在 source 中，统一使用这两个 Compose 文件。
docker compose --env-file .env.test -f compose.test.yml -f benchmarks/deploy/compose.eval.yml up -d postgres kafka seaweedfs
docker compose --env-file .env.test -f compose.test.yml -f benchmarks/deploy/compose.eval.yml run --rm migrate </dev/null
docker compose --env-file .env.test -f compose.test.yml -f benchmarks/deploy/compose.eval.yml exec -T postgres psql -v ON_ERROR_STOP=1 -U auraclaw_migration -d auraclaw < deploy/postgres/roles.sql
docker compose --env-file .env.test -f compose.test.yml -f benchmarks/deploy/compose.eval.yml exec -T postgres psql -v ON_ERROR_STOP=1 -U auraclaw_migration -d auraclaw < benchmarks/deploy/postgres.eval.sql
docker compose --env-file .env.test -f compose.test.yml -f benchmarks/deploy/compose.eval.yml exec -T postgres psql -v ON_ERROR_STOP=1 -U auraclaw_migration -d auraclaw < .runtime/app-password.sql
docker compose --env-file .env.test -f compose.test.yml -f benchmarks/deploy/compose.eval.yml up -d </dev/null
```

迁移目标 0071。PostgreSQL、Kafka、SeaweedFS 和 Nginx 使用 digest；仅使用新建的评测 volume。
12 个应用进程各 1 副本、2 CPU/2 GiB 上限；PostgreSQL/Kafka 各 4 GiB、SeaweedFS 2 GiB。
Kafka 的 network-none 初始化容器只负责新 volume 的属主。入口 `run_service.py` 复用
`composition.cli._run_service_process`，保留迁移校验、builder、生命周期和 worker 关闭；
拒绝非 development profile、SQL/Kafka 缺失及 Runtime 获得 Provider 密钥。

入口只监听 MASTER2 loopback：任务 API `18080`，评测 Model Gateway `18005`。
Provider 密钥仅给 Model Gateway；τ 用户模拟器在独立 SDK 客户端使用自己的本地环境配置。
没有 Vault、扫描器、外部 OTLP/告警验收，Credential Proxy 使用空内存 Vault；不能据此宣称生产就绪。

## AppWorld：SDK、隔离与连接

两个 SDK 安装在独立 Python 3.11 环境，不修改生产 `uv.lock`。固定 AppWorld 上游后完成 Git LFS
与受保护代码安装，下载官方数据时显式传 `--root`；CLI 默认当前目录不能靠 APPWORLD_ROOT 替代。
固定兼容性约束见 `appworld-constraints.txt`：Pydantic 2.12.5、FastAPI 0.121.3、Starlette 0.49.3。
两端安装后均执行 `pip check`，保存完整冻结依赖；不要使用上游未限制的最新版本直接部署。

`AppWorld.Containerfile` 接收已核验的 Python 3.11 base digest、固定 revision 与全部冻结 wheel。
离线安装后完成 `appworld install`，保留 wheel/锁文件指纹。禁止把解包代码、数据、答案上传本仓库。

安装仓库提供的 environment、HTTP socket/service 与 preflight 文件；将实际 UID/GID、镜像 ID
与数据盘 UUID 写入私有 `/etc/auraclaw-eval/appworld.env`。Environment 只通过 Unix socket 服务，
容器 network-none、非 root 用户、只读根、cap-drop ALL、no-new-privileges，8 CPU/16 GiB/256 PID。
只挂载 AppWorld 数据 RO、输出 RW、socket RW，无主机模型凭据、业务目录或容器引擎 socket。
宿主机 socket proxy 只监听 `127.0.0.1:18000`。

MASTER2 的 `auraclaw-appworld-tunnel.service` 使用本项目新建专用密钥与严格 known_hosts，
经 SSH 转发到 WORKER1 loopback 18000。WORKER1 专用 nologin 账号限制来源主机、只准转发
`127.0.0.1:18000`，禁止 shell。环境地址因此为 MASTER2 `http://127.0.0.1:18000`，
manifest 必须保留 `appworld_remote_docker=true`。该服务独立于安装阶段的本地下载代理。
代理环境需配置 NO_PROXY/no_proxy 为 localhost、127.0.0.1、::1。

AppWorld 服务只允许串行使用，不在模型任务运行期间初始化另一个 world。服务启动后分别验证
无动作任务得 0、公开 dev solution 的官方自检通过；自检结果与 Agent 结果分开保存。
Agent 只接收公开 API 文档查询用法，不接触 solution、测试或评分内部内容。

## 数据与运行证据

上游版本分别为 τ-bench `59a200c6d575d595120f1cb70fea53cef0632f6b`、
AppWorld `42b5bcf3cd334fee33f0c37c02070a9f5807add5`。CLI 每次尝试在加载任务前按排序的相对
路径与文件内容哈希核验 SHA-256 清单，拒绝符号链接，忽略 Python 字节码；不再只相信声明。
AppWorld 服务端也独立校验，14885 个文件、174032410 字节，与客户端一致。

- τ retail 清单：`sha256:11ce3166f30cb07df35c02d54e2d92c09736d1a2593aae56c9b9cdff25650441`。
- AppWorld 清单：`sha256:422c87067509dbc86a48da7b1011bb5f328a10841ee535508daf611d551ceb84`。
- AppWorld 运行镜像 ID：`sha256:2748fac89d5ae1b4abe5832b86e67a982f87af1649aadc38d4fc6d46a1013f54`。
- AppWorld 冻结依赖摘要：`sha256:39c1621ae9a0abff55a6156bde5e53822465c12b69001bf5ffcf061c123779bd`。

候选源码联调采用确定的 dev 小样本：τ retail 前两个索引、AppWorld 前两个不同任务族的首题，
各 1 trial。固定动作 30、总输出预算 24576、每回合 4096、墙钟 600 秒、配置 seed 10。
Qwen thinking=false、Gateway retry=1，temperature 使用 Provider 默认；模拟器与模型输出不保证确定性。

| 2026-10-21 联调轮次 | τ-bench | AppWorld | 用途 |
|---|---|---|---|
| 首轮诊断 | 1/1 成功 | 0/1，动作预算耗尽 | 保留失败；当时未核验数据且缺公开文档查询引导 |
| 数据核验后候选轮 | 2/2 成功 | 2/2 成功，官方 CLI 复评同样通过 | E2 工具/环境/评分联调 |

候选轮记录基于 E1 HEAD 加本阶段工作区修复，不作为不可变 Harness 正式基线。最终阶段 commit
部署后的确认轮、镜像 ID 和运行 ID 记录在 #106 与私有运维记录，不与候选轮合并统计。
AppWorld 单题报告中的 SGC 不适用于完整 scenario，本阶段只核验单题 TGC/reward。
成本未知保持 null；不发布完整测试集成绩，也不将这些已用于排错的 dev 任务用于 E3 保留集。

统一账号后的签名任务链路已验证：API 200、completed、Canonical 8 条事件与结果投影 source_version=8；
初始投影故障通过 Session 自有 outbox 接口恢复，没有直接改写 Canonical Events。修复包括 Task API
replay ledger、服务自身观测写权限，以及分布式 Collaboration 投影通过 Session root-feed 读取事实。
即使服务共用 SQL 账号，组件事实读取仍使用已有 Owner 契约。

本地质量验证：987 passed、74 skipped，Ruff、Mypy strict（328 文件）、23 条架构合同、release gate
通过；外部条件跳过项不算生产验收。真实 PostgreSQL 逐角色测试和统一应用账号登录/权限检查通过。
既有 CI 依赖审计/镜像扫描失败仍阻断生产发布，由本阶段开发联调结果不能解除。

## 恢复与停止

停止评测客户端后再停 MASTER2 专用 tunnel、WORKER1 socket proxy 与 Environment。
保留任务输出、依赖锁、数据摘要、镜像与数据库 volume；不要使用 `down -v`、清空输出或恢复旧 Docker。
AuraClaw 回滚时恢复上一个镜像及配套私有配置；统一账号切换前配置位于
`/root/maintenance/auraclaw-e2-20261021/pre-unified-db`。恢复旧分角色配置会重新遇到未修复投影权限，
优先回退镜像并保留统一应用账号。配置、系统单位、fstab/chrony 逐项从备份恢复，不执行整机回滚。
源码可整体回退阶段提交；本阶段没有新增数据库结构迁移，数据库账号与权限 bootstrap 独立管理。
