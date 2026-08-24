# AuraClaw 本地智问调试：Python 端改动说明

日期：2026-08-22

范围：本地对接 Chaintower 智问（Java Gateway `:48080`）+ Java MCP，使「查看价格洞察」能跑通。
本文只记录 **Python 端代码**（`src/auraclaw` 与对应单测）。已并入仓库 `build_change_summary.md` 第二节的 Python 改动。本地配置见文末附录。

调用链没有改：Agent Runtime **从不直接打 Java MCP**。

```
智问 UI → Java Gateway :48080 → AuraClaw Ingress :8080
  → Task API :8000 / Streaming :8010
  → Runtime :8004 → Hands :8006 → Credential Proxy :8008
  → Java MCP POST http://127.0.0.1:48080/rpc-api/agent-runtime/mcp
```

租户为 `"1"`。本地存储是内存，重启服务会清空会话，智问需要新开对话。Java 创建任务体是 `{"goal":"..."}`，**没有**传 `max_steps`；历史上的 16 步来自 Python Runtime 旧默认值，不是 Java 请求写死的。

---

## 1. 本机 HTTP MCP 出站被拒绝

**现象：** Java MCP 在 `http://127.0.0.1:48080`，Hands / Credential Proxy 默认只允许公网 HTTPS，本机回环被拦。

**改了什么：**

| 文件 | 改动 | 原因 |
|---|---|---|
| `src/auraclaw/contracts/capabilities.py` | MCP endpoint 允许 `http://`；增加 `allowed_private_hosts` | 合同层允许本机回环地址进入配置 |
| `src/auraclaw/infrastructure/credentials/mcp_egress.py` | 出站校验尊重 `allowed_private_hosts`，允许 127.0.0.1 私网/回环 | 真正发 JSON-RPC 的是 Credential Proxy，必须在这里放行 |
| `src/auraclaw/infrastructure/persistence/postgres_capability_catalog.py` | 持久化/读回 `allowed_private_hosts` | 与合同字段对齐，避免配置丢失 |
| `src/auraclaw/infrastructure/connectors/mcp/wire.py` | 协议/元数据相关补充（含 Java 协议修订、userId meta） | 对齐 Java Gateway MCP 协议与可信用户头 |
| `src/auraclaw/config.py` | 读取 debug vault 等开发配置 | 本机凭证不进 Server Registry |
| `tests/unit/test_m9_mcp_egress.py` | 回环 HTTP + allowlist 用例 | 防止再次把本机 MCP 挡掉 |
| `tests/unit/test_hands_mcp_debug.py` | Hands 调 MCP 的 debug 路径用例 | 覆盖本地 Java MCP 装配 |

---

## 2. Credential Proxy 策略动作对不上

**现象：** Hands 侧策略评估的是 `mcp.remote.invoke`，Credential Proxy 校验时用的是 `mcp.invoke`，决策对不上，远程调用被拒。

**改了什么：**

| 文件 | 改动 | 原因 |
|---|---|---|
| `src/auraclaw/credential_proxy/internal_service.py` | `mcp.invoke` → 校验 `mcp.remote.invoke`；`http.invoke` → `java-api.remote.invoke` | Hands 写入的是 remote-invoke 决策，Proxy 必须用同一 action 校验 |

---

## 3. 智问打到 8000，SSE / 任务进错入口

**现象：** Java `chaintower.agent-chat.python.base-url` 应对准 Ingress `:8080`（`/v1/streams/*` 去 Streaming，其余去 Task API）。直连 `:8000` 时流式和任务会错位。本地还缺一个类似 Nginx 的入口。

**改了什么：**

| 文件 | 改动 | 原因 |
|---|---|---|
| `src/auraclaw/composition/local_ingress.py` | 新增本地 Ingress：按路径转发；httpx `trust_env=False` | 本地模拟生产 Nginx；避免代理环境变量干扰 127.0.0.1 |
| `src/auraclaw/composition/cli.py` | `auraclaw serve` 拉起 12 个入口 + Ingress `:8080` | 一条命令起全套，智问只配 8080 |
| `src/auraclaw/composition/services.py` | 装配 Hands、MCP reconciler、Skill 发布、开发态产物存储 | 把 Java MCP、Skill、Hands 接到同一套本地拓扑 |
| `tests/unit/test_local_ingress.py` | Ingress 路径与 SSE 转发 | 保证 `/v1/streams/` 不会进 Task API |
| `tests/unit/test_s2_service_topology.py` | serve 拓扑 | 防止入口漏起或端口错 |

---

## 4. Java MCP 工具名和入参与 Skill 不一致

**现象（来自 `build_change_summary.md`）：** Java MCP 已能 `initialize` / `tools/list`，直接 `tools/call` 也能返回价格画像；但 AuraClaw Skill 用的是 `procurement.price.*`，Java 目录名是 `price_insight.*`。不映射时，目录能看见工具，模型和 Skill 却对不上规范名。Java 入参 Schema 还要求根对象是 `{"input": {...}}`，Skill 传的是业务参数对象，不包装会校验失败。

**改了什么：**

| 文件 | 改动 | 原因 |
|---|---|---|
| `src/auraclaw/infrastructure/connectors/mcp/connector.py` | `tool_name_aliases`：发现时对内暴露 `procurement.price.*`，`tools/call` 再还原成 Java 的 `price_insight.*` | 能力目录、Skill `required_tools`、模型调用使用同一套 canonical name |
| `src/auraclaw/infrastructure/connectors/mcp/connector.py` | 若远程 Schema 根参数是必填 `input`，调用时自动把业务参数包进 `{"input": ...}` | 满足 Java MCP 输入校验，Skill 不必改成 Java 形状 |
| `tests/unit/test_m9_catalog_reconciliation.py` | 别名解析 + `input` 包装 | 回归「对内规范名、对外 Java 名和入参」 |

---

## 5. 中文问题搜不到 Java 价格洞察工具

**现象：** 问「查看价格洞察2024年1月到2024年6月的数据」时，搜索把中文丢掉，只剩 `2024`，目录为空。模型认为没有价格洞察能力，Java `tools/call` 从未发生。

**改了什么：**

| 文件 | 改动 | 原因 |
|---|---|---|
| `src/auraclaw/action/capability_catalog.py` | 搜索分词增加 CJK 整词 + 二字 gram；原先只吃 `[A-Za-z0-9_.-]+` | 「价格洞察」必须能命中目录 |
| `src/auraclaw/action/catalog_reconciler.py` | `_capability_semver`：`"1"` → `"1.0.0"`；`_tool_search_tags`：别名、中文标签、`procurement.price.*` | Java 工具 version=`1` 无法入库；中文/别名搜不到 `price_insight.*` |
| `src/auraclaw/composition/services.py` | 开发态 Hands 使用内存 `ArtifactStore` 并装配 MCP reconciler | SeaweedFS `:8333` 没跑时，Skill 激活仍能读取本地产物 |
| `tests/fixtures/skills/procurement-price-insight/manifest.json` | `applies_when` 含「价格洞察」 | Skill 检索条件与用户说法对齐 |
| `tests/unit/test_m9_capability_catalog.py` | 中文查询不含年份也能命中 | 回归「只搜到 2024」的 bug |
| `tests/unit/test_m9_catalog_reconciliation.py` | 别名、semver、search_tags | 回归 Java 工具名映射与入库 |

---

## 6. Skill 激活后读输出契约 403

**现象：** 搜索/加载/激活已经成功，`resources/read` 第一次 200，读输出契约时报 `Resource media type is not allowed`。

**改了什么：**

| 文件 | 改动 | 原因 |
|---|---|---|
| `src/auraclaw/action/resource_gateway.py` | 允许 `application/schema+json`；`application/*+json` 当 JSON | 输出契约 mime 是 `application/schema+json`，不在旧白名单里 |
| `tests/unit/test_m9_resource_gateway.py` | 对应媒体类型用例 | 防止契约资源再次被 403 |

---

## 7. 工具已找到，却反复 `policy_denied`，最后 no-progress

**现象：** UI 报 `Runtime detected a repeated no-progress capability call`。轨迹里 `procurement.price.dataset.profile` 连续被拒，摘要是 `controlled execution boundary denied the tool call`。

根因：Java MCP 使用 `workload_trusted_context`，`tools/call` **必须带可信 `user_id`**。Hands 租约里有，但 `ConnectorToolExecutor` 重建上下文时丢掉了。

**改了什么：**

| 文件 | 改动 | 原因 |
|---|---|---|
| `src/auraclaw/contracts/tools.py` | `ToolInvocation` 增加 `user_id` | 工具调用要能携带任务所有者，不能只靠模型参数 |
| `src/auraclaw/action/hands.py` | `call_tool` 把 `trusted.user_id` 写入 invocation | Hands HTTP/进程内入口都有租约身份，这里是唯一交接处 |
| `src/auraclaw/internal/tool_client.py` | 开发态直连 ToolGateway 时带上 `assignment.user_id` | 与生产 Hands 路径行为一致 |
| `src/auraclaw/action/catalog_reconciler.py` | `ConnectorToolExecutor` 把 `user_id` 传给 MCP connector | 真正发 `tools/call` 的地方，缺 user_id 会直接 PolicyDenied |
| `src/auraclaw/action/tool_gateway.py` | 拒绝摘要改为真实异常信息 | 以前一律「controlled execution boundary…」，模型只能盲着重试同一调用 |
| `tests/unit/test_hands_contract.py` | 断言 invocation 带 `user_id` | 防止 Hands 再丢身份 |
| `tests/unit/test_m9_catalog_reconciliation.py` | 无 user_id 则 `trusted user` 拒绝；有则转发到 MCP identity / `_meta` | 回归 Java MCP 这条硬约束 |
| `tests/unit/test_m3_tool_security.py` | 边界拒绝原因会回到 ToolResult.summary | 回归可诊断性 |

修好后，`2024-01`～`2024-06` 可以调到 Java 工具；该区间源表无数据，智能体建议改时间范围，这是数据事实，不是 Python bug。

---

## 8. 步骤预算：16 → 32 → 48，以及 follow-up 耗尽

### 8.1 先把默认预算从 16 提到 32（`build_change_summary.md`）

**现象：** 页面报 `Runtime capability step budget was exhausted`。Java 侧 `tools/call` 其实已经能返回画像。Java **没有**在创建任务时传 `max_steps=16`；16 是旧 Python Runtime 进程的默认值。价格洞察完整流程包含能力搜索、加载、Skill 激活、依赖装载和八项原子指标，16 步不够一轮。

**改了什么（同一组文件，先统一到 32）：**

| 文件 | 改动 | 原因 |
|---|---|---|
| `src/auraclaw/control/ports.py` | `RuntimeBudget.max_steps` 默认 16 → 32 | 覆盖一整轮价格洞察 |
| `src/auraclaw/control/orchestrator.py` | 入队/调度缺省用同一值 | 避免 Orchestrator 仍恢复 16 |
| `src/auraclaw/control/runnable_feed.py` | 从 Session 事件恢复预算时用同一值 | 避免 feed 路径写回旧默认 |
| `src/auraclaw/infrastructure/clients/runtime.py` | Runtime claim assignment 时用同一值 | 避免 Runtime 进程自己带回 16 |
| `src/auraclaw/infrastructure/persistence/postgres_control_store.py` | 持久化读回缺省用同一值 | 各服务恢复预算口径一致 |

显式传入的任务预算仍然优先。改完必须重启 Python 服务，旧进程内存里仍是 16。

### 8.2 再把默认预算从 32 提到 48，并修质量检查 / follow-up 加载

**现象：** 第一次问 2024 上半年（无数据）成功给出建议后，再问「2024年12月到2025年6月」仍报步数耗尽。轨迹显示 profile 已有约 60 条、多个指标也算完了，但没写成结论。

叠加原因：

1. **质量检查被当成工具失败。** Java 返回 `structuredContent.status = "PASS"`（业务数据质量），MCP connector 把它当成 Hands 工具状态，`PASS ≠ success` 就抛错，白烧 3 步。
2. **Follow-up 是新 run。** 上一轮加载的工具不在新 run 的 `candidates` 里，`capabilities.load` 被过滤成空；模型又乱调 `resources.read`，再搜再加载，浪费很多步。
3. **一次 load 超过 8 个 id 直接 ValueError。** 价格洞察相关能力经常一次塞 10 个，变成 `tool_adapter_error`。
4. **32 步仍不够 follow-up。** 模型轮次和工具调用都计数。重发现 + 3 次画像 + 8 个指标，写结论前刚好耗尽。

**改了什么：**

| 文件 | 改动 | 原因 |
|---|---|---|
| `src/auraclaw/infrastructure/connectors/mcp/connector.py` | 只有 `status` 属于 `ToolResultStatus`（success/error/denied…）才当工具信封；`PASS`/`WARN` 整包当成功业务内容 | 质量检查的业务 status 不是 AuraClaw 工具调用状态 |
| `src/auraclaw/action/catalog_reconciler.py` | `_executor_payload` 只对 error/denied/timeout/cancelled 抛错 | 即使业务 status 漏过来，也不要把 PASS 打成 adapter error |
| `src/auraclaw/runtime/capability_controller.py` | load 不再要求 id 必须先出现在本 run 的 search candidates；加载成功的能力写回 candidates | 同一会话第二条消息可以按上一轮的 `cap_*` 直接 load |
| `src/auraclaw/action/capability_catalog.py` | 一次 load 上限 8 → 24（与 controller `max_loaded` 对齐） | 一次加载价格洞察工具+资源不再因 10 个 id 崩掉 |
| `src/auraclaw/control/ports.py` | 默认 `max_steps` 32 → 48，抽出 `DEFAULT_RUNTIME_MAX_STEPS` | 覆盖「完整价格洞察流程 + follow-up 重发现」 |
| `src/auraclaw/control/orchestrator.py` | fallback 用同一常量 | 避免各处仍写死 32 |
| `src/auraclaw/control/runnable_feed.py` | 同上 | 从 Session 事件恢复预算时不要缩回 32 |
| `src/auraclaw/infrastructure/clients/runtime.py` | 同上 | Runtime 认领 assignment 时对齐 |
| `src/auraclaw/infrastructure/persistence/postgres_control_store.py` | 同上 | 读预算缺省值对齐 |
| `tests/unit/test_m9_catalog_reconciliation.py` | `status=PASS` 的 structuredContent 必须是 success + 完整内容 | 回归质量检查误伤 |
| `tests/unit/test_m11_capability_agent_loop.py` | 空 state 也能按 capability_id load | 回归 follow-up 空 candidates |

---

## 9. 代码审查后的加固（Issue #47）

提交前审查补充了以下修复：

| 文件 | 改动 | 原因 |
|---|---|---|
| `src/auraclaw/composition/cli.py` | `auraclaw serve` 要求共享 SQL 或 Kafka Runtime Event 后端 | 多进程内存 Replay Bus 彼此隔离，Streaming Gateway 收不到 Runtime 进程的 SSE 事件 |
| `.env.dev.example` | 本地多进程示例改用 Kafka Runtime Event 后端 | 默认配置直接满足跨进程 SSE 条件 |
| `src/auraclaw/infrastructure/credentials/mcp_egress.py` | HTTP MCP 只允许解析到显式白名单中的私网/回环地址；公网地址仍强制 HTTPS | 防止把公网主机放入 private allowlist 后降级为明文 HTTP |
| `src/auraclaw/action/catalog_reconciler.py` | 可信远端工具注解缺失时回退到 `write-with-approval` + `high` | 避免可选字段为 `None` 时枚举构造失败，并保持保守安全默认值 |
| `.gitignore` | 忽略 `.host.env` 和误拼写的 `.eng.debug` | 防止本机地址或配置被误提交 |

新增回归测试覆盖纯内存多进程拒绝启动、公网 HTTP MCP 拒绝和远端注解安全默认值。

---

## 按文件速查（Python 源码）

```
src/auraclaw/action/capability_catalog.py          # 中文搜索；load 上限 24
src/auraclaw/action/catalog_reconciler.py          # 标签/semver；MCP user_id；PASS 不当失败
src/auraclaw/action/hands.py                       # 租约 user_id → ToolInvocation
src/auraclaw/action/resource_gateway.py            # schema+json 白名单
src/auraclaw/action/tool_gateway.py                # 拒绝原因回传模型
src/auraclaw/composition/cli.py                    # serve 拓扑 + Ingress
src/auraclaw/composition/local_ingress.py          # 本地 8080 入口（新增）
src/auraclaw/composition/services.py               # Hands/MCP/Skill/内存产物装配
src/auraclaw/config.py                             # MCP egress 配置读取
src/auraclaw/contracts/capabilities.py             # http + private hosts
src/auraclaw/contracts/tools.py                    # ToolInvocation.user_id
src/auraclaw/control/ports.py                      # 默认步数 16→32→48
src/auraclaw/control/orchestrator.py               # 预算缺省
src/auraclaw/control/runnable_feed.py              # 预算缺省
src/auraclaw/credential_proxy/internal_service.py  # 策略 action 映射
src/auraclaw/infrastructure/clients/runtime.py     # 预算缺省
src/auraclaw/infrastructure/connectors/mcp/connector.py
        # 工具别名；input 包装；Java 协议；PASS≠工具失败
src/auraclaw/infrastructure/connectors/mcp/wire.py
src/auraclaw/infrastructure/credentials/mcp_egress.py     # 本机回环出站
src/auraclaw/infrastructure/persistence/postgres_capability_catalog.py
src/auraclaw/infrastructure/persistence/postgres_control_store.py
src/auraclaw/internal/tool_client.py               # 开发态带 user_id
src/auraclaw/runtime/capability_controller.py      # follow-up 可按 id load
tests/fixtures/skills/procurement-price-insight/manifest.json
```

对应单测主要在：`test_m9_mcp_egress.py`、`test_hands_mcp_debug.py`、`test_local_ingress.py`、`test_s2_service_topology.py`、`test_m9_capability_catalog.py`、`test_m9_catalog_reconciliation.py`、`test_m9_resource_gateway.py`、`test_hands_contract.py`、`test_m3_tool_security.py`、`test_m11_capability_agent_loop.py`。

---

## 附录：本地配置（非 Python 逻辑，但调试依赖）

这些文件 **不要提交**（含地址/密钥占位）：

- `.env.dev`：项目默认自动读取该文件，也可用 `AURACLAW_ENV_FILE` 显式指定。关键项包括共享 SQL 或 Kafka Runtime Event 后端、`AURACLAW_DEBUG_VAULT_SECRETS_JSON`、模型名。
- Java / AuraMCP Server 用 `POST /v1/admin/mcp-servers` 热配置登记。本机 Java loopback body（含 `tool_name_aliases` / `search_tags`）见 [MCP 开发手册](./MCP%20开发手册.md) §5.2。
- `.vscode/launch.json` / `tasks.json` / `settings.json`：Windows 本机调试入口，不是业务逻辑。

Java 侧需要：`chaintower.agent-chat.python.base-url = http://127.0.0.1:8080`（不要写 8000）。本地联调可关 MCP RSA（`crypto.enabled=false`）；生产必须恢复加密与受控私钥。
