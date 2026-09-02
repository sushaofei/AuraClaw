# 代码组织与部署映射

本文是架构组件、Python 包和生产进程之间的当前映射。系统语义仍以
[系统架构文档](./system/00%20Managed%20Agent%20系统架构总览.md) 为准；本页用于回答“代码放在哪里、由哪个进程装配”。

## 依赖方向

```text
entrypoint
  -> composition
      -> api / gateways / business packages
      -> infrastructure adapters

domain -> contracts
business packages -> stable ports
infrastructure -> implements stable ports
```

硬约束由 `pyproject.toml` 中的 import-linter 合同执行：

- `contracts` 不依赖 FastAPI、业务包或基础设施；
- `domain` 只依赖 `contracts`；
- `api` 和 `gateways` 不选择具体基础设施适配器；
- 业务包不导入 `composition`；
- `infrastructure` 不依赖 `api`、`gateways` 或 `composition`；
- Query/Streaming 只读网关不调用 Session 写服务；
- Runtime 只调用协议无关的 Hands Contract，不嵌入 MCP Connector；
- Action Hands 不反向依赖 Runtime 或 Control。

## 包职责

| 包 | 主要职责 |
|---|---|
| `contracts/` | 跨边界 DTO、命令、事件和稳定协议 |
| `domain/` | Session、Collaboration、Approval 等领域状态机 |
| `session/` | Canonical Session 写路径与协作应用服务 |
| `projection/` | 可删除、可重建的投影规则与维护逻辑 |
| `control/` | Runnable、Assignment、Lease、Fencing 和调度 |
| `runtime/` | 可恢复 Agent Loop、角色执行和受控客户端 |
| `action/` | Capability Catalog、Tool/Resource/Skill 与 Hands 应用逻辑 |
| `policy/`、`credential_proxy/`、`artifact/` | 独立信任域的业务边界 |
| `delivery/`、`observability/` | 可靠结果交付、观测和审计 |
| `gateways/` | Task、Query、Streaming 接入边界 |
| `api/` | HTTP 表示、路由和鉴权上下文 |
| `infrastructure/` | SQL、Kafka、对象存储、模型和下游 Connector 适配器 |
| `composition/` | 唯一对象图、服务装配和 CLI 实现选择 |

`application/`、`admin/`、`internal/` 及各服务外观包承载兼容入口或跨边界用例；新增核心规则应优先进入上表对应的明确边界，而不是扩展通用杂项包。

## 生产进程映射

`auraclaw serve` 在本地启动与生产同构的 12 进程拓扑。`composition/services.py`
只保留服务规格、公共生命周期和薄分发入口；12 个生产对象图分别由
`composition/builders/` 下的独立 builder 装配。builder 之间不互相依赖，development 与
production 复用同一 builder，仅由 Settings/provider 选择不同 adapter。

| CLI | 服务 | 主归属代码 |
|---|---|---|
| `auraclaw api run` | Task API | `api/`、`gateways/task/` |
| `auraclaw session run` | Session Service | `session/`、`domain/` |
| `auraclaw projection run` | Projection Worker | `projection/`、`infrastructure/projection/` |
| `auraclaw orchestrator run` | Orchestrator | `control/` |
| `auraclaw runtime run` | Agent Runtime | `runtime/` |
| `auraclaw model-gateway run` | Model Gateway | `model_gateway/`、`infrastructure/model/` |
| `auraclaw hands run` | Action Hands | `action/`、`infrastructure/connectors/` |
| `auraclaw policy run` | Policy Service | `policy/` |
| `auraclaw credential-proxy run` | Credential Proxy | `credential_proxy/`、`infrastructure/credentials/` |
| `auraclaw artifact run` | Artifact Service | `artifact/`、`infrastructure/artifacts/` |
| `auraclaw streaming run` | Streaming Gateway | `gateways/streaming/`、`infrastructure/kafka/` |
| `auraclaw delivery run` | Delivery Worker | `delivery/`、`infrastructure/delivery/` |

包与部署单元不是 1:1。组件归属以职责和稳定端口为边界，具体适配器只在 `composition` 中选择。

## 变更检查

调整包边界或进程归属时，至少同步：

1. `pyproject.toml` 的 import-linter 合同；
2. 本页的包/进程映射；
3. 受影响的系统架构或 ADR；
4. [开发阶段校验清单](../development/stage-gates.md)。
