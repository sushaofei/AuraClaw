# Skill 生成、发布与维护手册

## 从 0 到发布的完整流程

首次准备 Publisher：

`确认租户与发布主体 → 生成 Ed25519 密钥对 → 注册 Publisher → 将公钥登记为 active key → 妥善保管私钥`

首次生成并发布 Skill：

`创建 Skill 目录 → 编写 manifest.json → 编写 SKILL.md → 添加 tests/*.json → 使用 Publisher 私钥签名 → 使用对应公钥执行 validate → 执行声明式 test → CLI 将 canonical archive 上传到 AuraClaw → AuraClaw 代理写入 OBS 并完成 Artifact finalize → 使用 artifact_ref 创建 Publication → AuraClaw 执行 digest、签名、Publisher、Source 和内容安全准入 → 创建或更新 Installation → 检查 Catalog、Publication、Installation 和 Admission 审计 → 发布完成`

后续版本维护：

`修改 Skill → 按 SemVer 提升版本 → 更新测试 → 重新签名 → validate → test → 通过 AuraClaw 代理上传 → 创建新版本 Publication → 验证准入与安装状态`

安全或下线维护：

`普通停止新任务使用 → disable Installation → 需要卸载时进入 draining → 全部引用结束后 uninstalled`

`来源退役或版本在连续完整快照中缺失 → Publication 进入 retired → 来源恢复后由管理员显式 restore`

`发现密钥或内容安全事件 → suspend Publisher 或 revoke key/Publication/Publisher → 选择 pause 或 cancel 运行时动作 → 检查审计与受影响 binding`

其中，Publisher 只把公钥登记到 AuraClaw；私钥不上传。AuraX、CLI 和其他客户端只连接 AuraClaw，不直接连接 OBS。

本文面向 Skill 作者、Publisher 管理员和发布运维人员，说明如何创建一个 AuraClaw Skill，完成本地校验、签名、发布、升级、下线与安全维护。

AuraX、CLI 和其他管理端只与 AuraClaw 通信。客户端不会直连 OBS，也不需要保存 OBS endpoint、AK/SK、上传 ID、分片 ETag 或预签名 URL；AuraClaw 负责对象存储上传、完整性校验和 Artifact 绑定。

## 1. 发布流程概览

推荐顺序如下：

1. 创建符合目录规范的 Skill。
2. 编写声明式测试；外部 Publisher 使用 Ed25519 私钥离线签名。
3. 使用签名对应的公钥在本地执行校验和测试。
4. 租户管理员登记 Publisher 公钥。
5. 使用 CLI 将包代理上传到 AuraClaw，并创建 Publication。
6. 检查 Publication、Installation 和 Admission 审计结果。
7. 后续升级使用新语义版本重新签名、发布；下线与安全事件使用对应管理命令。

平台自有 Skill 可继续使用内部兼容签名路径。第三方和业务 Publisher 应使用 Ed25519，Registry 只保存公钥，私钥始终留在 Publisher 自己的签名环境。

## 2. 准备环境

在 AuraClaw 仓库中安装开发依赖：

```bash
uv sync --extra dev
```

发布前准备以下环境变量。不要把真实令牌或私钥写入仓库、Shell 历史、日志或 Skill 包：

```bash
export AURACLAW_API_TOKEN='<AuraClaw access token>'
export AURACLAW_SKILL_SIGNING_KEY='<Ed25519 raw private key, unpadded base64url>'
export AURACLAW_SKILL_PUBLIC_KEY='<Ed25519 raw public key, unpadded base64url>'
```

私钥和公钥都是 32-byte raw key 的无 padding base64url 表示。CLI 不接受明文私钥参数；发布令牌同样只从环境变量读取。

## 3. 创建 Skill 目录

最小目录：

```text
example-skill/
├── manifest.json
└── SKILL.md
```

完整目录可以包含：

```text
example-skill/
├── manifest.json
├── SKILL.md
├── references/
├── scripts/
│   └── main.workflow.json
├── assets/
└── tests/
    └── basic.json
```

根目录只允许 `manifest.json`、`SKILL.md`、`references/`、`scripts/`、`assets/` 和 `tests/`。路径必须是规范相对路径，不得包含绝对路径、`.`、`..`、反斜杠或符号链接。`scripts/` 只允许 `*.workflow.json` 声明式工作流；Python、Shell、JavaScript、Wasm、二进制和可执行文件仍被拒绝。

本地打包限制为最多 512 个文件、文件内容合计不超过 16 MiB；AuraClaw 代理上传请求体上限为 24 MiB。包内不得包含任意代码脚本、可执行文件、私钥、令牌、Secret 赋值或指令劫持内容。

### 3.1 声明式 Workflow

需要确定性编排 Tool/Resource 时，在 Manifest 中声明：

```json
{
  "workflow": {
    "api_version": "skills.auraclaw.io/v1alpha1",
    "entrypoint": "scripts/main.workflow.json"
  },
  "required_references": [
    {
      "path": "references/mapping.json",
      "media_type": "application/json",
      "max_bytes": 65536,
      "preload": false
    }
  ]
}
```

Workflow 首版只支持顺序 `tool.call` 和 `resource.read`。参数值必须使用 `{"literal": ...}` 或
`{"from": "$input..."}` / `$state...` / `$references...` 结构化 selector；不支持模板代码、函数、循环或
动态网络请求。调用的 capability 必须同时出现在 Manifest 的 `required_tools` 或 `required_resources`。

```json
{
  "apiVersion": "skills.auraclaw.io/v1alpha1",
  "kind": "Workflow",
  "references": [
    {"id": "mapping", "path": "references/mapping.json", "required": true}
  ],
  "steps": [
    {
      "id": "lookup",
      "operation": "tool.call",
      "capability": "inventory.lookup",
      "arguments": {
        "sku": {"from": "$input.sku"},
        "region": {"from": "$references.mapping.region"}
      },
      "result": "item",
      "timeout_seconds": 20,
      "retry": {
        "max_attempts": 2,
        "strategy": "exponential",
        "retry_on": ["timeout", "unavailable"]
      }
    }
  ],
  "outputs": {"item": {"from": "$state.item"}}
}
```

`preload=true` 只用于模型确实需要看到的 reference，并计入 Runtime prompt bytes/token 门禁。Executor 使用的
JSON reference 由 Workflow 显式列出并按固定 package digest 加载；不要把整个 `references/` 目录预加载。

### 3.2 manifest.json

下面是一个外部 Publisher 的起始示例。首次签名前可以省略 `signature` 和 `signature_key_id`，`skills sign` 会原子写入这两个字段：

```json
{
  "name": "incident-triage",
  "version": "1.0.0",
  "description": "根据告警上下文生成结构化的故障分诊建议。",
  "publisher": "acme",
  "applies_when": ["收到需要分析的服务告警"],
  "not_when": ["请求直接修改生产环境"],
  "input_schema": {
    "type": "object",
    "properties": {
      "alert": {"type": "string"},
      "service": {"type": "string"}
    },
    "required": ["alert"]
  },
  "output_schema": {
    "type": "object",
    "properties": {
      "summary": {"type": "string"},
      "next_actions": {
        "type": "array",
        "items": {"type": "string"}
      }
    },
    "required": ["summary", "next_actions"]
  },
  "required_tools": [],
  "required_resources": [],
  "required_skills": [],
  "allowed_roles": ["coordinator", "worker"],
  "data_classification": "internal",
  "risk_level": "medium",
  "max_steps": 20,
  "timeout_seconds": 900
}
```

关键规则：

- `name` 使用小写字母、数字以及分隔符 `.`, `_`, `-`，每个分段必须以字母或数字开始和结束。
- `version` 使用严格 SemVer，例如 `1.2.0` 或 `2.0.0-rc.1`。
- `publisher` 必须与签名命令及租户 Publisher Registry 中的身份一致。
- `input_schema` 和 `output_schema` 必须是 `type: object` 的 JSON Schema。
- `max_steps` 范围为 1–1000，`timeout_seconds` 范围为 1–86400。
- Skill 依赖版本可使用 `*` 或逗号分隔的比较条件，例如 `>=1.2.0,<2.0.0`。
- 不允许重复依赖或直接依赖自身。

依赖声明示例：

```json
{
  "required_tools": [
    {"name": "ticket.create", "version": ">=1.0.0,<2.0.0"}
  ],
  "required_resources": [
    {"uri_template": "runbook://services/{service}"}
  ],
  "required_skills": [
    {"publisher": "platform", "name": "evidence-summary", "version": ">=1.0.0,<2.0.0"}
  ]
}
```

发布前应以 `skills validate` 的实际结果为准；版本约束必须使用当前实现支持的语法，推荐使用明确的比较条件而不是依赖非必要的简写。

### 3.3 SKILL.md

`SKILL.md` 是模型可读取的 Skill 指令正文。建议至少包含：

- Skill 的目标与适用范围；
- 输入和输出约定；
- 确定性的执行步骤；
- 允许调用的 Tool、Resource 和依赖 Skill；
- 失败、超时和不确定结果的处理；
- 数据分类、安全边界与禁止行为；
- 一个最小示例。

指令必须与 manifest 声明一致。不要在正文中嵌入凭证、环境配置或要求绕过系统策略的内容。

### 3.4 声明式测试

`tests/` 只接受 `.json` 测试向量，不执行包内代码。单个用例只支持 `name`、`input` 和可选的 `expected_output`：

```json
{
  "name": "basic alert",
  "input": {
    "alert": "HTTP 5xx rate is above threshold",
    "service": "checkout"
  },
  "expected_output": {
    "summary": "checkout 服务的 5xx 比例异常",
    "next_actions": ["检查最近部署", "查看应用错误日志"]
  }
}
```

`input` 和 `expected_output` 必须是对象。测试用于验证包结构和输入输出契约，不会启动 Agent 或执行任意代码。

## 4. 签名前检查

外部 Publisher 的 draft manifest 可以暂时没有 `signature` 和 `signature_key_id`，此时不能运行最终的 `skills validate` 或 `skills test`。先人工检查目录、字段、依赖和测试向量，再执行第 5 节的签名命令；签名命令会读取 draft、生成完整 manifest，并对最终包做校验。

平台兼容签名路径生成的完整包，以及已经签名的外部包，可以直接执行下面的最终校验。

## 5. 本地校验与测试

平台 Skill：

```bash
uv run auraclaw skills validate path/to/example-skill
uv run auraclaw skills test path/to/example-skill
```

外部 Publisher 的已签名 Skill：

```bash
AURACLAW_SKILL_PUBLIC_KEY='<public key>' \
  uv run auraclaw skills validate path/to/example-skill

AURACLAW_SKILL_PUBLIC_KEY='<public key>' \
  uv run auraclaw skills test path/to/example-skill
```

校验会覆盖目录布局、manifest、依赖、签名、digest、大小限制和安全内容扫描。任何失败都应修复；修改任一文件后，旧签名都会失效，必须重新签名并再次校验。

## 6. 签名与登记 Publisher

### 5.1 生成签名

```bash
AURACLAW_SKILL_SIGNING_KEY='<private key>' \
  uv run auraclaw skills sign path/to/example-skill \
  --publisher acme \
  --key-id key-2026-a
```

命令会原子更新 `manifest.json`，并输出 `publisher`、`name`、`version`、`signature_key_id`、公钥和 package digest，不输出私钥。

### 5.2 登记 Publisher 和公钥

第一次发布前，租户管理员需要：

1. 创建 Publisher；
2. 将签名命令输出的公钥登记为 active key；
3. 确认发布使用的 tenant、publisher 和 key id 完全一致。

具体请求见 [Skill 接口手册](./skill-api.md#6-publisher-与密钥管理)。私钥不得提交给 AuraClaw。

## 7. 发布 Skill

推荐使用 CLI：

```bash
AURACLAW_SKILL_PUBLIC_KEY='<public key>' \
AURACLAW_API_TOKEN='<access token>' \
  uv run auraclaw skills publish path/to/example-skill \
  --tenant tenant_1 \
  --publisher acme \
  --api-url https://auraclaw.test.example.com
```

CLI 会：

1. 再次执行本地校验和验签；
2. 生成确定性的 canonical archive 和 digest；
3. 将 archive 上传到 AuraClaw 的代理上传接口；
4. 由 AuraClaw 在服务端完成 OBS single/multipart 上传、ETag 校验和 finalize；
5. 使用返回的 `artifact_ref` 创建 Publication；
6. 进入统一的服务端验签、内容扫描、Source 授权和准入流程。

客户端不应实现 OBS 直传，也不应依赖 AuraClaw 内部对象存储结构。

如需发布后暂不参与新发现：

```bash
AURACLAW_SKILL_PUBLIC_KEY='<public key>' \
AURACLAW_API_TOKEN='<access token>' \
  uv run auraclaw skills publish path/to/example-skill \
  --tenant tenant_1 \
  --publisher acme \
  --staged
```

`--staged` 表示创建 `activate=false` 的 Publication，不是选择“分片上传”。上传方式始终由 AuraClaw 根据服务端对象存储配置决定。

常用可选项：

- `--source`：发布来源，默认 `sks_admin_upload`；
- `--actor`：审计 actor，默认 `skill-cli`；
- `--command-id`：显式提供幂等命令 ID；
- `--expected-revision`：创建时通常为 `0`；
- `--token-env`、`--public-key-env`：指定 Secret 所在环境变量名。

## 8. 发布后验证

至少检查以下内容：

1. `GET /v1/admin/skills/{publisher}/{name}` 返回新版本；
2. Publication 状态符合预期，正式发布应为 `active`，暂存发布应为 `staged`；
3. Installation 的固定 digest、版本约束和状态正确；
4. `GET /v1/admin/skill-admissions` 中该次准入结果为 `accepted`；
5. AuraX 中能看到相同 catalog、publication 和 installation 状态；
6. 客户端网络与日志中不存在 OBS endpoint、AK/SK、upload id、part ETag 或预签名 URL。

如果准入结果为 `rejected` 或 `quarantined`，按稳定 error/finding code 修复包内容后，提升版本并重新签名发布。审计接口不会返回包正文、命中片段、私钥或异常原文。

## 9. 升级与版本维护

发布新版本时：

1. 修改 Skill 文件；
2. 按 SemVer 提升 `version`；
3. 更新声明式测试；
4. 重新执行 `validate` 和 `test`；
5. 重新签名；
6. 使用相同 Publisher 发布；
7. 检查依赖解析、Installation 约束和固定 digest。

不可变 Publication 不应覆盖已有版本。同一个 Run 使用固定 binding；普通升级、禁用或退役不会悄悄替换一个已运行任务的 Skill 内容。

版本建议：

- PATCH：修正文案或兼容性缺陷，不改变输入输出契约；
- MINOR：新增向后兼容能力或可选字段；
- MAJOR：不兼容地修改输入、输出、依赖或行为边界。

## 10. 生命周期维护

### Installation

- `active`：允许新发现和新 binding；
- `disabled`：停止新发现，已有固定 binding 不受普通禁用影响；
- `draining`：普通卸载后的排空状态，已有 Run 按 `continue` 完成；
- `uninstalled`：已卸载。强制卸载会持久化 `cancel` 策略并作用于活动 Runtime。

普通卸载优先于强制卸载。只有明确需要立即阻止活动执行时才使用 `force=true`。

### Publication

- `retired`：普通退役，不参与新发现，但保留不可变内容供已有 binding 使用；可以显式 restore；
- `revoked`：安全撤销，不可恢复；必须选择 `continue`、`pause` 或 `cancel` 运行时动作，默认 `cancel`；
- `restoring`：正在重新读取原 Artifact 并复验 digest、Source、Publisher 和签名信任。

不要用安全 revoke 代替普通版本下线。`retired` 和 `revoked` 的恢复语义不同。

### Publisher 和密钥

- suspend 是可恢复的租户级信任断路器，会阻止新发布和持久包恢复；
- resume 只恢复仍有效的 active/retiring key；
- key revoke 与 Publisher revoke 是不可逆的安全操作，并动态作用于相关固定 binding；
- 密钥轮换后，新发布使用新 active key，旧 key 可保持 retiring 以支持历史包复验。

### Package 清理

Package 只有在 retention 到期、无 legal hold 且没有引用时才能 purge。服务端是最终判定方；不要通过对象存储控制台直接删除 Skill Artifact。

## 11. 故障处理

| 现象 | 常见原因 | 处理建议 |
| --- | --- | --- |
| 本地验签失败 | 包内容在签名后被修改、公钥不匹配 | 重新校验公钥，修改完成后重新签名 |
| 服务端验签失败 | Registry key 未登记、已撤销或 publisher/key id 不一致 | 检查当前 tenant 的 Publisher Registry |
| `409 Conflict` | expected revision 过期、幂等键复用到不同请求或状态转换非法 | 重新读取资源；相同请求沿用原幂等键，不同请求使用新键 |
| `413 Payload Too Large` | 代理上传体超过 24 MiB | 移除非必要资产并重新打包 |
| `415 Unsupported Media Type` | 上传 Content-Type 错误 | 使用 CLI，或按接口手册设置媒体类型 |
| Admission `quarantined` | 命中可执行内容、Secret 或指令劫持策略 | 根据稳定 finding code 清理内容并发布新版本 |
| restore 长时间停在 `restoring` | 原 Artifact、签名信任或 Source 复验失败 | 查询 Admission/运维日志，修复信任或 Artifact 问题后以同一命令安全重试 |

写请求重试必须沿用同一个 `Idempotency-Key` 且请求内容完全相同。revoke、force uninstall、purge、key revoke 和 Publisher revoke 等高风险操作不得在无人确认时自动重试。

## 12. 发布检查清单

- [ ] 目录只包含允许的根文件与目录，无符号链接。
- [ ] manifest 与 `SKILL.md` 的范围、依赖和安全边界一致。
- [ ] `uv run auraclaw skills validate` 通过。
- [ ] `uv run auraclaw skills test` 通过。
- [ ] 包内没有 Secret、私钥、令牌或可执行内容。
- [ ] 版本按 SemVer 提升，旧版本未被覆盖。
- [ ] Publisher、key id、公钥和 tenant 已登记并处于允许状态。
- [ ] 修改完成后重新签名，私钥未离开签名环境。
- [ ] 通过 AuraClaw 代理上传，客户端未配置或接触 OBS。
- [ ] Publication、Installation、Catalog 和 Admission 审计均验证通过。

接口字段、请求头和状态码见 [Skill 接口手册](./skill-api.md)。
