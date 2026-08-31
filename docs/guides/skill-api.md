# Skill 接口手册

本文面向 AuraX、管理后台、自动化发布器和其他 AuraClaw 客户端，说明 Skill 相关公开管理接口。所有路径均以 `/v1/admin` 为前缀。

推荐使用 `auraclaw skills publish` 完成包上传与发布。需要自行集成 HTTP 时，客户端也必须只调用 AuraClaw；不得直连 OBS 或依赖内部对象存储协议。

## 1. 基础约定

### 1.1 身份与租户

开发环境示例使用：

```http
Authorization: Bearer <token>
X-Tenant-ID: tenant_1
X-Actor-ID: aurax-user-123
X-Dept-ID: dept_1
X-Correlation-ID: <trace id>
```

生产环境应使用部署约定的可信身份断言或 Bearer Token，不应允许不受信的客户端自行声明租户和 actor。完整身份规则见 [公开 API 与身份接入](./public-api-and-identity.md)。

### 1.2 写命令头

大多数写接口使用以下请求头：

| 请求头 | 用途 |
| --- | --- |
| `Idempotency-Key` | 写命令幂等键；同一键只能重放完全相同的请求 |
| `X-Expected-Revision` | 乐观并发修订号；创建通常为 `0`，修改通常为当前 revision |
| `X-Reason-Code` | 禁用、卸载、撤销、恢复、清理等治理操作的原因码 |
| `X-Correlation-ID` | 跨服务追踪 ID |

发生 `409 Conflict` 时先重新读取资源和 revision。请求未确认成功时，可以使用相同幂等键重放完全相同的请求；改变请求体、目标或治理动作时必须使用新键。

### 1.3 分页

列表接口使用稳定 keyset cursor：

```text
?limit=100&cursor=<opaque cursor>
```

`limit` 范围 1–500，默认 100。将响应的 `next_cursor` 原样传入下一页；翻页期间不要改变筛选条件。客户端不得解析或构造 cursor。

### 1.4 状态模型

| 资源 | 状态 |
| --- | --- |
| Publication | `staged`, `validating`, `active`, `restoring`, `quarantined`, `retired`, `revoked` |
| Installation | `active`, `disabled`, `draining`, `uninstalled` |
| Package retention | `retained`, `purged` |
| Source desired state | `enabled`, `disabled`, `retired` |
| Publisher | `active`, `suspended`, `revoked` |
| Publisher key | `active`, `retiring`, `revoked` |

Catalog 的 `availability` 是派生值，常见值包括 `available`、`publication_unavailable`、`not_installed`、`installation_disabled`、`installation_draining` 和 `installation_uninstalled`。客户端应分别展示 Publication、Installation 和 availability，不要把三者合并为一个“启用”开关。

## 2. Catalog 与详情

### 2.1 查询 Skill Catalog

```http
GET /v1/admin/skills?q=&publisher=&risk_level=&publication_status=&installation_status=&source_id=&cursor=&limit=
```

响应同时保留兼容字段 `skills` 和权威字段 `items`；两者内容相同，新客户端应读取 `items` 与
`next_cursor`。列表、详情和版本均由 Task API 通过 Hands 内部契约读取 PostgreSQL lifecycle；Task API
本地 Registry/缓存为空、重启或与 Hands 分进程部署时不影响管理结果。

每个 item 包含：

- `publisher`, `name`, `latest_version`；
- Publication 和 Installation 摘要；
- `availability`；
- 当前版本的 description、risk level、package digest 和依赖摘要。

### 2.2 查询 Skill 与版本

```http
GET /v1/admin/skills/{publisher}/{name}
GET /v1/admin/skills/{publisher}/{name}/versions/{version}
GET /v1/admin/skills/{publisher}/{name}/management
GET /v1/admin/skills/{publisher}/{name}/installation
```

版本详情包含 `skill_markdown`。正文由 Hands 根据持久 package 的 Artifact 引用经受控 Artifact 下载边界
读取并校验 digest，Task API 不直连对象存储。只应在授权的管理界面按文本展示，不要将其作为 HTML
直接注入 WebView。

## 3. 管理列表与详情

```http
GET /v1/admin/skill-installations?status=&publisher=&source_id=&cursor=&limit=
GET /v1/admin/skill-publications?status=&publisher=&name=&source_id=&cursor=&limit=
GET /v1/admin/skill-packages?retention_status=&publisher=&name=&legal_hold=&cursor=&limit=
GET /v1/admin/skill-publications/{publisher}/{name}/versions/{version}
GET /v1/admin/skill-packages/{publisher}/{name}/versions/{version}
```

典型 Installation 摘要：

```json
{
  "publisher": "acme",
  "name": "incident-triage",
  "version_constraint": ">=1.0.0,<2.0.0",
  "pinned_package_digest": "sha256:...",
  "status": "active",
  "source_id": "sks_admin_upload",
  "auto_upgrade": false,
  "revision": 3,
  "reason_code": null,
  "updated_by": "aurax-user-123",
  "updated_at": "2026-08-31T10:00:00Z"
}
```

Publication 摘要包含 `publisher`、`name`、`version`、`package_digest`、`status`、`source_id`、`revision`、治理原因和撤销策略证据。Package 摘要包含 retention 状态、到期时间、legal hold、retention revision 和 purge 时间。

## 4. Source 管理

### 4.1 查询

```http
GET /v1/admin/skill-sources
GET /v1/admin/skill-sources/{source_id}
GET /v1/admin/skill-sources/{source_id}/sync-state
```

### 4.2 创建 Source

```http
POST /v1/admin/skill-sources
Idempotency-Key: <uuid>
Content-Type: application/json
```

```json
{
  "source_id": "sks_acme_mcp",
  "kind": "mcp",
  "desired_state": "enabled",
  "publisher_allowlist": ["acme"],
  "credential_ref": "vault/skills/acme#token",
  "config_metadata": {},
  "priority": 100
}
```

`source_id` 必须以 `sks_` 开头；allowlist 最多 256 项；priority 范围 -1000–1000。`config_metadata` 只能保存非敏感配置，Secret 应通过 `credential_ref` 间接引用。

### 4.3 修改、同步与退役

```http
PATCH /v1/admin/skill-sources/{source_id}
Idempotency-Key: <uuid>
X-Expected-Revision: <current revision>

POST /v1/admin/skill-sources/{source_id}:sync

DELETE /v1/admin/skill-sources/{source_id}
Idempotency-Key: <uuid>
X-Expected-Revision: <current revision>
X-Reason-Code: source-retired
```

DELETE 是软退役，不直接删除历史事实。一个 Publication 可以保留多个来源引用；目录会选择 enabled 且 available 的最高优先级来源，同优先级按 source id 稳定排序。

## 5. 代理上传与发布

### 5.1 代理上传包

```http
POST /v1/admin/skill-package-uploads
Authorization: Bearer <token>
X-Tenant-ID: tenant_1
X-Actor-ID: aurax-user-123
Idempotency-Key: <uuid>
X-Upload-Name: incident-triage-1.0.0.skill.json
X-Content-SHA256: <64 lowercase hex>
Content-Type: application/vnd.auraclaw.skill-package+json

<canonical archive bytes>
```

响应为 AuraClaw 已 finalize 的 `artifact_ref` 和上传状态。响应不会暴露 OBS endpoint、bucket、预签名 URL、upload id、part number 或 ETag。

请求要求：

- body 非空，最大 24 MiB；
- `X-Content-SHA256` 是 body 原始字节的 SHA-256 小写十六进制，不带 `sha256:` 前缀；
- 媒体类型必须准确；
- 优先使用 CLI 生成 canonical archive，避免客户端算法漂移。

Canonical archive 是一个紧凑 JSON 对象：

```json
{"files":{"SKILL.md":"<base64>","manifest.json":"<base64>"}}
```

文件路径按字典序排列，每个值是文件原始字节的标准 base64；JSON 键排序且不带多余空白。package digest 是 canonical archive 字节的 `sha256:<hex>`。业务客户端不应重复实现该算法，除非确实需要自建发布器并有跨版本契约测试。

### 5.2 创建 Publication

Artifact 模式是推荐模式：

```http
POST /v1/admin/skill-publications
Idempotency-Key: <uuid>
X-Expected-Revision: 0
Content-Type: application/json
```

```json
{
  "source_id": "sks_admin_upload",
  "activate": true,
  "artifact_ref": {
    "artifact_id": "...",
    "version": 1,
    "content_hash": "<artifact content hash>",
    "media_type": "application/vnd.auraclaw.skill-package+json",
    "size": 12345
  },
  "expected_digest": "sha256:<64 lowercase hex>"
}
```

也支持小包 direct 模式：

```json
{
  "source_id": "sks_admin_upload",
  "activate": true,
  "files": {
    "manifest.json": "<base64>",
    "SKILL.md": "<base64>"
  }
}
```

`files` 和 `artifact_ref` 必须二选一。两种模式都会进入同一套服务端 digest、签名、Publisher、Source 和内容安全准入；direct 不是绕过准入的接口。AuraX 应使用代理上传加 Artifact 发布流程，以统一处理大包和对象存储细节。

`activate=false` 创建 staged Publication。它与 OBS multipart 无关。

### 5.3 完整 curl 示例

以下示例假设 archive 已由受支持工具生成：

```bash
curl -X POST "${AURACLAW_API_URL}/v1/admin/skill-package-uploads" \
  -H "Authorization: Bearer ${AURACLAW_API_TOKEN}" \
  -H "X-Tenant-ID: tenant_1" \
  -H "X-Actor-ID: skill-publisher" \
  -H "Idempotency-Key: ${UPLOAD_COMMAND_ID}" \
  -H "X-Upload-Name: incident-triage-1.0.0.skill.json" \
  -H "X-Content-SHA256: ${ARCHIVE_SHA256}" \
  -H "Content-Type: application/vnd.auraclaw.skill-package+json" \
  --data-binary @incident-triage-1.0.0.skill.json
```

不要在命令行中直接写真实 Token；示例变量应由 Secret 管理器或受控环境注入。

## 6. Publisher 与密钥管理

### 6.1 查询与创建

```http
GET /v1/admin/skill-publishers?status=&q=&cursor=&limit=
GET /v1/admin/skill-publishers/{publisher}

POST /v1/admin/skill-publishers/{publisher}
Idempotency-Key: <uuid>
X-Expected-Revision: 0
Content-Type: application/json

{"display_name":"Acme Skills"}
```

### 6.2 轮换公钥

```http
POST /v1/admin/skill-publishers/{publisher}/keys:rotate
Idempotency-Key: <uuid>
X-Expected-Revision: <current revision>
Content-Type: application/json
```

```json
{
  "key_id": "key-2026-b",
  "public_key": "<32-byte Ed25519 public key, unpadded base64url>"
}
```

新发布使用 active key。历史 key 可以进入 retiring，以便历史包 restore 时继续复验。

### 6.3 撤销密钥

```http
POST /v1/admin/skill-publishers/{publisher}/keys/{key_id}:revoke
Idempotency-Key: <uuid>
X-Expected-Revision: <current revision>
X-Reason-Code: key-compromised
X-Revocation-Action: pause
X-Policy-Version: skill-revocation-v1
X-Policy-Decision-ID: <optional decision id>
```

`X-Revocation-Action` 允许 `pause` 或 `cancel`，默认 `cancel`。key revoke 不可逆，并动态作用于该 key 签名包的固定 binding。

### 6.4 suspend、resume 与永久 revoke

```http
POST /v1/admin/skill-publishers/{publisher}/status:suspend
POST /v1/admin/skill-publishers/{publisher}/status:resume
POST /v1/admin/skill-publishers/{publisher}/status:revoke
```

三者都需要 `Idempotency-Key`、`X-Expected-Revision` 和 `X-Reason-Code`。永久 revoke 还支持 `X-Revocation-Action: pause|cancel`、`X-Policy-Version` 和可选 decision id。

suspend 可恢复；永久 revoke 不可恢复。UI 必须在永久 revoke 前显示影响范围并要求用户明确确认。

## 7. Installation 管理

```http
POST /v1/admin/skills/{publisher}/{name}:install
POST /v1/admin/skills/{publisher}/{name}:enable
POST /v1/admin/skills/{publisher}/{name}:disable
POST /v1/admin/skills/{publisher}/{name}:uninstall?force=false
```

所有命令返回 `202 Accepted`，需要 `Idempotency-Key` 和当前 `X-Expected-Revision`。disable 与 uninstall 还需要 `X-Reason-Code`。

语义：

- install 创建或重新激活 Installation；
- enable 恢复新发现；
- disable 停止新发现，但普通禁用不改变已有 Run 的固定 binding；
- 普通 uninstall 进入 `draining`，已有 Run 继续，排空后收敛为 `uninstalled`；
- `force=true` 立即进入 `uninstalled` 并持久化 cancel 策略。

重新安装只应在状态已经为 `uninstalled` 后执行。

## 8. Publication 管理

### 8.1 安全撤销

```http
POST /v1/admin/skill-publications/{publisher}/{name}/versions/{version}:revoke
Idempotency-Key: <uuid>
X-Expected-Revision: <current revision>
X-Reason-Code: malicious-content
X-Skill-Revocation-Action: cancel
```

运行时动作允许 `continue`、`pause` 或 `cancel`，默认 `cancel`。安全 revoke 不可恢复；用于普通下线时应选择 retirement 流程而不是 revoke。

### 8.2 恢复退役版本

```http
POST /v1/admin/skill-publications/{publisher}/{name}/versions/{version}:restore
Idempotency-Key: <uuid>
X-Expected-Revision: <current revision>
X-Reason-Code: source-restored
```

只有 `retired` Publication 可以 restore。服务先进入 `restoring`，再从原 Artifact 重读并复验 digest、Source、Publisher 和签名信任；全部通过后才恢复为 `active`。`revoked` 版本不能 restore。

## 9. Package retention 与清理

```http
POST /v1/admin/skill-packages/{publisher}/{name}/versions/{version}:purge
Idempotency-Key: <uuid>
X-Expected-Revision: <current retention revision>
X-Reason-Code: retention-expired
```

接口返回 `202 Accepted`。只有 retention 到期、没有 legal hold 且没有引用时才能清理；AuraClaw 会执行权威校验。客户端不得直接删除 OBS 对象。

## 10. Admission 审计与指标

### 10.1 查询审计

```http
GET /v1/admin/skill-admissions?outcome=&stage=&content_policy_version=&since=&cursor=&limit=
```

筛选值：

- `outcome`: `accepted`, `rejected`, `quarantined`；
- `since`: 带时区的 ISO 8601 时间；
- `stage`: 失败或完成的准入阶段；
- `content_policy_version`: 例如 `skill-content-v1`。

响应记录 operation、actor、Source、命令和追踪上下文、可用的 Skill identity/digest、阶段、结果、稳定错误码和耗时，不包含包正文、Secret、私钥、命中片段或异常原文。

### 10.2 查询指标

```http
GET /v1/admin/skill-admissions/metrics?window_hours=24
```

`window_hours` 范围 1–2160。响应包含窗口、按结果或策略聚合的数量、平均延迟、quarantine ratio 和 `ok|firing|insufficient_data` 告警状态。

## 11. 错误与重试

| HTTP 状态 | 含义与处理 |
| --- | --- |
| `400` | 无效筛选、cursor 或请求语义；修正请求后重试 |
| `401` | 身份缺失或失效；刷新可信凭证 |
| `403` | 当前 actor/tenant 无策略权限；不要改租户头绕过 |
| `404` | 资源不存在，或为租户隔离而隐藏 |
| `409` | revision、幂等或状态冲突；重新读取资源后决策 |
| `413` | 上传体过大；缩小包 |
| `415` | 上传媒体类型错误 |
| `422` | header、checksum、base64、manifest 或请求字段不合法 |
| `503` | 依赖暂不可用；GET 可退避重试，写请求仅用原幂等键重放相同内容 |

客户端应优先读取稳定的 error code、message 和结构化 detail，不要依赖异常文本。高风险命令——Publication revoke、force uninstall、purge、key revoke、Publisher revoke——失败或超时后必须先查询最终状态，不得无条件自动重试。

## 12. AuraX 接入要求

- AuraX 只保存和调用 AuraClaw API 地址及身份凭证，不保存 OBS 配置。
- 上传界面把完整包发送到 `/skill-package-uploads`，再用返回的 `artifact_ref` 创建 Publication。
- 不向浏览器返回 OBS endpoint、bucket、AK/SK、上传 ID、part ETag 或预签名 URL。
- 以服务端 revision 驱动按钮状态；收到 409 后刷新详情，不在本地猜测 revision。
- staged 是 Publication 状态，不是上传策略。
- 独立展示 Publication、Installation、Package retention 和 Admission 状态。
- 对 suspend/revoke/force uninstall/purge 显示原因输入、影响范围和二次确认。
- 列表使用服务端 cursor，不自行拼接分页偏移量。
- `skill_markdown` 按不可信文本处理，渲染时启用严格的 Markdown/HTML 安全策略。

Skill 作者侧的完整操作流程见 [Skill 生成、发布与维护手册](./skill-authoring-publishing.md)。
