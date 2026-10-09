# Skill 与 Artifact 内容扫描

## 生产约束

- Skill 发布使用 `SkillPackageContentScanner` port。默认策略 `skill-content-v1` 拒绝可执行扩展、
  ELF/PE/Mach-O/WASM、疑似私钥/云凭据/token 和高置信 prompt injection；命中后准入账本记录
  `quarantined`，不会创建可启用 Publication。
- Artifact finalize 使用 `ArtifactContentScanner` port。生产 profile 必须配置
  `AURACLAW_ARTIFACT_SCANNER_BASE_URL`；未配置时 Artifact Service 拒绝启动。
- 对象完整性检查通过后，Artifact Service 才生成五分钟只读预签名 URL 并调用扫描器。扫描器不可用、
  非 200、非法响应、策略版本不一致或返回隔离结论时，元数据持久化为 `quarantined`，不会短暂暴露为
  `ready`。

## 远端扫描器契约

Artifact Service 使用自己的 workload identity Bearer token 调用：

- `GET /health/ready`：仅 HTTP 200 表示可接流量。
- `POST /v1/artifacts:scan`：请求包含 tenant/artifact/version、只读预签名 URL、media type、大小、
  checksum、classification 与请求的 policy version。
- 响应：`{"verdict":"clean","policy_version":"artifact-content-v1"}`，或
  `{"verdict":"quarantined","policy_version":"artifact-content-v1","finding_code":"malware_detected"}`。

`policy_version` 和 `finding_code` 只接受小写字母、数字、点、下划线和连字符，最长 128 字符；响应正文
不得进入错误消息或审计日志。扫描器必须能够访问对象存储的预签名地址，但不获得 OBS/S3 长期凭据。

## 发布前检查

1. 固定扫描策略版本，并在扫描器和 `.env.prod` 同步更新。
2. 验证 clean、恶意命中、DLP 命中、超时、5xx、非法 JSON、策略漂移七条路径。
3. 验证命中和故障后 Artifact 仅能查到 quarantined，下载接口不可见。
4. 监控扫描延迟、不可用率、隔离率与扫描策略版本分布；异常率进入发布与告警门禁。
