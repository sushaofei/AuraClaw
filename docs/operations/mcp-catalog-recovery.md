# MCP 目录隔离恢复记录（2026-09-03）

## 根因

ChainTowerMCP 的 `dashboard.chart.preview` 描述约 8 KB、深度 17，超过原 16 层限制。
另有 5 项同版本 Schema 变化：`dashboard.chart.preview`、`price_insight.dataset.status`、
`price_insight.history.average_price.query`、`semantic.query.compile`、`semantic.query.execute`。
远端发现 76 项 Tool，白名单内为 25 项。原目录只有第 2800 代的 20 条记录，
并非混入多个过期版本；generation 是发布代号，不是历史数据条数。

## 修复与授权恢复

- 深度上限改为 64，在序列化前迭代校验；保留 256 KiB 大小限制、字段约束、
  白名单、租户隔离与同版本漂移保护。深度/大小错误使用独立类型以供中文展示。
- 用户明确批准该测试服务器基线重建，并单独批准单模块修复上传及两个 Hands 副本更新。
- 重建前无 assigned/running 任务，旧目录和服务器投影已持久备份。
- 操作核对配置第 3 版、目录第 2800 代、旧记录数 20、新记录数 25 和上述 5 项漂移，
  持有带 fencing token 的目录租约后原子替换；未删除 Session Events、业务历史或服务器配置。
- 重建提交第 2801 代；随后后台自然同步至第 2803 代，active、stale=false、
  连续失败次数 0，25 项能力均 active。

## 恢复点与后续发布

测试服务主机上的备份：
`/home/jcroot/workspace/AuraClaw/.runtime/mcp-catalog-backups/20260903/chaintowermcp-baseline-20260903.json`。
旧镜像：`auraclaw:mcp-catalog-base-20260903`；单模块修复镜像：`auraclaw:mcp-catalog-fix-20260903`。
后续正常发布需重新构建包含源码修复的镜像，不要以旧镜像覆盖。
恢复目录须再次取得租约、核验配置及活跃任务，不能直接覆盖旧 generation/fencing token。
生产环境优先由 MCP 升级发生变更的能力版本；本次测试基线重建不作为自动跳过漂移保护的机制。

## 验证

- 对账测试 14 项通过，覆盖深层正常 Schema、超限、循环引用和体积约束。
- AuraClaw Ruff/Mypy 通过；AuraX SDK 51 项、浏览器端到端 14 项和构建通过。
- AuraX 中文提示源码已完成；前端发布尚未执行。
