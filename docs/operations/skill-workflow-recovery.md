# Skill 工作流状态与恢复（#96）

步骤只接受明确的 success；缺失/未知枚举为失败。业务 content 内的 status 不作框架状态。
unknown 或写调用 timeout 且副作用未确定，保留 activation、step 和原 invocation ID，进入 waiting_for_tool 分配状态。Runtime 不继续模型轮次、不补 completed、不释放执行引用。Control 的显式 wake_assignment 可恢复该分配；恢复先通过 Hands 查询原 invocation，确认 success 后才用原 ID 读取幂等结果并继续。查无证据或仍 unknown 保持暂停。不会通过更换写调用 ID 绕过去重。

只读工具允许签名工作流声明范围内的有界重试，后续 read attempt 使用固定的步骤 ID + attempt 序号，避免只读取第一次失败缓存；写调用不自动重试。Resource 同样限制单次 timeout、retry_on、max_attempts 和 backoff。失败 attempt 不推进步骤。

工作流 deadline 使用 UTC，初次执行前与 activation 一同 checkpoint；workflow 文档、reference、步骤和 backoff 全部计入该总预算，同时服从 assignment deadline。审批等待计入工作流墙钟预算。恢复不重置 deadline；旧 checkpoint 从 canonical skill.activated.occurred_at 推导 deadline。过期且仍有未知写结果保持暂停，需核验原远端结果，不能据本地超时推断远端停止。

声明式工作流仅由实际结果发出终态，聊天结束不为其补发 skill.completed。CanonicalEventCommitter 和 SkillRunner 共用每 activation 的 terminal command 身份及 expected_version，先读取权威终态，避免失败后补完成或重复终态。Canonical 已提交、checkpoint 未保存时，从 activation 事实恢复绑定和 deadline，从第一条 terminal 事实恢复终态；存量矛盾事件不改写。输出校验失败产生 skill.failed。

部署顺序：先更新 Control/Hands（支持 waiting_for_tool 和 invocation status），再更新 Runtime；两种 Control store 的状态字段已有字符串存储，无新 DDL。待查明未知结果的分配可以显式唤醒；自动唤醒/现场恢复验收与后续生命周期联调一起完成。发布前避免把旧 Runtime 分配到新增等待状态。

## 逐步撤销与定时恢复补充（#95/#96）

声明式工作流现在在每个新步骤前进行独立、不可缓存的 binding-status 查询；pause/cancel 阻止后续
Tool/Resource。已知终态绑定不再参与后续撤销检查。pause 使用合法 waiting_for_human 调度状态，
保留 workflow cursor；unknown 使用 waiting_for_tool，保留原 invocation。

Control feed 每五秒（既有 waiting_recovery_interval）最多唤醒 100 个等待工具的 assignment。
唤醒只接受仍为 acked 的 runnable item，不能重置正在排队/已被其他调度者领取的 claim。
Runtime 再读取原 invocation 状态；调度器不判断业务成功、不直接修改 Session 状态。

工作流总 deadline 已过时仍允许有界查询原写调用结果（单次五秒）。结果未知继续等待；确认为成功或
已知无未知副作用的失败后，以 workflow_budget_exhausted 结束，不再开始下一步或重放写操作。
完整 assignment 取消/终止与旧包引用的联合清理仍需 #94 drain 阶段验收；不能用本段替代清理证据。

本阶段全量 675 passed / 56 skipped；临时 PostgreSQL 调度、Runtime 与 invocation 联合验证通过。
无 DDL，Control/Runtime 应协调升级。

### 已完成调用的只读结果恢复（#96 M）

Hands invocation status 新增可选 result，直接返回已持久化的规范结果。查询严格匹配可信 tenant、
root session、session、run；同租户其他 Run 不能读取结果。Runtime 查询原 invocation 成功后使用
该结果继续，禁止再次进入 tools/call。老服务只返回 success 状态但无结果时仍保持 unknown，
不能用空结果或重新执行替代。失败/拒绝/取消只有副作用明确时才能结束等待。

此阶段无 DDL，Hands 严格 DTO 消费者需同步发布；先服务端后 Runtime。全量 686 passed /
60 skipped，PostgreSQL 双副本、Hands 隔离与工作流联合 47 passed。全 Run 取消和 Canonical
引用 drain 仍在后续阶段处理，不能凭这一项宣称 #96 全部完成。
