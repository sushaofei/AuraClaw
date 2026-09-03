# Skill 工作流状态与恢复（#96）

步骤只接受明确的 success；缺失/未知枚举为失败。业务 content 内的 status 不作框架状态。
unknown 或写调用 timeout 且副作用未确定，保留 activation、step 和原 invocation ID，进入 waiting_for_tool 分配状态。Runtime 不继续模型轮次、不补 completed、不释放执行引用。Control 的显式 wake_assignment 可恢复该分配；恢复先通过 Hands 查询原 invocation，确认 success 后才用原 ID 读取幂等结果并继续。查无证据或仍 unknown 保持暂停。不会通过更换写调用 ID 绕过去重。

只读工具允许签名工作流声明范围内的有界重试，后续 read attempt 使用固定的步骤 ID + attempt 序号，避免只读取第一次失败缓存；写调用不自动重试。Resource 同样限制单次 timeout、retry_on、max_attempts 和 backoff。失败 attempt 不推进步骤。

工作流 deadline 使用 UTC，初次执行前与 activation 一同 checkpoint；workflow 文档、reference、步骤和 backoff 全部计入该总预算，同时服从 assignment deadline。审批等待计入工作流墙钟预算。恢复不重置 deadline；旧 checkpoint 从 canonical skill.activated.occurred_at 推导 deadline。过期且仍有未知写结果保持暂停，需核验原远端结果，不能据本地超时推断远端停止。

声明式工作流仅由实际结果发出终态，聊天结束不为其补发 skill.completed。CanonicalEventCommitter 和 SkillRunner 共用每 activation 的 terminal command 身份及 expected_version，先读取权威终态，避免失败后补完成或重复终态。Canonical 已提交、checkpoint 未保存时，从 activation 事实恢复绑定和 deadline，从第一条 terminal 事实恢复终态；存量矛盾事件不改写。输出校验失败产生 skill.failed。

部署顺序：先更新 Control/Hands（支持 waiting_for_tool 和 invocation status），再更新 Runtime；两种 Control store 的状态字段已有字符串存储，无新 DDL。待查明未知结果的分配可以显式唤醒；自动唤醒/现场恢复验收与后续生命周期联调一起完成。发布前避免把旧 Runtime 分配到新增等待状态。
