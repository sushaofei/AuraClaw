# MCP 模型参数稳定性

模型输出与 Gateway 校验使用远端 Schema 的同一参数结构。工具声明 input 对象时，
模型必须生成嵌套 input；整数使用 JSON number。Runtime 不猜测日期/业务标识，
不静默加 input、不转换数字字符串，不把未知调用重放为新调用。

Runtime 为已加载 Tool 的模型描述附加来自同一 Schema 的有界提示：最多 24 个属性，
JSON Pointer 路径最多 256 字符，只检查至两层子对象；提示是辅助信息，完整 Schema 仍是权威。
保持模型工具 parameters 原样不变。局部引用/组合 Schema 等仍由完整 Schema 和 Gateway 校验处理，
不由提示生成器另建校验实现。

仅向模型暴露已加载类型可使用的 Skill 激活/Resource 读取入口。实际误用 Resource 入口调用 Tool
仍拒绝执行，并提示当前精确 Tool 函数名；没有自动把读资源操作转换成工具执行。

对于 Gateway 返回 tool_schema_invalid 且 side_effect_status=not_started 的调用，
Runtime 在结果 metadata.argument_guidance 附上精确函数名、必填路径和类型，要求修改后再调用。
原始错误保留；不增加自动重试，不改变现有步数、重复调用、审批或副作用未知保护。
下游 outputSchema 错误需要服务端修正契约或输出，不能靠客户端隐藏校验处理。

这是降低模型误用概率的改进，不保证所有模型输出始终合法。正式验收应统计首调合法率、
本地纠错后成功率和错误入口次数，分别记录本地输入校验与远端业务失败。
