from typing import Any

from auraclaw.action.tool_gateway import ToolGateway
from auraclaw.contracts.tools import ToolInvocation
from auraclaw.control.ports import RuntimeAssignment
from auraclaw.runtime.ports import ToolCall


class GatewayToolClient:
    """Development in-process adapter; production Runtime uses HttpHandsClient."""

    def __init__(self, gateway: ToolGateway) -> None:
        self._gateway = gateway

    async def execute(
        self, assignment: RuntimeAssignment, call: ToolCall
    ) -> dict[str, Any]:
        result = await self._gateway.execute(
            ToolInvocation(
                tool_invocation_id=call.tool_invocation_id,
                tenant_id=assignment.tenant_id,
                root_session_id=assignment.root_session_id,
                session_id=assignment.session_id,
                run_id=assignment.run_id,
                tool_name=call.name,
                tool_version=call.version,
                arguments=call.arguments,
                expected_side_effect=call.expected_side_effect,
                idempotency_key=call.idempotency_key or call.tool_invocation_id,
                deadline=assignment.deadline,
                fencing_token=assignment.fencing_token,
                actor_id=assignment.runtime_id,
                approval_id=call.approval_id,
                credential_ref=call.credential_ref,
                user_id=assignment.user_id,
                dept_id=assignment.dept_id,
                actor_role=assignment.role,
            )
        )
        return result.as_dict()
