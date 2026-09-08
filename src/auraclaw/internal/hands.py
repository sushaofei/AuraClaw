from __future__ import annotations

from auraclaw.action.hands import HandsGateway
from auraclaw.contracts.hands import (
    HandsInvocationStatusResponse,
    HandsPage,
    HandsPromptDescriptor,
    HandsPromptResult,
    HandsResourceContent,
    HandsResourceDescriptor,
    HandsToolCall,
    HandsToolDescriptor,
    HandsToolResult,
    HandsTrustedContext,
)
from auraclaw.control.ports import RuntimeAssignment


class InProcessHandsClient:
    """Direct Action Hands adapter; production Runtime uses HttpHandsClient."""

    def __init__(self, gateway: HandsGateway) -> None:
        self._gateway = gateway

    @staticmethod
    def _trusted(assignment: RuntimeAssignment) -> HandsTrustedContext:
        return HandsTrustedContext(
            tenant_id=assignment.tenant_id,
            root_session_id=assignment.root_session_id,
            session_id=assignment.session_id,
            run_id=assignment.run_id,
            runtime_id=assignment.runtime_id,
            lease_id=assignment.lease_id,
            fencing_token=assignment.fencing_token,
            deadline=assignment.deadline,
            lease_assertion=assignment.lease_assertion,
            user_id=assignment.user_id,
            dept_id=assignment.dept_id,
        )

    async def list_tools(
        self,
        assignment: RuntimeAssignment,
        *,
        cursor: str | None = None,
    ) -> HandsPage[HandsToolDescriptor]:
        return await self._gateway.list_tools(self._trusted(assignment), cursor=cursor)

    async def list_resources(
        self,
        assignment: RuntimeAssignment,
        *,
        cursor: str | None = None,
    ) -> HandsPage[HandsResourceDescriptor]:
        return await self._gateway.list_resources(
            self._trusted(assignment), cursor=cursor
        )

    async def list_resource_templates(
        self,
        assignment: RuntimeAssignment,
        *,
        cursor: str | None = None,
    ) -> HandsPage[HandsResourceDescriptor]:
        return await self._gateway.list_resource_templates(
            self._trusted(assignment), cursor=cursor
        )

    async def read_resource(
        self,
        assignment: RuntimeAssignment,
        uri: str,
    ) -> tuple[HandsResourceContent, ...]:
        return await self._gateway.read_resource(self._trusted(assignment), uri)

    async def list_prompts(
        self,
        assignment: RuntimeAssignment,
        *,
        cursor: str | None = None,
    ) -> HandsPage[HandsPromptDescriptor]:
        return await self._gateway.list_prompts(self._trusted(assignment), cursor=cursor)

    async def get_prompt(
        self,
        assignment: RuntimeAssignment,
        name: str,
        *,
        arguments: dict[str, str] | None = None,
    ) -> HandsPromptResult:
        return await self._gateway.get_prompt(
            self._trusted(assignment), name, arguments=arguments
        )

    async def call_tool(
        self,
        assignment: RuntimeAssignment,
        call: HandsToolCall,
    ) -> HandsToolResult:
        return await self._gateway.call_tool(self._trusted(assignment), call)

    async def cancel_invocation(
        self,
        assignment: RuntimeAssignment,
        tool_invocation_id: str,
    ) -> bool:
        result = await self._gateway.cancel_invocation(
            self._trusted(assignment), tool_invocation_id
        )
        return result.cancelled

    async def get_invocation_status(
        self,
        assignment: RuntimeAssignment,
        tool_invocation_id: str,
    ) -> HandsInvocationStatusResponse:
        return await self._gateway.get_invocation_status(
            self._trusted(assignment), tool_invocation_id
        )
