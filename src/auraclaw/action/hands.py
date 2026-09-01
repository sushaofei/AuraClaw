from __future__ import annotations

import base64
import binascii
from datetime import UTC, datetime
from typing import Any

from auraclaw.action.mcp_primitives import HandsPromptRegistry, HandsResourceRegistry
from auraclaw.action.ports import ResourceReader
from auraclaw.action.tool_gateway import ToolGateway, ToolRegistry
from auraclaw.contracts.hands import (
    HandsCancelResponse,
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
from auraclaw.contracts.tools import ArtifactRef, ToolInvocation, ToolResult


class HandsGateway:
    """Protocol-agnostic Action Hands service used by Runtime clients."""

    def __init__(
        self,
        *,
        registry: ToolRegistry,
        gateway: ToolGateway,
        resources: HandsResourceRegistry | None = None,
        resource_reader: ResourceReader | None = None,
        prompts: HandsPromptRegistry | None = None,
        page_size: int = 50,
    ) -> None:
        if page_size < 1 or page_size > 100:
            raise ValueError("Hands page_size must be between 1 and 100")
        self._registry = registry
        self._gateway = gateway
        self._resources = resources or HandsResourceRegistry()
        self._resource_reader = resource_reader
        self._prompts = prompts or HandsPromptRegistry()
        self._page_size = page_size

    async def list_tools(
        self,
        trusted: HandsTrustedContext,
        *,
        cursor: str | None = None,
    ) -> HandsPage[HandsToolDescriptor]:
        del trusted
        tools = tuple(
            HandsToolDescriptor(
                name=capability.name,
                version=capability.version,
                description=capability.description,
                input_schema=dict(capability.input_schema),
                output_schema=dict(capability.output_schema),
                read_only=capability.permission.value == "read-only",
                destructive=capability.risk_level.value == "critical",
                risk_level=capability.risk_level.value,
            )
            for capability in self._registry.discover()
        )
        return _page(tools, cursor, self._page_size)

    async def list_resources(
        self,
        trusted: HandsTrustedContext,
        *,
        cursor: str | None = None,
    ) -> HandsPage[HandsResourceDescriptor]:
        resources = tuple(self._resources.discover_resources(trusted.tenant_id))
        return _page(resources, cursor, self._page_size)

    async def list_resource_templates(
        self,
        trusted: HandsTrustedContext,
        *,
        cursor: str | None = None,
    ) -> HandsPage[HandsResourceDescriptor]:
        templates = tuple(self._resources.discover_templates(trusted.tenant_id))
        return _page(templates, cursor, self._page_size)

    async def read_resource(
        self,
        trusted: HandsTrustedContext,
        uri: str,
    ) -> tuple[HandsResourceContent, ...]:
        if self._resource_reader is not None:
            return await self._resource_reader.read(trusted, uri)
        return self._resources.read(trusted.tenant_id, uri)

    async def list_prompts(
        self,
        trusted: HandsTrustedContext,
        *,
        cursor: str | None = None,
    ) -> HandsPage[HandsPromptDescriptor]:
        prompts = tuple(self._prompts.discover(trusted.tenant_id))
        return _page(prompts, cursor, self._page_size)

    async def get_prompt(
        self,
        trusted: HandsTrustedContext,
        name: str,
        *,
        arguments: dict[str, str] | None = None,
    ) -> HandsPromptResult:
        raw_arguments = dict(arguments or {})
        if any(not isinstance(value, str) for value in raw_arguments.values()):
            raise ValueError("Prompt argument values must be strings")
        return self._prompts.get(
            trusted.tenant_id,
            name,
            {str(key): value for key, value in raw_arguments.items()},
            trusted,
        )

    async def call_tool(
        self,
        trusted: HandsTrustedContext,
        call: HandsToolCall,
    ) -> HandsToolResult:
        if not call.tool_invocation_id:
            raise ValueError("tool_invocation_id is required")
        deadline = _optional_utc(trusted.deadline)
        if call.deadline is not None:
            requested = _as_utc(call.deadline)
            deadline = min(deadline, requested) if deadline is not None else requested
        invocation = ToolInvocation(
            tool_invocation_id=call.tool_invocation_id,
            tenant_id=trusted.tenant_id,
            root_session_id=trusted.root_session_id,
            session_id=trusted.session_id,
            run_id=trusted.run_id,
            tool_name=call.name,
            tool_version=call.version,
            arguments=dict(call.arguments),
            expected_side_effect=call.expected_side_effect,
            idempotency_key=call.idempotency_key or call.tool_invocation_id,
            deadline=deadline,
            fencing_token=trusted.fencing_token,
            actor_id=trusted.runtime_id,
            approval_id=call.approval_id,
            credential_ref=call.credential_ref,
            user_id=trusted.user_id,
            actor_role=(
                trusted.lease_assertion.role if trusted.lease_assertion is not None else None
            ),
        )
        result = await self._gateway.execute(invocation)
        return _tool_result(result)

    async def cancel_invocation(
        self, trusted: HandsTrustedContext, tool_invocation_id: str
    ) -> HandsCancelResponse:
        cancelled = await self._gateway.cancel(tool_invocation_id, tenant_id=trusted.tenant_id)
        return HandsCancelResponse(cancelled=cancelled)

    async def get_invocation_status(
        self, trusted: HandsTrustedContext, tool_invocation_id: str
    ) -> HandsInvocationStatusResponse:
        status = await self._gateway.get_authoritative_status(trusted.tenant_id, tool_invocation_id)
        if status is None:
            return HandsInvocationStatusResponse(found=False)
        state, side_effect, error_code, cancel_requested = status
        return HandsInvocationStatusResponse(
            found=True,
            status=state,
            side_effect_status=side_effect,
            error_code=error_code,
            cancel_requested=cancel_requested,
        )


def _tool_result(result: ToolResult) -> HandsToolResult:
    serialized = result.as_dict()
    content = serialized.get("content")
    payload: str | dict[str, Any] | None
    if isinstance(result.content, ArtifactRef):
        payload = {"artifact_ref": result.content.as_dict()}
    elif isinstance(content, (str, dict)) or content is None:
        payload = content
    else:
        payload = dict(content) if isinstance(content, dict) else None
    return HandsToolResult(
        status=result.status.value,
        content=payload,
        summary=result.summary,
        metadata=dict(result.metadata),
        error_code=result.error_code,
        side_effect_status=result.side_effect_status,
    )


def _page(
    items: tuple[Any, ...],
    cursor: str | None,
    page_size: int,
) -> HandsPage[Any]:
    offset = _decode_cursor(cursor)
    if offset > len(items):
        raise ValueError("Hands cursor is outside the result set")
    next_offset = offset + page_size
    next_cursor = _encode_cursor(next_offset) if next_offset < len(items) else None
    return HandsPage(items=items[offset:next_offset], next_cursor=next_cursor)


def _encode_cursor(offset: int) -> str:
    return base64.urlsafe_b64encode(offset.to_bytes(8, "big")).decode("ascii")


def _decode_cursor(cursor: str | None) -> int:
    if cursor is None:
        return 0
    try:
        decoded = base64.b64decode(cursor, altchars=b"-_", validate=True)
    except (binascii.Error, ValueError) as exc:
        raise ValueError("Hands cursor is invalid") from exc
    if len(decoded) != 8:
        raise ValueError("Hands cursor is invalid")
    return int.from_bytes(decoded, "big")


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _optional_utc(value: datetime | None) -> datetime | None:
    return None if value is None else _as_utc(value)
