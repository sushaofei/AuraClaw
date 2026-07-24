from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from pydantic import ValidationError

from auraclaw.action.mcp import HandsMcpServer
from auraclaw.action.mcp_primitives import (
    McpPromptRegistry,
    McpResourceRegistry,
    RegisteredPrompt,
    RegisteredResource,
    RegisteredResourceTemplate,
)
from auraclaw.action.tool_gateway import ToolRegistry
from auraclaw.contracts.errors import AuraClawError
from auraclaw.contracts.mcp import (
    MCP_PROTOCOL_VERSION,
    McpAnnotations,
    McpPromptArgument,
    McpPromptDescriptor,
    McpPromptMessage,
    McpPromptResult,
    McpResourceContent,
    McpResourceDescriptor,
    McpResourceTemplateDescriptor,
)
from auraclaw.contracts.tools import RiskLevel, ToolCapability, ToolPermission
from auraclaw.control.ports import RuntimeAssignment
from auraclaw.internal.mcp import InProcessMcpTransport
from auraclaw.runtime.mcp_client import HandsMcpClient


class _UnusedToolGateway:
    async def execute(self, invocation: Any) -> Any:
        raise AssertionError(f"unexpected tool call: {invocation}")

    async def cancel(self, tool_invocation_id: str) -> bool:
        del tool_invocation_id
        return False


def _assignment(*, tenant_id: str = "tenant-a", runtime_id: str = "runtime-a") -> RuntimeAssignment:
    return RuntimeAssignment(
        tenant_id=tenant_id,
        root_session_id="session-root",
        session_id="session-child",
        run_id="run-1",
        runtime_id=runtime_id,
        lease_id="lease-1",
        fencing_token=1,
        role="worker",
        resource_profile={},
        deadline=datetime.now(UTC) + timedelta(minutes=1),
    )


def _server() -> HandsMcpServer:
    resources = McpResourceRegistry(
        resources=(
            RegisteredResource(
                descriptor=McpResourceDescriptor(
                    uri="memory://a",
                    name="a",
                    mime_type="text/plain",
                    size=1,
                    annotations=McpAnnotations(
                        audience=("assistant",),
                        priority=0.8,
                    ),
                ),
                contents=(
                    McpResourceContent(
                        uri="memory://a",
                        mime_type="text/plain",
                        text="a",
                    ),
                ),
            ),
            RegisteredResource(
                descriptor=McpResourceDescriptor(uri="memory://b", name="b"),
                contents=(McpResourceContent(uri="memory://b", text="b"),),
                tenant_ids=("tenant-a",),
            ),
            RegisteredResource(
                descriptor=McpResourceDescriptor(uri="memory://c", name="c"),
                contents=(McpResourceContent(uri="memory://c", text="c"),),
                tenant_ids=("tenant-a",),
            ),
            RegisteredResource(
                descriptor=McpResourceDescriptor(uri="memory://hidden", name="hidden"),
                contents=(McpResourceContent(uri="memory://hidden", text="hidden"),),
                tenant_ids=("tenant-b",),
            ),
        ),
        templates=(
            RegisteredResourceTemplate(
                descriptor=McpResourceTemplateDescriptor(
                    uri_template="memory://items/{id}",
                    name="item",
                    mime_type="application/json",
                )
            ),
        ),
    )

    def render_review(
        arguments: dict[str, str],
        trusted_context: Any,
    ) -> McpPromptResult:
        return McpPromptResult(
            description="Review a named target",
            messages=(
                McpPromptMessage(
                    role="user",
                    content={
                        "type": "text",
                        "text": (
                            f"Review {arguments['target']} for "
                            f"tenant {trusted_context.tenant_id}"
                        ),
                    },
                ),
            ),
        )

    prompts = McpPromptRegistry(
        (
            RegisteredPrompt(
                descriptor=McpPromptDescriptor(
                    name="review",
                    title="Review target",
                    arguments=(
                        McpPromptArgument(name="target", required=True),
                    ),
                ),
                renderer=render_review,
                tenant_ids=("tenant-a",),
            ),
            RegisteredPrompt(
                descriptor=McpPromptDescriptor(name="summarize"),
                renderer=lambda _arguments, _trusted: McpPromptResult(
                    messages=(
                        McpPromptMessage(
                            role="user",
                            content={"type": "text", "text": "Summarize"},
                        ),
                    )
                ),
            ),
        )
    )
    tools = tuple(
        ToolCapability(
            name=f"tool-{index}",
            version="1",
            description=f"tool {index}",
            input_schema={"type": "object"},
            output_schema={"type": "object"},
            permission=ToolPermission.READ_ONLY,
            risk_level=RiskLevel.LOW,
        )
        for index in range(3)
    )
    return HandsMcpServer(
        registry=ToolRegistry(tools),
        gateway=_UnusedToolGateway(),  # type: ignore[arg-type]
        resources=resources,
        prompts=prompts,
        page_size=2,
    )


def test_mcp_resources_prompts_and_tools_are_paginated_and_tenant_scoped() -> None:
    async def scenario() -> None:
        client = HandsMcpClient(InProcessMcpTransport(_server()))
        assignment = _assignment()

        initialized = await client.initialize(assignment)
        assert initialized["protocolVersion"] == MCP_PROTOCOL_VERSION
        assert initialized["capabilities"]["resources"]["subscribe"] is False
        assert initialized["capabilities"]["prompts"]["listChanged"] is False

        first_page, cursor = await client.list_resources_page(assignment)
        assert [item["uri"] for item in first_page] == ["memory://a", "memory://b"]
        assert cursor is not None
        second_page, final_cursor = await client.list_resources_page(
            assignment, cursor=cursor
        )
        assert [item["uri"] for item in second_page] == ["memory://c"]
        assert final_cursor is None
        assert [item["uri"] for item in await client.list_resources(assignment)] == [
            "memory://a",
            "memory://b",
            "memory://c",
        ]
        assert await client.read_resource(assignment, "memory://a") == [
            {
                "uri": "memory://a",
                "mimeType": "text/plain",
                "text": "a",
            }
        ]

        templates = await client.list_resource_templates(assignment)
        assert templates[0]["uriTemplate"] == "memory://items/{id}"
        assert [prompt["name"] for prompt in await client.list_prompts(assignment)] == [
            "review",
            "summarize",
        ]
        prompt = await client.get_prompt(
            assignment,
            "review",
            arguments={"target": "pull request"},
        )
        assert prompt["messages"][0]["content"]["text"] == (
            "Review pull request for tenant tenant-a"
        )
        assert [tool["name"] for tool in await client.list_tools(assignment)] == [
            "tool-0",
            "tool-1",
            "tool-2",
        ]

        other = _assignment(tenant_id="tenant-b", runtime_id="runtime-b")
        assert [item["uri"] for item in await client.list_resources(other)] == [
            "memory://a",
            "memory://hidden",
        ]
        with pytest.raises(AuraClawError):
            await client.read_resource(other, "memory://b")
        with pytest.raises(AuraClawError):
            await client.get_prompt(assignment, "review")

    asyncio.run(scenario())


def test_mcp_resource_content_requires_exactly_one_payload() -> None:
    with pytest.raises(ValidationError):
        McpResourceContent(uri="memory://invalid")
    with pytest.raises(ValidationError):
        McpResourceContent(uri="memory://invalid", text="x", blob="eA==")
