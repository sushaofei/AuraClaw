from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from pydantic import ValidationError

from auraclaw.action.hands import HandsGateway
from auraclaw.action.mcp_primitives import (
    HandsPromptRegistry,
    HandsResourceRegistry,
    RegisteredPrompt,
    RegisteredResource,
    RegisteredResourceTemplate,
)
from auraclaw.action.tool_gateway import ToolRegistry
from auraclaw.contracts.hands import (
    HandsPromptArgument,
    HandsPromptDescriptor,
    HandsPromptMessage,
    HandsPromptResult,
    HandsResourceContent,
    HandsResourceDescriptor,
)
from auraclaw.contracts.tools import RiskLevel, ToolCapability, ToolPermission
from auraclaw.control.ports import RuntimeAssignment
from auraclaw.internal.hands import InProcessHandsClient
from auraclaw.runtime.hands_adapter import HandsRuntimeAdapter


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


def _gateway() -> HandsGateway:
    resources = HandsResourceRegistry(
        resources=(
            RegisteredResource(
                descriptor=HandsResourceDescriptor(
                    uri="memory://a",
                    name="a",
                    mime_type="text/plain",
                    size=1,
                ),
                contents=(
                    HandsResourceContent(
                        uri="memory://a",
                        mime_type="text/plain",
                        text="a",
                    ),
                ),
            ),
            RegisteredResource(
                descriptor=HandsResourceDescriptor(uri="memory://b", name="b"),
                contents=(HandsResourceContent(uri="memory://b", text="b"),),
                tenant_ids=("tenant-a",),
            ),
            RegisteredResource(
                descriptor=HandsResourceDescriptor(uri="memory://c", name="c"),
                contents=(HandsResourceContent(uri="memory://c", text="c"),),
                tenant_ids=("tenant-a",),
            ),
            RegisteredResource(
                descriptor=HandsResourceDescriptor(uri="memory://hidden", name="hidden"),
                contents=(HandsResourceContent(uri="memory://hidden", text="hidden"),),
                tenant_ids=("tenant-b",),
            ),
        ),
        templates=(
            RegisteredResourceTemplate(
                descriptor=HandsResourceDescriptor(
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
    ) -> HandsPromptResult:
        return HandsPromptResult(
            description="Review a named target",
            messages=(
                HandsPromptMessage(
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

    prompts = HandsPromptRegistry(
        (
            RegisteredPrompt(
                descriptor=HandsPromptDescriptor(
                    name="review",
                    title="Review target",
                    arguments=(HandsPromptArgument(name="target", required=True),),
                ),
                renderer=render_review,
                tenant_ids=("tenant-a",),
            ),
            RegisteredPrompt(
                descriptor=HandsPromptDescriptor(name="summarize"),
                renderer=lambda arguments, trusted: HandsPromptResult(
                    messages=(
                        HandsPromptMessage(
                            role="user",
                            content={"type": "text", "text": "summarize"},
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
    return HandsGateway(
        registry=ToolRegistry(tools),
        gateway=_UnusedToolGateway(),  # type: ignore[arg-type]
        resources=resources,
        prompts=prompts,
        page_size=2,
    )


def test_hands_resources_prompts_and_tools_are_paginated_and_tenant_scoped() -> None:
    async def scenario() -> None:
        gateway = _gateway()
        raw = InProcessHandsClient(gateway)
        client = HandsRuntimeAdapter(raw)
        assignment = _assignment()

        first_page = await raw.list_resources(assignment)
        assert [item.uri for item in first_page.items] == ["memory://a", "memory://b"]
        assert first_page.next_cursor is not None
        second_page = await raw.list_resources(
            assignment, cursor=first_page.next_cursor
        )
        assert [item.uri for item in second_page.items] == ["memory://c"]
        assert second_page.next_cursor is None
        assert [item["uri"] for item in await client.list_resources(assignment)] == [
            "memory://a",
            "memory://b",
            "memory://c",
        ]
        loaded = await client.read_resource(assignment, "memory://a")
        assert loaded[0]["uri"] == "memory://a"
        assert loaded[0]["text"] == "a"
        assert loaded[0]["mimeType"] == "text/plain"

        templates = await client.list_resource_templates(assignment)
        assert templates[0]["uri_template"] == "memory://items/{id}"
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
        with pytest.raises(KeyError):
            await client.read_resource(other, "memory://b")
        with pytest.raises(ValueError):
            await client.get_prompt(assignment, "review")

    asyncio.run(scenario())


def test_hands_resource_content_requires_exactly_one_payload() -> None:
    with pytest.raises(ValidationError):
        HandsResourceContent(uri="memory://invalid")
    with pytest.raises(ValidationError):
        HandsResourceContent(uri="memory://invalid", text="x", blob="eA==")
