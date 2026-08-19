from __future__ import annotations

import json
from typing import Any

from auraclaw.contracts.errors import AuraClawError
from auraclaw.contracts.hands import (
    HandsResourceContent,
    HandsResourceDescriptor,
    HandsToolCall,
)
from auraclaw.contracts.skills import SkillBinding
from auraclaw.control.ports import RuntimeAssignment
from auraclaw.runtime.ports import HandsClient, ToolCall

_SKILL_RESOLVE_TOOL_NAME = "auraclaw.skills.resolve"


class HandsRuntimeAdapter:
    """CapabilityClient/ToolClient facade over the protocol-agnostic HandsClient."""

    def __init__(self, client: HandsClient) -> None:
        self._client = client

    async def list_tools(self, assignment: RuntimeAssignment) -> list[dict[str, Any]]:
        items: list[dict[str, Any]] = []
        cursor: str | None = None
        while True:
            page = await self._client.list_tools(assignment, cursor=cursor)
            items.extend(
                {
                    "name": item.name,
                    "description": item.description,
                    "inputSchema": item.input_schema,
                    "outputSchema": item.output_schema,
                    "version": item.version,
                    "readOnly": item.read_only,
                    "destructive": item.destructive,
                    "riskLevel": item.risk_level,
                }
                for item in page.items
            )
            cursor = page.next_cursor
            if cursor is None:
                return items

    async def list_resources(self, assignment: RuntimeAssignment) -> list[dict[str, Any]]:
        items: list[dict[str, Any]] = []
        cursor: str | None = None
        while True:
            page = await self._client.list_resources(assignment, cursor=cursor)
            items.extend(_resource_dict(item) for item in page.items)
            cursor = page.next_cursor
            if cursor is None:
                return items

    async def list_resource_templates(
        self, assignment: RuntimeAssignment
    ) -> list[dict[str, Any]]:
        items: list[dict[str, Any]] = []
        cursor: str | None = None
        while True:
            page = await self._client.list_resource_templates(assignment, cursor=cursor)
            items.extend(_resource_dict(item) for item in page.items)
            cursor = page.next_cursor
            if cursor is None:
                return items

    async def read_resource(
        self,
        assignment: RuntimeAssignment,
        uri: str,
    ) -> list[dict[str, Any]]:
        contents = await self._client.read_resource(assignment, uri)
        return [_content_dict(item) for item in contents]

    async def list_prompts(self, assignment: RuntimeAssignment) -> list[dict[str, Any]]:
        items: list[dict[str, Any]] = []
        cursor: str | None = None
        while True:
            page = await self._client.list_prompts(assignment, cursor=cursor)
            items.extend(item.model_dump(mode="json") for item in page.items)
            cursor = page.next_cursor
            if cursor is None:
                return items

    async def get_prompt(
        self,
        assignment: RuntimeAssignment,
        name: str,
        *,
        arguments: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        result = await self._client.get_prompt(
            assignment, name, arguments=arguments
        )
        return result.model_dump(mode="json")

    async def load_skill_manifest(
        self,
        assignment: RuntimeAssignment,
        *,
        publisher: str,
        name: str,
        version: str,
    ) -> dict[str, Any]:
        contents = await self.load_skill_part(
            assignment,
            publisher=publisher,
            name=name,
            version=version,
            path="manifest",
        )
        if len(contents) != 1 or not isinstance(contents[0].get("text"), str):
            raise AuraClawError("Skill manifest Resource is not textual")
        try:
            value = json.loads(str(contents[0]["text"]))
        except json.JSONDecodeError as exc:
            raise AuraClawError("Skill manifest Resource is not valid JSON") from exc
        if not isinstance(value, dict):
            raise AuraClawError("Skill manifest Resource must contain an object")
        return value

    async def load_skill_part(
        self,
        assignment: RuntimeAssignment,
        *,
        publisher: str,
        name: str,
        version: str,
        path: str,
    ) -> list[dict[str, Any]]:
        if not path or path.startswith("/") or ".." in path.split("/") or "\\" in path:
            raise ValueError("Skill part path is unsafe")
        uri = f"skill://{publisher}/{name}/{version}/{path}"
        return await self.read_resource(assignment, uri)

    async def resolve_skill(
        self,
        assignment: RuntimeAssignment,
        *,
        name: str,
        version: str = "*",
        publisher: str | None = None,
        active_skill_names: tuple[str, ...] = (),
    ) -> SkillBinding:
        result = await self.execute(
            assignment,
            ToolCall(
                tool_invocation_id=(
                    f"resolve_{assignment.run_id}_{name.replace('.', '_')}"
                ),
                name=_SKILL_RESOLVE_TOOL_NAME,
                version="1",
                arguments={
                    "name": name,
                    "version": version,
                    **({"publisher": publisher} if publisher is not None else {}),
                    "role": assignment.role,
                    "policy_version": "runtime",
                    "active_skill_names": list(active_skill_names),
                },
            ),
        )
        content = result.get("content")
        payload = dict(content) if isinstance(content, dict) else result
        binding = payload.get("binding")
        if not isinstance(binding, dict):
            raise AuraClawError("Skill resolver did not return a binding")
        return SkillBinding.model_validate(binding)

    async def execute(
        self, assignment: RuntimeAssignment, call: ToolCall
    ) -> dict[str, Any]:
        result = await self._client.call_tool(assignment, tool_call_to_hands(call))
        return result.as_dict()

    async def cancel(self, assignment: RuntimeAssignment, tool_invocation_id: str) -> bool:
        return await self._client.cancel_invocation(assignment, tool_invocation_id)


def tool_call_to_hands(call: ToolCall) -> HandsToolCall:
    return HandsToolCall(
        tool_invocation_id=call.tool_invocation_id,
        name=call.name,
        version=call.version,
        arguments=dict(call.arguments),
        expected_side_effect=call.expected_side_effect,
        idempotency_key=call.idempotency_key or call.tool_invocation_id,
        approval_id=call.approval_id,
        credential_ref=call.credential_ref,
    )


def _resource_dict(item: HandsResourceDescriptor) -> dict[str, Any]:
    return item.model_dump(mode="json")


def _content_dict(content: HandsResourceContent) -> dict[str, Any]:
    payload = content.model_dump(mode="json")
    payload["mimeType"] = payload.pop("mime_type")
    payload["_governance"] = {
        "contentDigest": content.content_digest,
        "sourceRevision": content.source_revision,
        "classification": content.classification,
        "policyDecisionId": content.policy_decision_id,
        "artifactRef": (
            content.artifact_ref.as_dict() if content.artifact_ref is not None else None
        ),
        "securityFindings": list(content.security_findings),
    }
    return payload
