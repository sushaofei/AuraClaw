from __future__ import annotations

import asyncio
import copy
from typing import Any, cast

from auraclaw.control.ports import RuntimeAssignment
from auraclaw.runtime.capability_controller import RuntimeCapabilityController
from auraclaw.runtime.ports import CapabilityClient, ToolCall


def _state() -> dict[str, Any]:
    return {"loaded": {"cap-evidence": {
        "kind": "tool", "canonical_name": "inventory.evidence.list", "version": "1",
        "permission": "read-only", "model_tool": {"type": "function", "function": {
            "name": "mcp_exact_alias", "description": "List evidence",
            "parameters": {"type": "object", "required": ["input"],
                "additionalProperties": False, "properties": {"input": {
                    "type": "object", "required": ["type", "limit"], "properties": {
                        "type": {"type": "string"}, "limit": {"type": "integer"},
                    },
                }},
            },
        }},
    }}}


class Client:
    def __init__(self, result: dict[str, Any]) -> None:
        self.result = result
        self.calls: list[ToolCall] = []

    async def execute(self, assignment: RuntimeAssignment, call: ToolCall) -> dict[str, Any]:
        self.calls.append(call)
        return self.result


def test_model_contract_explains_nested_numbers_without_changing_schema_or_state() -> None:
    state = _state()
    before = copy.deepcopy(state)
    controller = RuntimeCapabilityController(cast(CapabilityClient, Client({})))
    tools = controller.model_tools(state)
    names = {tool["function"]["name"] for tool in tools}
    assert "auraclaw.resources.read" not in names
    assert "auraclaw.skills.activate" not in names
    function = next(t["function"] for t in tools if t["function"]["name"] == "mcp_exact_alias")
    assert '"/input/limit":"integer"' in function["description"]
    assert "do not flatten" in function["description"]
    original = before["loaded"]["cap-evidence"]["model_tool"]["function"]
    assert function["parameters"] == original["parameters"]
    assert state == before
    state["loaded"]["resource"] = {"kind": "resource"}
    state["loaded"]["skill"] = {"kind": "skill"}
    names = {t["function"]["name"] for t in controller.model_tools(state)}
    assert {"auraclaw.resources.read", "auraclaw.skills.activate"} <= names


def test_local_rejection_adds_targeted_guidance_without_automatic_replay_or_coercion() -> None:
    async def scenario() -> None:
        original = {"status": "error", "error_code": "tool_schema_invalid",
                    "side_effect_status": "not_started", "metadata": {
                        "error_details": {"stage": "argument_validation", "origin": "local"}}}
        client = Client(original)
        controller = RuntimeCapabilityController(cast(CapabilityClient, client))
        arguments = {"type": "WAREHOUSE", "limit": "3"}
        result = await controller.execute(cast(RuntimeAssignment, object()), ToolCall(
            tool_invocation_id="bad", name="mcp_exact_alias", arguments=arguments), _state())
        assert len(client.calls) == 1
        assert client.calls[0].arguments == arguments
        guide = result.result["metadata"]["argument_guidance"]
        assert guide["tool_name"] == "mcp_exact_alias"
        assert guide["field_types"]["/input/limit"] == "integer"
        assert guide["required_paths"] == ["/input", "/input/type", "/input/limit"]
        assert "argument_guidance" not in original["metadata"]
        client.result = {"status": "error", "error_code": "mcp_tool_error",
                         "side_effect_status": "unknown"}
        downstream = await controller.execute(cast(RuntimeAssignment, object()), ToolCall(
            tool_invocation_id="remote", name="mcp_exact_alias",
            arguments={"input": {"type": "WAREHOUSE", "limit": 3}}), _state())
        assert downstream.result == client.result
        assert len(client.calls) == 2
    asyncio.run(scenario())


def test_wrong_resource_entry_points_to_loaded_tool_without_dispatch() -> None:
    async def scenario() -> None:
        client = Client({})
        controller = RuntimeCapabilityController(cast(CapabilityClient, client))
        result = await controller.execute(cast(RuntimeAssignment, object()), ToolCall(
            tool_invocation_id="wrong-kind", name="auraclaw.resources.read",
            arguments={"capability_id": "cap-evidence"}), _state())
        assert result.result["error_code"] == "resource_not_loaded"
        assert result.result["metadata"]["argument_guidance"]["tool_name"] == "mcp_exact_alias"
        assert not client.calls
    asyncio.run(scenario())
