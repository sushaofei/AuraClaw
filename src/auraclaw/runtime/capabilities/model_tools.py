from __future__ import annotations

import copy
from typing import Any

from auraclaw.runtime.capabilities.contracts import (
    CAPABILITY_LOAD,
    CAPABILITY_SEARCH,
    RESOURCE_READ,
    SKILL_ACTIVATE,
    SKILL_REFERENCE_READ,
    SKILL_RESOLVE_AND_ACTIVATE,
)
from auraclaw.runtime.capabilities.state import CapabilityState
from auraclaw.runtime.tool_argument_guidance import (
    tool_argument_description,
)


def model_tools(
    state: CapabilityState, *, max_candidates: int, max_loaded: int
) -> tuple[dict[str, Any], ...]:
    tools = [
        _function_tool(
            SKILL_RESOLVE_AND_ACTIVATE,
            "Resolve and activate one unambiguous policy-visible Skill in a single "
            "governed request. Prefer this tool when the task needs a Skill. It preserves "
            "package integrity, policy, dependency and activation checks; ambiguous matches "
            "are returned for another model decision instead of being activated.",
            {
                "type": "object",
                "properties": {
                    "query": {"type": "string", "maxLength": 1024},
                    "canonical_name": {"type": "string", "maxLength": 256},
                    "capability_id": {"type": "string", "maxLength": 256},
                    "inputs": {"type": "object"},
                },
                "required": ["inputs"],
                "additionalProperties": False,
            },
        ),
        _function_tool(
            CAPABILITY_SEARCH,
            "Search the policy-visible capability catalog when the task needs "
            "external data, an action, or a governed Skill. "
            'For MCP queries or actions use kinds=["tool"] or omit kinds to search '
            "all kinds. Prefer skills.resolve_and_activate when the task needs a Skill. "
            "Do not restrict ordinary data queries to Skills or Resources. "
            'To list Skills use kinds=["skill"] '
            'and query=""; use canonical_name for a known exact name. An empty result '
            "does not prove no Skill is installed; "
            "only currently executable matches are listed.",
            {
                "type": "object",
                "properties": {
                    "query": {"type": "string", "maxLength": 1024},
                    "capability_id": {"type": "string", "maxLength": 256},
                    "canonical_name": {"type": "string", "maxLength": 256},
                    "server_id": {"type": "string", "maxLength": 128},
                    "kinds": {
                        "type": "array",
                        "items": {
                            "type": "string",
                            "enum": [
                                "resource",
                                "resource_template",
                                "tool",
                                "skill",
                            ],
                        },
                    },
                    "required_permissions": {
                        "type": "array",
                        "items": {"type": "string"},
                    },
                    "limit": {
                        "type": "integer",
                        "minimum": 1,
                        "maximum": max_candidates,
                    },
                },
                "additionalProperties": False,
            },
        ),
        _function_tool(
            CAPABILITY_LOAD,
            "Load authoritative contracts for a small set of capability ids "
            "returned by capability search.",
            {
                "type": "object",
                "properties": {
                    "capability_ids": {
                        "type": "array",
                        "items": {"type": "string"},
                        "maxItems": max_loaded,
                    }
                },
                "required": ["capability_ids"],
                "additionalProperties": False,
            },
        ),
        _function_tool(
            SKILL_ACTIVATE,
            "Request activation of one loaded Skill. Runtime and Policy make "
            "the authoritative decision.",
            {
                "type": "object",
                "properties": {
                    "capability_id": {"type": "string"},
                    "inputs": {"type": "object"},
                },
                "required": ["capability_id", "inputs"],
                "additionalProperties": False,
            },
        ),
        _function_tool(
            RESOURCE_READ,
            "Read one loaded Resource or Resource Template through the governed Resource Gateway.",
            {
                "type": "object",
                "properties": {
                    "capability_id": {"type": "string"},
                    "arguments": {"type": "object"},
                },
                "required": ["capability_id"],
                "additionalProperties": False,
            },
        ),
        _function_tool(
            SKILL_REFERENCE_READ,
            "Read one reference declared by an active signed Skill. Use the exact "
            "skill_activation_id and path; undeclared paths, other packages, and "
            "oversized content are denied.",
            {
                "type": "object",
                "properties": {
                    "skill_activation_id": {"type": "string"},
                    "path": {"type": "string", "pattern": "^references/"},
                },
                "required": ["skill_activation_id", "path"],
                "additionalProperties": False,
            },
        ),
    ]
    loaded_items = [item for item in state.loaded.values() if isinstance(item, dict)]
    kinds = {item.get("kind") for item in loaded_items}
    tools = [
        tool
        for tool in tools
        if not (
            (tool["function"]["name"] == SKILL_ACTIVATE and "skill" not in kinds)
            or (
                tool["function"]["name"] == RESOURCE_READ
                and not kinds.intersection({"resource", "resource_template"})
            )
            or (
                tool["function"]["name"] == SKILL_REFERENCE_READ
                and not any(
                    isinstance(active, dict)
                    and any(
                        isinstance(requirement, dict) and requirement.get("preload") is not True
                        for requirement in active.get("reference_requirements", ())
                    )
                    for active in state.active_skills
                )
            )
        )
    ]
    for loaded in loaded_items:
        if not isinstance(loaded, dict):
            continue
        model_tool = loaded.get("model_tool")
        if isinstance(model_tool, dict):
            visible_tool = copy.deepcopy(model_tool)
            if loaded.get("kind") == "tool" and isinstance(visible_tool.get("function"), dict):
                function = visible_tool["function"]
                function["description"] = str(function.get("description", "")) + (
                    tool_argument_description(model_tool)
                )
            tools.append(visible_tool)
    return tuple(tools)


def _function_tool(name: str, description: str, parameters: dict[str, Any]) -> dict[str, Any]:
    return {
        "type": "function",
        "function": {
            "name": name,
            "description": description,
            "parameters": parameters,
        },
    }
