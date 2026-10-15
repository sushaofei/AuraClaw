from __future__ import annotations

from auraclaw.action.catalog.contracts import (
    CAPABILITY_LOAD_TOOL_NAME,
    CAPABILITY_SEARCH_TOOL_NAME,
    SKILL_BINDING_STATUS_TOOL_NAME,
    SKILL_RESOLVE_TOOL_NAME,
)
from auraclaw.contracts.capabilities import (
    CapabilityKind,
)
from auraclaw.contracts.tools import (
    RiskLevel,
    ToolCapability,
    ToolPermission,
)


def capability_search_tool() -> ToolCapability:
    return ToolCapability(
        name=CAPABILITY_SEARCH_TOOL_NAME,
        version="1",
        description=(
            "Search the policy-visible AuraClaw capability catalog without loading "
            "full Resource or Skill content."
        ),
        input_schema={
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
                        "enum": [kind.value for kind in CapabilityKind],
                    },
                },
                "required_permissions": {
                    "type": "array",
                    "items": {"type": "string"},
                },
                "limit": {"type": "integer", "minimum": 1, "maximum": 50},
            },
            "additionalProperties": False,
        },
        output_schema={
            "type": "object",
            "properties": {
                "capabilities": {
                    "type": "array",
                    "items": {"type": "object"},
                },
                "hint": {"type": "string"},
                "empty_reason": {"type": "string"},
                "truncated": {"type": "boolean"},
                "search_policy_version": {"type": "string"},
                "semantic_degraded": {"type": "boolean"},
                "semantic_degraded_reason": {"type": "string"},
                "available_domains": {
                    "type": "array",
                    "items": {"type": "string"},
                },
            },
            "required": [
                "capabilities",
                "search_policy_version",
                "semantic_degraded",
            ],
            "additionalProperties": False,
        },
        permission=ToolPermission.READ_ONLY,
        risk_level=RiskLevel.LOW,
        runtime_location="hands",
        cache_result=False,
        owner="platform",
    )


def capability_load_tool() -> ToolCapability:
    return ToolCapability(
        name=CAPABILITY_LOAD_TOOL_NAME,
        version="1",
        description=(
            "Load authoritative contracts for a bounded set of capability ids "
            "returned by capability search."
        ),
        input_schema={
            "type": "object",
            "properties": {
                "capability_ids": {
                    "type": "array",
                    "items": {"type": "string", "maxLength": 256},
                    "maxItems": 8,
                }
            },
            "required": ["capability_ids"],
            "additionalProperties": False,
        },
        output_schema={"type": "object"},
        permission=ToolPermission.READ_ONLY,
        risk_level=RiskLevel.LOW,
        runtime_location="hands",
        cache_result=False,
        owner="platform",
    )


def skill_resolve_tool() -> ToolCapability:
    return ToolCapability(
        name=SKILL_RESOLVE_TOOL_NAME,
        version="1",
        description="Resolve an exact Skill binding for the trusted Agent Runtime.",
        input_schema={
            "type": "object",
            "properties": {
                "name": {"type": "string", "minLength": 1, "maxLength": 256},
                "version": {"type": "string", "maxLength": 128},
                "publisher": {"type": "string", "maxLength": 128},
                "role": {"type": "string", "minLength": 1, "maxLength": 64},
                "policy_version": {"type": "string", "maxLength": 128},
                "active_skill_names": {
                    "type": "array",
                    "items": {"type": "string", "maxLength": 256},
                },
            },
            "required": ["name", "role"],
            "additionalProperties": False,
        },
        output_schema={"type": "object"},
        permission=ToolPermission.READ_ONLY,
        risk_level=RiskLevel.LOW,
        runtime_location="hands",
        cache_result=False,
        owner="platform-internal",
    )


def skill_binding_status_tool() -> ToolCapability:
    return ToolCapability(
        name=SKILL_BINDING_STATUS_TOOL_NAME,
        version="1",
        description=(
            "Evaluate the current governed disposition of an already-fixed Skill binding."
        ),
        input_schema={
            "type": "object",
            "properties": {
                "publisher": {"type": "string", "minLength": 1, "maxLength": 128},
                "name": {"type": "string", "minLength": 1, "maxLength": 256},
                "version": {"type": "string", "minLength": 1, "maxLength": 128},
                "package_digest": {
                    "type": "string",
                    "pattern": "^sha256:[0-9a-f]{64}$",
                },
            },
            "required": ["publisher", "name", "version", "package_digest"],
            "additionalProperties": False,
        },
        output_schema={"type": "object"},
        permission=ToolPermission.READ_ONLY,
        risk_level=RiskLevel.LOW,
        runtime_location="hands",
        cache_result=False,
        owner="platform-internal",
    )
