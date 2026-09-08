from __future__ import annotations

import json
from typing import Any


def tool_argument_guidance(model_tool: dict[str, Any]) -> dict[str, Any]:
    """Bounded reminders from the loaded contract; never generate business values."""
    function = model_tool.get("function", {})
    schema = function.get("parameters", {})
    required_paths: list[str] = []
    field_types: dict[str, Any] = {}
    visited = 0

    def visit(node: Any, prefix: str, depth: int) -> None:
        nonlocal visited
        if not isinstance(node, dict) or depth > 2 or visited >= 24:
            return
        required = node.get("required", [])
        properties = node.get("properties", {})
        if not isinstance(properties, dict):
            return
        for name, child in list(properties.items())[:24]:
            if not isinstance(child, dict) or visited >= 24:
                continue
            path = prefix + "/" + str(name).replace("~", "~0").replace("/", "~1")
            if len(path) > 256:
                continue
            visited += 1
            if name in required:
                required_paths.append(path)
            kind = child.get("type")
            if isinstance(kind, str) and kind in {
                "object", "array", "string", "number", "integer", "boolean", "null",
            }:
                field_types[path] = kind
            visit(child, path, depth + 1)

    visit(schema, "", 0)
    return {
        "tool_name": function.get("name"),
        "required_paths": required_paths,
        "field_types": field_types,
        "instruction": (
            "Call this exact Tool function using its full parameters schema. "
            "Keep nested objects (including input when declared); do not flatten fields. "
            "Use JSON numbers for integer/number fields, not quoted strings. "
            "Obtain missing business values from authorized data or ask the user; "
            "never invent them. "
            "A Tool is not a Resource or Skill. After local argument validation fails, "
            "correct the arguments before trying again; do not repeat unchanged invalid arguments. "
            "This reminder is partial; the full schema remains authoritative."
        ),
    }


def tool_argument_description(model_tool: dict[str, Any]) -> str:
    return (
        "\nThis loaded function is available to call for the user's authorized task. "
        "The gateway enforces the effective approval mode and may pause the call for approval. "
        "A write-with-approval label is not itself a denial or a pending approval. "
        "Do not claim approval is pending unless the gateway reports it; "
        "respect actual denials and never bypass the gateway.\nArguments guidance: "
    ) + json.dumps(
        tool_argument_guidance(model_tool), ensure_ascii=False, separators=(",", ":")
    )
