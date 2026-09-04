"""Decisions derived from canonical settlements; never cache or execute tool results."""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any

from auraclaw.control.ports import RuntimeAssignment
from auraclaw.runtime.ports import ToolCall


def digest(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"),
                                     ensure_ascii=False).encode()).hexdigest()


def obvious_shape_error(schema: Any, value: Any, depth: int = 0) -> bool:
    """Recognize only definite shape mistakes; Gateway remains the full validator.

    No references, regex, coercion or defaults are evaluated here. Unknown constraints
    are not grounds for suppression, so a corrected request can reach the Gateway.
    """
    if not isinstance(schema, dict) or depth > 3:
        return False
    expected = schema.get("type")
    checks = {"object": isinstance(value, dict), "array": isinstance(value, list),
              "string": isinstance(value, str), "boolean": isinstance(value, bool),
              "integer": isinstance(value, int) and not isinstance(value, bool),
              "number": isinstance(value, (int, float)) and not isinstance(value, bool),
              "null": value is None}
    if isinstance(expected, str) and expected in checks and not checks[expected]:
        return True
    if isinstance(value, dict):
        required = schema.get("required", [])
        if isinstance(required, list) and any(k not in value for k in required[:64]):
            return True
        properties = schema.get("properties", {})
        if isinstance(properties, dict):
            return any(obvious_shape_error(properties[k], v, depth + 1)
                       for k, v in list(value.items())[:64] if k in properties)
    return False


@dataclass(frozen=True)
class RepeatTarget:
    binding: str
    arguments: str
    read_only: bool
    schema_valid: bool


def repeat_target(
    assignment: RuntimeAssignment, call: ToolCall, capability_state: dict[str, Any]
) -> RepeatTarget | None:
    if call.name in {"auraclaw.capabilities.search", "auraclaw.capabilities.load"}:
        return RepeatTarget(
            binding=digest({"tenant": assignment.tenant_id, "user": assignment.user_id,
                            "dept": assignment.dept_id, "run": assignment.run_id,
                            "control_tool": call.name, "policy": "2"}),
            arguments=digest(call.arguments), read_only=True, schema_valid=True,
        )
    matches = [item for item in capability_state.get("loaded", {}).values()
               if isinstance(item, dict) and item.get("kind") == "tool"
               and item.get("model_tool", {}).get("function", {}).get("name") == call.name]
    if len(matches) != 1:
        return None  # Discovery and ambiguous/unknown aliases keep Gateway handling.
    item = matches[0]
    schema = item.get("model_tool", {}).get("function", {}).get("parameters", {})
    return RepeatTarget(
        binding=digest({"tenant": assignment.tenant_id, "user": assignment.user_id,
                        "dept": assignment.dept_id, "run": assignment.run_id, "loaded": item}),
        arguments=digest(call.arguments), read_only=item.get("permission") == "read-only",
        schema_valid=not obvious_shape_error(schema, call.arguments),
    )


def repeat_decision(
    assignment: RuntimeAssignment, call: ToolCall, target: RepeatTarget | None,
    events: list[Any],
) -> dict[str, Any] | None:
    if target is None:
        return None
    requested = {e.payload.get("tool_invocation_id"): e.payload for e in events
                 if e.run_id == assignment.run_id and e.type == "tool.call.requested"}
    history = []
    for event in events:
        if event.run_id != assignment.run_id or event.type != "tool.call.completed":
            continue
        invocation = event.payload.get("tool_invocation_id")
        request = requested.get(invocation, {})
        identity = request.get("repeat_identity", {})
        if invocation == call.tool_invocation_id or identity.get("binding") != target.binding:
            continue
        result = event.payload.get("result", {})
        # A suppression references an earlier fact; it is not a new execution result.
        if result.get("error_code") == "tool_repeat_suppressed":
            continue
        history.append((event, identity, result))
    invalid = [h for h in history if h[2].get("error_code") == "tool_schema_invalid"
               and h[2].get("side_effect_status") == "not_started"]
    source = None
    reason = None
    if not target.schema_valid and len(invalid) >= 3:
        source, _, _ = invalid[-1]
        reason = "schema_correction_exhausted"
    else:
        for event, identity, result in reversed(history):
            if identity.get("arguments") != target.arguments:
                continue
            if result.get("status") == "unknown" or result.get("side_effect_status") == "unknown":
                source, reason = event, "prior_execution_unknown"
                break
            if target.read_only and result.get("status") == "success":
                source, reason = event, "existing_read_result"
                break
            if result.get("error_code") == "tool_schema_invalid":
                continue  # Two correction opportunities; valid correction remains admissible.
            if result.get("status") in {"error", "denied", "failed"}:
                if result.get("retryable") is False:
                    source, reason = event, "non_retryable_failure"
                    break
                same = [h for h in history if h[1].get("arguments") == target.arguments]
                if len(same) >= 3:
                    source, reason = event, "retry_limit_reached"
                    break
    if source is None:
        return None
    return {
        "status": "denied", "error_code": "tool_repeat_suppressed",
        "side_effect_status": "not_started", "retryable": False,
        "summary": ("This request was not executed. Inspect the referenced prior result. "
                    "It is historical evidence, not a refreshed query. Do not repeat unchanged "
                    "requests; continue other authorized work or report the limitation."),
        "metadata": {"policy_version": "2", "decision": "not_dispatched", "reason": reason,
                     "source_invocation_id": source.payload["tool_invocation_id"],
                     "source_occurred_at": source.occurred_at.isoformat()},
    }


def no_progress(events: list[Any], run_id: str) -> bool:
    """A/B alternation cannot evade the bounded recent suppression window."""
    recent = [e.payload.get("result", {}) for e in events
              if e.run_id == run_id and e.type == "tool.call.completed"][-8:]
    return (sum(r.get("error_code") == "tool_repeat_suppressed" for r in recent) >= 4
            and not any(r.get("status") == "success" for r in recent))
