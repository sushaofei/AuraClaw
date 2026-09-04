"""Decisions derived from canonical settlements; never cache or execute tool results."""

from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from auraclaw.contracts.errors import InvalidToolSchemaError, SchemaValidationError
from auraclaw.contracts.json_schema import JsonSchemaValidator
from auraclaw.control.ports import RuntimeAssignment
from auraclaw.runtime.ports import ToolCall


def digest(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
    ).hexdigest()


def obvious_shape_error(schema: Any, value: Any, depth: int = 0) -> bool:
    """Recognize only definite shape mistakes; Gateway remains the full validator.

    No references, regex, coercion or defaults are evaluated here. Unknown constraints
    are not grounds for suppression, so a corrected request can reach the Gateway.
    """
    if not isinstance(schema, dict) or depth > 3:
        return False
    expected = schema.get("type")
    checks = {
        "object": isinstance(value, dict),
        "array": isinstance(value, list),
        "string": isinstance(value, str),
        "boolean": isinstance(value, bool),
        "integer": isinstance(value, int) and not isinstance(value, bool),
        "number": isinstance(value, (int, float)) and not isinstance(value, bool),
        "null": value is None,
    }
    if isinstance(expected, str) and expected in checks and not checks[expected]:
        return True
    if isinstance(value, dict):
        required = schema.get("required", [])
        if isinstance(required, list) and any(k not in value for k in required[:64]):
            return True
        properties = schema.get("properties", {})
        if isinstance(properties, dict):
            return any(
                obvious_shape_error(properties[k], v, depth + 1)
                for k, v in list(value.items())[:64]
                if k in properties
            )
    return False


@dataclass(frozen=True)
class RepeatTarget:
    binding: str
    arguments: str
    read_only: bool
    schema_valid: bool
    error_family: str | None = None
    capability_id: str | None = None


def repeat_target(
    assignment: RuntimeAssignment, call: ToolCall, capability_state: dict[str, Any]
) -> RepeatTarget | None:
    if call.name in {"auraclaw.capabilities.search", "auraclaw.capabilities.load"}:
        return RepeatTarget(
            binding=digest(
                {
                    "tenant": assignment.tenant_id,
                    "user": assignment.user_id,
                    "dept": assignment.dept_id,
                    "run": assignment.run_id,
                    "control_tool": call.name,
                    "policy": "2",
                }
            ),
            arguments=digest(call.arguments),
            read_only=True,
            schema_valid=True,
        )
    matches = [
        item
        for item in capability_state.get("loaded", {}).values()
        if isinstance(item, dict)
        and item.get("kind") == "tool"
        and item.get("model_tool", {}).get("function", {}).get("name") == call.name
    ]
    if len(matches) != 1:
        return None  # Discovery and ambiguous/unknown aliases keep Gateway handling.
    item = matches[0]
    schema = item.get("model_tool", {}).get("function", {}).get("parameters", {})
    family = None
    valid = True
    try:
        JsonSchemaValidator.validate(call.arguments, schema)
    except SchemaValidationError as error:
        family = error_family(error.validation_errors)
        valid = False
    except InvalidToolSchemaError:
        pass  # Gateway reports schema admission failures; never bypass its validation.
    return RepeatTarget(
        binding=digest(
            {
                "tenant": assignment.tenant_id,
                "user": assignment.user_id,
                "dept": assignment.dept_id,
                "run": assignment.run_id,
                "loaded": item,
            }
        ),
        arguments=digest(call.arguments),
        read_only=item.get("permission") == "read-only",
        schema_valid=valid,
        error_family=family,
        capability_id=str(item.get("capability_id", "")),
    )


def repeat_decision(
    assignment: RuntimeAssignment,
    call: ToolCall,
    target: RepeatTarget | None,
    events: list[Any],
    *,
    now: datetime | None = None,
) -> dict[str, Any] | None:
    if target is None:
        return None
    now = now or datetime.now(UTC)
    requested = {
        e.payload.get("tool_invocation_id"): e.payload
        for e in events
        if e.run_id == assignment.run_id and e.type == "tool.call.requested"
    }
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
    invalid = [
        h
        for h in history
        if h[2].get("error_code") == "tool_schema_invalid"
        and h[2].get("side_effect_status") == "not_started"
        and (
            not h[2].get("metadata", {}).get("error_details", {}).get("validation_errors")
            or error_family(h[2]["metadata"]["error_details"]["validation_errors"])
            == target.error_family
        )
    ]
    source = None
    reason = None
    if not target.schema_valid and len(invalid) >= 3:
        source, _, _ = invalid[-1]
        reason = "schema_correction_exhausted"
    else:
        for event, identity, result in reversed(history):
            details = result.get("metadata", {}).get("error_details", {})
            retryable = result.get("retryable", details.get("retryable"))
            if retryable is False and details.get("stage") in {
                "output_validation",
                "authorization",
                "identity",
            }:
                source, reason = event, "non_retryable_target_failure"
                break
            if identity.get("arguments") != target.arguments:
                continue
            if result.get("status") == "unknown" or result.get("side_effect_status") == "unknown":
                source, reason = event, "prior_execution_unknown"
                break
            if target.read_only and result.get("status") == "success":
                if read_refresh_allowed(target, history, events, event, now, requested):
                    return None
                source, reason = event, "existing_read_result"
                break
            if result.get("error_code") == "tool_schema_invalid":
                continue  # Two correction opportunities; valid correction remains admissible.
            if result.get("status") in {"error", "denied", "failed"}:
                if retryable is False:
                    source, reason = event, "non_retryable_failure"
                    break
                # Unknown effects were handled above; writes with uncertain completion do not retry.
                if not target.read_only and result.get("side_effect_status") != "not_started":
                    source, reason = event, "write_retry_requires_reconciliation"
                    break
                try:
                    backoff = float(result.get("retry_after", details.get("retry_after", 0)) or 0)
                except (TypeError, ValueError):
                    backoff = 0.0
                if math.isfinite(backoff) and backoff > 300:
                    source, reason = event, "retry_backoff_exceeds_policy"
                    break
                same = [h for h in history if h[1].get("arguments") == target.arguments]
                if len(same) >= 3:
                    source, reason = event, "retry_limit_reached"
                    break
    if source is None:
        return None
    return {
        "status": "denied",
        "error_code": "tool_repeat_suppressed",
        "side_effect_status": "not_started",
        "retryable": False,
        "summary": (
            "This request was not executed. Inspect the referenced prior result. "
            "It is historical evidence, not a refreshed query. Do not repeat unchanged "
            "requests; continue other authorized work or report the limitation."
        ),
        "metadata": {
            "policy_version": "2",
            "decision": "not_dispatched",
            "reason": reason,
            "source_invocation_id": source.payload["tool_invocation_id"],
            "source_occurred_at": source.occurred_at.isoformat(),
        },
    }


def no_progress(events: list[Any], run_id: str) -> bool:
    """A/B alternation cannot evade the bounded recent suppression window."""
    recent = [
        e.payload.get("result", {})
        for e in events
        if e.run_id == run_id and e.type == "tool.call.completed"
    ][-8:]
    return sum(r.get("error_code") == "tool_repeat_suppressed" for r in recent) >= 4 and not any(
        r.get("status") == "success" for r in recent
    )


def error_family(errors: list[dict[str, Any]]) -> str:
    return digest(
        sorted(
            (
                str(e.get("instance_path", "")),
                str(e.get("schema_path", "")),
                str(e.get("keyword", "")),
            )
            for e in errors
        )
    )


def read_refresh_allowed(
    target: RepeatTarget,
    history: list[Any],
    events: list[Any],
    source: Any,
    now: datetime,
    requested: dict[str, Any],
) -> bool:
    # Any confirmed write invalidates historical reads conservatively; no stale cache is served.
    if any(
        e.type == "tool.call.completed"
        and e.run_id == source.run_id
        and e.occurred_at > source.occurred_at
        and e.payload.get("result", {}).get("status") == "success"
        and (
            e.payload.get("result", {}).get("metadata", {}).get("tool_permission")
            in {"write-with-approval", "write-autonomous", "destructive/admin"}
            or requested.get(e.payload.get("tool_invocation_id"), {})
            .get("repeat_identity", {}).get("read_only") is False
        )
        for e in events
    ):
        return True
    grant = refresh_grant(target, events, source.run_id)
    if grant is None:
        return False
    return len(history) < int(grant["max_calls"]) and now < datetime.fromisoformat(
        grant["expires_at"]
    )


def refresh_grant(target: RepeatTarget, events: list[Any], run_id: str) -> dict[str, Any] | None:
    requested_run = next(
        (e for e in events if e.type == "run.requested" and e.payload.get("run_id") == run_id), None
    )
    if requested_run is None or not target.read_only or not target.capability_id:
        return None
    return next(
        (
            g
            for g in requested_run.payload.get("read_refresh", [])
            if g.get("capability_id") == target.capability_id
        ),
        None,
    )


def wait_seconds(
    target: RepeatTarget | None, events: list[Any], run_id: str, now: datetime | None = None
) -> float:
    if target is None:
        return 0
    now = now or datetime.now(UTC)
    requests = {
        e.payload.get("tool_invocation_id"): e.payload
        for e in events
        if e.run_id == run_id and e.type == "tool.call.requested"
    }
    for event in reversed(events):
        if event.run_id != run_id or event.type != "tool.call.completed":
            continue
        identity = requests.get(event.payload.get("tool_invocation_id"), {}).get(
            "repeat_identity", {}
        )
        if (
            identity.get("binding") != target.binding
            or identity.get("arguments") != target.arguments
        ):
            continue
        result = event.payload.get("result", {})
        if result.get("error_code") == "tool_repeat_suppressed":
            continue
        details = result.get("metadata", {}).get("error_details", {})
        delay = 0.0
        if result.get("status") == "success":
            grant = refresh_grant(target, events, run_id)
            delay = float(grant["min_interval_seconds"]) if grant else 0.0
        elif result.get("retryable", details.get("retryable")) is True:
            try:
                delay = float(result.get("retry_after", details.get("retry_after", 0)) or 0)
            except (TypeError, ValueError):
                delay = 0.0
            if not math.isfinite(delay):
                delay = 0.0
        return float(max(0.0, delay - (now - event.occurred_at).total_seconds()))
    return 0.0
