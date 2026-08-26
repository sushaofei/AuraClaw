"""Build the user-facing execution trace from Canonical Session Events."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Sequence
from datetime import datetime
from typing import Any

from auraclaw.contracts.events import CanonicalEvent
from auraclaw.observability.redaction import redact_sensitive

ACTIVITY_EVENT_TYPES = frozenset(
    {
        "session.created",
        "user.message.appended",
        "run.requested",
        "run.scheduled",
        "run.started",
        "run.retry_scheduled",
        "run.completed",
        "run.failed",
        "run.cancelled",
        "run.terminated",
        "model.input.prepared",
        "model.turn.completed",
        "model.output.completed",
        "tool.call.requested",
        "tool.call.completed",
        "tool.call.denied",
        "skill.activated",
        "skill.completed",
        "skill.failed",
        "skill.cancelled",
        "context.resource.used",
        "approval.requested",
        "approval.approved",
        "approval.rejected",
        "approval.expired",
        "approval.cancelled",
        "human.response.recorded",
        "session.paused",
        "session.resumed",
        "session.closed",
        "runtime.failed",
        "runtime.reprovisioned",
    }
)

_TERMINAL_RUN = {"run.completed", "run.failed", "run.cancelled", "run.terminated"}
_TERMINAL_SKILL = {
    "skill.completed",
    "skill.failed",
    "skill.cancelled",
}
_TERMINAL_APPROVAL = {
    "approval.approved",
    "approval.rejected",
    "approval.expired",
    "approval.cancelled",
}
_MAX_DETAIL_BYTES = 16_384
_MAX_PREVIEW = 800


def build_activity(events: Sequence[CanonicalEvent]) -> list[dict[str, Any]]:
    """Fold event lifecycles into stable product nodes.

    The returned list is chronological by the first aggregate version. Callers may
    page it by ``updated_version`` so a terminal event can update an existing node.
    """

    nodes: dict[str, dict[str, Any]] = {}
    ordered = sorted(events, key=lambda item: item.aggregate_version)
    for event in ordered:
        if event.type not in ACTIVITY_EVENT_TYPES:
            continue
        _apply_event(nodes, event)
    result = list(nodes.values())
    for node in result:
        node["duration_ms"] = _duration_ms(
            node.get("started_at"), node.get("completed_at")
        )
    return sorted(result, key=lambda item: (int(item["sequence"]), str(item["id"])))


def page_activity(
    nodes: Sequence[dict[str, Any]], *, after_version: int, limit: int
) -> dict[str, Any]:
    """Page node updates without losing requested→terminal lifecycle changes."""

    candidates = sorted(
        (
            dict(node)
            for node in nodes
            if int(node.get("updated_version", 0)) > after_version
        ),
        key=lambda item: (int(item["updated_version"]), int(item["sequence"])),
    )
    selected = candidates[:limit]
    next_after = max(
        (int(node["updated_version"]) for node in selected), default=after_version
    )
    return {
        "nodes": sorted(
            selected, key=lambda item: (int(item["sequence"]), str(item["id"]))
        ),
        "next_after_version": next_after,
        "has_more": len(candidates) > len(selected),
    }


def _apply_event(nodes: dict[str, dict[str, Any]], event: CanonicalEvent) -> None:
    payload = event.payload
    run_id = event.run_id or _text(payload.get("run_id")) or None
    timestamp = event.occurred_at.isoformat()

    if event.type in {"session.created", "user.message.appended"}:
        content = _text(payload.get("goal") or payload.get("message"))
        _new_event_node(
            nodes,
            event,
            node_type="user_prompt",
            status="completed",
            title="User prompt",
            summary=_preview(content),
            run_id=run_id,
            detail={"content": _preview(content, 2_000)},
        )
        return

    if event.type.startswith("run."):
        identity = run_id or event.event_id
        node = _upsert(
            nodes,
            f"run:{identity}",
            event,
            node_type="run",
            status=_run_status(event.type),
            title=f"Run {identity}",
            run_id=run_id,
        )
        node["summary"] = _run_summary(event)
        node["detail"] = _bounded_detail(payload)
        if event.type in _TERMINAL_RUN:
            node["completed_at"] = timestamp
        return

    if event.type == "model.input.prepared":
        model_call_id = _text(payload.get("model_call_id")) or event.event_id
        _new_event_node(
            nodes,
            event,
            node_id=f"model-input:{run_id or 'session'}:{model_call_id}",
            node_type="model_input",
            status="completed",
            title="Model input",
            summary=_model_input_summary(payload),
            run_id=run_id,
            detail=payload,
            correlation={"model_call_id": model_call_id},
        )
        return

    if event.type in {"model.turn.completed", "model.output.completed"}:
        model_call_id = _text(payload.get("model_call_id")) or event.event_id
        node = _upsert(
            nodes,
            f"model-output:{run_id or 'session'}:{model_call_id}",
            event,
            node_type="model_output",
            status="completed",
            title="Model output",
            run_id=run_id,
            correlation={"model_call_id": model_call_id},
        )
        output = _text(payload.get("output") or payload.get("completed_output"))
        tool_calls = payload.get("tool_calls")
        node["summary"] = _preview(output) or (
            f"Requested {len(tool_calls)} tool call(s)"
            if isinstance(tool_calls, list | tuple)
            else "Model turn completed"
        )
        node["detail"] = _bounded_detail(
            {
                key: value
                for key, value in payload.items()
                if key not in {"output", "completed_output", "tool_calls"}
            }
        )
        node["completed_at"] = timestamp
        return

    if event.type.startswith("tool.call."):
        invocation_id = _text(payload.get("tool_invocation_id")) or event.event_id
        node = _upsert(
            nodes,
            f"tool:{run_id or 'session'}:{invocation_id}",
            event,
            node_type="tool",
            status=_tool_status(event),
            title=_text(payload.get("name") or payload.get("tool_name")) or "Tool",
            run_id=run_id,
            correlation={"tool_invocation_id": invocation_id},
        )
        if event.type == "tool.call.requested":
            node["summary"] = _tool_source_summary(payload)
            node["detail"] = _bounded_detail(
                {
                    "arguments": payload.get("arguments", {}),
                    "version": payload.get("version"),
                    "expected_side_effect": payload.get("expected_side_effect"),
                    "activity": payload.get("activity", {}),
                }
            )
        else:
            node["summary"] = _tool_result_summary(payload)
            previous = node.get("detail")
            node["detail"] = _bounded_detail(
                {
                    "request": previous if isinstance(previous, dict) else {},
                    "result": payload.get("result", {}),
                    "error_code": payload.get("error_code"),
                    "approval_id": payload.get("approval_id"),
                }
            )
            node["completed_at"] = timestamp
        return

    if event.type.startswith("skill."):
        activation_id = _text(payload.get("skill_activation_id")) or event.event_id
        node = _upsert(
            nodes,
            f"skill:{run_id or 'session'}:{activation_id}",
            event,
            node_type="skill",
            status=_skill_status(event.type),
            title=_text(payload.get("skill_name")) or "Skill",
            run_id=run_id,
            correlation={"skill_activation_id": activation_id},
        )
        node["summary"] = _skill_summary(payload, event.type)
        node["detail"] = _bounded_detail(payload)
        if event.type in _TERMINAL_SKILL:
            node["completed_at"] = timestamp
        return

    if event.type == "context.resource.used":
        uri = _text(payload.get("uri"))
        _new_event_node(
            nodes,
            event,
            node_type="resource",
            status="completed",
            title=uri or "Resource",
            summary=_text(payload.get("summary")) or "Resource used",
            run_id=run_id,
            detail=payload,
            correlation={"capability_id": _text(payload.get("capability_id"))},
        )
        return

    if event.type.startswith("approval.") or event.type == "human.response.recorded":
        approval_id = _text(payload.get("approval_id")) or event.event_id
        node = _upsert(
            nodes,
            f"approval:{run_id or 'session'}:{approval_id}",
            event,
            node_type="approval",
            status=_approval_status(event.type),
            title=_text(payload.get("tool_name")) or "Approval",
            run_id=run_id,
            correlation={"approval_id": approval_id},
        )
        node["summary"] = _text(
            payload.get("reason") or payload.get("feedback") or payload.get("decision")
        ) or event.type
        node["detail"] = _bounded_detail(payload)
        if event.type in _TERMINAL_APPROVAL or event.type == "human.response.recorded":
            node["completed_at"] = timestamp
        return

    _new_event_node(
        nodes,
        event,
        node_type="session",
        status=_session_status(event.type),
        title=event.type,
        summary=_session_summary(event),
        run_id=run_id,
        detail=payload,
    )


def _new_event_node(
    nodes: dict[str, dict[str, Any]],
    event: CanonicalEvent,
    *,
    node_type: str,
    status: str,
    title: str,
    summary: str,
    run_id: str | None,
    detail: object,
    node_id: str | None = None,
    correlation: dict[str, str] | None = None,
) -> None:
    node = _upsert(
        nodes,
        node_id or f"event:{event.event_id}",
        event,
        node_type=node_type,
        status=status,
        title=title,
        run_id=run_id,
        correlation=correlation,
    )
    node["summary"] = summary
    node["detail"] = _bounded_detail(detail)
    node["completed_at"] = event.occurred_at.isoformat()


def _upsert(
    nodes: dict[str, dict[str, Any]],
    node_id: str,
    event: CanonicalEvent,
    *,
    node_type: str,
    status: str,
    title: str,
    run_id: str | None,
    correlation: dict[str, str] | None = None,
) -> dict[str, Any]:
    node = nodes.get(node_id)
    timestamp = event.occurred_at.isoformat()
    if node is None:
        node = {
            "id": node_id,
            "type": node_type,
            "status": status,
            "title": title,
            "summary": "",
            "sequence": event.aggregate_version,
            "updated_version": event.aggregate_version,
            "run_id": run_id,
            "started_at": timestamp,
            "completed_at": None,
            "duration_ms": None,
            "detail": {},
            "correlation": {
                "event_ids": [event.event_id],
                **(correlation or {}),
            },
        }
        nodes[node_id] = node
        return node
    node["status"] = status
    node["title"] = title or node["title"]
    node["updated_version"] = event.aggregate_version
    event_ids = node["correlation"].setdefault("event_ids", [])
    if event.event_id not in event_ids:
        event_ids.append(event.event_id)
    if correlation:
        node["correlation"].update(correlation)
    return node


def _bounded_detail(value: object) -> object:
    redacted = redact_sensitive(value)
    encoded = json.dumps(redacted, ensure_ascii=False, sort_keys=True, default=str).encode()
    if len(encoded) <= _MAX_DETAIL_BYTES:
        return redacted
    return {
        "truncated": True,
        "size_bytes": len(encoded),
        "sha256": hashlib.sha256(encoded).hexdigest(),
        "preview": encoded[:4_096].decode("utf-8", errors="ignore"),
    }


def _preview(value: str, limit: int = _MAX_PREVIEW) -> str:
    normalized = " ".join(value.split())
    return normalized if len(normalized) <= limit else f"{normalized[: limit - 1]}…"


def _text(value: object) -> str:
    return value.strip() if isinstance(value, str) else ""


def _duration_ms(started: object, completed: object) -> int | None:
    if not isinstance(started, str) or not isinstance(completed, str):
        return None
    try:
        delta = datetime.fromisoformat(completed) - datetime.fromisoformat(started)
    except ValueError:
        return None
    return max(0, round(delta.total_seconds() * 1_000))


def _run_status(event_type: str) -> str:
    return {
        "run.requested": "queued",
        "run.scheduled": "queued",
        "run.started": "running",
        "run.retry_scheduled": "waiting",
        "run.completed": "completed",
        "run.failed": "failed",
        "run.cancelled": "cancelled",
        "run.terminated": "failed",
    }.get(event_type, "running")


def _run_summary(event: CanonicalEvent) -> str:
    payload = event.payload
    return _text(
        payload.get("result_summary")
        or payload.get("reason")
        or payload.get("error")
    ) or event.type


def _model_input_summary(payload: dict[str, Any]) -> str:
    preview = _text(redact_sensitive(payload.get("user_prompt_preview")))
    count = payload.get("message_count", 0)
    return _preview(preview) or f"Prepared {count} message(s)"


def _tool_status(event: CanonicalEvent) -> str:
    if event.type == "tool.call.requested":
        return "running"
    if event.type == "tool.call.denied":
        return "waiting" if event.payload.get("error_code") == "approval_required" else "failed"
    result = event.payload.get("result")
    if isinstance(result, dict) and (
        result.get("error_code") or result.get("status") in {"error", "failed", "denied"}
    ):
        return "failed"
    return "completed"


def _tool_source_summary(payload: dict[str, Any]) -> str:
    activity = payload.get("activity")
    if not isinstance(activity, dict):
        return "Tool call requested"
    source = _text(activity.get("source"))
    server_id = _text(activity.get("server_id"))
    return " · ".join(item for item in (source, server_id) if item) or "Tool call requested"


def _tool_result_summary(payload: dict[str, Any]) -> str:
    if payload.get("error_code"):
        return _text(payload.get("error_code"))
    result = payload.get("result")
    if isinstance(result, dict):
        return _text(result.get("summary") or result.get("error_code") or result.get("status"))
    return "Tool call completed"


def _skill_status(event_type: str) -> str:
    return {
        "skill.activated": "running",
        "skill.completed": "completed",
        "skill.failed": "failed",
        "skill.cancelled": "cancelled",
    }.get(event_type, "running")


def _skill_summary(payload: dict[str, Any], event_type: str) -> str:
    version = _text(payload.get("skill_version"))
    summary = _text(payload.get("output_summary") or payload.get("error"))
    return _preview(summary) or (f"{event_type} · {version}" if version else event_type)


def _approval_status(event_type: str) -> str:
    return {
        "approval.requested": "waiting",
        "approval.approved": "completed",
        "approval.rejected": "failed",
        "approval.expired": "failed",
        "approval.cancelled": "cancelled",
        "human.response.recorded": "completed",
    }.get(event_type, "waiting")


def _session_status(event_type: str) -> str:
    return {
        "session.paused": "waiting",
        "session.resumed": "running",
        "session.closed": "completed",
        "runtime.failed": "failed",
        "runtime.reprovisioned": "running",
    }.get(event_type, "completed")


def _session_summary(event: CanonicalEvent) -> str:
    return _text(event.payload.get("reason") or event.payload.get("error")) or event.type
