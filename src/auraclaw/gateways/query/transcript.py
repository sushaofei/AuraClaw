"""Build a lightweight chat transcript from filtered Canonical Events."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from auraclaw.contracts.events import CanonicalEvent

TRANSCRIPT_MESSAGE_TYPES = frozenset(
    {
        "session.created",
        "user.message.appended",
        "model.output.completed",
    }
)

APPROVAL_TERMINAL_TYPES = frozenset(
    {
        "approval.approved",
        "approval.rejected",
        "approval.expired",
        "approval.cancelled",
        "human.response.recorded",
    }
)

TRANSCRIPT_EVENT_TYPES = frozenset(
    TRANSCRIPT_MESSAGE_TYPES | {"approval.requested"} | APPROVAL_TERMINAL_TYPES
)


def build_transcript(events: Sequence[CanonicalEvent]) -> dict[str, Any]:
    messages: list[dict[str, Any]] = []
    pending: dict[str, dict[str, Any]] = {}

    for event in events:
        if event.type == "session.created":
            content = str(event.payload.get("goal", "")).strip()
            if content:
                messages.append(_message("user", content, event))
        elif event.type == "user.message.appended":
            content = str(event.payload.get("message", "")).strip()
            if content:
                messages.append(_message("user", content, event))
        elif event.type == "model.output.completed":
            content = str(
                event.payload.get("output") or event.payload.get("completed_output") or ""
            ).strip()
            if content:
                messages.append(_message("assistant", content, event, run_id=event.run_id))
        elif event.type == "approval.requested":
            approval_id = str(event.payload.get("approval_id", "")).strip()
            if not approval_id:
                continue
            pending[approval_id] = {
                "approval_id": approval_id,
                "tool_name": str(event.payload.get("tool_name", "")),
                "reason": str(event.payload.get("reason", "")),
                "risk": str(event.payload.get("risk", "")),
                "redacted_arguments": dict(event.payload.get("redacted_arguments") or {}),
                "expected_effect": str(event.payload.get("expected_effect", "")),
                "status": str(event.payload.get("status", "waiting")),
            }
        elif event.type in APPROVAL_TERMINAL_TYPES:
            approval_id = str(event.payload.get("approval_id", "")).strip()
            if approval_id:
                pending.pop(approval_id, None)

    pending_approval = next(reversed(pending.values()), None) if pending else None
    return {
        "messages": messages,
        "pending_approval": pending_approval,
    }


def _message(
    role: str,
    content: str,
    event: CanonicalEvent,
    *,
    run_id: str | None = None,
) -> dict[str, Any]:
    item: dict[str, Any] = {
        "role": role,
        "content": content,
        "event_id": event.event_id,
        "occurred_at": event.occurred_at.isoformat(),
    }
    if run_id:
        item["run_id"] = run_id
    return item
