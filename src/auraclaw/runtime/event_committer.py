from __future__ import annotations

import hashlib
import json
from typing import Any

from auraclaw.contracts.errors import VersionConflictError
from auraclaw.contracts.events import NewEvent
from auraclaw.contracts.state import Visibility
from auraclaw.control.ports import RuntimeAssignment
from auraclaw.runtime.execution_guard import RuntimeExecutionGuard
from auraclaw.runtime.ports import SessionClient


class CanonicalEventCommitter:
    """Own append-once semantics for Runtime writes to Canonical Session Events."""

    _APPROVAL_TERMINAL_TYPES = frozenset(
        {
            "approval.approved",
            "approval.rejected",
            "approval.expired",
            "approval.cancelled",
            "human.response.recorded",
        }
    )

    def __init__(self, session: SessionClient, guard: RuntimeExecutionGuard) -> None:
        self._session = session
        self._guard = guard

    async def append_once(
        self,
        assignment: RuntimeAssignment,
        existing: list[Any],
        event_type: str,
        payload: dict[str, Any],
        *,
        identity: str,
        visibility: Visibility = Visibility.INTERNAL,
        recovery: bool = False,
    ) -> None:
        local_settlement = (
            event_type == "tool.call.requested"
            and payload.get("runtime_decision") == "not_dispatched"
        ) or (
            event_type == "tool.call.completed"
            and payload.get("result", {}).get("status") == "denied"
            and payload.get("result", {}).get("side_effect_status") == "not_started"
        )
        if (
            recovery
            and not local_settlement
            and event_type
            not in {
                "run.failed",
                "run.cancelled",
                "runtime.progress.recorded",
            }
        ):
            raise ValueError("Recovery cannot begin new execution")
        # Retry only the durable append, never the model/tool execution. Concurrent
        # policy or progress facts can advance the aggregate after load.
        for attempt in range(8):
            if event_type == "approval.requested":
                if self.approval_request_is_pending(existing, identity):
                    return
            elif any(
                event.type == event_type
                and (
                    event.payload.get("run_id") == identity
                    or event.payload.get("model_call_id") == identity
                    or event.payload.get("tool_invocation_id") == identity
                    or event.payload.get("approval_id") == identity
                    or event.payload.get("checkpoint_id") == identity
                    or event.payload.get("reservation_id") == identity
                )
                for event in existing
            ):
                return
            await (self._guard.fence(assignment) if recovery else self._guard.check(assignment))
            try:
                appended = await self._session.append(
                    assignment,
                    [NewEvent(type=event_type, payload=payload, visibility=visibility)],
                    command_id=f"runtime:{event_type}:{assignment.run_id}:{identity}",
                    operation=f"runtime.{event_type}",
                    expected_version=len(existing),
                )
            except VersionConflictError:
                if attempt == 7:
                    raise
                refreshed = await self._session.load(assignment)
                existing.clear()
                existing.extend(refreshed)
                continue
            if appended:
                existing.extend(appended)
            else:
                refreshed = await self._session.load(assignment)
                existing.clear()
                existing.extend(refreshed)
            return

    async def append_capability_event(
        self, assignment: RuntimeAssignment, event: NewEvent, *, recovery: bool = False
    ) -> None:
        if recovery and event.type not in {
            "skill.invocation.settled",
            "skill.completed",
            "skill.failed",
            "skill.cancelled",
        }:
            raise ValueError("Recovery cannot begin new execution")
        if event.type.startswith("skill.invocation."):
            identity = (
                str(event.payload["tool_invocation_id"])
                + ":"
                + str(event.payload.get("invocation_cycle", 0))
            )
        elif event.payload.get("skill_activation_id"):
            identity = str(event.payload["skill_activation_id"])
        else:
            identity = hashlib.sha256(
                json.dumps(
                    event.payload,
                    sort_keys=True,
                    separators=(",", ":"),
                    default=str,
                ).encode()
            ).hexdigest()[:24]
        if event.type == "skill.activated":
            existing = await self._session.load(assignment)
            if any(
                item.type == event.type and item.payload.get("skill_activation_id") == identity
                for item in existing
            ):
                return
        terminal_types = {"skill.completed", "skill.failed", "skill.cancelled"}
        if event.type in terminal_types:
            for _ in range(3):
                existing = await self._session.load(assignment)
                if any(
                    item.type in terminal_types
                    and item.payload.get("skill_activation_id") == identity
                    for item in existing
                ):
                    return
                await (self._guard.fence(assignment) if recovery else self._guard.check(assignment))
                try:
                    await self._session.append(
                        assignment,
                        [event],
                        command_id=f"runtime:skill.terminal:{identity}",
                        operation="runtime.skill.terminal",
                        expected_version=len(existing),
                    )
                    return
                except VersionConflictError:
                    continue
            raise VersionConflictError("Skill terminal event conflicted with concurrent events")
        await (self._guard.fence(assignment) if recovery else self._guard.check(assignment))
        await self._session.append(
            assignment,
            [event],
            command_id=f"runtime:{event.type}:{identity}",
            operation=f"runtime.{event.type}",
        )

    @classmethod
    def approval_request_is_pending(cls, existing: list[Any], approval_id: str) -> bool:
        open_request = False
        for event in existing:
            if str(event.payload.get("approval_id", "")) != approval_id:
                continue
            if event.type == "approval.requested":
                open_request = True
            elif event.type in cls._APPROVAL_TERMINAL_TYPES:
                open_request = False
        return open_request
