from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import Any

from auraclaw.contracts.errors import InvalidTransitionError
from auraclaw.contracts.events import CanonicalEvent, NewEvent
from auraclaw.contracts.state import TERMINAL_SESSION_STATUSES, SessionStatus, Visibility


@dataclass
class SessionAggregate:
    session_id: str
    root_session_id: str
    tenant_id: str
    version: int = 0
    status: SessionStatus | None = None
    goal: str = ""
    run_id: str | None = None
    parent_session_id: str | None = None
    role: str = "root"
    result_summary: str | None = None
    _pending: list[NewEvent] = field(default_factory=list, repr=False)

    @classmethod
    def empty(cls, session_id: str, tenant_id: str) -> SessionAggregate:
        return cls(session_id=session_id, root_session_id=session_id, tenant_id=tenant_id)

    @classmethod
    def from_events(cls, events: Iterable[CanonicalEvent]) -> SessionAggregate:
        event_list = list(events)
        if not event_list:
            raise ValueError("cannot restore a Session without events")
        first = event_list[0]
        aggregate = cls.empty(first.session_id, first.tenant_id)
        for event in event_list:
            aggregate.apply(event.type, event.payload)
            aggregate.version = event.aggregate_version
        return aggregate

    def create(self, *, goal: str, run_id: str) -> None:
        if self.version or self.status is not None:
            raise InvalidTransitionError("Session already exists")
        self._raise(
            NewEvent(
                type="session.created",
                visibility=Visibility.USER,
                payload={
                    "goal": goal,
                    "role": "root",
                    "root_session_id": self.session_id,
                    "parent_session_id": None,
                },
            )
        )
        self._raise(
            NewEvent(
                type="run.requested",
                visibility=Visibility.USER,
                payload={"run_id": run_id},
            )
        )

    def cancel(self, reason: str) -> None:
        self._require_existing()
        if self.status in TERMINAL_SESSION_STATUSES:
            raise InvalidTransitionError(f"cannot cancel Session in {self.status.value}")
        self._raise(
            NewEvent(
                type="run.cancelled",
                visibility=Visibility.USER,
                payload={"run_id": self.run_id, "reason": reason},
            )
        )

    def resume(self, run_id: str) -> None:
        self._require_existing()
        status = self.status
        assert status is not None
        allowed = {
            SessionStatus.PAUSED,
            SessionStatus.RETRY_WAIT,
            SessionStatus.WAITING_FOR_HUMAN,
        }
        if status not in allowed:
            raise InvalidTransitionError(f"cannot resume Session in {status.value}")
        self._raise(
            NewEvent(
                type="session.resumed",
                visibility=Visibility.USER,
                payload={"run_id": run_id},
            )
        )

    def release_pending_events(self) -> list[NewEvent]:
        events = list(self._pending)
        self._pending.clear()
        return events

    def apply(self, event_type: str, payload: dict[str, Any]) -> None:
        if event_type == "session.created":
            self.goal = str(payload["goal"])
            self.root_session_id = str(payload["root_session_id"])
            self.parent_session_id = payload.get("parent_session_id")
            self.role = str(payload.get("role", "root"))
            self.status = SessionStatus.CREATED
        elif event_type == "run.requested":
            self.run_id = str(payload["run_id"])
            self.status = SessionStatus.PENDING
        elif event_type == "run.scheduled":
            self.status = SessionStatus.RUNNABLE
        elif event_type == "run.started":
            self.status = SessionStatus.RUNNING
        elif event_type == "session.paused":
            self.status = SessionStatus.PAUSED
        elif event_type == "approval.requested":
            self.status = SessionStatus.WAITING_FOR_HUMAN
        elif event_type == "run.retry_scheduled":
            self.status = SessionStatus.RETRY_WAIT
        elif event_type == "session.resumed":
            self.run_id = str(payload["run_id"])
            self.status = SessionStatus.PENDING
        elif event_type == "run.completed":
            self.result_summary = payload.get("result_summary")
            self.status = SessionStatus.COMPLETED
        elif event_type == "run.failed":
            self.status = SessionStatus.FAILED
        elif event_type == "run.cancelled":
            self.status = SessionStatus.CANCELLED

    def _raise(self, event: NewEvent) -> None:
        self.apply(event.type, event.payload)
        self._pending.append(event)

    def _require_existing(self) -> None:
        if self.status is None:
            raise InvalidTransitionError("Session does not exist")
