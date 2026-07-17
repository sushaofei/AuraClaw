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
    result_ref: str | None = None
    artifact_refs: list[str] = field(default_factory=list)
    dependency_ids: list[str] = field(default_factory=list)
    output_contract: dict[str, Any] = field(default_factory=dict)
    owner: str | None = None
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

    @classmethod
    def from_snapshot(cls, state: dict[str, Any], version: int) -> SessionAggregate:
        aggregate = cls(
            session_id=str(state["session_id"]),
            root_session_id=str(state["root_session_id"]),
            tenant_id=str(state["tenant_id"]),
            version=version,
            status=SessionStatus(str(state["status"])),
            goal=str(state["goal"]),
            run_id=state.get("run_id"),
            parent_session_id=state.get("parent_session_id"),
            role=str(state.get("role", "root")),
            result_summary=state.get("result_summary"),
            result_ref=state.get("result_ref"),
            artifact_refs=list(state.get("artifact_refs", [])),
            dependency_ids=list(state.get("dependency_ids", [])),
            output_contract=dict(state.get("output_contract", {})),
            owner=state.get("owner"),
        )
        return aggregate

    def replay(self, events: Iterable[CanonicalEvent]) -> None:
        for event in events:
            if event.aggregate_version != self.version + 1:
                raise ValueError(
                    f"snapshot replay gap: expected {self.version + 1}, "
                    f"got {event.aggregate_version}"
                )
            self.apply(event.type, event.payload)
            self.version = event.aggregate_version

    def snapshot_state(self) -> dict[str, Any]:
        status = self.status
        if status is None:
            raise InvalidTransitionError("cannot snapshot a Session that does not exist")
        return {
            "session_id": self.session_id,
            "root_session_id": self.root_session_id,
            "tenant_id": self.tenant_id,
            "status": status.value,
            "goal": self.goal,
            "run_id": self.run_id,
            "parent_session_id": self.parent_session_id,
            "role": self.role,
            "result_summary": self.result_summary,
            "result_ref": self.result_ref,
            "artifact_refs": list(self.artifact_refs),
            "dependency_ids": list(self.dependency_ids),
            "output_contract": dict(self.output_contract),
            "owner": self.owner,
        }

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

    def append_message(self, *, message: str) -> None:
        self._require_existing()
        if self.status in TERMINAL_SESSION_STATUSES:
            status = self.status
            assert status is not None
            raise InvalidTransitionError(f"cannot append message to Session in {status.value}")
        self._raise(
            NewEvent(
                type="user.message.appended",
                visibility=Visibility.USER,
                payload={"message": message},
            )
        )

    def request_run(self, run_id: str) -> None:
        self._require_existing()
        status = self.status
        assert status is not None
        if status not in {SessionStatus.CREATED, SessionStatus.PAUSED}:
            raise InvalidTransitionError(f"cannot request run for Session in {status.value}")
        self._raise(
            NewEvent(
                type="run.requested",
                visibility=Visibility.USER,
                payload={"run_id": run_id},
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

    def record_human_response(
        self,
        *,
        approval_id: str,
        actor_id: str,
        decision: str,
        feedback: str | None,
    ) -> None:
        self._require_existing()
        if self.status is not SessionStatus.WAITING_FOR_HUMAN:
            status = self.status
            assert status is not None
            raise InvalidTransitionError(
                f"cannot record approval response for Session in {status.value}"
            )
        self._raise(
            NewEvent(
                type="human.response.recorded",
                visibility=Visibility.USER,
                payload={
                    "approval_id": approval_id,
                    "actor_id": actor_id,
                    "decision": decision,
                    "feedback": feedback,
                },
            )
        )
        self._raise(
            NewEvent(
                type=f"approval.{decision}",
                visibility=Visibility.USER,
                payload={
                    "approval_id": approval_id,
                    "decision": decision,
                    "feedback": feedback,
                },
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
        elif event_type == "child.created":
            self.goal = str(payload["goal"])
            self.root_session_id = str(payload.get("root_session_id", self.root_session_id))
            self.parent_session_id = str(payload["parent_session_id"])
            self.role = str(payload["role"])
            self.dependency_ids = list(payload.get("dependency_ids", []))
            self.output_contract = dict(payload.get("output_contract", {}))
            self.status = (
                SessionStatus.PENDING if self.dependency_ids else SessionStatus.RUNNABLE
            )
        elif event_type == "run.requested":
            self.run_id = str(payload["run_id"])
            if self.role == "root":
                self.status = SessionStatus.PENDING
        elif event_type == "run.scheduled":
            self.status = SessionStatus.RUNNABLE
        elif event_type == "run.started":
            self.status = SessionStatus.RUNNING
        elif event_type == "session.paused":
            self.status = SessionStatus.PAUSED
        elif event_type == "approval.requested":
            self.status = SessionStatus.WAITING_FOR_HUMAN
        elif event_type == "approval.approved":
            self.status = SessionStatus.RUNNABLE
        elif event_type == "approval.rejected":
            self.status = SessionStatus.RUNNABLE
        elif event_type == "run.retry_scheduled":
            self.status = SessionStatus.RETRY_WAIT
        elif event_type == "session.resumed":
            self.run_id = str(payload["run_id"])
            self.status = SessionStatus.PENDING
        elif event_type == "run.completed":
            self.result_summary = payload.get("result_summary")
            self.result_ref = payload.get("result_ref")
            self.artifact_refs = list(payload.get("artifact_refs", []))
            self.status = SessionStatus.COMPLETED
        elif event_type == "dependency.changed":
            self.dependency_ids = list(payload["dependency_ids"])
            self.status = (
                SessionStatus.PENDING if self.dependency_ids else SessionStatus.RUNNABLE
            )
        elif event_type in {"child.delegated", "session.handed_off"}:
            self.owner = str(payload["owner"])
        elif event_type == "child.result_published":
            self.result_summary = payload.get("summary")
            self.result_ref = payload.get("result_ref")
            self.artifact_refs = list(payload.get("artifact_refs", []))
            self.status = SessionStatus.COMPLETED
        elif event_type == "review.completed":
            self.result_summary = payload.get("decision")
            self.result_ref = payload.get("target_result_ref")
            self.status = SessionStatus.COMPLETED
        elif event_type == "run.failed":
            self.status = SessionStatus.FAILED
        elif event_type == "run.cancelled":
            self.status = SessionStatus.CANCELLED
        elif event_type == "runtime.failed":
            self.status = SessionStatus.RETRY_WAIT
        elif event_type == "runtime.reprovisioned":
            self.status = SessionStatus.RUNNABLE
        elif event_type == "run.terminated":
            self.status = SessionStatus.FAILED

    def _raise(self, event: NewEvent) -> None:
        self.apply(event.type, event.payload)
        self._pending.append(event)

    def _require_existing(self) -> None:
        if self.status is None:
            raise InvalidTransitionError("Session does not exist")
