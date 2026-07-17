from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from auraclaw.contracts.state import Visibility


def utc_now() -> datetime:
    return datetime.now(UTC)


@dataclass(frozen=True)
class Actor:
    type: str
    id: str


@dataclass(frozen=True)
class NewEvent:
    type: str
    payload: dict[str, Any] = field(default_factory=dict)
    visibility: Visibility = Visibility.INTERNAL


@dataclass(frozen=True)
class CanonicalEvent:
    event_id: str
    tenant_id: str
    root_session_id: str
    session_id: str
    run_id: str | None
    aggregate_version: int
    type: str
    occurred_at: datetime
    actor: Actor
    correlation_id: str
    causation_id: str
    visibility: Visibility
    schema_version: int
    payload: dict[str, Any]

    def as_dict(self) -> dict[str, Any]:
        return {
            "event_id": self.event_id,
            "tenant_id": self.tenant_id,
            "root_session_id": self.root_session_id,
            "session_id": self.session_id,
            "run_id": self.run_id,
            "aggregate_version": self.aggregate_version,
            "type": self.type,
            "occurred_at": self.occurred_at.isoformat(),
            "actor": {"type": self.actor.type, "id": self.actor.id},
            "correlation_id": self.correlation_id,
            "causation_id": self.causation_id,
            "visibility": self.visibility.value,
            "schema_version": self.schema_version,
            "payload": self.payload,
        }
