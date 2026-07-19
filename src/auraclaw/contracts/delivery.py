from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from typing import Any


class DeliveryStatus(StrEnum):
    PENDING = "pending"
    ATTEMPTING = "attempting"
    RETRY_WAIT = "retry_wait"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    DEAD_LETTERED = "dead_lettered"


@dataclass(frozen=True)
class ResultSinkConfig:
    sink_id: str
    tenant_id: str
    session_id: str
    sink_type: str
    target_ref: str
    event_types: tuple[str, ...] = ("run.completed", "run.failed")
    credential_ref: str | None = None
    enabled: bool = True


@dataclass(frozen=True)
class DeliveryJob:
    delivery_id: str
    event_id: str
    tenant_id: str
    root_session_id: str
    session_id: str
    run_id: str | None
    sink_id: str
    sink_type: str
    sink_target_ref: str
    payload: dict[str, Any]
    status: DeliveryStatus
    attempt_count: int
    next_attempt_at: datetime | None
    last_response_summary: str | None
    created_at: datetime
    completed_at: datetime | None = None


@dataclass(frozen=True)
class DeliveryAttempt:
    delivery_id: str
    attempt_number: int
    started_at: datetime
    completed_at: datetime
    outcome: str
    response_summary: str
    retryable: bool


@dataclass(frozen=True)
class SinkResponse:
    succeeded: bool
    retryable: bool = False
    summary: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)
