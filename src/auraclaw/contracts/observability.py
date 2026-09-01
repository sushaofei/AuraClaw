from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from typing import Any


class AlertSeverity(StrEnum):
    WARNING = "warning"
    CRITICAL = "critical"


@dataclass(frozen=True)
class TraceContext:
    trace_id: str
    span_id: str
    tenant_id: str
    root_session_id: str | None = None
    session_id: str | None = None
    run_id: str | None = None
    event_id: str | None = None
    command_id: str | None = None
    tool_invocation_id: str | None = None
    runtime_id: str | None = None
    delivery_id: str | None = None
    approval_id: str | None = None

    def correlation_fields(self) -> dict[str, str]:
        return {
            key: value
            for key, value in self.__dict__.items()
            if value is not None
        }


@dataclass(frozen=True)
class TraceSpan:
    context: TraceContext
    component: str
    operation: str
    started_at: datetime
    ended_at: datetime
    status: str
    attributes: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class MetricPoint:
    name: str
    value: float
    observed_at: datetime
    tenant_id: str | None = None
    root_session_id: str | None = None
    session_id: str | None = None
    run_id: str | None = None
    labels: dict[str, str] = field(default_factory=dict)
    deduplication_key: str | None = None


@dataclass(frozen=True)
class MetricSummary:
    name: str
    count: int
    total: float
    average: float
    minimum: float
    maximum: float
    p50: float
    p95: float
    p99: float


@dataclass(frozen=True)
class AuditEvent:
    audit_id: str
    occurred_at: datetime
    action: str
    outcome: str
    actor_type: str
    actor_id: str
    tenant_id: str
    trace_id: str
    root_session_id: str | None = None
    session_id: str | None = None
    run_id: str | None = None
    event_id: str | None = None
    command_id: str | None = None
    tool_invocation_id: str | None = None
    delivery_id: str | None = None
    approval_id: str | None = None
    resource_ref: str | None = None
    payload_ref: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class Alert:
    alert_id: str
    rule: str
    severity: AlertSeverity
    status: str
    summary: str
    fired_at: datetime
    tenant_id: str | None = None
    root_session_id: str | None = None
    session_id: str | None = None
    run_id: str | None = None
    labels: dict[str, str] = field(default_factory=dict)
