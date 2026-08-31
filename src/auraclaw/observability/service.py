from __future__ import annotations

import hashlib
from collections.abc import Mapping, Sequence
from dataclasses import asdict
from datetime import UTC, datetime
from typing import Any, Protocol
from uuid import uuid4

from auraclaw.contracts.events import CanonicalEvent
from auraclaw.contracts.observability import (
    Alert,
    AlertSeverity,
    AuditEvent,
    MetricPoint,
    TraceContext,
    TraceSpan,
)
from auraclaw.observability.redaction import redact_sensitive


class ObservabilityStore(Protocol):
    async def write_span(self, span: TraceSpan) -> None: ...

    async def write_metric(self, metric: MetricPoint) -> None: ...

    async def write_audit(self, event: AuditEvent) -> None: ...

    async def write_alert(self, alert: Alert) -> None: ...

    async def session_records(self, tenant_id: str, session_id: str) -> dict[str, list[Any]]: ...

    async def metric_snapshot(self) -> list[MetricPoint]: ...


class EventReader(Protocol):
    async def load(
        self,
        tenant_id: str,
        session_id: str,
        *,
        from_version: int = 1,
        event_types: Sequence[str] | None = None,
        limit: int | None = None,
    ) -> list[CanonicalEvent]: ...


class ObservabilityService:
    ALERT_RULES: Mapping[str, tuple[float, AlertSeverity, str]] = {
        "projection.lag.seconds": (
            2.0,
            AlertSeverity.WARNING,
            "Projection freshness exceeds SLO",
        ),
        "runtime.lease_lost.count": (
            0.0,
            AlertSeverity.CRITICAL,
            "Runtime lease ownership was lost",
        ),
        "tool.side_effect_unknown.count": (
            0.0,
            AlertSeverity.CRITICAL,
            "Tool side effect requires reconciliation",
        ),
        "delivery.dlq.count": (
            0.0,
            AlertSeverity.CRITICAL,
            "Delivery entered the dead-letter queue",
        ),
    }

    def __init__(self, store: ObservabilityStore, events: EventReader) -> None:
        self._store = store
        self._events = events

    async def record_span(
        self,
        *,
        context: TraceContext,
        component: str,
        operation: str,
        started_at: datetime,
        status: str,
        attributes: dict[str, Any] | None = None,
    ) -> TraceSpan:
        span = TraceSpan(
            context=context,
            component=component,
            operation=operation,
            started_at=started_at,
            ended_at=datetime.now(UTC),
            status=status,
            attributes=redact_sensitive(attributes or {}),
        )
        await self._store.write_span(span)
        return span

    async def audit(
        self,
        *,
        context: TraceContext,
        action: str,
        outcome: str,
        actor_type: str,
        actor_id: str,
        resource_ref: str | None = None,
        payload_ref: str | None = None,
        metadata: dict[str, Any] | None = None,
        audit_id: str | None = None,
    ) -> AuditEvent:
        event = AuditEvent(
            audit_id=audit_id or f"aud_{uuid4().hex}",
            occurred_at=datetime.now(UTC),
            action=action,
            outcome=outcome,
            actor_type=actor_type,
            actor_id=actor_id,
            tenant_id=context.tenant_id,
            trace_id=context.trace_id,
            root_session_id=context.root_session_id,
            session_id=context.session_id,
            run_id=context.run_id,
            event_id=context.event_id,
            command_id=context.command_id,
            tool_invocation_id=context.tool_invocation_id,
            delivery_id=context.delivery_id,
            approval_id=context.approval_id,
            resource_ref=resource_ref,
            payload_ref=payload_ref,
            metadata=redact_sensitive(metadata or {}),
        )
        await self._store.write_audit(event)
        return event

    async def metric(
        self,
        name: str,
        value: float,
        *,
        context: TraceContext | None = None,
        labels: dict[str, str] | None = None,
        deduplication_key: str | None = None,
    ) -> MetricPoint:
        point = MetricPoint(
            name=name,
            value=value,
            observed_at=datetime.now(UTC),
            tenant_id=context.tenant_id if context else None,
            root_session_id=context.root_session_id if context else None,
            session_id=context.session_id if context else None,
            run_id=context.run_id if context else None,
            labels=labels or {},
            deduplication_key=deduplication_key,
        )
        await self._store.write_metric(point)
        rule = self.ALERT_RULES.get(name)
        if rule is not None and value > rule[0]:
            threshold, severity, summary = rule
            identity = ":".join(
                (name, point.tenant_id or "global", point.session_id or "global")
            )
            digest = hashlib.sha256(identity.encode()).hexdigest()[:24]
            await self._store.write_alert(
                Alert(
                    alert_id=f"alt_{digest}",
                    rule=name,
                    severity=severity,
                    status="firing",
                    summary=summary,
                    fired_at=point.observed_at,
                    tenant_id=point.tenant_id,
                    root_session_id=point.root_session_id,
                    session_id=point.session_id,
                    run_id=point.run_id,
                    labels={"threshold": str(threshold), **point.labels},
                )
            )
        return point

    async def timeline(self, tenant_id: str, session_id: str) -> dict[str, Any]:
        canonical = await self._events.load(tenant_id, session_id)
        records = await self._store.session_records(tenant_id, session_id)
        entries: list[dict[str, Any]] = []
        for event in canonical:
            entries.append(
                {
                    "kind": "canonical_event",
                    "timestamp": event.occurred_at,
                    "type": event.type,
                    "status": "committed",
                    "correlation": {
                        "event_id": event.event_id,
                        "root_session_id": event.root_session_id,
                        "session_id": event.session_id,
                        "run_id": event.run_id,
                        "command_id": event.causation_id,
                        "correlation_id": event.correlation_id,
                    },
                    "detail": redact_sensitive(event.payload),
                }
            )
        for span in records["spans"]:
            item = self._as_mapping(span)
            context = self._as_mapping(item.get("context", {}))
            entries.append(
                {
                    "kind": "trace_span",
                    "timestamp": item.get("started_at"),
                    "type": f"{item.get('component')}.{item.get('operation')}",
                    "status": item.get("status"),
                    "correlation": context,
                    "detail": redact_sensitive(item.get("attributes", {})),
                }
            )
        for audit in records["audits"]:
            item = self._as_mapping(audit)
            entries.append(
                {
                    "kind": "audit_event",
                    "timestamp": item.get("occurred_at"),
                    "type": item.get("action"),
                    "status": item.get("outcome"),
                    "correlation": self._correlation(item),
                    "detail": redact_sensitive(item.get("metadata", {})),
                }
            )
        for alert in records["alerts"]:
            item = self._as_mapping(alert)
            entries.append(
                {
                    "kind": "alert",
                    "timestamp": item.get("fired_at"),
                    "type": item.get("rule"),
                    "status": item.get("status"),
                    "correlation": self._correlation(item),
                    "detail": {"severity": item.get("severity"), "summary": item.get("summary")},
                }
            )
        entries.sort(key=lambda item: str(item["timestamp"]))
        return {
            "tenant_id": tenant_id,
            "session_id": session_id,
            "entries": [self._json_safe(item) for item in entries],
        }

    async def metrics(self) -> list[MetricPoint]:
        return await self._store.metric_snapshot()

    @staticmethod
    def _as_mapping(value: Any) -> dict[str, Any]:
        if isinstance(value, dict):
            return dict(value)
        try:
            return asdict(value)
        except TypeError:
            return {}

    @staticmethod
    def _correlation(item: Mapping[str, Any]) -> dict[str, Any]:
        keys: Sequence[str] = (
            "trace_id", "root_session_id", "session_id", "run_id", "event_id",
            "command_id", "tool_invocation_id", "delivery_id", "approval_id",
        )
        return {key: item[key] for key in keys if item.get(key) is not None}

    @classmethod
    def _json_safe(cls, value: Any) -> Any:
        if isinstance(value, datetime):
            return value.isoformat()
        if isinstance(value, dict):
            return {str(key): cls._json_safe(item) for key, item in value.items()}
        if isinstance(value, list):
            return [cls._json_safe(item) for item in value]
        return value


class ObservabilityProjector:
    """Best-effort derived telemetry from durable Canonical Events."""

    AUDITED_EVENTS = {
        "approval.requested",
        "approval.approved",
        "approval.rejected",
        "human.response.recorded",
        "child.delegated",
        "session.handed_off",
        "run.cancelled",
        "tool.call.requested",
        "tool.call.completed",
        "delivery.retrying",
        "delivery.succeeded",
        "delivery.failed",
        "delivery.dead_lettered",
        "delivery.reconciling",
    }

    def __init__(self, service: ObservabilityService) -> None:
        self._service = service

    async def project(self, events: Sequence[CanonicalEvent]) -> None:
        for event in events:
            try:
                await self._project_event(event)
            except Exception:
                # Telemetry must not turn a successfully committed fact into a failed command.
                continue

    async def _project_event(self, event: CanonicalEvent) -> None:
        trace_id = hashlib.sha256(event.correlation_id.encode()).hexdigest()[:32]
        span_id = hashlib.sha256(event.event_id.encode()).hexdigest()[:16]
        context = TraceContext(
            trace_id=trace_id,
            span_id=span_id,
            tenant_id=event.tenant_id,
            root_session_id=event.root_session_id,
            session_id=event.session_id,
            run_id=event.run_id,
            event_id=event.event_id,
            command_id=event.causation_id,
            tool_invocation_id=self._string(event.payload.get("tool_invocation_id")),
            runtime_id=self._runtime_id(event),
            delivery_id=self._string(event.payload.get("delivery_id")),
            approval_id=self._string(event.payload.get("approval_id")),
        )
        await self._service.record_span(
            context=context,
            component=self._component(event.type),
            operation=event.type,
            started_at=event.occurred_at,
            status="committed",
            attributes={"aggregate_version": event.aggregate_version},
        )
        lag = max((datetime.now(UTC) - event.occurred_at).total_seconds(), 0.0)
        await self._service.metric(
            "projection.lag.seconds",
            lag,
            context=context,
            deduplication_key=f"{event.event_id}:projection.lag.seconds",
        )
        if event.type in self.AUDITED_EVENTS:
            digest = hashlib.sha256(f"{event.event_id}:{event.type}".encode()).hexdigest()[:32]
            await self._service.audit(
                context=context,
                action=event.type,
                outcome=self._outcome(event),
                actor_type=event.actor.type,
                actor_id=event.actor.id,
                metadata=event.payload,
                audit_id=f"aud_{digest}",
            )
        if event.type == "runtime.reprovisioned":
            await self._service.metric(
                "runtime.lease_lost.count",
                1,
                context=context,
                deduplication_key=f"{event.event_id}:runtime.lease_lost.count",
            )
        if event.type == "delivery.dead_lettered":
            await self._service.metric(
                "delivery.dlq.count",
                1,
                context=context,
                deduplication_key=f"{event.event_id}:delivery.dlq.count",
            )
        result = event.payload.get("result")
        if (
            event.type == "tool.call.completed"
            and isinstance(result, dict)
            and result.get("side_effect_status") == "unknown"
        ):
            await self._service.metric(
                "tool.side_effect_unknown.count",
                1,
                context=context,
                deduplication_key=f"{event.event_id}:tool.side_effect_unknown.count",
            )

    @staticmethod
    def _component(event_type: str) -> str:
        prefix = event_type.split(".", 1)[0]
        return {
            "approval": "policy_approval",
            "delivery": "result_delivery",
            "tool": "tool_gateway",
            "runtime": "agent_runtime",
            "child": "collaboration",
            "session": "session_service",
            "run": "session_service",
        }.get(prefix, "projection")

    @staticmethod
    def _outcome(event: CanonicalEvent) -> str:
        return str(
            event.payload.get("status")
            or event.payload.get("decision")
            or event.payload.get("side_effect_status")
            or "recorded"
        )

    @staticmethod
    def _string(value: Any) -> str | None:
        return str(value) if value is not None else None

    @staticmethod
    def _runtime_id(event: CanonicalEvent) -> str | None:
        if event.actor.type == "runtime":
            return event.actor.id
        value = event.payload.get("runtime_id")
        return str(value) if value is not None else None
