from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import asdict
from datetime import UTC, datetime
from typing import Any

from auraclaw.contracts.observability import Alert, AuditEvent, MetricPoint, TraceSpan
from auraclaw.infrastructure.persistence.postgres_common import (
    LazyPool as _LazyPool,
)
from auraclaw.infrastructure.persistence.postgres_common import (
    json_dumps as _json,
)
from auraclaw.observability.redaction import redact_sensitive


class StructuredLogger:
    def __init__(self, name: str = "auraclaw") -> None:
        self._logger = logging.getLogger(name)

    def emit(self, level: int, message: str, **fields: Any) -> dict[str, Any]:
        record = {
            "timestamp": datetime.now(UTC).isoformat(),
            "level": logging.getLevelName(level).lower(),
            "message": message,
            **redact_sensitive(fields),
        }
        self._logger.log(level, json.dumps(record, sort_keys=True, separators=(",", ":")))
        return record


class InMemoryObservabilityStore:
    def __init__(self) -> None:
        self._spans: dict[str, TraceSpan] = {}
        self._metrics: list[MetricPoint] = []
        self._metric_keys: set[str] = set()
        self._audits: dict[str, AuditEvent] = {}
        self._alerts: dict[str, Alert] = {}
        self._lock = asyncio.Lock()

    async def write_span(self, span: TraceSpan) -> None:
        async with self._lock:
            self._spans[span.context.span_id] = span

    async def write_metric(self, metric: MetricPoint) -> None:
        async with self._lock:
            if (
                metric.deduplication_key is not None
                and metric.deduplication_key in self._metric_keys
            ):
                return
            self._metrics.append(metric)
            if metric.deduplication_key is not None:
                self._metric_keys.add(metric.deduplication_key)

    async def write_audit(self, event: AuditEvent) -> None:
        async with self._lock:
            self._audits[event.audit_id] = event

    async def write_alert(self, alert: Alert) -> None:
        async with self._lock:
            self._alerts[alert.alert_id] = alert

    async def session_records(self, tenant_id: str, session_id: str) -> dict[str, list[Any]]:
        return {
            "spans": [
                span
                for span in self._spans.values()
                if span.context.tenant_id == tenant_id
                and (
                    span.context.session_id == session_id
                    or span.context.root_session_id == session_id
                )
            ],
            "audits": [
                event
                for event in self._audits.values()
                if event.tenant_id == tenant_id
                and (event.session_id == session_id or event.root_session_id == session_id)
            ],
            "alerts": [
                alert
                for alert in self._alerts.values()
                if (alert.tenant_id in {None, tenant_id})
                and (alert.session_id == session_id or alert.root_session_id == session_id)
            ],
        }

    async def metric_snapshot(self) -> list[MetricPoint]:
        return list(self._metrics)


class PostgresObservabilityStore(_LazyPool):
    async def write_span(self, span: TraceSpan) -> None:
        pool = await self.pool()
        fields = span.context.correlation_fields()
        await pool.execute(
            """INSERT INTO observability.trace_span
            (trace_id,span_id,tenant_id,root_session_id,session_id,run_id,event_id,command_id,
             tool_invocation_id,runtime_id,delivery_id,approval_id,component,operation,
             started_at,ended_at,status,attributes)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18::jsonb)
            ON CONFLICT (span_id) DO NOTHING""",
            fields["trace_id"], fields["span_id"], fields["tenant_id"],
            fields.get("root_session_id"), fields.get("session_id"), fields.get("run_id"),
            fields.get("event_id"), fields.get("command_id"), fields.get("tool_invocation_id"),
            fields.get("runtime_id"), fields.get("delivery_id"), fields.get("approval_id"),
            span.component, span.operation, span.started_at, span.ended_at, span.status,
            _json(redact_sensitive(span.attributes)),
        )

    async def write_metric(self, metric: MetricPoint) -> None:
        pool = await self.pool()
        await pool.execute(
            """INSERT INTO observability.metric_point
            (metric_name,value,observed_at,tenant_id,root_session_id,session_id,run_id,labels,
             deduplication_key)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8::jsonb,$9)
            ON CONFLICT (deduplication_key) DO NOTHING""",
            metric.name, metric.value, metric.observed_at, metric.tenant_id,
            metric.root_session_id, metric.session_id, metric.run_id, _json(metric.labels),
            metric.deduplication_key,
        )

    async def write_audit(self, event: AuditEvent) -> None:
        pool = await self.pool()
        await pool.execute(
            """INSERT INTO observability.audit_event
            (audit_id,occurred_at,action,outcome,actor_type,actor_id,tenant_id,trace_id,
             root_session_id,session_id,run_id,event_id,command_id,tool_invocation_id,
             delivery_id,approval_id,resource_ref,payload_ref,metadata)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19::jsonb)
            ON CONFLICT (audit_id) DO NOTHING""",
            event.audit_id, event.occurred_at, event.action, event.outcome, event.actor_type,
            event.actor_id, event.tenant_id, event.trace_id, event.root_session_id,
            event.session_id, event.run_id, event.event_id, event.command_id,
            event.tool_invocation_id, event.delivery_id, event.approval_id,
            event.resource_ref, event.payload_ref, _json(redact_sensitive(event.metadata)),
        )

    async def write_alert(self, alert: Alert) -> None:
        pool = await self.pool()
        await pool.execute(
            """INSERT INTO observability.alert
            (alert_id,rule,severity,status,summary,fired_at,tenant_id,root_session_id,
             session_id,run_id,labels)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11::jsonb)
            ON CONFLICT (alert_id) DO UPDATE SET status=EXCLUDED.status,
            summary=EXCLUDED.summary, labels=EXCLUDED.labels""",
            alert.alert_id, alert.rule, alert.severity.value, alert.status, alert.summary,
            alert.fired_at, alert.tenant_id, alert.root_session_id, alert.session_id,
            alert.run_id, _json(alert.labels),
        )

    async def session_records(self, tenant_id: str, session_id: str) -> dict[str, list[Any]]:
        pool = await self.pool()
        spans = await pool.fetch(
            """SELECT * FROM observability.trace_span WHERE tenant_id=$1
            AND (session_id=$2 OR root_session_id=$2) ORDER BY started_at""",
            tenant_id, session_id,
        )
        audits = await pool.fetch(
            """SELECT * FROM observability.audit_event WHERE tenant_id=$1
            AND (session_id=$2 OR root_session_id=$2) ORDER BY occurred_at""",
            tenant_id, session_id,
        )
        alerts = await pool.fetch(
            """SELECT * FROM observability.alert WHERE (tenant_id IS NULL OR tenant_id=$1)
            AND (session_id=$2 OR root_session_id=$2) ORDER BY fired_at""",
            tenant_id, session_id,
        )
        return {
            "spans": [dict(row) for row in spans],
            "audits": [dict(row) for row in audits],
            "alerts": [dict(row) for row in alerts],
        }

    async def metric_snapshot(self) -> list[MetricPoint]:
        pool = await self.pool()
        rows = await pool.fetch(
            """SELECT DISTINCT ON (
                 metric_name, coalesce(tenant_id,''), coalesce(session_id,'')
               )
            * FROM observability.metric_point
            ORDER BY metric_name, coalesce(tenant_id,''), coalesce(session_id,''),
                     observed_at DESC"""
        )
        return [
            MetricPoint(
                name=str(row["metric_name"]), value=float(row["value"]),
                observed_at=row["observed_at"], tenant_id=row["tenant_id"],
                root_session_id=row["root_session_id"], session_id=row["session_id"],
                run_id=row["run_id"], labels=dict(row["labels"]),
                deduplication_key=row["deduplication_key"],
            )
            for row in rows
        ]

    @staticmethod
    def serialize(value: Any) -> dict[str, Any]:
        return asdict(value)
