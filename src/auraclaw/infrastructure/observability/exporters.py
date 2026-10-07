from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime
from typing import Any

import httpx

from auraclaw.contracts.observability import (
    Alert,
    AuditEvent,
    MetricPoint,
    MetricSummary,
    TraceSpan,
)
from auraclaw.observability.service import ObservabilityStore, TelemetryExporter

logger = logging.getLogger(__name__)


def _unix_nanos(value: datetime) -> str:
    return str(int(value.timestamp() * 1_000_000_000))


def _otlp_value(value: Any) -> dict[str, Any]:
    if isinstance(value, bool):
        return {"boolValue": value}
    if isinstance(value, int):
        return {"intValue": str(value)}
    if isinstance(value, float):
        return {"doubleValue": value}
    if isinstance(value, str):
        return {"stringValue": value}
    return {
        "stringValue": json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)
    }


def _attributes(values: dict[str, Any]) -> list[dict[str, Any]]:
    return [
        {"key": key, "value": _otlp_value(value)}
        for key, value in sorted(values.items())
        if value is not None
    ]


class HttpTelemetryExporter:
    """Export OTLP/HTTP JSON telemetry and Alertmanager v2 alerts.

    The canonical PostgreSQL observability store remains the durable source. This
    adapter performs bounded delivery after persistence and raises on exhaustion;
    ``ObservabilityService`` deliberately contains that failure so telemetry
    outages cannot change Session business state.
    """

    def __init__(
        self,
        *,
        otlp_endpoint: str,
        alert_receiver_url: str,
        service_name: str,
        timeout_seconds: float = 2.0,
        retry_attempts: int = 2,
        queue_capacity: int = 2048,
        otlp_bearer_token: str | None = None,
        alert_bearer_token: str | None = None,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        self._otlp_endpoint = otlp_endpoint.rstrip("/")
        self._alert_receiver_url = alert_receiver_url
        self._service_name = service_name
        self._timeout_seconds = timeout_seconds
        self._retry_attempts = retry_attempts
        self._otlp_headers = self._headers(otlp_bearer_token)
        self._alert_headers = self._headers(alert_bearer_token)
        self._client = client or httpx.AsyncClient(trust_env=False)
        self._owns_client = client is None
        self._queue: asyncio.Queue[tuple[str, Any, dict[str, str]]] = asyncio.Queue(
            maxsize=queue_capacity
        )
        self._worker: asyncio.Task[None] | None = None
        self.last_error: str | None = None

    @staticmethod
    def _headers(token: str | None) -> dict[str, str]:
        headers = {"Content-Type": "application/json"}
        if token:
            headers["Authorization"] = f"Bearer {token}"
        return headers

    async def export_span(self, span: TraceSpan) -> None:
        context = span.context.correlation_fields()
        attributes = {
            **span.attributes,
            **{key: value for key, value in context.items() if key not in {"trace_id", "span_id"}},
        }
        payload = {
            "resourceSpans": [
                {
                    "resource": {
                        "attributes": _attributes({"service.name": self._service_name})
                    },
                    "scopeSpans": [
                        {
                            "scope": {"name": "auraclaw"},
                            "spans": [
                                {
                                    "traceId": span.context.trace_id,
                                    "spanId": span.context.span_id,
                                    "name": f"{span.component}.{span.operation}",
                                    "startTimeUnixNano": _unix_nanos(span.started_at),
                                    "endTimeUnixNano": _unix_nanos(span.ended_at),
                                    "attributes": _attributes(attributes),
                                    "status": {
                                        "code": "STATUS_CODE_OK"
                                        if span.status == "ok"
                                        else "STATUS_CODE_ERROR"
                                    },
                                }
                            ],
                        }
                    ],
                }
            ]
        }
        await self._enqueue(
            f"{self._otlp_endpoint}/v1/traces", payload, self._otlp_headers
        )

    async def export_metric(self, metric: MetricPoint) -> None:
        attributes = {
            **metric.labels,
            "tenant_id": metric.tenant_id,
            "root_session_id": metric.root_session_id,
            "session_id": metric.session_id,
            "run_id": metric.run_id,
        }
        payload = {
            "resourceMetrics": [
                {
                    "resource": {
                        "attributes": _attributes({"service.name": self._service_name})
                    },
                    "scopeMetrics": [
                        {
                            "scope": {"name": "auraclaw"},
                            "metrics": [
                                {
                                    "name": metric.name,
                                    "gauge": {
                                        "dataPoints": [
                                            {
                                                "timeUnixNano": _unix_nanos(metric.observed_at),
                                                "asDouble": metric.value,
                                                "attributes": _attributes(attributes),
                                            }
                                        ]
                                    },
                                }
                            ],
                        }
                    ],
                }
            ]
        }
        await self._enqueue(
            f"{self._otlp_endpoint}/v1/metrics", payload, self._otlp_headers
        )

    async def deliver_alert(self, alert: Alert) -> None:
        labels = {
            "alertname": alert.rule,
            "severity": alert.severity.value,
            "service": self._service_name,
            **alert.labels,
        }
        for key, value in {
            "tenant_id": alert.tenant_id,
            "root_session_id": alert.root_session_id,
            "session_id": alert.session_id,
            "run_id": alert.run_id,
        }.items():
            if value is not None:
                labels[key] = value
        payload = [
            {
                "labels": labels,
                "annotations": {"summary": alert.summary, "alert_id": alert.alert_id},
                "startsAt": alert.fired_at.isoformat(),
            }
        ]
        await self._enqueue(self._alert_receiver_url, payload, self._alert_headers)

    async def _enqueue(
        self, url: str, payload: Any, headers: dict[str, str]
    ) -> None:
        if self._worker is None:
            self._worker = asyncio.create_task(
                self._run(), name=f"{self._service_name}-telemetry-export"
            )
        self._queue.put_nowait((url, payload, headers))

    async def _run(self) -> None:
        while True:
            url, payload, headers = await self._queue.get()
            try:
                await self._post(url, payload, headers)
            except Exception as exc:
                logger.error(
                    "observability_export_failed",
                    extra={
                        "structured_fields": {
                            "service": self._service_name,
                            "error_type": type(exc).__name__,
                        }
                    },
                )
            finally:
                self._queue.task_done()

    async def _post(
        self, url: str, payload: Any, headers: dict[str, str]
    ) -> None:
        error: Exception | None = None
        for attempt in range(self._retry_attempts):
            try:
                response = await self._client.post(
                    url,
                    json=payload,
                    headers=headers,
                    timeout=self._timeout_seconds,
                )
                response.raise_for_status()
                self.last_error = None
                return
            except (httpx.HTTPError, TimeoutError) as exc:
                error = exc
                if attempt + 1 < self._retry_attempts:
                    await asyncio.sleep(min(0.1 * (2**attempt), 1.0))
        assert error is not None
        self.last_error = type(error).__name__
        raise error

    async def aclose(self) -> None:
        if self._worker is not None:
            drain_timeout = min(
                self._timeout_seconds * self._retry_attempts * (self._queue.qsize() + 1),
                30.0,
            )
            try:
                await asyncio.wait_for(self._queue.join(), timeout=drain_timeout)
            except TimeoutError:
                logger.error(
                    "observability_export_shutdown_timeout",
                    extra={
                        "structured_fields": {
                            "service": self._service_name,
                            "pending": self._queue.qsize(),
                        }
                    },
                )
            self._worker.cancel()
            try:
                await self._worker
            except asyncio.CancelledError:
                pass
        if self._owns_client:
            await self._client.aclose()


class ExportingObservabilityStore:
    """Persist first, then export without letting exporter failure affect business state."""

    def __init__(self, store: ObservabilityStore, exporter: TelemetryExporter) -> None:
        self._store = store
        self._exporter = exporter

    async def write_span(self, span: TraceSpan) -> None:
        await self._store.write_span(span)
        await self._export_safely("span", self._exporter.export_span(span))

    async def write_metric(self, metric: MetricPoint) -> None:
        await self._store.write_metric(metric)
        await self._export_safely("metric", self._exporter.export_metric(metric))

    async def write_metrics(self, metrics: list[MetricPoint]) -> None:
        bulk = getattr(self._store, "write_metrics", None)
        if bulk is None:
            for metric in metrics:
                await self._store.write_metric(metric)
        else:
            await bulk(metrics)
        for metric in metrics:
            await self._export_safely("metric", self._exporter.export_metric(metric))

    async def write_audit(self, event: AuditEvent) -> None:
        await self._store.write_audit(event)

    async def write_alert(self, alert: Alert) -> None:
        await self._store.write_alert(alert)
        await self._export_safely("alert", self._exporter.deliver_alert(alert))

    async def session_records(
        self, tenant_id: str, session_id: str
    ) -> dict[str, list[Any]]:
        return await self._store.session_records(tenant_id, session_id)

    async def metric_snapshot(
        self, tenant_id: str | None = None, *, limit: int = 2000
    ) -> list[MetricPoint]:
        return await self._store.metric_snapshot(tenant_id, limit=limit)

    async def metric_summary(
        self, tenant_id: str, *, window_hours: int
    ) -> list[MetricSummary]:
        return await self._store.metric_summary(tenant_id, window_hours=window_hours)

    async def search_audits(
        self,
        tenant_id: str,
        *,
        action: str | None = None,
        outcome: str | None = None,
        actor_id: str | None = None,
        session_id: str | None = None,
        before: datetime | None = None,
        before_id: str | None = None,
        limit: int = 50,
    ) -> list[AuditEvent]:
        return await self._store.search_audits(
            tenant_id,
            action=action,
            outcome=outcome,
            actor_id=actor_id,
            session_id=session_id,
            before=before,
            before_id=before_id,
            limit=limit,
        )

    @staticmethod
    async def _export_safely(kind: str, operation: Any) -> None:
        try:
            await operation
        except Exception as exc:
            logger.error(
                "observability_export_failed",
                extra={
                    "structured_fields": {
                        "signal": kind,
                        "error_type": type(exc).__name__,
                    }
                },
            )

    async def aclose(self) -> None:
        close_exporter = getattr(self._exporter, "aclose", None)
        if close_exporter is not None:
            await close_exporter()
        close_store = getattr(self._store, "aclose", None)
        if close_store is None:
            close_store = getattr(self._store, "close", None)
        if close_store is not None:
            await close_store()
