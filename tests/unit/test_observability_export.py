from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime
from typing import Any

import httpx

from auraclaw.config import Settings
from auraclaw.contracts.observability import (
    Alert,
    AlertSeverity,
    MetricPoint,
    TraceContext,
    TraceSpan,
)
from auraclaw.infrastructure.observability.exporters import (
    ExportingObservabilityStore,
    HttpTelemetryExporter,
)
from auraclaw.infrastructure.observability.stores import InMemoryObservabilityStore


def test_http_exporter_emits_otlp_json_and_alertmanager_v2_contracts() -> None:
    requests: list[httpx.Request] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        requests.append(request)
        return httpx.Response(202)

    async def scenario() -> None:
        client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        exporter = HttpTelemetryExporter(
            otlp_endpoint="https://otel.internal:4318/",
            alert_receiver_url="https://alerts.internal/api/v2/alerts",
            service_name="task-api",
            otlp_bearer_token="otel-token",
            alert_bearer_token="alert-token",
            client=client,
        )
        observed_at = datetime(2026, 10, 7, tzinfo=UTC)
        context = TraceContext(
            trace_id="1" * 32,
            span_id="2" * 16,
            tenant_id="tenant-a",
            session_id="session-a",
        )
        await exporter.export_span(
            TraceSpan(
                context=context,
                component="task_gateway",
                operation="POST /v1/tasks",
                started_at=observed_at,
                ended_at=observed_at,
                status="ok",
                attributes={"http_status": 202},
            )
        )
        await exporter.export_metric(
            MetricPoint(
                name="http.request.duration_ms",
                value=12.5,
                observed_at=observed_at,
                tenant_id="tenant-a",
                labels={"method": "POST"},
            )
        )
        await exporter.deliver_alert(
            Alert(
                alert_id="alt-1",
                rule="delivery.dlq.count",
                severity=AlertSeverity.CRITICAL,
                status="firing",
                summary="Delivery entered the dead-letter queue",
                fired_at=observed_at,
                tenant_id="tenant-a",
            )
        )
        await exporter.aclose()
        await client.aclose()

    asyncio.run(scenario())

    assert [request.url.path for request in requests] == [
        "/v1/traces",
        "/v1/metrics",
        "/api/v2/alerts",
    ]
    assert requests[0].headers["authorization"] == "Bearer otel-token"
    assert requests[2].headers["authorization"] == "Bearer alert-token"
    span_payload = json.loads(requests[0].content)
    assert span_payload["resourceSpans"][0]["scopeSpans"][0]["spans"][0][
        "traceId"
    ] == "1" * 32
    metric_payload = json.loads(requests[1].content)
    assert metric_payload["resourceMetrics"][0]["scopeMetrics"][0]["metrics"][0][
        "name"
    ] == "http.request.duration_ms"
    alert_payload = json.loads(requests[2].content)
    assert alert_payload[0]["labels"]["alertname"] == "delivery.dlq.count"


def test_export_failure_is_bounded_after_durable_write() -> None:
    calls = 0

    async def handler(_: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(503)

    async def scenario() -> list[MetricPoint]:
        client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        exporter = HttpTelemetryExporter(
            otlp_endpoint="https://otel.internal",
            alert_receiver_url="https://alerts.internal/api/v2/alerts",
            service_name="projection-worker",
            retry_attempts=2,
            client=client,
        )
        store = InMemoryObservabilityStore()
        exporting = ExportingObservabilityStore(store, exporter)
        await exporting.write_metric(
            MetricPoint(
                name="projection.lag.seconds",
                value=1.0,
                observed_at=datetime.now(UTC),
                tenant_id="tenant-a",
            )
        )
        await exporting.aclose()
        await client.aclose()
        return await store.metric_snapshot("tenant-a")

    persisted = asyncio.run(scenario())
    assert calls == 2
    assert [point.name for point in persisted] == ["projection.lag.seconds"]


def test_production_observability_endpoints_require_tls() -> None:
    for field, value in (
        ("observability_otlp_http_endpoint", "http://otel.internal:4318"),
        ("alert_receiver_url", "http://alerts.internal/api/v2/alerts"),
    ):
        values: dict[str, Any] = {
            "_env_file": None,
            "deployment_profile": "production",
            field: value,
        }
        try:
            Settings(**values)
        except ValueError as exc:
            assert "must use HTTPS" in str(exc)
        else:
            raise AssertionError(f"insecure production endpoint accepted: {field}")
