from __future__ import annotations

from auraclaw.config import Settings
from auraclaw.infrastructure.observability.exporters import (
    ExportingObservabilityStore,
    HttpTelemetryExporter,
)
from auraclaw.observability.service import ObservabilityStore


def exporting_observability_store(
    settings: Settings,
    *,
    service_name: str,
    store: ObservabilityStore,
) -> ObservabilityStore:
    """Attach production telemetry sinks at the composition boundary."""

    if not settings.observability_otlp_http_endpoint or not settings.alert_receiver_url:
        return store
    exporter_token = (
        settings.observability_exporter_token.get_secret_value()
        if settings.observability_exporter_token is not None
        else None
    )
    alert_token = (
        settings.alert_receiver_token.get_secret_value()
        if settings.alert_receiver_token is not None
        else None
    )
    exporter = HttpTelemetryExporter(
        otlp_endpoint=settings.observability_otlp_http_endpoint,
        alert_receiver_url=settings.alert_receiver_url,
        service_name=service_name,
        timeout_seconds=settings.observability_export_timeout_seconds,
        retry_attempts=settings.observability_export_retry_attempts,
        queue_capacity=settings.observability_export_queue_capacity,
        otlp_bearer_token=exporter_token,
        alert_bearer_token=alert_token,
    )
    return ExportingObservabilityStore(store, exporter)
