from __future__ import annotations

import asyncio
from datetime import UTC, datetime

from auraclaw.action.capability_search import (
    SearchOutcome,
)
from auraclaw.action.catalog.contracts import CapabilitySearchMetricWriter
from auraclaw.contracts.observability import MetricPoint


class CatalogTelemetry:
    def __init__(self, metric_writer: CapabilitySearchMetricWriter | None) -> None:
        self._metric_writer = metric_writer

    async def record_search(
        self,
        tenant_id: str,
        outcome: SearchOutcome,
        mode: str,
        latency: float,
    ) -> None:
        if self._metric_writer is None:
            return
        metric_writer = self._metric_writer
        points = [
            (
                "capability_search_requests_total",
                1.0,
                {"mode": mode, "outcome": "hit" if outcome.matches else "empty"},
            ),
            ("capability_search_latency_seconds", latency, {"mode": mode}),
            ("capability_search_index_generation_lag", float(outcome.generation_lag), {}),
            *[
                ("capability_search_candidate_count", float(count), {"channel": channel})
                for channel, count in outcome.candidate_counts.items()
            ],
        ]
        if not outcome.matches:
            points.append(("capability_search_zero_result_total", 1.0, {}))
        if outcome.semantic_degraded:
            points.append(
                (
                    "capability_search_semantic_degraded_total",
                    1.0,
                    {"reason": outcome.degraded_reason or "unknown"},
                )
            )

        async def write(name: str, value: float, labels: dict[str, str]) -> None:
            try:
                await asyncio.wait_for(
                    metric_writer.write_metric(
                        MetricPoint(
                            name=name,
                            value=value,
                            observed_at=datetime.now(UTC),
                            tenant_id=tenant_id,
                            labels=labels,
                        )
                    ),
                    timeout=0.1,
                )
            except Exception:
                return

        await asyncio.gather(*(write(name, value, labels) for name, value, labels in points))

    async def emit(
        self,
        name: str,
        value: float,
        tenant_id: str | None,
        **labels: str,
    ) -> None:
        if self._metric_writer is None:
            return
        try:
            await asyncio.wait_for(
                self._metric_writer.write_metric(
                    MetricPoint(
                        name=name,
                        value=value,
                        observed_at=datetime.now(UTC),
                        tenant_id=tenant_id,
                        labels=labels,
                    )
                ),
                timeout=0.1,
            )
        except Exception:
            return
