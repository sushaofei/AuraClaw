from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import UTC, datetime
from statistics import quantiles
from typing import Protocol

from auraclaw.contracts.observability import MetricPoint


class CapabilitySearchQualityMetricWriter(Protocol):
    async def write_metric(self, metric: MetricPoint) -> None: ...


@dataclass(frozen=True)
class GoldenSearchCase:
    case_id: str
    tenant_id: str
    query: str
    expected: tuple[str, ...]
    forbidden: tuple[str, ...] = ()
    dangerous: tuple[str, ...] = ()
    expect_empty: bool = False


@dataclass(frozen=True)
class SearchObservation:
    case: GoldenSearchCase
    ranking: tuple[str, ...]
    latency_seconds: float
    semantic_degraded: bool
    index_generation_lag: int = 0


@dataclass(frozen=True)
class CapabilitySearchQualityThresholds:
    version: str = "capability-search-gate-v1"
    recall_at_1: float = 0.80
    recall_at_3: float = 0.95
    recall_at_5: float = 1.00
    mrr: float = 0.90
    ndcg_at_5: float = 0.90
    negative_accuracy: float = 1.00
    forbidden_hit_rate: float = 0.00
    dangerous_false_positive_rate: float = 0.00
    p95_latency_seconds: float = 10.00
    p99_latency_seconds: float = 12.00
    max_index_generation_lag: int = 0
    semantic_degraded_rate: float = 0.01


@dataclass(frozen=True)
class CapabilitySearchQualityReport:
    dataset: str
    dataset_version: str
    threshold_version: str
    metrics: dict[str, float]
    passed: bool
    failures: tuple[str, ...]


DEFAULT_CAPABILITY_SEARCH_QUALITY_THRESHOLDS = CapabilitySearchQualityThresholds()


def evaluate_capability_search(
    observations: tuple[SearchObservation, ...],
    *,
    dataset: str,
    dataset_version: str,
    thresholds: CapabilitySearchQualityThresholds = DEFAULT_CAPABILITY_SEARCH_QUALITY_THRESHOLDS,
) -> CapabilitySearchQualityReport:
    if not observations:
        raise ValueError("Capability search quality evaluation requires observations")
    positive = tuple(item for item in observations if not item.case.expect_empty)
    negative = tuple(item for item in observations if item.case.expect_empty)
    if not positive or not negative:
        raise ValueError("Quality dataset requires positive and negative cases")

    def recall_at(limit: int) -> float:
        hits = 0
        total = 0
        for item in positive:
            expected = set(item.case.expected)
            total += len(expected)
            hits += len(expected.intersection(item.ranking[:limit]))
        return hits / total

    reciprocal_ranks: list[float] = []
    ndcg_values: list[float] = []
    forbidden_hits = 0
    dangerous_hits = 0
    for item in observations:
        expected = set(item.case.expected)
        rank = next(
            (position for position, value in enumerate(item.ranking, start=1) if value in expected),
            None,
        )
        if not item.case.expect_empty:
            reciprocal_ranks.append(0.0 if rank is None else 1.0 / rank)
            gains = [1.0 if value in expected else 0.0 for value in item.ranking[:5]]
            dcg = sum(gain / math.log2(position + 1) for position, gain in enumerate(gains, 1))
            ideal = sum(
                1.0 / math.log2(position + 1) for position in range(1, min(5, len(expected)) + 1)
            )
            ndcg_values.append(0.0 if ideal == 0 else dcg / ideal)
        forbidden_hits += int(bool(set(item.case.forbidden).intersection(item.ranking)))
        dangerous_hits += int(bool(set(item.case.dangerous).intersection(item.ranking)))

    latencies = sorted(item.latency_seconds for item in observations)
    percentile_values = (
        quantiles(latencies, n=100, method="inclusive")
        if len(latencies) > 1
        else [latencies[0]] * 99
    )
    metrics = {
        "recall_at_1": recall_at(1),
        "recall_at_3": recall_at(3),
        "recall_at_5": recall_at(5),
        "mrr": sum(reciprocal_ranks) / len(reciprocal_ranks),
        "ndcg_at_5": sum(ndcg_values) / len(ndcg_values),
        "negative_accuracy": sum(not item.ranking for item in negative) / len(negative),
        "forbidden_hit_rate": forbidden_hits / len(observations),
        "dangerous_false_positive_rate": dangerous_hits / len(observations),
        "p50_latency_seconds": percentile_values[49],
        "p95_latency_seconds": percentile_values[94],
        "p99_latency_seconds": percentile_values[98],
        "max_index_generation_lag": float(max(item.index_generation_lag for item in observations)),
        "semantic_degraded_rate": sum(item.semantic_degraded for item in observations)
        / len(observations),
        "zero_result_rate": sum(not item.ranking for item in observations) / len(observations),
    }
    comparisons = {
        "recall_at_1": metrics["recall_at_1"] >= thresholds.recall_at_1,
        "recall_at_3": metrics["recall_at_3"] >= thresholds.recall_at_3,
        "recall_at_5": metrics["recall_at_5"] >= thresholds.recall_at_5,
        "mrr": metrics["mrr"] >= thresholds.mrr,
        "ndcg_at_5": metrics["ndcg_at_5"] >= thresholds.ndcg_at_5,
        "negative_accuracy": metrics["negative_accuracy"] >= thresholds.negative_accuracy,
        "forbidden_hit_rate": metrics["forbidden_hit_rate"] <= thresholds.forbidden_hit_rate,
        "dangerous_false_positive_rate": (
            metrics["dangerous_false_positive_rate"] <= thresholds.dangerous_false_positive_rate
        ),
        "p95_latency_seconds": (metrics["p95_latency_seconds"] <= thresholds.p95_latency_seconds),
        "p99_latency_seconds": (metrics["p99_latency_seconds"] <= thresholds.p99_latency_seconds),
        "max_index_generation_lag": (
            metrics["max_index_generation_lag"] <= thresholds.max_index_generation_lag
        ),
        "semantic_degraded_rate": (
            metrics["semantic_degraded_rate"] <= thresholds.semantic_degraded_rate
        ),
    }
    failures = tuple(name for name, passed in comparisons.items() if not passed)
    return CapabilitySearchQualityReport(
        dataset=dataset,
        dataset_version=dataset_version,
        threshold_version=thresholds.version,
        metrics=metrics,
        passed=not failures,
        failures=failures,
    )


async def publish_capability_search_quality_metrics(
    report: CapabilitySearchQualityReport,
    *,
    tenant_id: str,
    writer: CapabilitySearchQualityMetricWriter,
) -> None:
    observed_at = datetime.now(UTC)
    for metric, value in sorted(report.metrics.items()):
        await writer.write_metric(
            MetricPoint(
                name="capability_search_quality_gate",
                value=value,
                observed_at=observed_at,
                tenant_id=tenant_id,
                labels={
                    "dataset": report.dataset,
                    "metric": metric,
                    "version": report.dataset_version,
                    "threshold_version": report.threshold_version,
                    "outcome": "passed" if report.passed else "failed",
                },
                deduplication_key=(
                    f"capability-search-quality:{tenant_id}:{report.dataset}:"
                    f"{report.dataset_version}:{report.threshold_version}:{metric}"
                ),
            )
        )
