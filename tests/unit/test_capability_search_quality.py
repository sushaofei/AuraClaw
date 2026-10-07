from __future__ import annotations

import asyncio

from auraclaw.action.capability_search_quality import (
    GoldenSearchCase,
    SearchObservation,
    evaluate_capability_search,
    publish_capability_search_quality_metrics,
)
from auraclaw.contracts.observability import MetricPoint


class Metrics:
    def __init__(self) -> None:
        self.points: list[MetricPoint] = []

    async def write_metric(self, metric: MetricPoint) -> None:
        self.points.append(metric)


def observation(
    case_id: str,
    expected: tuple[str, ...],
    ranking: tuple[str, ...],
    *,
    expect_empty: bool = False,
    forbidden: tuple[str, ...] = (),
    dangerous: tuple[str, ...] = (),
) -> SearchObservation:
    return SearchObservation(
        case=GoldenSearchCase(
            case_id=case_id,
            tenant_id="tenant-a",
            query=case_id,
            expected=expected,
            expect_empty=expect_empty,
            forbidden=forbidden,
            dangerous=dangerous,
        ),
        ranking=ranking,
        latency_seconds=0.02,
        semantic_degraded=False,
    )


def test_quality_gate_reports_all_release_metrics_and_passes_good_dataset() -> None:
    report = evaluate_capability_search(
        (
            observation("中文缺陷", ("github.issue.create",), ("github.issue.create",)),
            observation(
                "supplier risk",
                ("supplier.risk.profile",),
                ("supplier.risk.profile", "supplier.profile.read"),
            ),
            observation(
                "negative",
                (),
                (),
                expect_empty=True,
                forbidden=("tenant-b.secret",),
                dangerous=("payment.execute",),
            ),
        ),
        dataset="business-expressions",
        dataset_version="v1",
    )
    assert report.passed
    assert report.threshold_version == "capability-search-gate-v1"
    assert set(report.metrics) == {
        "recall_at_1",
        "recall_at_3",
        "recall_at_5",
        "mrr",
        "ndcg_at_5",
        "negative_accuracy",
        "forbidden_hit_rate",
        "dangerous_false_positive_rate",
        "p50_latency_seconds",
        "p95_latency_seconds",
        "p99_latency_seconds",
        "max_index_generation_lag",
        "semantic_degraded_rate",
        "zero_result_rate",
    }


def test_quality_gate_rejects_hidden_and_dangerous_candidates() -> None:
    report = evaluate_capability_search(
        (
            observation(
                "safe read",
                ("supplier.read",),
                ("payment.execute", "tenant-b.secret"),
                forbidden=("tenant-b.secret",),
                dangerous=("payment.execute",),
            ),
            observation("negative", (), (), expect_empty=True),
        ),
        dataset="security-negatives",
        dataset_version="v1",
    )
    assert not report.passed
    assert "forbidden_hit_rate" in report.failures
    assert "dangerous_false_positive_rate" in report.failures
    assert "recall_at_1" in report.failures


def test_quality_gate_metrics_are_versioned_and_tenant_scoped() -> None:
    async def scenario() -> None:
        report = evaluate_capability_search(
            (
                observation("positive", ("read",), ("read",)),
                observation("negative", (), (), expect_empty=True),
            ),
            dataset="golden",
            dataset_version="1.0.0",
        )
        metrics = Metrics()
        await publish_capability_search_quality_metrics(
            report,
            tenant_id="tenant-a",
            writer=metrics,
        )
        assert len(metrics.points) == len(report.metrics)
        assert {point.name for point in metrics.points} == {"capability_search_quality_gate"}
        assert {point.tenant_id for point in metrics.points} == {"tenant-a"}
        assert all(point.labels["version"] == "1.0.0" for point in metrics.points)

    asyncio.run(scenario())
