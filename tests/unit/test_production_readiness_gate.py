from __future__ import annotations

import json
from pathlib import Path

from scripts.production_readiness_gate import REQUIRED_SCENARIOS, main, validate_evidence


def _passing_evidence() -> dict[str, object]:
    return {
        "schema_version": 1,
        "environment": "prod-drill-2026-10-07",
        "image_ref": "registry.example/auraclaw@sha256:" + "a" * 64,
        "git_commit": "b" * 40,
        "operator": "release-engineering",
        "started_at": "2026-10-07T01:00:00Z",
        "ended_at": "2026-10-07T02:00:00Z",
        "scenarios": [
            {
                "id": scenario_id,
                "status": "passed",
                "started_at": "2026-10-07T01:00:00Z",
                "ended_at": "2026-10-07T01:01:00Z",
                "evidence_refs": [f"artifacts/{scenario_id}.json#sha256={'c' * 64}"],
            }
            for scenario_id in sorted(REQUIRED_SCENARIOS)
        ],
        "slo_metrics": {
            "canonical_append_availability_ratio": 0.9995,
            "canonical_append_p95_ms": 80,
            "projection_lag_p95_seconds": 1.5,
            "task_start_p95_seconds": 4,
            "runtime_recovery_p95_seconds": 20,
            "sse_latency_p95_seconds": 0.8,
            "delivery_success_60s_ratio": 0.995,
            "unknown_side_effect_count": 0,
            "duplicate_side_effect_count": 0,
        },
        "capacity": {
            "target_sessions_per_minute": 120,
            "cpu_peak_ratio": 0.65,
            "memory_peak_ratio": 0.72,
            "database_pool_peak_ratio": 0.61,
            "queue_rejection_count": 0,
        },
    }


def test_complete_production_evidence_passes() -> None:
    assert validate_evidence(_passing_evidence()) == []


def test_gate_rejects_missing_drill_slo_breach_and_placeholder() -> None:
    evidence = _passing_evidence()
    evidence["environment"] = "pending-production-environment"
    evidence["scenarios"] = []
    slo_metrics = evidence["slo_metrics"]
    assert isinstance(slo_metrics, dict)
    slo_metrics["projection_lag_p95_seconds"] = 2.1

    failures = validate_evidence(evidence)

    assert "environment must identify the actual exercised environment" in failures
    assert "missing required scenario: database_backup_restore" in failures
    assert any("projection_lag_p95_seconds=2.1" in failure for failure in failures)


def test_example_is_intentionally_non_passing_and_cli_is_fail_closed(
    tmp_path: Path,
) -> None:
    root = Path(__file__).parents[2]
    example = root / "deploy/production-readiness.evidence.example.json"
    assert main(["--evidence", str(example)]) == 1

    evidence = tmp_path / "evidence.json"
    evidence.write_text(json.dumps(_passing_evidence()))
    assert main(["--evidence", str(evidence)]) == 0
