from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

REQUIRED_SCENARIOS = frozenset(
    {
        "dual_cluster_cutover",
        "ingress_connection_drain",
        "database_backup_restore",
        "migration_rollback",
        "kafka_replay",
        "projection_rebuild",
        "capacity_baseline",
        "fault_injection",
    }
)

SLO_LIMITS: dict[str, tuple[str, float]] = {
    "canonical_append_availability_ratio": ("min", 0.999),
    "canonical_append_p95_ms": ("max", 100.0),
    "projection_lag_p95_seconds": ("max", 2.0),
    "task_start_p95_seconds": ("max", 5.0),
    "runtime_recovery_p95_seconds": ("max", 30.0),
    "sse_latency_p95_seconds": ("max", 1.0),
    "delivery_success_60s_ratio": ("min", 0.99),
    "unknown_side_effect_count": ("max", 0.0),
    "duplicate_side_effect_count": ("max", 0.0),
}

CAPACITY_LIMITS: dict[str, tuple[str, float]] = {
    "target_sessions_per_minute": ("min", 1.0),
    "cpu_peak_ratio": ("max", 0.70),
    "memory_peak_ratio": ("max", 0.80),
    "database_pool_peak_ratio": ("max", 0.70),
    "queue_rejection_count": ("max", 0.0),
}

IMAGE_RE = re.compile(r"^[a-z0-9]+(?:[._-][a-z0-9]+)*(?::[0-9]+)?/.+@sha256:[0-9a-f]{64}$")
COMMIT_RE = re.compile(r"^[0-9a-f]{40}$")
PLACEHOLDERS = ("example", "replace", "todo", "pending", "changeme")


def _timestamp(value: Any, field: str, failures: list[str]) -> datetime | None:
    if not isinstance(value, str):
        failures.append(f"{field} must be an RFC3339 timestamp")
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        failures.append(f"{field} must be an RFC3339 timestamp")
        return None
    if parsed.tzinfo is None:
        failures.append(f"{field} must include a timezone")
        return None
    return parsed.astimezone(UTC)


def _validate_limits(
    values: Any,
    limits: dict[str, tuple[str, float]],
    prefix: str,
    failures: list[str],
) -> None:
    if not isinstance(values, dict):
        failures.append(f"{prefix} must be an object")
        return
    for name, (comparison, limit) in limits.items():
        value = values.get(name)
        if not isinstance(value, int | float) or isinstance(value, bool):
            failures.append(f"{prefix}.{name} must be numeric")
            continue
        failed = value < limit if comparison == "min" else value > limit
        if failed:
            failures.append(
                f"{prefix}.{name}={value} violates {comparison} threshold {limit}"
            )


def validate_evidence(document: Any) -> list[str]:
    failures: list[str] = []
    if not isinstance(document, dict):
        return ["evidence root must be an object"]
    if document.get("schema_version") != 1:
        failures.append("schema_version must equal 1")
    environment = document.get("environment")
    if not isinstance(environment, str) or not environment.strip():
        failures.append("environment is required")
    elif any(marker in environment.lower() for marker in PLACEHOLDERS):
        failures.append("environment must identify the actual exercised environment")
    image_ref = document.get("image_ref")
    if not isinstance(image_ref, str) or not IMAGE_RE.fullmatch(image_ref):
        failures.append("image_ref must be an immutable OCI digest reference")
    elif image_ref.endswith("sha256:" + "0" * 64):
        failures.append("image_ref cannot use the zero placeholder digest")
    git_commit = document.get("git_commit")
    if not isinstance(git_commit, str) or not COMMIT_RE.fullmatch(git_commit):
        failures.append("git_commit must be a full lowercase commit SHA")
    operator = document.get("operator")
    if not isinstance(operator, str) or not operator.strip():
        failures.append("operator is required")

    started = _timestamp(document.get("started_at"), "started_at", failures)
    ended = _timestamp(document.get("ended_at"), "ended_at", failures)
    if started is not None and ended is not None and ended <= started:
        failures.append("ended_at must be after started_at")

    scenarios = document.get("scenarios")
    seen: set[str] = set()
    if not isinstance(scenarios, list):
        failures.append("scenarios must be an array")
    else:
        for index, scenario in enumerate(scenarios):
            if not isinstance(scenario, dict):
                failures.append(f"scenarios[{index}] must be an object")
                continue
            scenario_id = scenario.get("id")
            if not isinstance(scenario_id, str):
                failures.append(f"scenarios[{index}].id is required")
                continue
            if scenario_id in seen:
                failures.append(f"duplicate scenario: {scenario_id}")
            seen.add(scenario_id)
            if scenario.get("status") != "passed":
                failures.append(f"scenario {scenario_id} is not passed")
            refs = scenario.get("evidence_refs")
            if not isinstance(refs, list) or not refs or not all(
                isinstance(ref, str) and ref.strip() for ref in refs
            ):
                failures.append(f"scenario {scenario_id} requires evidence_refs")
            _timestamp(scenario.get("started_at"), f"scenario {scenario_id}.started_at", failures)
            _timestamp(scenario.get("ended_at"), f"scenario {scenario_id}.ended_at", failures)
        for missing in sorted(REQUIRED_SCENARIOS - seen):
            failures.append(f"missing required scenario: {missing}")

    _validate_limits(document.get("slo_metrics"), SLO_LIMITS, "slo_metrics", failures)
    _validate_limits(document.get("capacity"), CAPACITY_LIMITS, "capacity", failures)
    return failures


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Validate production drill, capacity, and SLO release evidence."
    )
    parser.add_argument("--evidence", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        document = json.loads(args.evidence.read_text())
    except (OSError, json.JSONDecodeError) as exc:
        print(f"production readiness gate failed: {exc}", file=sys.stderr)
        return 2
    failures = validate_evidence(document)
    if failures:
        print("production readiness gate failed", file=sys.stderr)
        for failure in failures:
            print(f"- {failure}", file=sys.stderr)
        return 1
    print("production readiness gate passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
