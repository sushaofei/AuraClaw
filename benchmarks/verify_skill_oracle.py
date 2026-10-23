"""Require official oracle and verifier evidence; a zero CLI exit is insufficient."""

from __future__ import annotations

import argparse
import json
from decimal import Decimal, InvalidOperation
from pathlib import Path


def verify(job: Path, *, task: str | None = None) -> dict[str, object]:
    rewards = list(job.rglob("reward.txt"))
    reports = list(job.rglob("ctrf.json"))
    results = list(job.rglob("result.json"))
    reasons = []
    # SDK.run emits result.json; the batch CLI additionally emits summary.json.
    if len(results) != 1:
        reasons.append("missing_or_ambiguous_official_result")
        result = {}
    else:
        result = json.loads(results[0].read_text())
        if (
            result.get("agent") != "oracle"
            or any(
                result.get(k)
                for k in ("error", "verifier_error", "export_error", "partial_trajectory")
            )
            or result.get("rewards", {}).get("reward") != 1
        ):
            reasons.append("official_result_not_passed")
        if task is not None and result.get("task_name") != task:
            reasons.append("task_identity_mismatch")
    task = task or result.get("task_name")
    summary_path = job / "summary.json"
    if summary_path.exists():
        summary = json.loads(summary_path.read_text())
        if summary.get("agent") != "oracle" or summary.get("total") != 1:
            reasons.append("expected_single_oracle_attempt")
        if summary.get("passed") != 1 or any(
            summary.get(k, 0) for k in ("failed", "errored", "verifier_errored")
        ):
            reasons.append("official_summary_not_passed")
    try:
        reward_passed = len(rewards) == 1 and Decimal(rewards[0].read_text().strip()) == 1
    except InvalidOperation:
        reward_passed = False
    if not reward_passed:
        reasons.append("missing_or_nonpassing_reward")
    if task == "python-scala-translation":
        logs = list(job.rglob("output.log"))
        output = logs[0].read_text() if len(logs) == 1 else ""
        if "Tests passed: 10/10" not in output or "Status: ✓ PASSED" not in output:
            reasons.append("scala_official_verifier_not_passed")
    elif len(reports) != 1:
        reasons.append("missing_or_ambiguous_test_report")
    else:
        report = json.loads(reports[0].read_text())
        counts = report.get("results", {}).get("summary", {})
        if (
            not isinstance(counts.get("passed"), int)
            or counts["passed"] <= 0
            or counts.get("failed") != 0
        ):
            reasons.append("verifier_tests_not_passed")
    return {
        "scope": "oracle-admission",
        "oracle_passed": not reasons,
        "reasons": reasons,
        "benchmark_score": None,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("job", type=Path)
    parser.add_argument("--task")
    args = parser.parse_args()
    result = verify(args.job, task=args.task)
    print(json.dumps(result))
    raise SystemExit(0 if result["oracle_passed"] else 1)


if __name__ == "__main__":
    main()
