"""Require official oracle and verifier evidence; a zero CLI exit is insufficient."""

from __future__ import annotations

import argparse
import json
from pathlib import Path


def verify(job: Path) -> dict[str, object]:
    summary = json.loads((job / "summary.json").read_text())
    rewards = list(job.rglob("reward.txt"))
    reports = list(job.rglob("ctrf.json"))
    reasons = []
    if summary.get("agent") != "oracle" or summary.get("total") != 1:
        reasons.append("expected_single_oracle_attempt")
    if summary.get("passed") != 1 or any(
        summary.get(k, 0) for k in ("failed", "errored", "verifier_errored")
    ):
        reasons.append("official_summary_not_passed")
    if len(rewards) != 1 or rewards[0].read_text().strip() != "1":
        reasons.append("missing_or_nonpassing_reward")
    if len(reports) != 1:
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
    result = verify(parser.parse_args().job)
    print(json.dumps(result))
    raise SystemExit(0 if result["oracle_passed"] else 1)


if __name__ == "__main__":
    main()
