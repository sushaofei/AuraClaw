"""Admission must use official outcome and actual tests, not subprocess success."""

import importlib.util
import json
from pathlib import Path

import pytest

SPEC = importlib.util.spec_from_file_location(
    "oracle_gate", Path(__file__).parents[2] / "benchmarks/verify_skill_oracle.py"
)
assert SPEC and SPEC.loader
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


def evidence(path, task="shock-analysis-supply"):
    (path / "result.json").write_text(
        json.dumps(
            {
                "agent": "oracle",
                "task_name": task,
                "rewards": {"reward": 1.0},
                "error": None,
            }
        )
    )
    (path / "reward.txt").write_text("1.0\n")
    (path / "ctrf.json").write_text(
        json.dumps(
            {
                "results": {
                    "summary": {
                        "passed": 9,
                        "failed": 0,
                    }
                }
            }
        )
    )


def test_sdk_result_without_cli_summary_and_decimal_reward(tmp_path):
    evidence(tmp_path)
    assert MODULE.verify(tmp_path)["oracle_passed"]


@pytest.mark.parametrize("failure", ["missing_tests", "reward_zero", "wrong_task", "sdk_error"])
def test_reject_incomplete_or_conflicting_evidence(tmp_path, failure):
    evidence(tmp_path)
    if failure == "missing_tests":
        (tmp_path / "ctrf.json").unlink()
    elif failure == "reward_zero":
        (tmp_path / "reward.txt").write_text("0")
    elif failure == "sdk_error":
        result = json.loads((tmp_path / "result.json").read_text())
        result["verifier_error"] = "dependency failure"
        (tmp_path / "result.json").write_text(json.dumps(result))
    assert not MODULE.verify(tmp_path, task="wrong" if failure == "wrong_task" else None)[
        "oracle_passed"
    ]


def test_scala_uses_its_official_report_not_ctrf(tmp_path):
    evidence(tmp_path, "python-scala-translation")
    (tmp_path / "ctrf.json").unlink()
    (tmp_path / "output.log").write_text("Tests passed: 10/10\nStatus: ✓ PASSED\n")
    assert MODULE.verify(tmp_path)["oracle_passed"]
    (tmp_path / "output.log").write_text("Tests passed: 9/10\nStatus: ❌ NEEDS IMPROVEMENT\n")
    assert not MODULE.verify(tmp_path)["oracle_passed"]
