from __future__ import annotations

import copy

import pytest

from auraclaw.evaluation.contracts import UPSTREAMS
from auraclaw.evaluation.official import reconcile_official


def inputs():
    records = [
        {
            "task_id": str(t),
            "trial": 0,
            "config": {"benchmark": "appworld"},
            "status": "completed" if t == 0 else "runtime_error",
            "success": t == 0,
            "official_reward": 1 if t == 0 else None,
            "cost": None,
        }
        for t in range(2)
    ]
    reference = {
        "upstream_revision": UPSTREAMS["appworld"][1],
        "regraded_attempts": 2,
        "environment_image": "sha256:" + "a" * 64,
        "regraded_rewards": [{"task_id": str(t), "trial": 0, "reward": 0} for t in range(2)],
    }
    return records, [reference, copy.deepcopy(reference)]


def test_final_official_goal_score_corrects_live_diagnostics_without_overwriting_them():
    records, references = inputs()
    original = copy.deepcopy(records)
    final, report = reconcile_official(
        records, task_ids=["0", "1"], trials=1, references=references
    )
    assert records == original
    assert final[0]["episode_success"] and not final[0]["success"]
    assert final[0]["episode_reward"] == 1 and final[0]["official_reward"] == 0
    assert final[1]["episode_reward"] is None and final[1]["status"] == "runtime_error"
    assert report["success_rate"] == 0 and report["error_attempts"] == 1
    assert report["episode_success_adjustments"] == 1


@pytest.mark.parametrize("failure", ["disagree", "missing", "duplicate", "revision", "image"])
def test_finalization_rejects_invalid_official_evidence(failure):
    records, references = inputs()
    if failure == "disagree":
        references[1]["regraded_rewards"][0]["reward"] = 1
    if failure == "missing":
        references[0]["regraded_rewards"].pop()
    if failure == "duplicate":
        references[0]["regraded_rewards"].append(references[0]["regraded_rewards"][0])
    if failure == "revision":
        references[0]["upstream_revision"] = "wrong"
    if failure == "image":
        for r in references:
            r.pop("environment_image")
    with pytest.raises(ValueError):
        reconcile_official(records, task_ids=["0", "1"], trials=1, references=references)


def test_finalization_cli_keeps_inputs_and_writes_private_immutable_judgment(tmp_path):
    import json
    import os
    import subprocess
    import sys
    from pathlib import Path

    run = tmp_path / "run"
    run.mkdir()
    tasks = ["family_1", "family_2", "family_3"]
    (run / "manifest.json").write_text(json.dumps({"task_ids": tasks, "trials": 1}))
    for index, task in enumerate(tasks):
        (run / f"attempt_{index:06d}.json").write_text(
            json.dumps(
                {
                    "config": {"benchmark": "appworld"},
                    "task_id": task,
                    "trial": 0,
                    "status": "completed",
                    "success": True,
                    "official_reward": 1,
                    "cost": None,
                    "validation_scope": "official-sdk-adapter",
                }
            )
        )
    reference = {
        "upstream_revision": UPSTREAMS["appworld"][1],
        "regraded_attempts": 3,
        "environment_image": "sha256:" + "a" * 64,
        "regraded_rewards": [{"task_id": task, "trial": 0, "reward": 0} for task in tasks],
    }
    peers = [tmp_path / "peer-a.json", tmp_path / "peer-b.json"]
    for peer in peers:
        peer.write_text(json.dumps(reference))
    output = tmp_path / "official-metrics.json"
    root = Path(__file__).resolve().parents[2]
    command = [
        sys.executable,
        str(root / "benchmarks/finalize_agents.py"),
        "--run",
        str(run),
        "--reference",
        str(peers[0]),
        "--reference",
        str(peers[1]),
        "--output",
        str(output),
    ]
    env = {**os.environ, "PYTHONPATH": str(root / "src"), "AURACLAW_DISABLE_ENV_FILE": "1"}
    subprocess.run(command, env=env, check=True, capture_output=True)
    result = json.loads(output.read_text())
    assert result["summary"]["success_rate"] == 0 and result["episode_success_adjustments"] == 3
    assert output.stat().st_mode & 0o777 == 0o600
    assert json.loads((run / "attempt_000000.json").read_text())["success"] is True
    assert subprocess.run(command, env=env, capture_output=True).returncode != 0
