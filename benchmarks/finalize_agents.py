"""Finalize AppWorld from two independent post-close official judgments; no model calls."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import subprocess
from pathlib import Path

from auraclaw.evaluation.official import reconcile_official


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run", type=Path, required=True)
    parser.add_argument("--reference", type=Path, action="append", required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    manifest = json.loads((args.run / "manifest.json").read_text())
    paths = sorted(p for p in args.run.iterdir() if re.fullmatch(r"attempt_\d+\.json", p.name))
    records = [json.loads(p.read_text()) for p in paths]
    if any(r.get("validation_scope") != "official-sdk-adapter" for r in records):
        raise ValueError("Official SDK records required")
    references = [json.loads(path.read_text()) for path in args.reference]
    final, summary = reconcile_official(
        records, task_ids=manifest["task_ids"], trials=manifest["trials"], references=references
    )
    groups: dict[str, list[str]] = {}
    for task in manifest["task_ids"]:
        family, _, variant = task.rpartition("_")
        if variant not in {"1", "2", "3"}:
            raise ValueError("Complete AppWorld scenarios required")
        groups.setdefault(family, []).append(task)
    if any(len(tasks) != 3 for tasks in groups.values()):
        raise ValueError("Every scenario must include all three tasks")
    metrics = []
    for trial in range(manifest["trials"]):
        scores = {r["task_id"]: r["official_reward"] == 1 for r in final if r["trial"] == trial}
        metrics.append(
            {
                "trial": trial,
                "tasks": len(scores),
                "scenarios": len(groups),
                "task_goal_completion": round(sum(scores.values()) / len(scores) * 100, 1) / 100,
                "scenario_goal_completion": round(
                    sum(all(scores[t] for t in tasks) for tasks in groups.values())
                    / len(groups)
                    * 100,
                    1,
                )
                / 100,
            }
        )
    revision = subprocess.check_output(
        ["git", "-C", str(Path(__file__).resolve().parents[1]), "rev-parse", "HEAD"], text=True
    ).strip()
    receipt = {
        "schema_version": 1,
        "verified": True,
        "verification_basis": "two-post-close-official-graders",
        "verifier_revision": revision,
        "upstream_revision": references[0]["upstream_revision"],
        "regraded_attempts": len(final),
        "reward_mismatches": 0,
        "episode_success_adjustments": summary["episode_success_adjustments"],
        "trials": metrics,
        "regraded_rewards": [
            {"task_id": r["task_id"], "trial": r["trial"], "reward": r["official_reward"]}
            for r in final
        ],
        "summary": summary,
        "reference_sha256": [hashlib.sha256(p.read_bytes()).hexdigest() for p in args.reference],
        "input_sha256": {p.name: hashlib.sha256(p.read_bytes()).hexdigest() for p in paths},
    }
    # Immutable inputs; refuse overwriting a previous judgment.
    with open(args.output, "x", opener=lambda name, flags: os.open(name, flags, 0o600)) as f:
        json.dump(receipt, f, indent=2, allow_nan=False)
        f.write("\n")
    print(json.dumps({"verified": True, "summary": summary}, allow_nan=False))


if __name__ == "__main__":
    main()
