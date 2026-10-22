"""Final-state judgments are separate from live Runtime execution diagnostics."""

from __future__ import annotations

import re
from typing import Any

from auraclaw.evaluation.contracts import UPSTREAMS
from auraclaw.evaluation.report import summarize


def reconcile_official(
    records: list[dict[str, Any]],
    *,
    task_ids: list[str],
    trials: int,
    references: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    if len(references) != 2 or not task_ids or len(set(task_ids)) != len(task_ids):
        raise ValueError("Two independent references and a unique plan required")
    expected = {(task, trial) for task in task_ids for trial in range(trials)}
    identities = [(r["task_id"], r["trial"]) for r in records]
    if len(identities) != len(expected) or set(identities) != expected:
        raise ValueError("Every planned attempt must be present exactly once")
    if any(r["config"]["benchmark"] != "appworld" for r in records):
        raise ValueError("Post-close AppWorld judgments only")
    maps = []
    for reference in references:
        if reference.get("upstream_revision") != UPSTREAMS["appworld"][1]:
            raise ValueError("Official SDK revision mismatch")
        if reference.get("regraded_attempts") != len(expected):
            raise ValueError("Official reference is incomplete")
        scores = {}
        for row in reference["regraded_rewards"]:
            identity = (row["task_id"], row["trial"])
            reward = row["reward"]
            if identity in scores or type(reward) not in {int, float} or reward not in {0, 1}:
                raise ValueError("Duplicate or invalid official reward")
            scores[identity] = float(reward)
        if set(scores) != expected:
            raise ValueError("Official reference does not match the plan")
        maps.append(scores)
    if maps[0] != maps[1]:
        raise ValueError("Independent official references disagree")
    if not any(
        isinstance(reference.get("environment_image"), str)
        and re.fullmatch(r"sha256:[a-f0-9]{64}", reference["environment_image"])
        for reference in references
    ):
        raise ValueError("A pinned environment reference is required")
    final = []
    for record in records:
        reward = maps[0][(record["task_id"], record["trial"])]
        final.append(
            {
                **record,
                "episode_reward": record.get("official_reward"),
                "episode_success": record["success"],
                "official_reward": reward,
                "success": record["status"] == "completed" and reward == 1,
                "grading_basis": "post-close-official",
            }
        )
    report = summarize(final, trials=trials, task_ids=task_ids)
    report["grading_basis"] = "post-close-official"
    report["episode_success_adjustments"] = sum(r["episode_success"] != r["success"] for r in final)
    return final, report
