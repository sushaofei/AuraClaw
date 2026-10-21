from __future__ import annotations

import math
from collections import defaultdict
from typing import Any


def summarize(
    records: list[dict[str, Any]], *, trials: int, task_ids: list[str] | None = None
) -> dict[str, Any]:
    if not isinstance(trials, int) or isinstance(trials, bool) or trials < 1:
        raise ValueError("Positive trials required")
    if task_ids is not None and (not task_ids or len(set(task_ids)) != len(task_ids)):
        raise ValueError("Unique nonempty planned tasks required")
    if not records and task_ids is None:
        raise ValueError("Results or planned tasks required")
    configurations = {repr(sorted(item["config"].items())) for item in records}
    if len(configurations) > 1:
        raise ValueError("Cannot combine different evaluation configurations")
    grouped: dict[str, dict[int, bool]] = defaultdict(dict)
    if task_ids is not None:
        for task in task_ids:
            grouped[task] = {}
    for record in records:
        task = str(record["task_id"])
        if task_ids is not None and task not in grouped:
            raise ValueError("Unexpected task outside the plan")
        trial = record["trial"]
        if not isinstance(trial, int) or not 0 <= trial < trials or trial in grouped[task]:
            raise ValueError("Duplicate or invalid trial identity")
        grouped[task][trial] = record["success"] is True
    complete = all(len(values) == trials for values in grouped.values())
    pass_k = {}
    if complete:
        for k in range(1, trials + 1):
            pass_k[str(k)] = sum(
                math.comb(sum(values.values()), k) / math.comb(trials, k)
                for values in grouped.values()
            ) / len(grouped)
    return {
        "schema_version": 1,
        "tasks": len(grouped),
        "attempts": len(records),
        "expected_attempts": len(grouped) * trials,
        "complete": complete,
        "success_rate": sum(item["success"] is True for item in records) / (len(grouped) * trials),
        "scored_attempts": sum(item.get("official_reward") is not None for item in records),
        "error_attempts": sum(
            item["status"] in {"infra_error", "runtime_error", "evaluation_error", "timeout"}
            for item in records
        ),
        "pass_power_k": pass_k,
        "cost_scope": "agent_model_only",
        "total_cost": None,
        "cost": (
            sum(item["cost"] for item in records)
            if records and complete and all(item.get("cost") is not None for item in records)
            else None
        ),
    }
