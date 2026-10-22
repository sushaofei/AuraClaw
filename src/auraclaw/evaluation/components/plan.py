"""Content-blind cohorts and condition coverage; no scoring or task execution."""

from __future__ import annotations

import hashlib
from dataclasses import asdict
from typing import Any

from auraclaw.evaluation.components.catalog import COMPONENT_BENCHMARKS

SELECTION_SEED = "auraclaw-components-v1:20261022"


def select_tasks(groups: dict[str, list[str]], count: int) -> list[str]:
    """Round-robin over metadata strata; never inspect prompts, gold or observed rewards."""
    all_ids = [task for tasks in groups.values() for task in tasks]
    if count < 1 or len(set(all_ids)) != len(all_ids) or len(all_ids) < count:
        raise ValueError("Invalid or insufficient cohort")
    ranked = {
        group: sorted(
            tasks, key=lambda task: hashlib.sha256(f"{SELECTION_SEED}:{task}".encode()).hexdigest()
        )
        for group, tasks in sorted(groups.items())
    }
    selected: list[str] = []
    while len(selected) < count:
        for tasks in ranked.values():
            if tasks and len(selected) < count:
                selected.append(tasks.pop(0))
    return selected


def build_plan(benchmark: str, task_ids: list[str], *, trials: int = 3) -> dict[str, Any]:
    spec = COMPONENT_BENCHMARKS[benchmark]
    if trials != 3 or not task_ids or len(set(task_ids)) != len(task_ids):
        raise ValueError("Component protocol requires unique tasks and three trials")
    return {
        "schema_version": 1,
        "protocol": "auraclaw-components-v1",
        "benchmark": benchmark,
        "upstream": asdict(spec),
        "selection_seed": SELECTION_SEED,
        "task_ids": list(task_ids),
        "trials": trials,
        "conditions": list(spec.conditions),
        "expected_attempts": len(task_ids) * trials * len(spec.conditions),
        "limits": {
            "max_actions": 30,
            "max_output_tokens": 24576,
            "per_turn_output_tokens": 4096,
            "timeout_seconds": 600,
        },
        "execution_state": "not_started",
        "score": None,
    }
