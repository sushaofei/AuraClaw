from __future__ import annotations

import pytest

from auraclaw.evaluation.cohort import select_holdout
from auraclaw.evaluation.report import summarize


def test_tau_excludes_all_previously_exposed_tasks():
    assert select_holdout("tau-bench", [str(i) for i in range(20)], used_task_ids={"0", "1"}) == [
        str(i) for i in range(2, 20)
    ]


def test_appworld_selects_whole_unexposed_scenarios_independent_of_file_order():
    tasks = [f"family{i}_{v}" for i in range(19) for v in range(1, 4)]
    selected = select_holdout("appworld", tasks, used_task_ids={"family0_1", "family1_1"})
    assert len(selected) == 18
    assert not any(t.startswith(("family0_", "family1_")) for t in selected)
    assert (
        select_holdout("appworld", tasks[::-1], used_task_ids={"family0_1", "family1_1"})
        == selected
    )
    for family in {t.rpartition("_")[0] for t in selected}:
        assert {f"{family}_{v}" for v in range(1, 4)}.issubset(selected)


@pytest.mark.parametrize(
    "tasks,used",
    [(["x_1", "x_2"], set()), (["x_1", "x_1"], set()), (["x_1", "x_2", "x_3"], {"missing_1"})],
)
def test_rejects_ambiguous_or_incomplete_selection(tasks, used):
    with pytest.raises(ValueError):
        select_holdout("appworld", tasks, used_task_ids=used, scenarios=1)


def record(task, trial, success):
    return {
        "config": {"model": "fixture"},
        "task_id": task,
        "trial": trial,
        "success": success,
        "official_reward": float(success),
        "status": "completed",
        "cost": None,
    }


def test_partial_reports_keep_unstarted_tasks_in_the_denominator():
    report = summarize([record("a", 0, True)], trials=3, task_ids=["a", "b"])
    assert report["expected_attempts"] == 6 and report["tasks"] == 2
    assert report["success_rate"] == 1 / 6
    assert not report["complete"] and report["pass_power_k"] == {}
    assert summarize([], trials=3, task_ids=["a"])["cost"] is None


def test_repeated_consistency_is_not_pass_at_k():
    records = [record("a", t, t < 2) for t in range(3)] + [record("b", t, True) for t in range(3)]
    report = summarize(records, trials=3, task_ids=["a", "b"])
    assert report["pass_power_k"] == pytest.approx({"1": 5 / 6, "2": 2 / 3, "3": 0.5})
    with pytest.raises(ValueError):
        summarize(records, trials=3, task_ids=["unexpected"])
