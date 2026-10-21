"""Content-blind selection of development holdouts, before any model is run."""

from __future__ import annotations

import hashlib
from collections import defaultdict

PROTOCOL = "e3-heldout-v1"


def select_holdout(
    benchmark: str,
    task_ids: list[str],
    *,
    used_task_ids: set[str],
    seed: int = 20261021,
    scenarios: int = 6,
) -> list[str]:
    if len(set(task_ids)) != len(task_ids) or not task_ids:
        raise ValueError("Unique, nonempty source task ids required")
    if not used_task_ids.issubset(task_ids):
        raise ValueError("Prior exposure must belong to this split")
    if benchmark == "tau-bench":
        if any(not t.isascii() or not t.isdecimal() or str(int(t)) != t for t in task_ids):
            raise ValueError("Canonical tau task indices required")
        return sorted(set(task_ids) - used_task_ids, key=int)
    if benchmark != "appworld" or scenarios < 1:
        raise ValueError("Unsupported selection")
    groups: dict[str, list[str]] = defaultdict(list)
    for task in task_ids:
        family, separator, variant = task.rpartition("_")
        if not separator or variant not in {"1", "2", "3"}:
            raise ValueError("Expected three-variant AppWorld scenarios")
        groups[family].append(task)
    exposed = {t.rpartition("_")[0] for t in used_task_ids}
    candidates = [family for family in groups if family not in exposed]
    if len(candidates) < scenarios or any(len(groups[f]) != 3 for f in candidates):
        raise ValueError("Not enough complete unexposed scenarios")
    ordered = sorted(
        candidates, key=lambda f: hashlib.sha256(f"{PROTOCOL}:{seed}:{f}".encode()).digest()
    )
    return [task for family in ordered[:scenarios] for task in sorted(groups[family])]
