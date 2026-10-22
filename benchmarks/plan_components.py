"""Freeze a private component benchmark cohort. Does not execute code, tools or models."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
from pathlib import Path
from typing import Any

from auraclaw.evaluation.components.catalog import (
    COMPONENT_BENCHMARKS,
    LONGMEMEVAL_DATA_REVISION,
    LONGMEMEVAL_DATA_SHA256,
)
from auraclaw.evaluation.components.longmemeval import MemoryCase
from auraclaw.evaluation.components.plan import build_plan, select_tasks


def checked_checkout(root: Path, expected: str) -> None:
    head = subprocess.check_output(["git", "-C", str(root), "rev-parse", "HEAD"], text=True).strip()
    dirty = subprocess.check_output(
        ["git", "-C", str(root), "status", "--porcelain", "--untracked-files=no"], text=True
    )
    if head != expected or dirty:
        raise ValueError("Upstream checkout must match the clean pinned revision")


def freeze(benchmark: str, root: Path, dataset: Path | None) -> dict[str, Any]:
    spec = COMPONENT_BENCHMARKS[benchmark]
    checked_checkout(root, spec.revision)
    if benchmark == "longmemeval":
        if dataset is None or dataset.is_symlink():
            raise ValueError("An explicit cleaned S data file is required")
        raw = dataset.read_bytes()
        if hashlib.sha256(raw).hexdigest() != LONGMEMEVAL_DATA_SHA256:
            raise ValueError("Dataset is not the pinned official cleaned S release")
        records = json.loads(raw)
        groups: dict[str, list[str]] = {}
        for item in records:
            MemoryCase.from_official(item)
            group = "abstention" if item["question_id"].endswith("_abs") else item["question_type"]
            groups.setdefault(group, []).append(item["question_id"])
        plan = build_plan(benchmark, select_tasks(groups, 12))
        plan["dataset_sha256"] = hashlib.sha256(raw).hexdigest()
        plan["dataset_variant"] = "s_cleaned"
        plan["dataset_revision"] = LONGMEMEVAL_DATA_REVISION
        plan["requirements"] = [
            "isolated-memory-namespace",
            "official-grader-and-fixed-judge",
            "full-history-context-admission",
        ]
    elif benchmark == "skillsbench":
        raw = (root / "registry.json").read_bytes()
        registry = next(item for item in json.loads(raw) if item["version"] == "1.1")
        tasks = {item["name"]: item for item in registry["tasks"]}
        chosen = select_tasks({"registry-v1.1": list(tasks)}, 6)
        plan = build_plan(benchmark, chosen)
        plan["registry_sha256"] = hashlib.sha256(raw).hexdigest()
        plan["task_packages"] = [tasks[name] for name in chosen]
        plan["requirements"] = [
            "benchflow>=0.6.3,<0.7",
            "docker-sandbox",
            "official-oracle-pass",
            "task-package-digest",
            "skill-port",
            "per-task-runtime-budget-review",
        ]
    else:
        # Original MCP-Universe local environment track, not the separate MCPMark suite.
        folder = root / "mcpuniverse/benchmark/configs/mcpuniverse/3d_design"
        files = sorted(folder.glob("*.json"))
        if any(path.is_symlink() for path in files):
            raise ValueError("Task symlinks are not permitted")
        chosen = select_tasks({"3d_design": [path.stem for path in files]}, 6)
        plan = build_plan(benchmark, chosen)
        plan["task_sha256"] = {
            name: hashlib.sha256((folder / (name + ".json")).read_bytes()).hexdigest()
            for name in chosen
        }
        plan["requirements"] = [
            "isolated-blender-4.4",
            "official-blender-mcp",
            "official-evaluator",
            "no-personal-accounts",
            "per-task-runtime-budget-review",
        ]
    # Not a readiness claim: the runtime worker must verify all requirements before calling models.
    plan["admission"] = "pending-environment-and-grader-validation"
    return plan


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--benchmark", choices=tuple(COMPONENT_BENCHMARKS), required=True)
    parser.add_argument("--upstream-root", type=Path, required=True)
    parser.add_argument("--dataset", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    plan = freeze(args.benchmark, args.upstream_root, args.dataset)
    with open(
        args.output, "x", encoding="utf-8", opener=lambda p, flags: os.open(p, flags, 0o600)
    ) as output:
        json.dump(plan, output, indent=2, allow_nan=False)
        output.write("\n")
    print(
        json.dumps(
            {
                "benchmark": args.benchmark,
                "tasks": len(plan["task_ids"]),
                "expected_attempts": plan["expected_attempts"],
                "admission": plan["admission"],
            }
        )
    )


if __name__ == "__main__":
    main()
