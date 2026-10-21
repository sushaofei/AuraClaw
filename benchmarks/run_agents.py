"""Opt-in, process-isolated benchmarks. See docs/development/agent-evaluation.md."""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import importlib
import json
import os
import random
import subprocess
import sys
from dataclasses import asdict
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit
from uuid import uuid4

from auraclaw.composition.evaluation import run_benchmark_episode
from auraclaw.evaluation.cohort import PROTOCOL
from auraclaw.evaluation.contracts import UPSTREAMS, BenchmarkEnvironment, EvaluationConfig
from auraclaw.evaluation.environments import AppWorldEnvironment, TauEnvironment
from auraclaw.evaluation.fixtures import FixtureModel, fixture_environment
from auraclaw.evaluation.report import summarize
from auraclaw.infrastructure.clients.model import RemoteModelClient
from auraclaw.runtime.ports import ModelClient


def write_private(path: Path, value: Any) -> None:
    # Refuse overwrite and symlinks; artifacts may contain simulated credentials / task content.
    with open(
        path, "x", encoding="utf-8", opener=lambda name, flags: os.open(name, flags, 0o600)
    ) as f:
        json.dump(value, f, ensure_ascii=False, indent=2, allow_nan=False)
        f.write("\n")


def revision(root: Path) -> str:
    return subprocess.run(
        ["git", "-C", str(root), "rev-parse", "HEAD"], capture_output=True, text=True, check=True
    ).stdout.strip()


def verify_dataset(root: Path, declared: str) -> None:
    """Bind results to the bytes actually used by the official SDK, excluding bytecode."""
    if not declared.startswith("sha256:") or len(declared) != 71:
        raise ValueError("Official evaluation requires a SHA-256 dataset manifest")
    digest = hashlib.sha256()
    files = 0
    for path in sorted(root.rglob("*")):
        if "__pycache__" in path.parts or path.suffix in {".pyc", ".pyo"}:
            continue
        if path.is_symlink():
            raise ValueError("Dataset symlinks are not allowed")
        if not path.is_file():
            continue
        digest.update(path.relative_to(root).as_posix().encode() + b"\0")
        digest.update(hashlib.sha256(path.read_bytes()).digest())
        files += 1
    if not files or "sha256:" + digest.hexdigest() != declared:
        raise ValueError("Official dataset bytes do not match the declared manifest")


def upstream_module(root: Path, name: str, expected: str) -> Any:
    if revision(root) != expected:
        raise ValueError("Upstream checkout does not match the pinned revision")
    # Local patches invalidate the revision-only provenance claim.
    dirty = subprocess.run(
        ["git", "-C", str(root), "status", "--porcelain", "--untracked-files=no"],
        capture_output=True,
        text=True,
        check=True,
    ).stdout
    if dirty:
        raise ValueError("Upstream tracked files have local modifications")
    sys.path.insert(0, str(root / "src" if name == "appworld" else root))
    module = importlib.import_module(name)
    module_file = module.__file__
    if module_file is None or not Path(module_file).resolve().is_relative_to(root.resolve()):
        raise ValueError("Imported SDK does not belong to the verified checkout")
    return module


async def worker(args: argparse.Namespace) -> None:
    manifest = json.loads(args.manifest.read_text())
    config = EvaluationConfig(**manifest["config"])
    random.seed(config.seed + args.trial)
    model: RemoteModelClient | None = None
    delegate: ModelClient
    env: BenchmarkEnvironment | None = None
    runner_owns_environment = False
    experiment_name: str | None = None
    try:
        if args.smoke:
            env = fixture_environment(config.benchmark)
            delegate = FixtureModel()
        else:
            root = Path(manifest["upstream_root"])
            sha = UPSTREAMS[config.benchmark][1]
            if config.benchmark == "tau-bench":
                upstream_module(root, "tau_bench", sha)
                verify_dataset(
                    root / "tau_bench" / "envs" / str(config.domain), config.dataset_revision
                )
                get_env = importlib.import_module("tau_bench.envs").get_env
                action = importlib.import_module("tau_bench.types").Action
                raw = get_env(
                    config.domain,
                    user_strategy="llm",
                    user_model=config.user_model,
                    user_provider=config.user_provider,
                    task_split=config.split,
                    task_index=int(args.task_id),
                )
                env = TauEnvironment(raw, int(args.task_id), action)
            else:
                sdk = upstream_module(root, "appworld", sha)
                paths = importlib.import_module("appworld.common.path_store").path_store
                verify_dataset(Path(paths.data), config.dataset_revision)
                if args.task_id not in sdk.load_task_ids(config.split):
                    raise ValueError("Task id does not belong to the requested split")
                experiment_name = "auraclaw_" + uuid4().hex
                env = AppWorldEnvironment(
                    sdk.AppWorld(
                        task_id=args.task_id,
                        experiment_name=experiment_name,
                        remote_environment_url=manifest["appworld_environment_url"],
                        remote_docker=manifest.get("appworld_remote_docker", True),
                        raise_on_unsafe_syntax=True,
                        raise_on_unsafe_execution=True,
                        random_seed=config.seed + args.trial,
                        max_interactions=config.max_actions,
                        timeout_seconds=int(min(60, config.timeout_seconds)),
                    )
                )
            model = RemoteModelClient(
                manifest["model_gateway_url"],
                bearer_token=os.environ["AURACLAW_EVAL_GATEWAY_TOKEN"],
            )
            delegate = model
        assert env is not None
        runner_owns_environment = True
        record, trace = await run_benchmark_episode(
            env,
            delegate,
            config,
            task_id=args.task_id,
            trial=args.trial,
        )
        if experiment_name is not None:
            record["environment_experiment_name"] = experiment_name
        record["validation_scope"] = "fixture" if args.smoke else "official-sdk-adapter"
        record["dataset_revision_verified"] = not args.smoke
        write_private(args.result.with_suffix(".trace.json"), trace)
    except Exception as error:
        record = {
            "schema_version": 1,
            "config": asdict(config),
            "task_id": args.task_id,
            "trial": args.trial,
            "status": "infra_error",
            "error_type": type(error).__name__,
            "success": False,
            "official_reward": None,
            "cost": None,
            "validation_scope": "fixture" if args.smoke else "official-sdk-adapter",
        }
    finally:
        if env is not None and not runner_owns_environment:
            env.close()
        if model is not None:
            await model.aclose()
    write_private(args.result, record)


def run(args: argparse.Namespace) -> None:
    root = Path(__file__).resolve().parents[1]
    manifest: dict[str, Any]
    if args.command == "smoke":
        config = EvaluationConfig(
            benchmark=args.benchmark,
            split="dev",
            model="scripted-fixture",
            harness_revision=revision(root),
            dataset_revision="fixture",
            max_actions=4,
            domain="retail" if args.benchmark == "tau-bench" else None,
            user_model="scripted",
            user_provider="fixture",
        )
        manifest = {"config": asdict(config), "task_ids": ["0"], "trials": 2}
    else:
        manifest = json.loads(args.manifest.read_text())
        allowed = {
            "config",
            "task_ids",
            "trials",
            "upstream_root",
            "model_gateway_url",
            "appworld_environment_url",
            "appworld_remote_docker",
            "protocol",
        }
        if set(manifest) - allowed:
            raise ValueError("Unknown manifest fields; credentials belong in environment variables")
        if not isinstance(manifest.get("appworld_remote_docker", True), bool):
            raise ValueError("appworld_remote_docker must be a boolean")
        for key in ("model_gateway_url", "appworld_environment_url"):
            if key in manifest:
                url = urlsplit(manifest[key])
                if (
                    url.scheme not in {"http", "https"}
                    or not url.netloc
                    or url.username
                    or url.password
                    or url.query
                    or url.fragment
                ):
                    raise ValueError("Evaluation endpoint must be a plain HTTP(S) URL")
        if manifest.get("protocol") not in {None, PROTOCOL}:
            raise ValueError("Unsupported evaluation protocol")
        config = EvaluationConfig(**manifest["config"])
        manifest["upstream_root"] = str(Path(manifest["upstream_root"]).resolve())
        if config.harness_revision != revision(root):
            raise ValueError("Harness revision does not match the current checkout")
        if config.benchmark == "appworld" and not manifest.get("appworld_environment_url"):
            raise ValueError("AppWorld requires an isolated environment server URL")
    tasks, trials = manifest["task_ids"], manifest["trials"]
    if not tasks or len(set(tasks)) != len(tasks) or any(not isinstance(t, str) for t in tasks):
        raise ValueError("Explicit, unique string task ids required")
    if not isinstance(trials, int) or isinstance(trials, bool) or trials < 1:
        raise ValueError("Positive trial count required")
    if config.benchmark == "tau-bench" and any(
        not task.isascii() or not task.isdecimal() or str(int(task)) != task for task in tasks
    ):
        raise ValueError("tau-bench task ids must be canonical nonnegative indices")
    if manifest.get("protocol") == PROTOCOL and (trials != 3 or config.split != "dev"):
        raise ValueError("E3 holdout protocol requires dev split and exactly three trials")
    output = args.output.resolve() / ("eval_" + uuid4().hex)
    output.mkdir(parents=True, mode=0o700)
    write_private(output / "manifest.json", manifest)
    records: list[dict[str, Any]] = []
    for task in tasks:
        for trial in range(trials):
            target = output / f"attempt_{len(records):06d}.json"
            command = [
                sys.executable,
                str(Path(__file__).resolve()),
                "worker",
                "--manifest",
                str(output / "manifest.json"),
                "--task-id",
                task,
                "--trial",
                str(trial),
                "--result",
                str(target),
            ]
            if args.command == "smoke":
                command.append("--smoke")
            try:
                process = subprocess.run(
                    command,
                    capture_output=True,
                    text=True,
                    timeout=config.timeout_seconds,
                    env={**os.environ, "PYTHONHASHSEED": str(config.seed + trial)},
                )
                if process.returncode != 0 or not target.exists():
                    raise RuntimeError("Worker failed")
                record = json.loads(target.read_text())
            except (subprocess.TimeoutExpired, RuntimeError) as error:
                record = {
                    "schema_version": 1,
                    "validation_scope": "fixture"
                    if args.command == "smoke"
                    else "official-sdk-adapter",
                    "dataset_revision_verified": False,
                    "config": asdict(config),
                    "task_id": task,
                    "trial": trial,
                    "status": "timeout"
                    if isinstance(error, subprocess.TimeoutExpired)
                    else "infra_error",
                    "error_type": type(error).__name__,
                    "success": False,
                    "official_reward": None,
                    "cost": None,
                }
                if not target.exists():
                    write_private(target, record)
            records.append(record)
    report = summarize(records, trials=trials, task_ids=tasks)
    report["validation_scope"] = "fixture" if args.command == "smoke" else "official-sdk-adapter"
    write_private(output / "summary.json", report)
    print(json.dumps({"output": str(output), **report}, ensure_ascii=False))
    if report["error_attempts"]:
        raise SystemExit(2)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    smoke = commands.add_parser("smoke")
    smoke.add_argument("--benchmark", choices=tuple(UPSTREAMS), required=True)
    smoke.add_argument("--output", type=Path, required=True)
    evaluate = commands.add_parser("run")
    evaluate.add_argument("--manifest", type=Path, required=True)
    evaluate.add_argument("--output", type=Path, required=True)
    child = commands.add_parser("worker")
    child.add_argument("--manifest", type=Path, required=True)
    child.add_argument("--task-id", required=True)
    child.add_argument("--trial", type=int, required=True)
    child.add_argument("--result", type=Path, required=True)
    child.add_argument("--smoke", action="store_true")
    args = parser.parse_args()
    if args.command == "worker":
        asyncio.run(worker(args))
    else:
        run(args)


if __name__ == "__main__":
    main()
