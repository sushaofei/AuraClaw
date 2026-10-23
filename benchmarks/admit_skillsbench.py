"""Run the pinned official oracle with a verified, offline uv bootstrap."""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import importlib.metadata
import json
from pathlib import Path

from benchflow._utils.task_authoring import task_digest
from benchflow.sdk import SDK
from verify_skill_oracle import verify

UV_SHA256 = "b26fcc8dfa1c39b5a5613445af3be3eefda45d9a39359bee271eafe34913583e"
UV_TASKS = {"shock-analysis-supply", "weighted-gdp-calc", "python-scala-translation"}


def dependency_hook(archive: Path, task_name: str):
    if importlib.metadata.version("benchflow") != "0.6.3":
        raise ValueError("This environment adapter requires BenchFlow 0.6.3")
    if hashlib.sha256(archive.read_bytes()).hexdigest() != UV_SHA256:
        raise ValueError("Official uv archive checksum mismatch")

    async def prepare(env):
        # Pinned SDK's exec-wide transport setting. It changes only the dependency
        # source, not the task, solver, verifier, package version or network policy.
        env._persistent_env["UV_DOWNLOAD_URL"] = "file:///opt/eval-deps"
        await env.upload_file(archive, "/opt/eval-deps/uv-x86_64-unknown-linux-gnu.tar.gz")
        command = (
            "mkdir -p /root/.local/bin && "
            "tar -xzf /opt/eval-deps/uv-x86_64-unknown-linux-gnu.tar.gz "
            "-C /root/.local/bin --strip-components=1 "
            "uv-x86_64-unknown-linux-gnu/uv uv-x86_64-unknown-linux-gnu/uvx && "
            "printf 'export PATH=\"/root/.local/bin:$PATH\"\\n' > /root/.local/bin/env && "
            "/root/.local/bin/uv --version"
        )
        result = await env.exec(command, user="root", timeout_sec=120)
        if result.return_code:
            raise RuntimeError("Offline uv bootstrap failed")
        if task_name in {"shock-analysis-supply", "weighted-gdp-calc"}:
            result = await env.exec(
                "/root/.local/bin/uvx --with pytest==8.4.1 "
                "--with pytest-json-ctrf==0.3.5 --with openpyxl==3.1.5 pytest --version",
                user="root",
                timeout_sec=600,
            )
            if result.return_code:
                raise RuntimeError("Official verifier dependencies could not be prepared")

    return prepare


async def execute(args):
    plan = json.loads(args.plan.read_text())
    item = next(i for i in plan["task_packages"] if i["name"] == args.task.name)
    if task_digest(args.task) != item["digest"]:
        raise ValueError("Task package does not match frozen digest")
    args.output.mkdir(exist_ok=False, parents=True, mode=0o700)
    hooks = [dependency_hook(args.uv_archive, args.task.name)] if args.task.name in UV_TASKS else []
    await SDK().run(
        task_path=args.task,
        agent="oracle",
        environment="docker",
        jobs_dir=args.output,
        pre_agent_hooks=hooks,
        source_provenance={
            "task_digest": item["digest"],
            "uv_sha256": UV_SHA256 if hooks else None,
        },
    )
    if task_digest(args.task) != item["digest"]:
        raise ValueError("Task package changed during admission")
    receipt = verify(args.output, task=args.task.name)
    (args.output / "admission.json").write_text(json.dumps(receipt, indent=2))
    print(json.dumps(receipt))
    if not receipt["oracle_passed"]:
        raise SystemExit(1)


def main():
    p = argparse.ArgumentParser(description=__doc__)
    for name in ("plan", "task", "uv-archive", "output"):
        p.add_argument("--" + name, required=True, type=Path)
    asyncio.run(execute(p.parse_args()))


if __name__ == "__main__":
    main()
