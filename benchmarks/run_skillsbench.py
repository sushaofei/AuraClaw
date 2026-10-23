"""AuraClaw Runtime over a private official SkillsBench sandbox worker."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
from dataclasses import asdict
from pathlib import Path

from plan_components import checked_checkout
from run_longmemeval import write_private

from auraclaw.composition.evaluation import run_benchmark_episode
from auraclaw.evaluation.components.skillsbench import SkillsBenchEnvironment
from auraclaw.evaluation.contracts import EvaluationConfig
from auraclaw.infrastructure.clients.model import RemoteModelClient


class DiagnosticModelClient(RemoteModelClient):
    """Keep transport errors in private diagnostics without altering retry policy."""

    diagnostic_path: Path

    async def generate_stream(self, request):
        try:
            async for chunk in super().generate_stream(request):
                yield chunk
        except Exception as exc:
            write_private(self.diagnostic_path, {"type": type(exc).__name__, "message": str(exc)})
            raise


class SkillFiles:
    def __init__(self, root: Path):
        self.root = root.resolve()
        paths = list(root.glob("*/SKILL.md"))
        if any(p.is_symlink() or not p.resolve().is_relative_to(self.root) for p in paths):
            raise ValueError("Skill metadata escapes the frozen package")
        self.entries = {p.parent.name: p.parent for p in paths}

    async def list_skills(self):
        result = []
        for name, folder in sorted(self.entries.items()):
            text = (folder / "SKILL.md").read_text()
            description = next(
                (
                    line.split(":", 1)[1].strip()
                    for line in text.splitlines()
                    if line.startswith("description:")
                ),
                name,
            )
            result.append({"name": name, "description": description, "version": "frozen-task"})
        return result

    async def read(self, name: str, path: str) -> str:
        folder = self.entries[name]
        target = folder / path
        if target.is_symlink() or not target.resolve().is_relative_to(folder.resolve()):
            raise ValueError("Skill path escapes admitted package")
        if not target.is_file() or target.stat().st_size > 256000:
            raise ValueError("Skill text part is absent or exceeds declared 256KB cap")
        return target.read_text()


class RemoteSandbox:
    def __init__(self, process):
        self.process = process

    async def receive(self, wait_seconds=900):
        line = await asyncio.wait_for(self.process.stdout.readline(), wait_seconds)
        if not line:
            raise RuntimeError("Official sandbox worker disconnected")
        result = json.loads(line)
        if result.get("error"):
            raise RuntimeError(result["error"] + ": " + result.get("message", ""))
        return result

    async def request(self, value, wait_seconds=900):
        self.process.stdin.write(json.dumps(value).encode() + b"\n")
        await self.process.stdin.drain()
        return await self.receive(wait_seconds)

    async def execute(self, command):
        return json.dumps(await self.request({"method": "execute", "command": command}, 150))

    async def close(self):
        if self.process.returncode is None:
            try:
                await self.request({"method": "close"}, 30)
                self.process.stdin.close()
                await asyncio.wait_for(self.process.wait(), 60)
            except (TimeoutError, BrokenPipeError, RuntimeError):
                self.process.kill()
                await self.process.wait()


async def execute(args):
    checked_checkout(args.packages, args.package_revision)
    command = json.loads(args.worker_command.read_text())
    with args.output.with_suffix(".worker.log").open("x") as log:
        process = await asyncio.create_subprocess_exec(
            *command,
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=log,
            limit=1_000_000,
        )
        sandbox = RemoteSandbox(process)
        client = DiagnosticModelClient(
            args.gateway_url, bearer_token=os.environ["AURACLAW_EVAL_GATEWAY_TOKEN"], timeout=180
        )
        client.diagnostic_path = args.output.with_suffix(".model-error.json")
        try:
            ready = await sandbox.receive()
            if not ready.get("ready") or not ready.get("hidden_files_removed"):
                raise ValueError("Sandbox was not admitted")
            plan = json.loads(args.plan.read_text())
            item = next(i for i in plan["task_packages"] if i["name"] == args.task)
            if ready["task_digest"] != item["digest"]:
                raise ValueError("Worker task differs from frozen cohort")
            skills = (
                SkillFiles(args.packages / item["path"] / "environment/skills")
                if args.condition == "with-skill"
                else None
            )

            async def grade():
                result = await sandbox.request({"method": "grade"}, 3000)
                write_private(args.output.with_suffix(".grader.json"), result)
                return float(result["reward"])

            config = EvaluationConfig(
                "skillsbench",
                "registry-v1.1",
                args.model,
                args.revision,
                "sha256:" + plan["registry_sha256"],
                max_actions=30,
                max_output_tokens=49152,
                per_turn_output_tokens=8192,
                timeout_seconds=ready["timeout_seconds"],
                seed=20261022,
            )
            env = SkillsBenchEnvironment(
                ready["instruction"],
                sandbox=sandbox,
                skills=skills,
                verify=grade,
                oracle_verified=True,
            )
            record, trace = await asyncio.wait_for(
                run_benchmark_episode(
                    env,
                    client,
                    config,
                    task_id=args.task,
                    trial=args.trial,
                    profile="auraclaw-skills-reference-v1",
                ),
                timeout=ready["timeout_seconds"] + 3000,
            )
            record.update(
                validation_scope="official-sdk-adapter",
                condition=args.condition,
                dataset_revision_verified=True,
                task_digest=item["digest"],
                config=asdict(config),
                run_scope=args.scope,
                model_snapshot_locked=False,
                skill_scope="reference-file-port-not-production-skill-service",
            )
            write_private(args.output.with_suffix(".trace.json"), trace)
            write_private(args.output, record)
            print(
                json.dumps(
                    {k: record[k] for k in ("status", "success", "official_reward", "tool_calls")}
                )
            )
        finally:
            await client.aclose()
            await sandbox.close()


def main():
    os.umask(0o077)
    p = argparse.ArgumentParser(description=__doc__)
    for name in ("packages", "plan", "output", "worker-command"):
        p.add_argument("--" + name, required=True, type=Path)
    for name in ("task", "package-revision", "model", "revision", "gateway-url"):
        p.add_argument("--" + name, required=True)
    p.add_argument("--condition", choices=("with-skill", "without-skill"), required=True)
    p.add_argument("--trial", type=int, default=0)
    p.add_argument("--scope", choices=("canary", "controlled-batch"), default="canary")
    asyncio.run(execute(p.parse_args()))


if __name__ == "__main__":
    main()
