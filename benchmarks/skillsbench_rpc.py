"""Private stdio worker: official sandbox lifecycle, never model/provider credentials."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
import shlex
import sys
from pathlib import Path

from admit_skillsbench import UV_TASKS, dependency_hook
from benchflow._utils.task_authoring import task_digest
from benchflow.environment.manifest import EnvironmentManifest
from benchflow.rollout import Rollout, RolloutConfig
from verify_skill_oracle import verify


def emit(value):
    print(json.dumps(value, ensure_ascii=False), flush=True)


async def serve(args):
    if args.image and not re.fullmatch(r"sha256:[0-9a-f]{64}", args.image):
        raise ValueError("Prebuilt admission requires an immutable image ID")
    item = next(
        i for i in json.loads(args.plan.read_text())["task_packages"] if i["name"] == args.task.name
    )
    if task_digest(args.task) != item["digest"]:
        raise ValueError("Frozen task digest mismatch")
    if not verify(args.oracle, task=args.task.name)["oracle_passed"]:
        raise ValueError("Oracle admission is required")
    config = RolloutConfig(
        environment_manifest=EnvironmentManifest(name="skillsbench-admitted", image=args.image)
        if args.image
        else None,
        task_path=args.task,
        agent="claude-agent-acp",
        environment="docker",
        skip_agent_install=True,
        jobs_dir=args.output,
        skill_mode="no-skill",
        agent_env={},
        pre_agent_hooks=(
            [dependency_hook(args.uv_archive, args.task.name)] if args.task.name in UV_TASKS else []
        ),
    )
    rollout = Rollout(config)
    graded = False
    try:
        await rollout.setup()
        if args.image:
            # SDK cleanup(delete=True) removes even shared immutable images.
            # Its public stop(False) still removes this attempt's containers and
            # network, while retaining the admitted image for the next trial.
            original_stop = rollout.env.stop

            async def stop_preserving_image(delete):
                await original_stop(delete=False)

            rollout.env.stop = stop_preserving_image
        await rollout.start()
        await rollout.install_agent()
        # Official start uploads oracle files. Remove them before exposing any shell.
        cleaned = await rollout.env.exec(
            "rm -rf /oracle /solution /verifier /tests && "
            "test ! -e /oracle && test ! -e /solution && test ! -e /verifier && test ! -e /tests",
            user="root",
            timeout_sec=30,
        )
        if cleaned.return_code:
            raise RuntimeError("Could not isolate hidden grading files")
        probe = await rollout.env.exec("id -u; test ! -r /var/run/docker.sock", user="agent")
        if probe.return_code or probe.stdout.splitlines()[0] == "0":
            raise RuntimeError("Agent sandbox isolation failed")
        emit(
            {
                "ready": True,
                "instruction": "\n".join(rollout._resolved_prompts),
                "timeout_seconds": rollout._task.config.agent.timeout_sec,
                "task_digest": item["digest"],
                "hidden_files_removed": True,
            }
        )
        while line := await asyncio.to_thread(sys.stdin.readline):
            request = json.loads(line)
            try:
                if request["method"] == "close":
                    emit({"closed": True})
                    break
                if request["method"] == "execute" and not graded:
                    result = await rollout.env.exec(
                        "timeout --signal=TERM --kill-after=5s 120s bash -lc "
                        + shlex.quote(request["command"]),
                        user="agent",
                        cwd=rollout._agent_cwd,
                        timeout_sec=140,
                    )
                    output = (result.stdout or "") + (result.stderr or "")
                    emit(
                        {
                            "return_code": result.return_code,
                            "output": output[:32000],
                            "truncated": len(output) > 32000,
                        }
                    )
                elif request["method"] == "grade" and not graded:
                    graded = True
                    reward = await rollout.verify()
                    if rollout._verifier_error or reward is None:
                        raise RuntimeError(
                            "Official verifier failed: " + str(rollout._verifier_error)
                        )
                    logs = rollout._rollout_paths.verifier_dir
                    if args.task.name == "python-scala-translation":
                        output = logs / "output.log"
                        if not output.exists() or "EVALUATION SUMMARY" not in output.read_text():
                            raise RuntimeError("Official Scala verifier did not finish")
                    else:
                        report = logs / "ctrf.json"
                        if not report.exists():
                            raise RuntimeError("Official verifier produced no test report")
                        counts = json.loads(report.read_text())["results"]["summary"]
                        if not isinstance(counts.get("tests"), int) or counts["tests"] <= 0:
                            raise RuntimeError("Official verifier ran no tests")
                    emit({"reward": reward["reward"]})
                else:
                    raise ValueError("Unknown method or attempt is already graded")
            except Exception as exc:
                emit({"error": type(exc).__name__, "message": str(exc)[:1000]})
    finally:
        await rollout.cleanup()
        if task_digest(args.task) != item["digest"]:
            raise RuntimeError("Original task package changed")


def main():
    os.umask(0o077)
    p = argparse.ArgumentParser(description=__doc__)
    for name in ("plan", "task", "oracle", "uv-archive", "output"):
        p.add_argument("--" + name, required=True, type=Path)
    p.add_argument("--image")
    asyncio.run(serve(p.parse_args()))


if __name__ == "__main__":
    main()
