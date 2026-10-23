"""Run a frozen private command schedule exactly once, retaining failed attempts."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import signal
import time
from pathlib import Path

from run_longmemeval import write_private


def interrupted_record(job, elapsed, reason):
    return {
        **job["record"],
        "schema_version": 1,
        "status": "infra_error",
        "error_type": reason,
        "success": False,
        "official_reward": None,
        "elapsed_seconds": elapsed,
        "model_calls": None,
        "tool_calls": None,
        "dataset_revision_verified": False,
    }


async def run_job(job):
    output = Path(job["output"])
    started = output.with_suffix(".started.json")
    if await asyncio.to_thread(output.exists):
        record = json.loads(await asyncio.to_thread(output.read_text))
        if any(record[k] != job["record"][k] for k in ("task_id", "trial", "config")):
            raise ValueError("Existing attempt differs from frozen schedule")
        return
    if started.exists():
        # A crash after dispatch may already have consumed a model attempt.
        # Do not silently rerun it or replace its incomplete evidence.
        raise RuntimeError("Unresolved dispatched attempt: " + str(output))
    write_private(started, {"started_at_unix": time.time()})
    begin = time.monotonic()
    with output.with_suffix(".process.log").open("x") as log:
        process = await asyncio.create_subprocess_exec(
            *job["command"],
            stdout=log,
            stderr=log,
            start_new_session=True,
        )
        reason = "WorkerExit"
        try:
            code = await asyncio.wait_for(process.wait(), job["process_timeout"])
        except TimeoutError:
            reason = "WorkerProcessTimeout"
            os.killpg(process.pid, signal.SIGTERM)
            try:
                await asyncio.wait_for(process.wait(), 30)
            except TimeoutError:
                os.killpg(process.pid, signal.SIGKILL)
                await process.wait()
            code = 124
    if not await asyncio.to_thread(output.exists):
        write_private(output, interrupted_record(job, time.monotonic() - begin, reason))
    write_private(output.with_suffix(".finished.json"), {"exit_code": code})
    record = json.loads(await asyncio.to_thread(output.read_text))
    print(
        json.dumps(
            {
                "task": record["task_id"],
                "trial": record["trial"],
                "condition": record["condition"],
                "status": record["status"],
                "reward": record["official_reward"],
            }
        ),
        flush=True,
    )


async def execute(path):
    plan = json.loads(path.read_text())
    jobs = plan["jobs"]
    if len({j["output"] for j in jobs}) != len(jobs):
        raise ValueError("Duplicate output in schedule")

    # Each lane is serial; independent benchmark lanes may share the Gateway.
    async def lane(name):
        for job in jobs:
            if job["lane"] == name:
                await run_job(job)
                await asyncio.sleep(plan["between_attempts_seconds"])

    await asyncio.gather(*(lane(name) for name in sorted({j["lane"] for j in jobs})))
    print("Complete scheduled attempts: " + str(len(jobs)), flush=True)


if __name__ == "__main__":
    os.umask(0o077)
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("schedule", type=Path)
    asyncio.run(execute(parser.parse_args().schedule))
