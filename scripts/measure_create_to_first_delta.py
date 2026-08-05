#!/usr/bin/env python3
"""Measure create-task → first SSE model.output.delta on a live AuraClaw stack.

Intended for production-isomorphic topologies (compose.services / DEV_SERVICE),
not `auraclaw serve`.

Example:
  python scripts/measure_create_to_first_delta.py \\
    --base-url http://10.244.16.131:8080 \\
    --goal '用一句话介绍你自己'
"""

from __future__ import annotations

import argparse
import json
import sys
import time
import uuid
from typing import Any

import httpx


def _parse_sse_block(block: str) -> tuple[str | None, dict[str, Any] | None]:
    event_type: str | None = None
    data_lines: list[str] = []
    for line in block.splitlines():
        if line.startswith("event:"):
            event_type = line[6:].strip()
        elif line.startswith("data:"):
            data_lines.append(line[5:].lstrip())
    if not data_lines:
        return event_type, None
    raw = "\n".join(data_lines)
    try:
        return event_type, json.loads(raw)
    except json.JSONDecodeError:
        return event_type, None


def measure(
    *,
    base_url: str,
    goal: str,
    tenant_id: str,
    actor_id: str,
    timeout: float,
) -> int:
    base = base_url.rstrip("/")
    headers = {
        "X-Tenant-ID": tenant_id,
        "X-Actor-ID": actor_id,
        "Content-Type": "application/json",
    }
    started = time.perf_counter()
    create_ms: float | None = None
    authorize_ms: float | None = None
    first_delta_ms: float | None = None
    session_id = ""
    run_id = ""

    with httpx.Client(timeout=timeout) as client:
        create = client.post(
            f"{base}/v1/tasks",
            headers={
                **headers,
                "Idempotency-Key": f"ttft-{uuid.uuid4().hex}",
            },
            json={"goal": goal},
        )
        create.raise_for_status()
        create_ms = (time.perf_counter() - started) * 1_000
        body = create.json()
        session_id = str(body["session_id"])
        run_id = str(body["run_id"])

        # Projection may lag slightly; retry authorize/open until ready.
        stream_url = f"{base}/v1/streams/{session_id}"
        deadline = time.perf_counter() + timeout
        while time.perf_counter() < deadline:
            with client.stream("GET", stream_url, headers=headers) as response:
                if response.status_code == 404:
                    time.sleep(0.05)
                    continue
                response.raise_for_status()
                authorize_ms = (time.perf_counter() - started) * 1_000
                buffer = ""
                for chunk in response.iter_text():
                    buffer += chunk
                    while "\n\n" in buffer:
                        block, buffer = buffer.split("\n\n", 1)
                        event_type, data = _parse_sse_block(block)
                        if event_type == "model.output.delta" or (
                            isinstance(data, dict)
                            and data.get("type") == "model.output.delta"
                        ):
                            first_delta_ms = (time.perf_counter() - started) * 1_000
                            print(
                                json.dumps(
                                    {
                                        "session_id": session_id,
                                        "run_id": run_id,
                                        "create_ms": round(create_ms, 2),
                                        "stream_open_ms": round(authorize_ms, 2),
                                        "first_delta_ms": round(first_delta_ms, 2),
                                        "goal": goal,
                                    },
                                    ensure_ascii=False,
                                )
                            )
                            return 0
                    if time.perf_counter() >= deadline:
                        break
            if first_delta_ms is not None:
                break
            time.sleep(0.05)

    print(
        json.dumps(
            {
                "session_id": session_id,
                "run_id": run_id,
                "create_ms": None if create_ms is None else round(create_ms, 2),
                "stream_open_ms": None
                if authorize_ms is None
                else round(authorize_ms, 2),
                "first_delta_ms": None,
                "error": "timeout_waiting_for_first_delta",
            },
            ensure_ascii=False,
        ),
        file=sys.stderr,
    )
    return 1


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--base-url",
        default="http://127.0.0.1:8080",
        help="AuraClaw ingress base URL",
    )
    parser.add_argument("--goal", default="用一句话介绍你自己")
    parser.add_argument("--tenant-id", default="local")
    parser.add_argument("--actor-id", default="ttft-probe")
    parser.add_argument("--timeout", type=float, default=60.0)
    args = parser.parse_args(argv)
    return measure(
        base_url=args.base_url,
        goal=args.goal,
        tenant_id=args.tenant_id,
        actor_id=args.actor_id,
        timeout=args.timeout,
    )


if __name__ == "__main__":
    raise SystemExit(main())
