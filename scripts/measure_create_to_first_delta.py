#!/usr/bin/env python3
"""Measure create-task → first SSE model.output.delta on a live AuraClaw stack.

Intended for production-isomorphic topologies (compose.services / DEV_SERVICE),
not `auraclaw serve`. Uses only the Python standard library.

Example:
  python scripts/measure_create_to_first_delta.py \\
    --base-url http://127.0.0.1:8080 \\
    --goal '用一句话介绍你自己'
"""

from __future__ import annotations

import argparse
import json
import sys
import time
import uuid
import urllib.error
import urllib.request
from typing import Any


def _request(
    method: str,
    url: str,
    *,
    headers: dict[str, str],
    body: bytes | None = None,
    timeout: float,
) -> tuple[int, bytes]:
    req = urllib.request.Request(url, data=body, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as response:
            return int(response.status), response.read()
    except urllib.error.HTTPError as exc:
        return int(exc.code), exc.read()


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
        "Accept": "text/event-stream",
    }
    started = time.perf_counter()
    create_ms: float | None = None
    visible_ms: float | None = None
    first_delta_ms: float | None = None
    session_id = ""
    run_id = ""

    status, payload = _request(
        "POST",
        f"{base}/v1/tasks",
        headers={
            **headers,
            "Idempotency-Key": f"ttft-{uuid.uuid4().hex}",
        },
        body=json.dumps({"goal": goal}).encode(),
        timeout=timeout,
    )
    if status >= 400:
        print(f"create failed status={status} body={payload!r}", file=sys.stderr)
        return 1
    create_ms = (time.perf_counter() - started) * 1_000
    body = json.loads(payload.decode())
    session_id = str(body["session_id"])
    run_id = str(body["run_id"])

    deadline = time.perf_counter() + timeout
    while time.perf_counter() < deadline:
        code, _ = _request(
            "GET",
            f"{base}/v1/tasks/{session_id}",
            headers={"X-Tenant-ID": tenant_id, "X-Actor-ID": actor_id},
            timeout=min(5.0, timeout),
        )
        if code == 200:
            visible_ms = (time.perf_counter() - started) * 1_000
            break
        time.sleep(0.02)

    stream_req = urllib.request.Request(
        f"{base}/v1/streams/{session_id}",
        headers={
            "X-Tenant-ID": tenant_id,
            "X-Actor-ID": actor_id,
            "Accept": "text/event-stream",
        },
        method="GET",
    )
    try:
        with urllib.request.urlopen(stream_req, timeout=timeout) as response:
            buffer = ""
            while time.perf_counter() < deadline:
                chunk = response.read(256)
                if not chunk:
                    break
                buffer += chunk.decode("utf-8", errors="replace")
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
                                    "task_visible_ms": None
                                    if visible_ms is None
                                    else round(visible_ms, 2),
                                    "first_delta_ms": round(first_delta_ms, 2),
                                    "goal": goal,
                                },
                                ensure_ascii=False,
                            )
                        )
                        return 0
    except Exception as exc:  # noqa: BLE001 - probe reports failure payload
        print(
            json.dumps(
                {
                    "session_id": session_id,
                    "run_id": run_id,
                    "create_ms": None if create_ms is None else round(create_ms, 2),
                    "task_visible_ms": None
                    if visible_ms is None
                    else round(visible_ms, 2),
                    "first_delta_ms": None,
                    "error": str(exc),
                },
                ensure_ascii=False,
            ),
            file=sys.stderr,
        )
        return 1

    print(
        json.dumps(
            {
                "session_id": session_id,
                "run_id": run_id,
                "create_ms": None if create_ms is None else round(create_ms, 2),
                "task_visible_ms": None if visible_ms is None else round(visible_ms, 2),
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
