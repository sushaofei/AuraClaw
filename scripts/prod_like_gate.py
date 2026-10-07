from __future__ import annotations

import argparse
import asyncio
import socket
import sys
import time
import xml.etree.ElementTree as ET
from pathlib import Path

ROOT = Path(__file__).parents[1]


def wait_for_endpoints(endpoints: tuple[tuple[str, int], ...], timeout: float) -> None:
    deadline = time.monotonic() + timeout
    pending = set(endpoints)
    while pending and time.monotonic() < deadline:
        for endpoint in tuple(pending):
            try:
                with socket.create_connection(endpoint, timeout=1):
                    pending.remove(endpoint)
            except OSError:
                pass
        if pending:
            time.sleep(1)
    if pending:
        rendered = ", ".join(f"{host}:{port}" for host, port in sorted(pending))
        raise RuntimeError(f"prod-like dependencies did not become ready: {rendered}")


async def apply_roles(database_url: str) -> None:
    import asyncpg

    connection = await asyncpg.connect(
        database_url.replace("postgresql+asyncpg://", "postgresql://", 1)
    )
    try:
        await connection.execute((ROOT / "deploy/postgres/roles.sql").read_text())
    finally:
        await connection.close()


def verify_junit(path: Path, minimum_tests: int) -> None:
    root = ET.parse(path).getroot()
    suites = [root] if root.tag == "testsuite" else list(root.findall("testsuite"))
    totals = {
        name: sum(int(suite.attrib.get(name, "0")) for suite in suites)
        for name in ("tests", "failures", "errors", "skipped")
    }
    if totals["tests"] < minimum_tests:
        raise RuntimeError(
            f"prod-like suite ran {totals['tests']} tests; expected at least {minimum_tests}"
        )
    bad = {name: totals[name] for name in ("failures", "errors", "skipped") if totals[name]}
    if bad:
        rendered = ", ".join(f"{name}={count}" for name, count in bad.items())
        raise RuntimeError(f"prod-like suite is not clean: {rendered}")


def main() -> int:
    parser = argparse.ArgumentParser(description="enforce AuraClaw prod-like CI evidence")
    subparsers = parser.add_subparsers(dest="command", required=True)
    wait = subparsers.add_parser("wait")
    wait.add_argument("--endpoint", action="append", required=True)
    wait.add_argument("--timeout", type=float, default=90)
    roles = subparsers.add_parser("apply-roles")
    roles.add_argument("--database-url", required=True)
    report = subparsers.add_parser("verify-junit")
    report.add_argument("--report", type=Path, required=True)
    report.add_argument("--minimum-tests", type=int, required=True)
    args = parser.parse_args()
    try:
        if args.command == "wait":
            endpoints = tuple(
                (value.rsplit(":", 1)[0], int(value.rsplit(":", 1)[1]))
                for value in args.endpoint
            )
            wait_for_endpoints(endpoints, args.timeout)
        elif args.command == "apply-roles":
            asyncio.run(apply_roles(args.database_url))
        else:
            verify_junit(args.report, args.minimum_tests)
    except (OSError, RuntimeError, ValueError, ET.ParseError) as exc:
        print(f"prod-like gate failed: {exc}")
        return 1
    print(f"prod-like gate passed: {args.command}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
