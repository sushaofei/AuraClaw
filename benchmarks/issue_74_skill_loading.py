"""Reproducible capacity and Runtime hot-path benchmark for Issue #74.

Run from the repository root:

    PYTHONPATH=src .venv/bin/python benchmarks/issue_74_skill_loading.py

The capacity matrix is deterministic and reports object-store demand. The Runtime
section measures this process and is intentionally reported separately because its
latencies depend on the host running the benchmark.
"""

from __future__ import annotations

import asyncio
import statistics
import time
from dataclasses import dataclass
from typing import Any

from auraclaw.control.ports import RuntimeAssignment
from auraclaw.runtime.capability_controller import RuntimeCapabilityController

PUBLICATION_COUNTS = (10, 100, 1000)
PACKAGE_SIZES = (10 * 1024, 100 * 1024, 1024 * 1024)
REPLICA_COUNTS = (1, 2, 4)


def _mib(value: int) -> float:
    return value / (1024 * 1024)


def capacity_matrix() -> None:
    print("capacity_matrix")
    print("replicas,publications,package_kib,phase,downloads,download_mib")
    for replicas in REPLICA_COUNTS:
        for publications in PUBLICATION_COUNTS:
            for package_size in PACKAGE_SIZES:
                # Every new replica must establish an authoritative local snapshot.
                cold_downloads = replicas * publications
                # Registry digest reuse makes unchanged reconciliation download-free.
                hot_downloads = 0
                # A single replacement replica is the rolling-restart burst unit.
                restart_downloads = publications
                for phase, downloads in (
                    ("cold_all_replicas", cold_downloads),
                    ("hot_reconcile", hot_downloads),
                    ("one_replica_restart", restart_downloads),
                ):
                    print(
                        f"{replicas},{publications},{package_size // 1024},"
                        f"{phase},{downloads},{_mib(downloads * package_size):.3f}"
                    )


@dataclass
class _ContentClient:
    body: str
    calls: int = 0

    async def load_skill_part(self, *args: Any, **kwargs: Any) -> list[dict[str, Any]]:
        del args, kwargs
        self.calls += 1
        await asyncio.sleep(0)
        return [{"text": self.body}]


def _assignment(replica: int) -> RuntimeAssignment:
    return RuntimeAssignment(
        tenant_id="benchmark",
        root_session_id="session",
        session_id="session",
        run_id="run",
        runtime_id=f"runtime-{replica}",
        lease_id=f"lease-{replica}",
        fencing_token=1,
        role="root",
        resource_profile={},
    )


def _state(active_skills: int) -> dict[str, Any]:
    return {
        "active_skills": [
            {
                "binding": {
                    "publisher": "benchmark",
                    "skill_name": f"skill-{index}",
                    "skill_version": "1.0.0",
                    "package_digest": f"digest-{index}",
                    "resolved_skills": (),
                }
            }
            for index in range(active_skills)
        ]
    }


def _percentile(samples: list[float], fraction: float) -> float:
    ordered = sorted(samples)
    index = min(len(ordered) - 1, max(0, round((len(ordered) - 1) * fraction)))
    return ordered[index]


async def runtime_hotpath() -> None:
    print("runtime_hotpath")
    print(
        "replicas,active_skills,body_kib,cold_downloads,hot_downloads,"
        "hot_p50_ms,hot_p95_ms,hot_p99_ms"
    )
    for replicas in REPLICA_COUNTS:
        for active_skills in (1, 4, 16):
            for body_size in PACKAGE_SIZES:
                clients = [_ContentClient("x" * body_size) for _ in range(replicas)]
                controllers = [
                    RuntimeCapabilityController(
                        client,  # type: ignore[arg-type]
                        skill_content_cache_max_bytes=max(
                            16 * 1024 * 1024, active_skills * body_size
                        ),
                        skill_prompt_max_bytes=max(
                            256 * 1024, active_skills * (body_size + 256)
                        ),
                        skill_prompt_max_estimated_tokens=max(
                            65_536, active_skills * (body_size + 256)
                        ),
                    )
                    for client in clients
                ]
                state = _state(active_skills)
                for replica, controller in enumerate(controllers):
                    await controller.trusted_messages(_assignment(replica), state)
                cold_downloads = sum(client.calls for client in clients)

                samples: list[float] = []
                before_hot = cold_downloads
                for _ in range(20):
                    started = time.perf_counter()
                    await asyncio.gather(
                        *(
                            controller.trusted_messages(_assignment(replica), state)
                            for replica, controller in enumerate(controllers)
                        )
                    )
                    samples.append((time.perf_counter() - started) * 1000)
                hot_downloads = sum(client.calls for client in clients) - before_hot
                print(
                    f"{replicas},{active_skills},{body_size // 1024},"
                    f"{cold_downloads},{hot_downloads},"
                    f"{statistics.median(samples):.3f},"
                    f"{_percentile(samples, 0.95):.3f},"
                    f"{_percentile(samples, 0.99):.3f}"
                )
                for replica, controller in enumerate(controllers):
                    await controller.release_run(_assignment(replica))


async def main() -> None:
    capacity_matrix()
    await runtime_hotpath()


if __name__ == "__main__":
    asyncio.run(main())
