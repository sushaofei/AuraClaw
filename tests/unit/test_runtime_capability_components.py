from __future__ import annotations

import asyncio
from dataclasses import replace
from typing import Any

import pytest

from auraclaw.control.ports import RuntimeAssignment
from auraclaw.runtime.capabilities.content_cache import SkillContentCache
from auraclaw.runtime.capabilities.loading import CapabilityLoader
from auraclaw.runtime.capabilities.state import CapabilityState


def _assignment(run: str = "run-a", tenant: str = "tenant-a") -> RuntimeAssignment:
    return RuntimeAssignment(
        tenant_id=tenant,
        root_session_id="root",
        session_id="session",
        run_id=run,
        runtime_id="runtime",
        lease_id="lease",
        fencing_token=1,
        role="worker",
        resource_profile={},
    )


async def _load(
    cache: SkillContentCache,
    assignment: RuntimeAssignment,
    *,
    digest: str = "digest-a",
    path: str = "SKILL.md",
) -> tuple[str, bool]:
    return await cache.load(
        assignment,
        publisher="platform",
        name="skill",
        version="1.0.0",
        package_digest=digest,
        path=path,
    )


class _ContentClient:
    def __init__(self, *, blocked: bool = False) -> None:
        self.calls = 0
        self.started = asyncio.Event()
        self.two_started = asyncio.Event()
        self.ready = asyncio.Event()
        self.cancelled: list[str] = []
        if not blocked:
            self.ready.set()

    async def load_skill_part(
        self,
        assignment: RuntimeAssignment,
        **kwargs: Any,
    ) -> list[dict[str, Any]]:
        self.calls += 1
        self.started.set()
        if self.calls >= 2:
            self.two_started.set()
        try:
            await self.ready.wait()
        except asyncio.CancelledError:
            self.cancelled.append(assignment.run_id)
            raise
        return [{"text": "abcdef"}]


def _cache(client: _ContentClient, **kwargs: Any) -> SkillContentCache:
    return SkillContentCache(
        client,
        max_bytes=kwargs.pop("max_bytes", 1024),  # type: ignore[arg-type]
        max_entries=kwargs.pop("max_entries", 10),
        ttl_seconds=10,
        **kwargs,
    )


def test_legacy_checkpoint_roundtrips_without_defaults_or_nested_aliases() -> None:
    checkpoint = {
        "loaded": {"cap": {"model_tool": {"function": {"name": "original"}}}},
        "active_skills": [{"workflow_state": {"nested": [1]}}],
        "extension": {"future_schema": [None, "keep"]},
    }
    state = CapabilityState.from_checkpoint(checkpoint)
    snapshot = state.to_checkpoint()
    assert snapshot == checkpoint
    assert state.search_count == state.load_count == 0
    assert "search_count" not in snapshot and "candidates" not in snapshot

    checkpoint["extension"]["future_schema"].append("external-change")
    snapshot["loaded"]["cap"]["model_tool"]["function"]["name"] = "changed"
    active = state.active_skills
    active[0]["workflow_state"]["nested"].append(2)
    assert state.loaded["cap"]["model_tool"]["function"]["name"] == "original"
    assert state.active_skills[0]["workflow_state"]["nested"] == [1]
    assert state.to_checkpoint()["extension"]["future_schema"] == [None, "keep"]


def test_loader_output_owns_existing_and_new_nested_descriptors() -> None:
    state = {"loaded": {"old": {"kind": "resource", "resource": {"uri": "old"}}}}
    incoming = {"capability_id": "new", "kind": "resource", "resource": {"uri": "new"}}
    loaded = CapabilityLoader(max_loaded=2, max_schema_bytes=1024).merge_loaded(
        CapabilityState.from_checkpoint(state),
        [incoming],
        allowed_ids={"new"},
    )
    loaded["old"]["resource"]["uri"] = "changed"
    loaded["new"]["resource"]["uri"] = "changed"
    assert state["loaded"]["old"]["resource"]["uri"] == "old"
    assert incoming["resource"]["uri"] == "new"


@pytest.mark.asyncio
async def test_content_cache_coalesces_loads_and_shields_other_waiters() -> None:
    client = _ContentClient(blocked=True)
    cache = _cache(client)
    first = asyncio.create_task(_load(cache, _assignment()))
    await asyncio.wait_for(client.started.wait(), 1)
    second = asyncio.create_task(_load(cache, _assignment()))
    first.cancel()
    with pytest.raises(asyncio.CancelledError):
        await first
    client.ready.set()
    assert await asyncio.wait_for(second, 1) == ("abcdef", False)
    assert client.calls == 1 and client.cancelled == []
    assert await _load(cache, _assignment()) == ("abcdef", True)


@pytest.mark.asyncio
async def test_parallel_runs_count_one_cached_body_once() -> None:
    client = _ContentClient(blocked=True)
    cache = _cache(client, max_bytes=6)
    first = asyncio.create_task(_load(cache, _assignment()))
    second = asyncio.create_task(_load(cache, _assignment("run-b")))
    await asyncio.wait_for(client.two_started.wait(), 1)
    client.ready.set()
    await asyncio.wait_for(asyncio.gather(first, second), 1)
    assert client.calls == 2  # Authority and cancellation are scoped to each run.
    assert await _load(cache, _assignment("run-c")) == ("abcdef", True)
    assert client.calls == 2


@pytest.mark.asyncio
async def test_releasing_one_run_leaves_another_authenticated_load_running() -> None:
    client = _ContentClient(blocked=True)
    cache = _cache(client)
    first = asyncio.create_task(_load(cache, _assignment()))
    second = asyncio.create_task(_load(cache, _assignment("run-b")))
    await asyncio.wait_for(client.two_started.wait(), 1)
    await cache.release_run(_assignment())
    with pytest.raises(asyncio.CancelledError):
        await first
    client.ready.set()
    assert await asyncio.wait_for(second, 1) == ("abcdef", False)
    assert client.cancelled == ["run-a"]


@pytest.mark.asyncio
async def test_cache_isolates_tenant_digest_path_and_retains_bytes_after_run_release() -> None:
    client = _ContentClient()
    cache = _cache(client)
    assignment = _assignment()
    await _load(cache, assignment)
    await cache.release_run(assignment)
    assert await _load(cache, replace(assignment, run_id="new")) == ("abcdef", True)
    assert await _load(cache, _assignment(tenant="tenant-b")) == ("abcdef", False)
    assert await _load(cache, assignment, digest="digest-b") == ("abcdef", False)
    assert await _load(cache, assignment, path="references/a.md") == ("abcdef", False)
    assert client.calls == 4


@pytest.mark.asyncio
@pytest.mark.parametrize("limits", [{"max_entries": 1}, {"max_bytes": 6}])
async def test_cache_evicts_lru_and_expires_without_wall_clock_waits(
    limits: dict[str, int],
) -> None:
    now = [100.0]
    client = _ContentClient()
    cache = _cache(client, clock=lambda: now[0], **limits)
    assignment = _assignment()
    await _load(cache, assignment)
    await _load(cache, assignment, digest="digest-b")
    assert await _load(cache, assignment, digest="digest-b") == ("abcdef", True)
    assert await _load(cache, assignment) == ("abcdef", False)
    now[0] += 10
    assert await _load(cache, assignment) == ("abcdef", False)
    assert client.calls == 4
