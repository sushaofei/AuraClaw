from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from auraclaw.contracts.errors import RuntimeCancelledError, VersionConflictError
from auraclaw.runtime.event_committer import CanonicalEventCommitter


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["tool.call.completed", "runtime.progress.recorded"])
async def test_concurrent_policy_fact_rebases_only_event_commit(kind):
    policy = SimpleNamespace(type="policy.review.completed", payload={})
    identity_key = "tool_invocation_id" if kind.startswith("tool") else "checkpoint_id"
    settled = SimpleNamespace(type=kind, payload={identity_key: "invocation-1"})
    session = SimpleNamespace(
        append=AsyncMock(side_effect=[VersionConflictError("race"), [settled]]),
        load=AsyncMock(return_value=[policy]),
    )
    guard = SimpleNamespace(check=AsyncMock(), fence=AsyncMock())
    committer = CanonicalEventCommitter(session, guard)
    assignment = SimpleNamespace(run_id="run-1")
    existing = []
    await committer.append_once(
        assignment, existing, kind, settled.payload, identity="invocation-1"
    )
    assert existing == [policy, settled]
    calls = session.append.call_args_list
    assert [c.kwargs["expected_version"] for c in calls] == [0, 1]
    assert calls[0].kwargs["command_id"] == calls[1].kwargs["command_id"]
    assert calls[0].args[1] == calls[1].args[1]
    assert guard.check.await_count == 2
    await committer.append_once(
        assignment, existing, kind, settled.payload, identity="invocation-1"
    )
    assert session.append.await_count == 2


@pytest.mark.asyncio
async def test_conflict_retry_rechecks_cancellation():
    session = SimpleNamespace(
        append=AsyncMock(side_effect=VersionConflictError("race")), load=AsyncMock(return_value=[])
    )
    guard = SimpleNamespace(check=AsyncMock(side_effect=[None, RuntimeCancelledError("cancel")]))
    with pytest.raises(RuntimeCancelledError):
        await CanonicalEventCommitter(session, guard).append_once(
            SimpleNamespace(run_id="r"), [], "tool.call.requested", {}, identity="i"
        )
    assert session.append.await_count == 1


@pytest.mark.asyncio
async def test_conflict_retry_is_bounded():
    session = SimpleNamespace(
        append=AsyncMock(side_effect=VersionConflictError("race")), load=AsyncMock(return_value=[])
    )
    guard = SimpleNamespace(check=AsyncMock())
    with pytest.raises(VersionConflictError):
        await CanonicalEventCommitter(session, guard).append_once(
            SimpleNamespace(run_id="r"), [], "tool.call.completed", {}, identity="i"
        )
    assert session.append.await_count == 8


@pytest.mark.asyncio
async def test_conflict_reload_observes_already_committed_fact():
    event = SimpleNamespace(type="tool.call.completed", payload={"tool_invocation_id": "i"})
    session = SimpleNamespace(
        append=AsyncMock(side_effect=VersionConflictError("race")),
        load=AsyncMock(return_value=[event]),
    )
    guard = SimpleNamespace(check=AsyncMock())
    await CanonicalEventCommitter(session, guard).append_once(
        SimpleNamespace(run_id="r"), [], event.type, event.payload, identity="i"
    )
    assert session.append.await_count == 1
