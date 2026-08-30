from __future__ import annotations

import asyncio
from datetime import timedelta

import httpx
import pytest

from auraclaw.contracts.commands import CommandContext
from auraclaw.contracts.events import Actor, NewEvent
from auraclaw.contracts.internal import ServiceIdentity
from auraclaw.control.internal_service import ControlInternalService
from auraclaw.control.ports import RunnableItem, RuntimeAssignment, RuntimeInstance
from auraclaw.control.runnable_feed import RunnableFeedConsumer
from auraclaw.infrastructure.clients.runtime import RemoteRuntimeControlClient
from auraclaw.infrastructure.persistence.memory_control_store import (
    InMemoryControlStateStore,
)
from auraclaw.infrastructure.persistence.memory_event_store import InMemoryEventStore
from auraclaw.internal.http import create_contract_app
from auraclaw.internal.routes import control_routes
from auraclaw.internal.security import (
    InMemoryFencingTokenLedger,
    LeaseAssertionSigner,
    LeaseAssertionVerifier,
)


def _item(task_id: str = "tenant-hitl:session-hitl:run-hitl") -> RunnableItem:
    return RunnableItem(
        task_id=task_id,
        tenant_id="tenant-hitl",
        root_session_id="session-hitl",
        session_id="session-hitl",
        run_id="run-hitl",
        source_version=2,
    )


async def _assigned_store() -> tuple[
    InMemoryControlStateStore, RuntimeAssignment, RunnableItem
]:
    store = InMemoryControlStateStore()
    item = _item()
    runtime = RuntimeInstance(
        runtime_id="runtime-hitl",
        runtime_type="agent",
        role="root",
        node_id="node-hitl",
        capabilities={},
        capacity=1,
        registration_id="legacy",
    )
    await store.register_runtime(runtime)
    assert await store.enqueue(item)
    claim = (await store.claim("orchestrator-hitl"))[0]
    lease = await store.acquire_lease(
        "session:tenant-hitl:session-hitl",
        "orchestrator-hitl",
        ttl=timedelta(minutes=1),
    )
    assert lease is not None
    assignment = RuntimeAssignment(
        tenant_id=item.tenant_id,
        root_session_id=item.root_session_id,
        session_id=item.session_id,
        run_id=item.run_id,
        runtime_id=runtime.runtime_id,
        lease_id=lease.lease_id,
        fencing_token=lease.fencing_token,
        role=item.role,
        resource_profile={},
        lease_expires_at=lease.expires_at,
    )
    assert await store.assign(
        item.task_id, assignment, claim_token=claim.claim_token
    )
    return store, assignment, item


@pytest.mark.asyncio
async def test_remote_waiting_for_human_ack_releases_runtime_capacity() -> None:
    store, assignment, item = await _assigned_store()
    key = b"test-hitl-runtime-signing-key-00001"
    service = ControlInternalService(
        store,
        lease_verifier=LeaseAssertionVerifier(
            {"test": key},
            ledger=InMemoryFencingTokenLedger(),
            audience=("control", "runtime"),
        ),
        lease_signer=LeaseAssertionSigner(key_id="test", signing_key=key),
    )
    app = create_contract_app(
        "orchestrator",
        control_routes(service),
        workload_identities={"runtime-token": ServiceIdentity.AGENT_RUNTIME},
    )
    client = RemoteRuntimeControlClient(
        "http://control.test",
        bearer_token="runtime-token",
        runtime_id=assignment.runtime_id,
        role="root",
        node_id="node-hitl",
        capacity=1,
        registration_id="legacy",
        transport=httpx.ASGITransport(app=app),
    )
    await client.register()
    claimed = await client.claim()
    assert len(claimed) == 1

    await client.finish_assignment(item.task_id, "waiting_for_human")

    assert await store.select_runtime(_item("tenant-hitl:other:run-other")) is not None
    replacement = await store.acquire_lease(
        "session:tenant-hitl:session-hitl",
        "orchestrator-resume",
        ttl=timedelta(minutes=1),
    )
    assert replacement is not None
    await client.aclose()


@pytest.mark.asyncio
async def test_approval_event_requeues_waiting_assignment() -> None:
    store, assignment, item = await _assigned_store()
    await store.finish_assignment(item.task_id, "waiting_for_human")
    events = InMemoryEventStore()
    context = CommandContext(
        command_id="create-hitl",
        tenant_id=item.tenant_id,
        actor=Actor(type="user", id="user-hitl"),
        correlation_id="corr-hitl",
        expected_version=0,
        operation="create_task",
    )
    await events.append(
        root_session_id=item.root_session_id,
        session_id=item.session_id,
        run_id=item.run_id,
        context=context,
        events=(
            NewEvent(
                type="session.created",
                payload={
                    "goal": "approve tool",
                    "role": "root",
                    "root_session_id": item.root_session_id,
                },
            ),
            NewEvent(type="run.requested", payload={"run_id": item.run_id}),
        ),
        command_result={},
    )
    feed = RunnableFeedConsumer(events, store, worker_id="orchestrator-resume")
    await feed.run_once()
    await asyncio.sleep(0)
    await events.append(
        root_session_id=item.root_session_id,
        session_id=item.session_id,
        run_id=item.run_id,
        context=CommandContext(
            command_id="approve-hitl",
            tenant_id=item.tenant_id,
            actor=Actor(type="user", id="approver-hitl"),
            correlation_id="corr-hitl",
            expected_version=2,
            operation="record_approval_response",
        ),
        events=(
            NewEvent(
                type="approval.approved",
                payload={"approval_id": "approval-hitl", "decision": "approved"},
            ),
        ),
        command_result={},
    )

    assert await feed.run_once() == 1
    resumed_claim = (await store.claim("orchestrator-resume"))[0]
    lease = await store.acquire_lease(
        "session:tenant-hitl:session-hitl",
        "orchestrator-resume",
        ttl=timedelta(minutes=1),
    )
    assert lease is not None
    resumed = RuntimeAssignment(
        tenant_id=item.tenant_id,
        root_session_id=item.root_session_id,
        session_id=item.session_id,
        run_id=item.run_id,
        runtime_id=assignment.runtime_id,
        lease_id=lease.lease_id,
        fencing_token=lease.fencing_token,
        role=item.role,
        resource_profile={},
        lease_expires_at=lease.expires_at,
    )
    assert await store.assign(
        item.task_id, resumed, claim_token=resumed_claim.claim_token
    )
