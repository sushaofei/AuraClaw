import asyncio
from collections import defaultdict
from datetime import timedelta

import pytest

from auraclaw.contracts.commands import CommandContext
from auraclaw.contracts.events import Actor, CanonicalEvent, NewEvent
from auraclaw.control.orchestrator import ManagedOrchestrator, RegisteredRuntimeProvisioner
from auraclaw.control.ports import RuntimeAssignment, RuntimeInstance
from auraclaw.control.runnable_feed import RunnableFeedConsumer
from auraclaw.infrastructure.persistence.memory_control_store import (
    InMemoryControlStateStore,
)
from auraclaw.infrastructure.persistence.memory_event_store import InMemoryEventStore
from auraclaw.runtime.clients import (
    FencedSessionClient,
    FencedToolClient,
    IdempotentToolClient,
    InMemoryRuntimeEventBus,
)
from auraclaw.runtime.harness import AgentHarness
from auraclaw.runtime.model_gateway import ModelGateway, StaticCredentialResolver
from auraclaw.runtime.ports import ModelRequest, ModelResponse


class _StressProvider:
    name = "stress"

    def __init__(self) -> None:
        self.calls = 0

    async def generate(self, request: ModelRequest, *, credential: str) -> ModelResponse:
        del credential
        self.calls += 1
        return ModelResponse(
            model_call_id=request.model_call_id,
            provider=self.name,
            model="stress-model",
            completed_output="done",
            deltas=("done",),
            usage={"input_tokens": 1, "output_tokens": 1},
        )


class _SessionRecorder:
    def __init__(self) -> None:
        self.assignments: list[RuntimeAssignment] = []

    async def append(
        self,
        assignment: RuntimeAssignment,
        events: list[NewEvent],
        *,
        command_id: str,
        operation: str,
        expected_version: int | None = None,
    ) -> list[CanonicalEvent]:
        del events, command_id, operation, expected_version
        self.assignments.append(assignment)
        return []


async def _seed_sessions(events: InMemoryEventStore, count: int) -> None:
    for index in range(count):
        session_id = f"session-stress-{index}"
        run_id = f"run-stress-{index}"
        await events.append(
            root_session_id=session_id,
            session_id=session_id,
            run_id=run_id,
            context=CommandContext(
                command_id=f"command-stress-{index}",
                tenant_id="tenant-stress",
                actor=Actor(type="user", id="stress"),
                correlation_id=run_id,
                expected_version=0,
                operation="stress_seed",
            ),
            events=(
                NewEvent(type="session.created", payload={"goal": str(index)}),
                NewEvent(type="run.requested", payload={"run_id": run_id}),
            ),
            command_result={},
        )


@pytest.mark.asyncio
async def test_four_replicas_process_200_sessions_without_duplicates_or_reordering() -> None:
    session_count = 200
    events = InMemoryEventStore()
    control = InMemoryControlStateStore()
    await _seed_sessions(events, session_count)

    observed: dict[str, list[int]] = defaultdict(list)

    async def relay(worker_id: str) -> None:
        while True:
            claimed = await events.claim_outbox(
                "projection",
                worker_id,
                limit=25,
                claim_ttl=timedelta(seconds=30),
            )
            if not claimed:
                return
            for record in claimed:
                observed[record.event.session_id].append(record.event.aggregate_version)
                assert await events.disposition_outbox(
                    "projection",
                    worker_id,
                    record.outbox_id,
                    record.claim_token,
                    "ack",
                )
            await asyncio.sleep(0)

    await asyncio.gather(*(relay(f"projection-{index}") for index in range(4)))
    assert len(observed) == session_count
    assert all(versions == [1, 2] for versions in observed.values())

    feeds = [
        RunnableFeedConsumer(events, control, worker_id=f"orchestrator-{index}")
        for index in range(4)
    ]
    while sum(await asyncio.gather(*(feed.run_once(limit=25) for feed in feeds))):
        pass

    for index in range(4):
        await control.register_runtime(
            RuntimeInstance(
                runtime_id=f"runtime-stress-{index}",
                runtime_type="agent",
                role="root",
                node_id=f"node-stress-{index}",
                capabilities={},
                capacity=session_count,
            )
        )
    session = _SessionRecorder()
    orchestrators = [
        ManagedOrchestrator(
            orchestrator_id=f"orchestrator-stress-{index}",
            control_store=control,
            session=session,
            provisioner=RegisteredRuntimeProvisioner(control),
        )
        for index in range(4)
    ]
    while True:
        batch = await asyncio.gather(
            *(orchestrator.schedule_once() for orchestrator in orchestrators)
        )
        if not any(batch):
            break
    task_ids = {
        f"{item.tenant_id}:{item.session_id}:{item.run_id}"
        for item in session.assignments
    }
    assert len(session.assignments) == len(task_ids) == session_count
    assert len({item.runtime_id for item in session.assignments}) == 4

    provider = _StressProvider()
    harness = AgentHarness(
        control_store=control,
        session=FencedSessionClient(events, control),
        model=ModelGateway(
            (provider,),
            StaticCredentialResolver({provider.name: "test-only"}),
            default_provider=provider.name,
        ),
        tools=FencedToolClient(IdempotentToolClient(), control),
        runtime_events=InMemoryRuntimeEventBus(),
    )
    claimed = []
    for index in range(4):
        claimed.extend(
            await control.claim_assignments(
                f"runtime-stress-{index}", "root", limit=session_count
            )
        )
    assert len(claimed) == session_count
    await asyncio.gather(*(harness.execute(item.assignment) for item in claimed))
    assert provider.calls == session_count
    completed = 0
    for index in range(session_count):
        canonical = await events.load("tenant-stress", f"session-stress-{index}")
        completed += sum(event.type == "run.completed" for event in canonical)
    assert completed == session_count
