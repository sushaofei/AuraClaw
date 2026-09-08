import asyncio
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from auraclaw.composition.adapters.runtime_worker import RuntimeWorker
from auraclaw.contracts.commands import CommandContext
from auraclaw.contracts.errors import FencingTokenError
from auraclaw.contracts.events import Actor, NewEvent
from auraclaw.control.orchestrator import LocalRuntimeProvisioner, ManagedOrchestrator
from auraclaw.control.ports import (
    RuntimeAssignment,
    RuntimeCheckpoint,
    RuntimeInstance,
    RuntimeLease,
)
from auraclaw.gateways.task.admission import AllowAllAdmissionController
from auraclaw.infrastructure.persistence.memory_control_store import InMemoryControlStateStore
from auraclaw.infrastructure.persistence.memory_event_store import InMemoryEventStore
from auraclaw.projection.relay import OutboxRelay
from auraclaw.projection.task.projector import InMemoryTaskProjection
from auraclaw.runtime.clients import (
    FencedSessionClient,
    FencedToolClient,
    IdempotentToolClient,
    InMemoryRuntimeEventBus,
)
from auraclaw.runtime.harness import AgentHarness, InjectionPoint
from auraclaw.runtime.model_gateway import ModelGateway, StaticCredentialResolver
from auraclaw.runtime.ports import ModelRequest, ModelResponse, ToolCall
from auraclaw.session.task_service import TaskService


class RecordingProvider:
    def __init__(self, name: str) -> None:
        self.name = name
        self.calls = 0
        self.credentials: list[str] = []

    async def generate(self, request: ModelRequest, *, credential: str) -> ModelResponse:
        self.calls += 1
        self.credentials.append(credential)
        return ModelResponse(
            model_call_id=request.model_call_id,
            provider=self.name,
            model=f"{self.name}-model",
            completed_output="managed answer",
            deltas=("managed ", "answer"),
            tool_calls=(
                ToolCall(
                    tool_invocation_id=f"tool_{request.run_id}_1",
                    name="lookup",
                    arguments={"query": "state"},
                ),
            ),
            usage={"input_tokens": 2, "output_tokens": 2},
        )


def _context() -> CommandContext:
    return CommandContext(
        command_id="create-m2",
        tenant_id="tenant-m2",
        actor=Actor(type="user", id="test-user"),
        correlation_id="corr-m2",
        expected_version=0,
        operation="create_task",
    )


async def _pending_task() -> tuple[
    dict[str, Any], InMemoryEventStore, InMemoryTaskProjection
]:
    event_store = InMemoryEventStore()
    projection = InMemoryTaskProjection()
    service = TaskService(
        event_store=event_store,
        relay=OutboxRelay(event_store, projection),
        reader=projection,
        admission=AllowAllAdmissionController(),
    )
    created = await service.create_task(goal="exercise managed runtime", context=_context())
    task = await projection.get_task("tenant-m2", str(created["session_id"]))
    assert task is not None
    return task, event_store, projection


def test_two_orchestrators_compete_but_only_one_gets_assignment() -> None:
    async def scenario() -> None:
        task, event_store, _ = await _pending_task()
        control = InMemoryControlStateStore()
        session = FencedSessionClient(event_store, control)
        provisioner = LocalRuntimeProvisioner()
        first = ManagedOrchestrator(
            orchestrator_id="orch-1",
            control_store=control,
            session=session,
            provisioner=provisioner,
        )
        second = ManagedOrchestrator(
            orchestrator_id="orch-2",
            control_store=control,
            session=session,
            provisioner=provisioner,
        )
        assert await first.watch([task]) == 1
        assert await second.watch([task]) == 0

        assignments = await asyncio.gather(first.schedule_once(), second.schedule_once())
        winners = [assignment for assignment in assignments if assignment is not None]
        assert len(winners) == 1
        events = await event_store.load("tenant-m2", str(task["session_id"]))
        assert [event.type for event in events].count("run.scheduled") == 1

    asyncio.run(scenario())


def test_expired_runtime_session_and_tool_writes_are_fenced() -> None:
    async def scenario() -> None:
        task, event_store, _ = await _pending_task()
        control = InMemoryControlStateStore()
        session = FencedSessionClient(event_store, control)
        orchestrator = ManagedOrchestrator(
            orchestrator_id="orch-fence",
            control_store=control,
            session=session,
            provisioner=LocalRuntimeProvisioner(),
            lease_ttl=timedelta(milliseconds=5),
        )
        await orchestrator.watch([task])
        old = await orchestrator.schedule_once()
        assert old is not None
        await asyncio.sleep(0.01)
        assert await control.recover_expired() == 1
        current = await orchestrator.schedule_once()
        assert current is not None
        assert current.fencing_token > old.fencing_token

        with pytest.raises(FencingTokenError):
            await session.append(
                old,
                [NewEvent(type="run.started", payload={"run_id": old.run_id})],
                command_id="stale-session-write",
                operation="runtime.run.started",
            )
        tool = FencedToolClient(IdempotentToolClient(), control)
        with pytest.raises(FencingTokenError):
            await tool.execute(
                old,
                ToolCall(tool_invocation_id="stale-tool", name="lookup", arguments={}),
            )

    asyncio.run(scenario())


@pytest.mark.parametrize("point", list(InjectionPoint))
def test_runtime_recovers_at_all_required_failure_injection_points(
    point: InjectionPoint,
) -> None:
    async def scenario() -> None:
        task, event_store, _ = await _pending_task()
        control = InMemoryControlStateStore()
        session = FencedSessionClient(event_store, control)
        orchestrator = ManagedOrchestrator(
            orchestrator_id=f"orch-{point}",
            control_store=control,
            session=session,
            provisioner=LocalRuntimeProvisioner(),
            lease_ttl=timedelta(milliseconds=100),
        )
        await orchestrator.watch([task])
        original = await orchestrator.schedule_once()
        assert original is not None

        provider = RecordingProvider("provider-a")
        gateway = ModelGateway(
            (provider,),
            StaticCredentialResolver({"provider-a": "super-secret-provider-key"}),
            default_provider="provider-a",
        )
        tool_delegate = IdempotentToolClient()
        tool = FencedToolClient(tool_delegate, control)
        runtime_bus = InMemoryRuntimeEventBus()
        fired = False

        def crash(selected: InjectionPoint) -> None:
            nonlocal fired
            if selected == point and not fired:
                fired = True
                raise RuntimeError(f"killed at {point}")

        first_harness = AgentHarness(
            control_store=control,
            session=session,
            model=gateway,
            tools=tool,
            runtime_events=runtime_bus,
            failure_injector=crash,
        )
        with pytest.raises(RuntimeError, match="killed at"):
            await first_harness.execute(original)

        await asyncio.sleep(0.11)
        assert await control.recover_expired() == 1
        recovered = await orchestrator.schedule_once()
        assert recovered is not None
        assert recovered.fencing_token > original.fencing_token
        assert recovered.runtime_id != original.runtime_id
        with pytest.raises(FencingTokenError):
            await control.save_checkpoint(
                RuntimeCheckpoint(
                    tenant_id=original.tenant_id,
                    session_id=original.session_id,
                    run_id=original.run_id,
                    fencing_token=original.fencing_token,
                    phase="stale-runtime",
                    state={},
                    updated_at=datetime.now(UTC),
                )
            )
        recovered_harness = AgentHarness(
            control_store=control,
            session=session,
            model=gateway,
            tools=tool,
            runtime_events=runtime_bus,
        )
        await recovered_harness.execute(recovered)

        events = await event_store.load("tenant-m2", str(task["session_id"]))
        event_types = [event.type for event in events]
        assert event_types.count("runtime.failed") == 1
        assert event_types.count("runtime.reprovisioned") == 1
        assert event_types.count("model.input.prepared") == 1
        assert event_types.count("model.output.completed") == 1
        assert event_types.count("tool.call.requested") == 1
        assert event_types.count("tool.call.completed") == 1
        assert event_types.count("run.completed") == 1
        golden_types = {
            "run.scheduled",
            "run.started",
            "model.input.prepared",
            "model.output.completed",
            "tool.call.requested",
            "tool.call.completed",
            "run.completed",
        }
        assert [item for item in event_types if item in golden_types] == [
            "run.scheduled",
            "run.started",
            "model.input.prepared",
            "model.output.completed",
            "tool.call.requested",
            "tool.call.completed",
            "run.completed",
        ]
        assert provider.calls == 1
        assert tool_delegate.calls == 1
        serialized_events = repr([event.as_dict() for event in events])
        assert "super-secret-provider-key" not in serialized_events

    asyncio.run(scenario())


def test_provider_can_be_replaced_without_session_or_orchestrator_changes() -> None:
    async def scenario() -> None:
        request = ModelRequest(
            model_call_id="model-call",
            tenant_id="tenant-m2",
            run_id="run-m2",
            messages=({"role": "user", "content": "hello"},),
        )
        provider_a = RecordingProvider("provider-a")
        provider_b = RecordingProvider("provider-b")
        gateway_a = ModelGateway(
            (provider_a,),
            StaticCredentialResolver({"provider-a": "secret-a"}),
            default_provider="provider-a",
        )
        gateway_b = ModelGateway(
            (provider_b,),
            StaticCredentialResolver({"provider-b": "secret-b"}),
            default_provider="provider-b",
        )

        first = await gateway_a.generate(request)
        second = await gateway_b.generate(request)
        assert (first.provider, second.provider) == ("provider-a", "provider-b")
        assert provider_a.credentials == ["secret-a"]
        assert provider_b.credentials == ["secret-b"]
        assert not hasattr(request, "credential")

    asyncio.run(scenario())


def test_runtime_event_bus_failure_does_not_lose_canonical_model_output() -> None:
    async def scenario() -> None:
        task, event_store, _ = await _pending_task()
        control = InMemoryControlStateStore()
        session = FencedSessionClient(event_store, control)
        orchestrator = ManagedOrchestrator(
            orchestrator_id="orch-stream-failure",
            control_store=control,
            session=session,
            provisioner=LocalRuntimeProvisioner(),
        )
        await orchestrator.watch([task])
        assignment = await orchestrator.schedule_once()
        assert assignment is not None
        provider = RecordingProvider("provider-a")
        harness = AgentHarness(
            control_store=control,
            session=session,
            model=ModelGateway(
                (provider,),
                StaticCredentialResolver({"provider-a": "secret"}),
                default_provider="provider-a",
            ),
            tools=FencedToolClient(IdempotentToolClient(), control),
            runtime_events=InMemoryRuntimeEventBus(fail_publish=True),
        )
        await harness.execute(assignment)
        events = await event_store.load("tenant-m2", str(task["session_id"]))
        output = next(event for event in events if event.type == "model.output.completed")
        assert output.payload["output"] == "managed answer"
        assert any(event.type == "run.completed" for event in events)

    asyncio.run(scenario())


def test_runtime_worker_renews_lease_during_slow_model_calls() -> None:
    async def scenario() -> None:
        task, event_store, projection = await _pending_task()
        control = InMemoryControlStateStore()
        session = FencedSessionClient(event_store, control)
        orchestrator = ManagedOrchestrator(
            orchestrator_id="orch-keepalive",
            control_store=control,
            session=session,
            provisioner=LocalRuntimeProvisioner(),
            lease_ttl=timedelta(milliseconds=80),
        )

        class SlowProvider(RecordingProvider):
            async def generate(self, request: ModelRequest, *, credential: str) -> ModelResponse:
                await asyncio.sleep(0.25)
                return await super().generate(request, credential=credential)

        provider = SlowProvider("provider-a")
        harness = AgentHarness(
            control_store=control,
            session=session,
            model=ModelGateway(
                (provider,),
                StaticCredentialResolver({"provider-a": "secret"}),
                default_provider="provider-a",
            ),
            tools=FencedToolClient(IdempotentToolClient(), control),
            runtime_events=InMemoryRuntimeEventBus(),
        )
        worker = RuntimeWorker(
            event_store=event_store,
            reader=projection,
            relay=OutboxRelay(event_store, projection),
            orchestrator=orchestrator,
            harness=harness,
            heartbeat_interval=timedelta(milliseconds=25),
        )
        completed = await worker.run_once()
        assert completed == 1
        events = await event_store.load("tenant-m2", str(task["session_id"]))
        assert any(event.type == "run.completed" for event in events)
        refreshed = await projection.get_task("tenant-m2", str(task["session_id"]))
        assert refreshed is not None
        assert refreshed["run_status"] == "completed"

    asyncio.run(scenario())


def test_claim_does_not_reclaim_a_running_assignment_by_elapsed_time() -> None:
    async def scenario() -> None:
        control = InMemoryControlStateStore()
        control.orphan_running_grace = timedelta(0)
        runtime = RuntimeInstance(
            runtime_id="runtime-orphan",
            runtime_type="agent",
            role="root",
            node_id="n1",
            capabilities={},
            capacity=1,
        )
        await control.register_runtime(runtime)
        assignment = RuntimeAssignment(
            tenant_id="tenant-m2",
            root_session_id="ses_root",
            session_id="ses_1",
            run_id="run_1",
            runtime_id=runtime.runtime_id,
            lease_id="lea_1",
            fencing_token=1,
            role="root",
            resource_profile={},
        )
        task_id = "tenant-m2:ses_1:run_1"
        resource_id = "session:tenant-m2:ses_1"
        async with control._lock:
            control._leases[resource_id] = RuntimeLease(
                resource_id=resource_id,
                lease_id="lea_1",
                owner="orch",
                fencing_token=1,
                expires_at=datetime.now(UTC) + timedelta(seconds=30),
            )
        control._assignments[task_id] = (assignment, "running")
        control._assignment_started_at[task_id] = datetime.now(UTC) - timedelta(
            seconds=1
        )

        claimed = await control.claim_assignments(runtime.runtime_id, "root", limit=1)
        assert claimed == []

    asyncio.run(scenario())


def test_recover_expired_reclaims_missing_or_stale_runtime_not_live() -> None:
    async def scenario() -> None:
        control = InMemoryControlStateStore()
        control.stale_heartbeat_after = timedelta(milliseconds=20)
        live = RuntimeInstance(
            runtime_id="runtime-live",
            runtime_type="agent",
            role="root",
            node_id="n1",
            capabilities={},
            capacity=1,
        )
        dead = RuntimeInstance(
            runtime_id="runtime-dead",
            runtime_type="agent",
            role="root",
            node_id="n2",
            capabilities={},
            capacity=1,
        )
        await control.register_runtime(live)
        await control.register_runtime(dead)

        live_assignment = RuntimeAssignment(
            tenant_id="tenant-m2",
            root_session_id="ses_root",
            session_id="ses_live",
            run_id="run_live",
            runtime_id=live.runtime_id,
            lease_id="lea_live",
            fencing_token=1,
            role="root",
            resource_profile={},
        )
        dead_assignment = RuntimeAssignment(
            tenant_id="tenant-m2",
            root_session_id="ses_root",
            session_id="ses_dead",
            run_id="run_dead",
            runtime_id=dead.runtime_id,
            lease_id="lea_dead",
            fencing_token=1,
            role="root",
            resource_profile={},
        )
        missing_assignment = RuntimeAssignment(
            tenant_id="tenant-m2",
            root_session_id="ses_root",
            session_id="ses_missing",
            run_id="run_missing",
            runtime_id="runtime-gone",
            lease_id="lea_missing",
            fencing_token=1,
            role="root",
            resource_profile={},
        )
        now = datetime.now(UTC)
        control._leases = {
            "session:tenant-m2:ses_live": RuntimeLease(
                resource_id="session:tenant-m2:ses_live",
                lease_id="lea_live",
                owner="orch",
                fencing_token=1,
                expires_at=now + timedelta(minutes=5),
            ),
            "session:tenant-m2:ses_dead": RuntimeLease(
                resource_id="session:tenant-m2:ses_dead",
                lease_id="lea_dead",
                owner="orch",
                fencing_token=1,
                expires_at=now + timedelta(minutes=5),
            ),
            "session:tenant-m2:ses_missing": RuntimeLease(
                resource_id="session:tenant-m2:ses_missing",
                lease_id="lea_missing",
                owner="orch",
                fencing_token=1,
                expires_at=now + timedelta(minutes=5),
            ),
        }
        control._assignments = {
            "t:ses_live:run_live": (live_assignment, "running"),
            "t:ses_dead:run_dead": (dead_assignment, "running"),
            "t:ses_missing:run_missing": (missing_assignment, "running"),
        }
        control._runtimes[dead.runtime_id] = (
            dead,
            now - timedelta(milliseconds=50),
        )

        repaired = await control.recover_expired()
        assert repaired == 2
        assert control._assignments["t:ses_live:run_live"][1] == "running"
        assert control._assignments["t:ses_dead:run_dead"][1] == "expired"
        assert control._assignments["t:ses_missing:run_missing"][1] == "expired"

    asyncio.run(scenario())


def test_remote_runtime_worker_heartbeats_during_slow_execute() -> None:
    async def scenario() -> None:
        from auraclaw.composition.services import RemoteRuntimeWorker

        # Event-driven: execute finishes only after keep-alive has recovered
        # from one failure and delivered two successful pulses (no wall-clock race).
        recovered = asyncio.Event()
        heartbeats = 0
        failures_left = 1

        class FakeControl:
            runtime_id = "runtime-remote"

            async def register(self) -> None:
                return None

            async def heartbeat(self) -> None:
                nonlocal heartbeats, failures_left
                if failures_left > 0:
                    failures_left -= 1
                    raise ConnectionError("transient control-plane blip")
                heartbeats += 1
                if heartbeats >= 2:
                    recovered.set()

            async def claim(self, *, limit: int = 1) -> list[RuntimeAssignment]:
                del limit
                return [
                    RuntimeAssignment(
                        tenant_id="tenant-m2",
                        root_session_id="ses_root",
                        session_id="ses_1",
                        run_id="run_1",
                        runtime_id=self.runtime_id,
                        lease_id="lea_1",
                        fencing_token=1,
                        role="root",
                        resource_profile={},
                    )
                ]

            async def finish_assignment(self, task_id: str, outcome: str) -> None:
                del task_id, outcome

        class SlowHarness:
            async def execute(self, assignment: RuntimeAssignment) -> None:
                del assignment
                await asyncio.wait_for(recovered.wait(), timeout=2.0)

            async def record_failure(
                self, assignment: RuntimeAssignment, exc: Exception
            ) -> None:
                del assignment, exc

        worker = RemoteRuntimeWorker(
            FakeControl(),  # type: ignore[arg-type]
            SlowHarness(),  # type: ignore[arg-type]
            heartbeat_interval=timedelta(milliseconds=5),
        )
        assert await worker.tick() == 1
        assert failures_left == 0
        assert heartbeats >= 2
        assert recovered.is_set()

    asyncio.run(scenario())
