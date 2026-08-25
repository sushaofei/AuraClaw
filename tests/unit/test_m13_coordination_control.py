import asyncio
from datetime import UTC, datetime, timedelta

from auraclaw.contracts.collaboration import (
    ChildResult,
    ChildSpec,
    CollaborationRole,
    OutputContract,
)
from auraclaw.contracts.commands import CommandContext
from auraclaw.contracts.events import Actor
from auraclaw.control.ports import (
    AGENT_RUNTIME_POOL,
    RunnableItem,
    RuntimeAssignment,
    RuntimeCheckpoint,
    RuntimeInstance,
)
from auraclaw.control.runnable_feed import RunnableFeedConsumer
from auraclaw.gateways.task.admission import AllowAllAdmissionController
from auraclaw.infrastructure.persistence.memory_control_store import (
    InMemoryControlStateStore,
)
from auraclaw.infrastructure.persistence.memory_event_store import InMemoryEventStore
from auraclaw.projection.approval.projector import CompositeProjection
from auraclaw.projection.collaboration.projector import InMemoryCollaborationProjection
from auraclaw.projection.relay import OutboxRelay
from auraclaw.projection.task.projector import InMemoryTaskProjection
from auraclaw.session.collaboration_service import CollaborationService
from auraclaw.session.task_service import TaskService


async def _context(
    store: InMemoryEventStore,
    session_id: str,
    *,
    command_id: str,
    actor_type: str,
    actor_id: str,
    operation: str,
    tenant_id: str = "tenant-m13",
) -> CommandContext:
    events = await store.load(tenant_id, session_id)
    return CommandContext(
        command_id=command_id,
        tenant_id=tenant_id,
        actor=Actor(type=actor_type, id=actor_id),
        correlation_id="corr-m13",
        expected_version=len(events),
        operation=operation,
    )


def test_agent_runtime_pool_accepts_every_semantic_role() -> None:
    async def scenario() -> None:
        control = InMemoryControlStateStore()
        runtime = RuntimeInstance(
            runtime_id="runtime-shared",
            runtime_type="agent",
            role=AGENT_RUNTIME_POOL,
            node_id="node-a",
            capabilities={},
            capacity=4,
        )
        await control.register_runtime(runtime)
        for role in ("root", "worker", "reviewer", "repair"):
            selected = await control.select_runtime(
                RunnableItem(
                    task_id=f"tenant:session-{role}:run-{role}",
                    tenant_id="tenant",
                    root_session_id="root",
                    session_id=f"session-{role}",
                    run_id=f"run-{role}",
                    source_version=1,
                    role=role,
                )
            )
            assert selected == runtime

    asyncio.run(scenario())


def test_child_completion_requeues_newly_runnable_dependency() -> None:
    async def scenario() -> None:
        store = InMemoryEventStore()
        task_projection = InMemoryTaskProjection()
        collaboration_projection = InMemoryCollaborationProjection()
        relay = OutboxRelay(
            store,
            CompositeProjection(task_projection, collaboration_projection),
        )
        tasks = TaskService(
            event_store=store,
            relay=relay,
            reader=task_projection,
            admission=AllowAllAdmissionController(),
        )
        collaboration = CollaborationService(event_store=store, relay=relay)
        control = InMemoryControlStateStore()
        feed = RunnableFeedConsumer(store, control, worker_id="orchestrator-m13")

        root_response = await tasks.create_task(
            goal="run a serial graph",
            context=CommandContext(
                command_id="create-root",
                tenant_id="tenant-m13",
                actor=Actor(type="user", id="user-m13"),
                correlation_id="corr-m13",
                expected_version=0,
                operation="create_task",
            ),
        )
        root_id = str(root_response["session_id"])
        root_run_id = str(root_response["run_id"])

        async def create_child(task_key: str, dependencies: tuple[str, ...] = ()) -> str:
            response = await collaboration.create_child(
                root_session_id=root_id,
                parent_session_id=root_id,
                spec=ChildSpec(
                    task_key=task_key,
                    role=CollaborationRole.WORKER,
                    goal=f"complete {task_key}",
                    output_contract=OutputContract(),
                    dependency_ids=dependencies,
                ),
                context=CommandContext(
                    command_id=f"create-{task_key}",
                    tenant_id="tenant-m13",
                    actor=Actor(type="coordinator", id="coordinator-m13"),
                    correlation_id="corr-m13",
                    expected_version=0,
                    operation="collaboration.create_child",
                ),
            )
            return str(response["session_id"])

        first = await create_child("first")
        second = await create_child("second", (first,))

        await feed.run_once(limit=100)
        await asyncio.sleep(0)
        initial = await control.claim("orchestrator-claim", limit=10)
        initial_ids = {claim.item.session_id for claim in initial}
        assert root_id in initial_ids
        assert first in initial_ids
        assert second not in initial_ids

        root_claim = next(claim for claim in initial if claim.item.session_id == root_id)
        lease = await control.acquire_lease(
            f"session:tenant-m13:{root_id}",
            "orchestrator-claim",
            ttl=timedelta(seconds=30),
        )
        assert lease is not None
        root_assignment = RuntimeAssignment(
            tenant_id="tenant-m13",
            root_session_id=root_id,
            session_id=root_id,
            run_id=root_run_id,
            runtime_id="runtime-shared",
            lease_id=lease.lease_id,
            fencing_token=lease.fencing_token,
            role="root",
            resource_profile={},
            lease_expires_at=lease.expires_at,
        )
        assert await control.assign(
            root_claim.item.task_id,
            root_assignment,
            claim_token=root_claim.claim_token,
        )
        await control.save_checkpoint(
            RuntimeCheckpoint(
                tenant_id="tenant-m13",
                session_id=root_id,
                run_id=root_run_id,
                fencing_token=lease.fencing_token,
                phase="agent.waiting_children",
                state={"waiting_child_ids": [first, second]},
                updated_at=datetime.now(UTC),
            )
        )
        await control.suspend_assignment(root_claim.item.task_id, "waiting_children")

        async def publish(child_id: str, suffix: str) -> None:
            await collaboration.publish_child_result(
                root_session_id=root_id,
                child_session_id=child_id,
                child_result=ChildResult(
                    summary=suffix,
                    result_ref=f"result://{suffix}",
                ),
                context=await _context(
                    store,
                    child_id,
                    command_id=f"publish-{suffix}",
                    actor_type="worker",
                    actor_id=f"worker-{suffix}",
                    operation="collaboration.publish_result",
                ),
            )

        await publish(first, "first")
        await feed.run_once(limit=100)
        await asyncio.sleep(0)
        root_events = await store.load_root("tenant-m13", root_id)
        assert second in {
            item.session_id for item in feed._derive_collaboration(root_events)
        }
        after_first = await control.claim("orchestrator-after-first", limit=10)
        assert second in {claim.item.session_id for claim in after_first}
        assert root_id not in {claim.item.session_id for claim in after_first}

        await publish(second, "second")
        await feed.run_once(limit=100)
        await asyncio.sleep(0)
        after_second = await control.claim("orchestrator-after-second", limit=10)
        assert root_id in {claim.item.session_id for claim in after_second}

    asyncio.run(scenario())
