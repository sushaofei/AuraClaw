import asyncio
from collections.abc import Sequence
from typing import Any

import pytest

from auraclaw.contracts.collaboration import (
    ChildResult,
    ChildSpec,
    CollaborationLimits,
    CollaborationRole,
    OutputContract,
    ReviewDecision,
    ReviewResult,
)
from auraclaw.contracts.commands import CommandContext
from auraclaw.contracts.errors import AuthorizationError, CollaborationValidationError
from auraclaw.contracts.events import Actor
from auraclaw.gateways.task.admission import AllowAllAdmissionController
from auraclaw.infrastructure.persistence.memory_event_store import InMemoryEventStore
from auraclaw.projection.approval.projector import CompositeProjection
from auraclaw.projection.collaboration.projector import InMemoryCollaborationProjection
from auraclaw.projection.relay import OutboxRelay
from auraclaw.projection.task.projector import InMemoryTaskProjection
from auraclaw.session.collaboration_service import CollaborationService
from auraclaw.session.task_service import TaskService


class M4Harness:
    def __init__(self, limits: CollaborationLimits | None = None) -> None:
        self.store = InMemoryEventStore()
        self.tasks = InMemoryTaskProjection()
        self.collaboration = InMemoryCollaborationProjection()
        self.relay = OutboxRelay(
            self.store, CompositeProjection(self.tasks, self.collaboration)
        )
        self.task_service = TaskService(
            event_store=self.store,
            relay=self.relay,
            reader=self.tasks,
            admission=AllowAllAdmissionController(),
        )
        self.service = CollaborationService(
            event_store=self.store, relay=self.relay, limits=limits
        )
        self.root_session_id = ""

    async def create_root(self, tenant_id: str = "tenant-m4") -> str:
        response = await self.task_service.create_task(
            goal="coordinate a complex task",
            context=CommandContext(
                command_id=f"create-root-{tenant_id}",
                tenant_id=tenant_id,
                actor=Actor(type="user", id="user-m4"),
                correlation_id=f"corr-{tenant_id}",
                expected_version=0,
                operation="create_task",
            ),
        )
        self.root_session_id = str(response["session_id"])
        return self.root_session_id

    async def context(
        self,
        session_id: str,
        *,
        command_id: str,
        actor_type: str = "coordinator",
        actor_id: str = "coordinator-m4",
        operation: str,
        tenant_id: str = "tenant-m4",
    ) -> CommandContext:
        events = await self.store.load(tenant_id, session_id)
        return CommandContext(
            command_id=command_id,
            tenant_id=tenant_id,
            actor=Actor(type=actor_type, id=actor_id),
            correlation_id=f"corr-{tenant_id}",
            expected_version=len(events),
            operation=operation,
        )

    async def child(
        self,
        task_key: str,
        *,
        parent_session_id: str | None = None,
        dependencies: Sequence[str] = (),
        role: CollaborationRole = CollaborationRole.WORKER,
        metadata: dict[str, Any] | None = None,
        contract: OutputContract | None = None,
    ) -> str:
        response = await self.service.create_child(
            root_session_id=self.root_session_id,
            parent_session_id=parent_session_id or self.root_session_id,
            spec=ChildSpec(
                task_key=task_key,
                role=role,
                goal=f"complete {task_key}",
                output_contract=contract or OutputContract(require_artifacts=True),
                dependency_ids=tuple(dependencies),
                metadata=metadata or {},
            ),
            context=CommandContext(
                command_id=f"create-{task_key}",
                tenant_id="tenant-m4",
                actor=Actor(type="coordinator", id="coordinator-m4"),
                correlation_id="corr-tenant-m4",
                expected_version=0,
                operation="collaboration.create_child",
            ),
        )
        return str(response["session_id"])

    async def publish(self, child_session_id: str, suffix: str) -> dict[str, Any]:
        return await self.service.publish_child_result(
            root_session_id=self.root_session_id,
            child_session_id=child_session_id,
            child_result=ChildResult(
                summary=f"result {suffix}",
                result_ref=f"result://{suffix}",
                artifact_refs=(f"artifact://{suffix}",),
                evidence_refs=(f"evidence://{suffix}",),
            ),
            context=await self.context(
                child_session_id,
                command_id=f"publish-{suffix}",
                actor_type="worker",
                actor_id=f"worker-{suffix}",
                operation="collaboration.publish_result",
            ),
        )


@pytest.mark.parametrize("shape", ["serial", "parallel", "tree", "mixed"])
def test_four_dag_shapes_complete_end_to_end(shape: str) -> None:
    async def scenario() -> None:
        harness = M4Harness()
        root = await harness.create_root()
        a = await harness.child(f"{shape}-a")
        if shape == "serial":
            b = await harness.child(f"{shape}-b", dependencies=(a,))
            assert [item["session_id"] for item in await harness.collaboration.list_runnable(
                "tenant-m4", root
            )] == [a]
            await harness.publish(a, f"{shape}-a")
            assert {item["session_id"] for item in await harness.collaboration.list_runnable(
                "tenant-m4", root
            )} == {b}
            await harness.publish(b, f"{shape}-b")
            children = (a, b)
        elif shape == "parallel":
            b = await harness.child(f"{shape}-b")
            assert {item["session_id"] for item in await harness.collaboration.list_runnable(
                "tenant-m4", root
            )} == {a, b}
            await asyncio.gather(
                harness.publish(a, f"{shape}-a"),
                harness.publish(b, f"{shape}-b"),
            )
            children = (a, b)
        elif shape == "tree":
            b = await harness.child(f"{shape}-b", parent_session_id=a, dependencies=(a,))
            await harness.publish(a, f"{shape}-a")
            await harness.publish(b, f"{shape}-b")
            children = (a, b)
        else:
            b = await harness.child(f"{shape}-b")
            c = await harness.child(f"{shape}-c", dependencies=(a, b))
            await asyncio.gather(
                harness.publish(a, f"{shape}-a"),
                harness.publish(b, f"{shape}-b"),
            )
            assert {item["session_id"] for item in await harness.collaboration.list_runnable(
                "tenant-m4", root
            )} == {c}
            await harness.publish(c, f"{shape}-c")
            children = (a, b, c)

        response = await harness.service.join(
            root_session_id=root,
            child_session_ids=children,
            result_summary=f"{shape} complete",
            result_ref=f"result://root/{shape}",
            context=await harness.context(
                root,
                command_id=f"join-{shape}",
                operation="collaboration.join",
            ),
        )
        assert response["status"] == "completed"
        assert {item["session_id"] for item in response["lineage"]["child_results"]} == set(
            children
        )
        assert len(response["lineage"]["artifact_lineage"]) == len(children)

    asyncio.run(scenario())


def test_coordinator_restart_is_idempotent_and_guards_dag_limits() -> None:
    async def scenario() -> None:
        harness = M4Harness(
            CollaborationLimits(
                max_depth=2, max_children_per_parent=2, max_children=3, max_budget=3
            )
        )
        await harness.create_root()
        first = await harness.child("stable-child")
        restarted = await harness.child("stable-child")
        assert restarted == first
        child_events = await harness.store.load("tenant-m4", first)
        assert [event.type for event in child_events].count("child.created") == 1
        with pytest.raises(CollaborationValidationError, match="different specification"):
            await harness.service.create_child(
                root_session_id=harness.root_session_id,
                parent_session_id=harness.root_session_id,
                spec=ChildSpec(
                    task_key="stable-child",
                    role=CollaborationRole.WORKER,
                    goal="changed goal",
                    output_contract=OutputContract(require_artifacts=True),
                ),
                context=CommandContext(
                    command_id="changed-stable-child",
                    tenant_id="tenant-m4",
                    actor=Actor(type="coordinator", id="coordinator-m4"),
                    correlation_id="corr-tenant-m4",
                    expected_version=0,
                    operation="collaboration.create_child",
                ),
            )

        second = await harness.child("second-child")
        with pytest.raises(CollaborationValidationError, match="width"):
            await harness.child("too-wide")
        await harness.service.set_dependencies(
            root_session_id=harness.root_session_id,
            child_session_id=second,
            dependency_ids=(first,),
            context=await harness.context(
                second,
                command_id="second-depends-first",
                operation="collaboration.set_dependencies",
            ),
        )
        with pytest.raises(CollaborationValidationError, match="acyclic"):
            await harness.service.set_dependencies(
                root_session_id=harness.root_session_id,
                child_session_id=first,
                dependency_ids=(second,),
                context=await harness.context(
                    first,
                    command_id="first-depends-second",
                    operation="collaboration.set_dependencies",
                ),
            )

    asyncio.run(scenario())


def test_worker_reviewer_isolation_and_root_lineage() -> None:
    async def scenario() -> None:
        harness = M4Harness()
        root = await harness.create_root()
        worker = await harness.child("worker-output")
        await harness.service.delegate(
            root_session_id=root,
            child_session_id=worker,
            owner="worker-one",
            context=await harness.context(
                worker,
                command_id="delegate-worker",
                operation="collaboration.delegate",
            ),
        )
        worker_result = await harness.service.publish_child_result(
            root_session_id=root,
            child_session_id=worker,
            child_result=ChildResult(
                summary="worker output",
                result_ref="result://worker",
                artifact_refs=("artifact://worker-v1",),
                evidence_refs=("evidence://worker",),
            ),
            context=await harness.context(
                worker,
                command_id="publish-worker",
                actor_type="worker",
                actor_id="worker-one",
                operation="collaboration.publish_result",
            ),
        )
        review_session = await harness.child(
            "review-worker",
            dependencies=(worker,),
            role=CollaborationRole.REVIEWER,
            metadata={"target_session_id": worker},
            contract=OutputContract(required_fields=()),
        )
        await harness.service.delegate(
            root_session_id=root,
            child_session_id=review_session,
            owner="reviewer-one",
            context=await harness.context(
                review_session,
                command_id="delegate-reviewer",
                operation="collaboration.delegate",
            ),
        )
        with pytest.raises(AuthorizationError, match="Worker"):
            await harness.service.publish_child_result(
                root_session_id=root,
                child_session_id=review_session,
                child_result=ChildResult("overwrite", "result://bad"),
                context=await harness.context(
                    review_session,
                    command_id="reviewer-overwrite",
                    actor_type="reviewer",
                    actor_id="reviewer-one",
                    operation="collaboration.publish_result",
                ),
            )
        with pytest.raises(AuthorizationError, match="Root"):
            await harness.service.publish_child_result(
                root_session_id=root,
                child_session_id=root,
                child_result=ChildResult("root overwrite", "result://bad-root"),
                context=await harness.context(
                    root,
                    command_id="worker-root-write",
                    actor_type="worker",
                    actor_id="worker-one",
                    operation="collaboration.publish_result",
                ),
            )

        review = await harness.service.publish_review(
            root_session_id=root,
            review_session_id=review_session,
            review=ReviewResult(
                decision=ReviewDecision.ACCEPTED,
                evidence_refs=("evidence://review-test",),
                findings=("contract and tests pass",),
            ),
            context=await harness.context(
                review_session,
                command_id="publish-review",
                actor_type="reviewer",
                actor_id="reviewer-one",
                operation="collaboration.publish_review",
            ),
        )
        root_result = await harness.service.join(
            root_session_id=root,
            child_session_ids=(worker, review_session),
            result_summary="reviewed result",
            result_ref="result://root/reviewed",
            context=await harness.context(
                root, command_id="join-reviewed", operation="collaboration.join"
            ),
        )
        assert worker_result["artifact_refs"] == ["artifact://worker-v1"]
        assert review["target_result_ref"] == "result://worker"
        assert root_result["lineage"]["reviews"][0]["evidence_refs"] == [
            "evidence://review-test"
        ]
        worker_view = await harness.collaboration.get("tenant-m4", worker)
        assert worker_view is not None
        assert worker_view["artifact_refs"] == ["artifact://worker-v1"]

    asyncio.run(scenario())
