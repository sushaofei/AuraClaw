from __future__ import annotations

from collections.abc import Sequence
from typing import Any
from uuid import NAMESPACE_URL, uuid5

from auraclaw.contracts.collaboration import (
    ChildResult,
    ChildSpec,
    CollaborationLimits,
    CollaborationRole,
    OutputContract,
    ReviewResult,
)
from auraclaw.contracts.commands import CommandContext
from auraclaw.contracts.errors import AuthorizationError, CollaborationValidationError
from auraclaw.contracts.events import NewEvent
from auraclaw.contracts.state import Visibility
from auraclaw.domain.collaboration import CollaborationAggregate, CollaborationNode
from auraclaw.session.ports import EventStore, OutboxRelayPort


class CollaborationService:
    """Command boundary for Root/Child DAG facts and role-scoped writes."""

    def __init__(
        self,
        *,
        event_store: EventStore,
        relay: OutboxRelayPort,
        limits: CollaborationLimits | None = None,
    ) -> None:
        self._events = event_store
        self._relay = relay
        self._limits = limits or CollaborationLimits()

    async def graph(self, tenant_id: str, root_session_id: str) -> CollaborationAggregate:
        events = await self._events.load_root(tenant_id, root_session_id)
        return CollaborationAggregate.from_events(tenant_id, root_session_id, events)

    async def create_child(
        self,
        *,
        root_session_id: str,
        parent_session_id: str,
        spec: ChildSpec,
        context: CommandContext,
    ) -> dict[str, Any]:
        self._require_coordinator(context)
        graph = await self.graph(context.tenant_id, root_session_id)
        child_session_id = self.child_id(context.tenant_id, root_session_id, spec.task_key)
        existing = graph.nodes.get(child_session_id)
        if existing is not None and existing.task_key == spec.task_key:
            if (
                existing.parent_session_id != parent_session_id
                or existing.role is not spec.role
                or existing.goal != spec.goal
                or existing.output_contract != spec.output_contract
                or existing.dependency_ids != spec.dependency_ids
                or existing.budget != spec.budget
                or existing.runtime_budget != spec.runtime_budget
            ):
                raise CollaborationValidationError(
                    "existing child task_key was reused with a different specification"
                )
            return {
                "session_id": child_session_id,
                "root_session_id": root_session_id,
                "parent_session_id": existing.parent_session_id,
                "role": existing.role.value,
                "status": existing.status,
            }
        graph.validate_new_child(
            parent_session_id=parent_session_id,
            child_session_id=child_session_id,
            spec=spec,
            limits=self._limits,
        )
        target_session_id = spec.metadata.get("target_session_id")
        payload = {
            "task_key": spec.task_key,
            "root_session_id": root_session_id,
            "parent_session_id": parent_session_id,
            "role": spec.role.value,
            "goal": spec.goal,
            "input_refs": list(spec.input_refs),
            "output_contract": spec.output_contract.as_dict(),
            "dependency_ids": list(spec.dependency_ids),
            "tool_permissions": list(spec.tool_permissions),
            "budget": spec.budget,
            "runtime_budget": dict(spec.runtime_budget),
            "target_session_id": target_session_id,
            "metadata": dict(spec.metadata),
        }
        response = {
            "session_id": child_session_id,
            "root_session_id": root_session_id,
            "parent_session_id": parent_session_id,
            "role": spec.role.value,
            "status": "blocked" if spec.dependency_ids else "runnable",
        }
        result = await self._events.append(
            root_session_id=root_session_id,
            session_id=child_session_id,
            run_id=None,
            context=context,
            events=[
                NewEvent(type="child.created", visibility=Visibility.INTERNAL, payload=payload),
                NewEvent(
                    type="run.requested",
                    visibility=Visibility.INTERNAL,
                    payload={"run_id": f"run_{child_session_id[4:]}"},
                ),
            ],
            command_result=response,
        )
        await self._relay.relay_once()
        return result.command_result

    async def set_dependencies(
        self,
        *,
        root_session_id: str,
        child_session_id: str,
        dependency_ids: tuple[str, ...],
        context: CommandContext,
    ) -> dict[str, Any]:
        self._require_coordinator(context)
        graph = await self.graph(context.tenant_id, root_session_id)
        self._require_child(graph, child_session_id)
        graph.validate_dependencies(child_session_id, dependency_ids)
        return await self._append(
            graph=graph,
            session_id=child_session_id,
            context=context,
            event=NewEvent(
                type="dependency.changed",
                payload={"dependency_ids": list(dependency_ids)},
            ),
            response={"session_id": child_session_id, "dependency_ids": list(dependency_ids)},
        )

    async def delegate(
        self,
        *,
        root_session_id: str,
        child_session_id: str,
        owner: str,
        context: CommandContext,
    ) -> dict[str, Any]:
        self._require_coordinator(context)
        graph = await self.graph(context.tenant_id, root_session_id)
        self._require_child(graph, child_session_id)
        if not owner.strip():
            raise CollaborationValidationError("delegate owner is required")
        return await self._append(
            graph=graph,
            session_id=child_session_id,
            context=context,
            event=NewEvent(type="child.delegated", payload={"owner": owner}),
            response={"session_id": child_session_id, "owner": owner},
        )

    async def handoff(
        self,
        *,
        root_session_id: str,
        child_session_id: str,
        owner: str,
        reason: str,
        context: CommandContext,
    ) -> dict[str, Any]:
        self._require_coordinator(context)
        graph = await self.graph(context.tenant_id, root_session_id)
        node = self._require_child(graph, child_session_id)
        if not owner.strip():
            raise CollaborationValidationError("handoff owner is required")
        return await self._append(
            graph=graph,
            session_id=child_session_id,
            context=context,
            event=NewEvent(
                type="session.handed_off",
                payload={"previous_owner": node.owner, "owner": owner, "reason": reason},
            ),
            response={"session_id": child_session_id, "owner": owner},
        )

    async def publish_child_result(
        self,
        *,
        root_session_id: str,
        child_session_id: str,
        child_result: ChildResult,
        context: CommandContext,
    ) -> dict[str, Any]:
        graph = await self.graph(context.tenant_id, root_session_id)
        if child_session_id == root_session_id:
            raise AuthorizationError("a Worker cannot write the Root Session")
        node = self._require_child(graph, child_session_id)
        if node.role is CollaborationRole.REVIEWER or context.actor.type != "worker":
            raise AuthorizationError("only a Worker can publish its Child Result")
        if node.owner is not None and node.owner != context.actor.id:
            raise AuthorizationError("Worker does not own this Child Session")
        payload = child_result.as_dict()
        payload["contract_version"] = node.output_contract.version
        try:
            node.output_contract.validate(payload)
        except ValueError as exc:
            raise CollaborationValidationError(str(exc)) from exc
        return await self._append(
            graph=graph,
            session_id=child_session_id,
            context=context,
            event=(
                NewEvent(
                    type="child.result_published",
                    visibility=Visibility.USER,
                    payload=payload,
                ),
                NewEvent(
                    type="run.completed",
                    visibility=Visibility.USER,
                    payload={
                        "run_id": node.run_id,
                        "result_summary": child_result.summary,
                        "result_ref": child_result.result_ref,
                    },
                ),
            ),
            response={"session_id": child_session_id, "status": "completed", **payload},
        )

    async def publish_review(
        self,
        *,
        root_session_id: str,
        review_session_id: str,
        review: ReviewResult,
        context: CommandContext,
    ) -> dict[str, Any]:
        graph = await self.graph(context.tenant_id, root_session_id)
        node = self._require_child(graph, review_session_id)
        if node.role is not CollaborationRole.REVIEWER or context.actor.type != "reviewer":
            raise AuthorizationError("only a Reviewer can publish a review decision")
        if node.owner is not None and node.owner != context.actor.id:
            raise AuthorizationError("Reviewer does not own this Review Session")
        if node.target_session_id is None:
            raise CollaborationValidationError("Review Session has no target")
        target = self._require_child(graph, node.target_session_id)
        if target.result is None:
            raise CollaborationValidationError("Reviewer target has no published result")
        payload = {
            "target_session_id": node.target_session_id,
            "target_result_ref": target.result["result_ref"],
            "decision": review.decision.value,
            "evidence_refs": list(review.evidence_refs),
            "findings": list(review.findings),
            "repair_suggestions": list(review.repair_suggestions),
        }
        return await self._append(
            graph=graph,
            session_id=review_session_id,
            context=context,
            event=(
                NewEvent(
                    type="review.completed",
                    visibility=Visibility.USER,
                    payload=payload,
                ),
                NewEvent(
                    type="run.completed",
                    visibility=Visibility.USER,
                    payload={
                        "run_id": node.run_id,
                        "result_summary": review.decision.value,
                    },
                ),
            ),
            response={"session_id": review_session_id, "status": "completed", **payload},
        )

    async def request_review(
        self,
        *,
        root_session_id: str,
        parent_session_id: str,
        task_key: str,
        target_session_id: str,
        goal: str,
        budget: float,
        runtime_budget: dict[str, int | float | None],
        context: CommandContext,
    ) -> dict[str, Any]:
        graph = await self.graph(context.tenant_id, root_session_id)
        self._require_child(graph, target_session_id)
        return await self.create_child(
            root_session_id=root_session_id,
            parent_session_id=parent_session_id,
            spec=ChildSpec(
                task_key=task_key,
                role=CollaborationRole.REVIEWER,
                goal=goal,
                output_contract=OutputContract(required_fields=()),
                dependency_ids=(target_session_id,),
                budget=budget,
                runtime_budget=runtime_budget,
                metadata={"target_session_id": target_session_id},
            ),
            context=context,
        )

    async def cancel_child(
        self,
        *,
        root_session_id: str,
        child_session_id: str,
        reason: str,
        context: CommandContext,
    ) -> dict[str, Any]:
        self._require_coordinator(context)
        graph = await self.graph(context.tenant_id, root_session_id)
        node = self._require_child(graph, child_session_id)
        if node.status in {"completed", "cancelled"}:
            return {"session_id": child_session_id, "status": node.status}
        return await self._append(
            graph=graph,
            session_id=child_session_id,
            context=context,
            event=NewEvent(
                type="run.cancelled",
                visibility=Visibility.USER,
                payload={"run_id": node.run_id, "reason": reason},
            ),
            response={"session_id": child_session_id, "status": "cancelled"},
        )

    async def join(
        self,
        *,
        root_session_id: str,
        child_session_ids: tuple[str, ...],
        result_summary: str,
        result_ref: str,
        context: CommandContext,
    ) -> dict[str, Any]:
        self._require_coordinator(context)
        graph = await self.graph(context.tenant_id, root_session_id)
        graph.require_joinable(child_session_ids)
        child_results: list[dict[str, Any]] = []
        reviews: list[dict[str, Any]] = []
        for session_id in child_session_ids:
            node = graph.nodes[session_id]
            node_result = node.result
            assert node_result is not None
            if node.role is CollaborationRole.REVIEWER:
                reviews.append(
                    {
                        "review_session_id": session_id,
                        "target_session_id": node_result["target_session_id"],
                        "target_result_ref": node_result["target_result_ref"],
                        "decision": node_result["decision"],
                        "evidence_refs": node_result["evidence_refs"],
                    }
                )
            else:
                child_results.append(
                    {
                        "session_id": session_id,
                        "role": node.role.value,
                        "result_ref": node_result.get("result_ref"),
                        "artifact_refs": node_result.get("artifact_refs", []),
                    }
                )
        artifact_lineage = [
            {"artifact_ref": artifact_ref, "source_session_id": item["session_id"]}
            for item in child_results
            for artifact_ref in item["artifact_refs"]
        ]
        lineage = {
            "child_results": child_results,
            "reviews": reviews,
            "artifact_lineage": artifact_lineage,
        }
        response = {
            "session_id": root_session_id,
            "status": "completed",
            "result_summary": result_summary,
            "result_ref": result_ref,
            "lineage": lineage,
        }
        result = await self._events.append(
            root_session_id=root_session_id,
            session_id=root_session_id,
            run_id=graph.nodes[root_session_id].run_id,
            context=context,
            events=[
                NewEvent(
                    type="join.completed",
                    payload={"child_session_ids": list(child_session_ids), **lineage},
                ),
                NewEvent(
                    type="run.completed",
                    visibility=Visibility.USER,
                    payload={
                        "result_summary": result_summary,
                        "result_ref": result_ref,
                        "artifact_refs": [
                            item["artifact_ref"] for item in artifact_lineage
                        ],
                        "lineage": lineage,
                    },
                ),
            ],
            command_result=response,
        )
        await self._relay.relay_once()
        return result.command_result

    async def _append(
        self,
        *,
        graph: CollaborationAggregate,
        session_id: str,
        context: CommandContext,
        event: NewEvent | Sequence[NewEvent],
        response: dict[str, Any],
    ) -> dict[str, Any]:
        events = [event] if isinstance(event, NewEvent) else list(event)
        result = await self._events.append(
            root_session_id=graph.root_session_id,
            session_id=session_id,
            run_id=graph.nodes[session_id].run_id,
            context=context,
            events=events,
            command_result=response,
        )
        await self._relay.relay_once()
        return result.command_result


    @staticmethod
    def child_id(tenant_id: str, root_session_id: str, task_key: str) -> str:
        value = uuid5(NAMESPACE_URL, f"auraclaw:{tenant_id}:{root_session_id}:{task_key}")
        return f"ses_{value.hex}"

    @staticmethod
    def _require_coordinator(context: CommandContext) -> None:
        if context.actor.type != "coordinator":
            raise AuthorizationError("only a Coordinator can change the Task DAG")

    @staticmethod
    def _require_child(
        graph: CollaborationAggregate, child_session_id: str
    ) -> CollaborationNode:
        node = graph.nodes.get(child_session_id)
        if node is None or node.role is CollaborationRole.ROOT:
            raise CollaborationValidationError("Child must belong to the same Root and tenant")
        return node


class CoordinatorRole:
    """Semantic role facade; resource scheduling remains in ManagedOrchestrator."""

    def __init__(self, service: CollaborationService) -> None:
        self._service = service

    async def runnable(self, tenant_id: str, root_session_id: str) -> list[str]:
        graph = await self._service.graph(tenant_id, root_session_id)
        return [node.session_id for node in graph.runnable()]


class WorkerRole:
    def __init__(self, service: CollaborationService) -> None:
        self._service = service

    async def publish(self, **kwargs: Any) -> dict[str, Any]:
        return await self._service.publish_child_result(**kwargs)


class ReviewerRole:
    def __init__(self, service: CollaborationService) -> None:
        self._service = service

    async def publish(self, **kwargs: Any) -> dict[str, Any]:
        return await self._service.publish_review(**kwargs)
