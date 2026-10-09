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
from auraclaw.contracts.routing import RouteKind, RoutingPlan, RoutingPlanStep
from auraclaw.contracts.state import Visibility
from auraclaw.domain.collaboration import CollaborationAggregate, CollaborationNode
from auraclaw.domain.session import SessionAggregate
from auraclaw.session.ports import AtomicBatchEventStore, OutboxRelayPort, StreamAppend


class CollaborationService:
    """Command boundary for Root/Child DAG facts and role-scoped writes."""

    def __init__(
        self,
        *,
        event_store: AtomicBatchEventStore,
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
                or not _same_child_budget(existing.runtime_budget, spec.runtime_budget)
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
        parent = SessionAggregate.from_events(
            await self._events.load(context.tenant_id, parent_session_id)
        )
        inherited = parent.approval.model_copy(
            update={"approval_mode_source": "inherited"}
        ).public_dict()
        target_session_id = spec.metadata.get("target_session_id")
        payload = {
            "approval": inherited,
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
                    payload={"run_id": f"run_{child_session_id[4:]}", "approval": inherited},
                ),
            ],
            command_result=response,
        )
        await self._relay.relay_once()
        return result.command_result

    async def submit_plan(
        self,
        *,
        root_session_id: str,
        plan: RoutingPlan,
        context: CommandContext,
    ) -> dict[str, Any]:
        """Materialize a validated child DAG as one root-scoped transaction."""
        self._require_coordinator(context)
        if plan.route_kind is not RouteKind.COORDINATOR_DAG:
            raise CollaborationValidationError("submit_plan requires a coordinator DAG")
        expected_plan = RoutingPlan.create(
            route_kind=plan.route_kind,
            success_criteria=plan.success_criteria,
            constraints=plan.constraints,
            risk_class=plan.risk_class,
            steps=plan.steps,
        )
        if expected_plan.plan_digest != plan.plan_digest:
            raise CollaborationValidationError("routing plan digest does not match its content")
        child_steps = tuple(
            step for step in plan.steps if step.assignment.execution_scope == "child"
        )
        if not child_steps or len(child_steps) != len(plan.steps):
            raise CollaborationValidationError(
                "submit_plan requires every plan step to use child execution scope"
            )
        if len({step.task_key for step in child_steps}) != len(child_steps):
            raise CollaborationValidationError("routing plan task keys must be unique")
        graph = await self.graph(context.tenant_id, root_session_id)
        parent = SessionAggregate.from_events(
            await self._events.load(context.tenant_id, root_session_id)
        )
        inherited = parent.approval.model_copy(
            update={"approval_mode_source": "inherited"}
        ).public_dict()
        child_ids = {
            step.task_key: self.child_id(context.tenant_id, root_session_id, step.task_key)
            for step in child_steps
        }
        existing = [child_ids[step.task_key] in graph.nodes for step in child_steps]
        if any(existing):
            if not all(existing):
                raise CollaborationValidationError(
                    "plan overlaps an incomplete existing Child DAG"
                )
            children = [
                self._existing_plan_child(
                    graph,
                    root_session_id=root_session_id,
                    step=step,
                    child_ids=child_ids,
                    plan_digest=plan.plan_digest,
                )
                for step in child_steps
            ]
            return {
                "root_session_id": root_session_id,
                "plan_digest": plan.plan_digest,
                "status": "submitted",
                "children": children,
            }

        by_key = {step.task_key: step for step in child_steps}
        ordered = self._topological_steps(by_key)
        appends: list[StreamAppend] = []
        responses: dict[str, dict[str, Any]] = {}
        for step in ordered:
            spec = self._plan_child_spec(step, child_ids, plan.plan_digest)
            child_session_id = child_ids[step.task_key]
            graph.stage_new_child(
                parent_session_id=root_session_id,
                child_session_id=child_session_id,
                spec=spec,
                limits=self._limits,
            )
            payload = self._child_created_payload(
                root_session_id=root_session_id,
                parent_session_id=root_session_id,
                spec=spec,
                inherited_approval=inherited,
            )
            run_id = f"run_{child_session_id[4:]}"
            appends.append(
                StreamAppend(
                    session_id=child_session_id,
                    run_id=None,
                    expected_version=0,
                    events=(
                        NewEvent(
                            type="child.created",
                            visibility=Visibility.INTERNAL,
                            payload=payload,
                        ),
                        NewEvent(
                            type="run.requested",
                            visibility=Visibility.INTERNAL,
                            payload={"run_id": run_id, "approval": inherited},
                        ),
                    ),
                )
            )
            responses[step.task_key] = {
                "task_key": step.task_key,
                "session_id": child_session_id,
                "run_id": run_id,
                "role": spec.role.value,
                "dependency_ids": list(spec.dependency_ids),
                "status": "blocked" if spec.dependency_ids else "runnable",
            }
        response = {
            "root_session_id": root_session_id,
            "plan_digest": plan.plan_digest,
            "status": "submitted",
            "children": [responses[step.task_key] for step in child_steps],
        }
        result = await self._events.append_batch(
            root_session_id=root_session_id,
            context=context,
            appends=appends,
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
                        "artifact_refs": [item["artifact_ref"] for item in artifact_lineage],
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

    def _plan_child_spec(
        self,
        step: RoutingPlanStep,
        child_ids: dict[str, str],
        plan_digest: str,
    ) -> ChildSpec:
        try:
            dependency_ids = tuple(child_ids[key] for key in step.dependencies)
        except KeyError as exc:
            raise CollaborationValidationError(
                f"plan references a non-child dependency: {exc.args[0]}"
            ) from exc
        role = CollaborationRole(step.assignment.role)
        target_session_id = dependency_ids[0] if role is CollaborationRole.REVIEWER else None
        if role is CollaborationRole.REVIEWER and target_session_id is None:
            raise CollaborationValidationError("reviewer plan steps require a target dependency")
        required_fields = (
            () if role is CollaborationRole.REVIEWER else step.output_contract.required_fields
        )
        runtime_budget: dict[str, int | float | None] = {
            key: value
            for key, value in {
                "max_steps": step.budget.max_steps,
                "max_output_tokens": step.budget.max_output_tokens,
                "max_cost": step.budget.max_cost,
            }.items()
            if value is not None
        }
        metadata: dict[str, Any] = {
            "routing_plan_digest": plan_digest,
            "risk_class": step.risk_class.value,
            "requires_review": step.requires_review,
        }
        if (
            step.capability is not None
            and step.capability.kind != "skill"
            and step.capability.capability_id is not None
        ):
            metadata["required_capabilities"] = [
                {
                    "capability_id": step.capability.capability_id,
                    "version": step.capability.version,
                    **(
                        {"content_digest": step.capability.content_digest}
                        if step.capability.content_digest is not None
                        else {}
                    ),
                }
            ]
        if step.capability is not None and step.capability.kind == "skill":
            skill_name = (
                f"{step.capability.publisher}/{step.capability.name}"
                if step.capability.publisher is not None
                else step.capability.name
            )
            metadata["skill_names"] = [skill_name]
            metadata["required_skills"] = [
                {
                    "publisher": step.capability.publisher,
                    "name": step.capability.name,
                    "version": step.capability.version,
                    "package_digest": step.capability.content_digest,
                    "binding_id": step.capability.binding_id,
                }
            ]
        for key, value in {
            "profile_id": step.assignment.profile_id,
            "model": step.assignment.model,
            "harness": step.assignment.harness,
            "parallel_group": step.parallel_group,
            "target_session_id": target_session_id,
        }.items():
            if value is not None:
                metadata[key] = value
        return ChildSpec(
            task_key=step.task_key,
            role=role,
            goal=step.goal,
            output_contract=OutputContract(
                required_fields=required_fields,
                require_artifacts=step.output_contract.require_artifacts,
                require_evidence=step.output_contract.require_evidence,
            ),
            dependency_ids=dependency_ids,
            input_refs=step.input_refs,
            tool_permissions=(step.capability.name,) if step.capability is not None else (),
            budget=step.budget.fraction * self._limits.max_budget,
            runtime_budget=runtime_budget,
            metadata=metadata,
        )

    @staticmethod
    def _topological_steps(
        by_key: dict[str, RoutingPlanStep],
    ) -> tuple[RoutingPlanStep, ...]:
        ordered: list[RoutingPlanStep] = []
        visiting: set[str] = set()
        visited: set[str] = set()

        def visit(key: str) -> None:
            if key in visiting:
                raise CollaborationValidationError("Task DAG must be acyclic")
            if key in visited:
                return
            step = by_key[key]
            visiting.add(key)
            for dependency in step.dependencies:
                if dependency not in by_key:
                    raise CollaborationValidationError(
                        f"plan references an unknown dependency: {dependency}"
                    )
                visit(dependency)
            visiting.remove(key)
            visited.add(key)
            ordered.append(step)

        for key in by_key:
            visit(key)
        return tuple(ordered)

    def _existing_plan_child(
        self,
        graph: CollaborationAggregate,
        *,
        root_session_id: str,
        step: RoutingPlanStep,
        child_ids: dict[str, str],
        plan_digest: str,
    ) -> dict[str, Any]:
        node = graph.nodes[child_ids[step.task_key]]
        spec = self._plan_child_spec(
            step,
            child_ids,
            plan_digest,
        )
        if (
            node.parent_session_id != root_session_id
            or node.role is not spec.role
            or node.goal != spec.goal
            or node.output_contract != spec.output_contract
            or node.dependency_ids != spec.dependency_ids
            or node.input_refs != spec.input_refs
            or node.tool_permissions != spec.tool_permissions
            or node.budget != spec.budget
            or not _same_child_budget(node.runtime_budget, spec.runtime_budget)
            or node.metadata.get("routing_plan_digest") != plan_digest
        ):
            raise CollaborationValidationError(
                "existing child task_key was reused with a different plan specification"
            )
        payload = {
            "task_key": step.task_key,
            "session_id": node.session_id,
            "run_id": f"run_{node.session_id[4:]}",
            "role": node.role.value,
            "dependency_ids": list(node.dependency_ids),
            "status": "blocked" if node.dependency_ids else "runnable",
        }
        return payload

    @staticmethod
    def _child_created_payload(
        *,
        root_session_id: str,
        parent_session_id: str,
        spec: ChildSpec,
        inherited_approval: dict[str, Any],
    ) -> dict[str, Any]:
        payload = {
            "approval": inherited_approval,
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
            "target_session_id": spec.metadata.get("target_session_id"),
            "metadata": dict(spec.metadata),
        }
        skill_names = spec.metadata.get("skill_names")
        if isinstance(skill_names, list) and skill_names:
            payload["skill_names"] = [str(item) for item in skill_names]
        return payload

    @staticmethod
    def child_id(tenant_id: str, root_session_id: str, task_key: str) -> str:
        value = uuid5(NAMESPACE_URL, f"auraclaw:{tenant_id}:{root_session_id}:{task_key}")
        return f"ses_{value.hex}"

    @staticmethod
    def _require_coordinator(context: CommandContext) -> None:
        if context.actor.type != "coordinator":
            raise AuthorizationError("only a Coordinator can change the Task DAG")

    @staticmethod
    def _require_child(graph: CollaborationAggregate, child_session_id: str) -> CollaborationNode:
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


def _same_child_budget(stored: dict[str, Any], requested: dict[str, Any]) -> bool:
    if stored.get("policy_version") != "2":
        return stored == requested
    expected = {"max_steps": 48, "max_output_tokens": 8192, **requested}
    actual = {k: v for k, v in stored.items() if k not in {"policy_version", "scope_id"}}
    return actual == expected
