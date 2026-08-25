from __future__ import annotations

from collections import Counter
from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import Any

from auraclaw.contracts.collaboration import (
    ChildSpec,
    CollaborationLimits,
    CollaborationRole,
    OutputContract,
    ReviewDecision,
)
from auraclaw.contracts.errors import CollaborationValidationError
from auraclaw.contracts.events import CanonicalEvent


@dataclass
class CollaborationNode:
    tenant_id: str
    session_id: str
    root_session_id: str
    parent_session_id: str | None
    role: CollaborationRole
    goal: str
    task_key: str
    output_contract: OutputContract = field(default_factory=OutputContract)
    dependency_ids: tuple[str, ...] = ()
    owner: str | None = None
    budget: float = 0.0
    runtime_budget: dict[str, int | float | None] = field(default_factory=dict)
    status: str = "blocked"
    result: dict[str, Any] | None = None
    target_session_id: str | None = None
    run_id: str | None = None


class CollaborationAggregate:
    """Root-scoped graph rebuilt exclusively from Canonical Session Events."""

    def __init__(self, tenant_id: str, root_session_id: str) -> None:
        self.tenant_id = tenant_id
        self.root_session_id = root_session_id
        self.nodes: dict[str, CollaborationNode] = {}
        self.reviews: dict[str, dict[str, Any]] = {}
        self.joined_children: tuple[str, ...] = ()

    @classmethod
    def from_events(
        cls, tenant_id: str, root_session_id: str, events: Iterable[CanonicalEvent]
    ) -> CollaborationAggregate:
        aggregate = cls(tenant_id, root_session_id)
        selected = sorted(
            (
                event
                for event in events
                if event.tenant_id == tenant_id and event.root_session_id == root_session_id
            ),
            key=lambda item: (item.occurred_at, item.session_id, item.aggregate_version),
        )
        for event in selected:
            aggregate.apply(event)
        return aggregate

    def apply(self, event: CanonicalEvent) -> None:
        payload = event.payload
        if event.type == "session.created" and event.session_id == self.root_session_id:
            self.nodes[event.session_id] = CollaborationNode(
                tenant_id=event.tenant_id,
                session_id=event.session_id,
                root_session_id=event.root_session_id,
                parent_session_id=None,
                role=CollaborationRole.ROOT,
                goal=str(payload["goal"]),
                task_key="root",
                status="pending",
            )
        elif event.type == "child.created":
            self.nodes[event.session_id] = CollaborationNode(
                tenant_id=event.tenant_id,
                session_id=event.session_id,
                root_session_id=event.root_session_id,
                parent_session_id=str(payload["parent_session_id"]),
                role=CollaborationRole(str(payload["role"])),
                goal=str(payload["goal"]),
                task_key=str(payload["task_key"]),
                output_contract=OutputContract.from_dict(dict(payload["output_contract"])),
                dependency_ids=tuple(str(item) for item in payload.get("dependency_ids", ())),
                budget=float(payload.get("budget", 1.0)),
                runtime_budget=dict(payload.get("runtime_budget", {})),
                target_session_id=payload.get("target_session_id"),
            )
            self._refresh_runnable()
        elif event.type == "dependency.changed":
            node = self._node(event.session_id)
            node.dependency_ids = tuple(str(item) for item in payload["dependency_ids"])
            self._refresh_runnable()
        elif event.type == "run.requested" and event.session_id in self.nodes:
            self.nodes[event.session_id].run_id = str(payload["run_id"])
        elif event.type in {"child.delegated", "session.handed_off"}:
            node = self._node(event.session_id)
            node.owner = str(payload["owner"])
            if node.status == "blocked" and self._dependencies_satisfied(node):
                node.status = "runnable"
        elif event.type == "run.started" and event.session_id in self.nodes:
            self.nodes[event.session_id].status = "running"
        elif event.type == "child.result_published":
            node = self._node(event.session_id)
            node.result = dict(payload)
            node.status = "completed"
            self._refresh_runnable()
        elif event.type == "review.completed":
            review = dict(payload)
            review["review_session_id"] = event.session_id
            self.reviews[event.session_id] = review
            node = self._node(event.session_id)
            node.status = "completed"
            node.result = review
            self._refresh_runnable()
        elif event.type == "join.completed":
            self.joined_children = tuple(str(item) for item in payload["child_session_ids"])
        elif event.type == "run.cancelled" and event.session_id in self.nodes:
            self.nodes[event.session_id].status = "cancelled"
            self._refresh_runnable()
        elif event.type == "run.failed" and event.session_id in self.nodes:
            self.nodes[event.session_id].status = "failed"
            self._refresh_runnable()

    def validate_new_child(
        self,
        *,
        parent_session_id: str,
        child_session_id: str,
        spec: ChildSpec,
        limits: CollaborationLimits,
    ) -> None:
        if not self.nodes or self.root_session_id not in self.nodes:
            raise CollaborationValidationError("Root Session does not exist")
        if parent_session_id not in self.nodes:
            raise CollaborationValidationError("parent must belong to the same Root and tenant")
        if child_session_id in self.nodes:
            raise CollaborationValidationError("Child Session already exists")
        if any(node.task_key == spec.task_key for node in self.nodes.values()):
            raise CollaborationValidationError(f"duplicate child task_key: {spec.task_key}")
        children = [
            node for node in self.nodes.values() if node.parent_session_id is not None
        ]
        if len(children) >= limits.max_children:
            raise CollaborationValidationError("root child count limit exceeded")
        width = Counter(node.parent_session_id for node in children)
        if width[parent_session_id] >= limits.max_children_per_parent:
            raise CollaborationValidationError("parent child width limit exceeded")
        if self.depth(parent_session_id) + 1 > limits.max_depth:
            raise CollaborationValidationError("child depth limit exceeded")
        if sum(node.budget for node in children) + spec.budget > limits.max_budget:
            raise CollaborationValidationError("root collaboration budget exceeded")
        self.validate_dependencies(child_session_id, spec.dependency_ids)

    def validate_dependencies(
        self, child_session_id: str, dependency_ids: tuple[str, ...]
    ) -> None:
        if child_session_id in dependency_ids:
            raise CollaborationValidationError("a Child cannot depend on itself")
        unknown = [dependency for dependency in dependency_ids if dependency not in self.nodes]
        if unknown:
            raise CollaborationValidationError(
                "dependencies must belong to the same Root and tenant"
            )
        graph = {
            session_id: tuple(node.dependency_ids)
            for session_id, node in self.nodes.items()
        }
        graph[child_session_id] = dependency_ids
        visiting: set[str] = set()
        visited: set[str] = set()

        def visit(session_id: str) -> None:
            if session_id in visiting:
                raise CollaborationValidationError("Task DAG must be acyclic")
            if session_id in visited:
                return
            visiting.add(session_id)
            for dependency in graph.get(session_id, ()):
                visit(dependency)
            visiting.remove(session_id)
            visited.add(session_id)

        for session_id in graph:
            visit(session_id)

    def require_joinable(self, child_session_ids: tuple[str, ...]) -> None:
        if not child_session_ids:
            raise CollaborationValidationError("join requires at least one Child")
        for session_id in child_session_ids:
            node = self.nodes.get(session_id)
            if node is None or node.root_session_id != self.root_session_id:
                raise CollaborationValidationError("join Child is outside this Root")
            if node.status != "completed" or node.result is None:
                raise CollaborationValidationError(f"Child is not completed: {session_id}")
            if node.role is CollaborationRole.REVIEWER:
                decision = node.result.get("decision")
                if decision != ReviewDecision.ACCEPTED.value:
                    raise CollaborationValidationError(
                        f"review is not accepted: {session_id}"
                    )

    def depth(self, session_id: str) -> int:
        depth = 0
        current = self.nodes[session_id]
        while current.parent_session_id is not None:
            depth += 1
            current = self.nodes[current.parent_session_id]
        return depth

    def runnable(self) -> list[CollaborationNode]:
        return [node for node in self.nodes.values() if node.status == "runnable"]

    def _refresh_runnable(self) -> None:
        for node in self.nodes.values():
            if node.role is CollaborationRole.ROOT or node.status in {
                "running",
                "completed",
                "failed",
                "cancelled",
            }:
                continue
            node.status = "runnable" if self._dependencies_satisfied(node) else "blocked"

    def _dependencies_satisfied(self, node: CollaborationNode) -> bool:
        return all(
            self.nodes[dependency].status == "completed"
            for dependency in node.dependency_ids
            if dependency in self.nodes
        ) and len(node.dependency_ids) == sum(
            dependency in self.nodes for dependency in node.dependency_ids
        )

    def _node(self, session_id: str) -> CollaborationNode:
        try:
            return self.nodes[session_id]
        except KeyError as exc:
            raise CollaborationValidationError(f"unknown Child Session: {session_id}") from exc
