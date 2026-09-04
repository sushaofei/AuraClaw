from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Sequence
from dataclasses import replace
from datetime import timedelta
from typing import Protocol

from auraclaw.contracts.events import CanonicalEvent
from auraclaw.control.ports import (
    DEFAULT_RUNTIME_MAX_STEPS,
    ControlStateStore,
    RunnableItem,
    RuntimeBudget,
)
from auraclaw.domain.collaboration import CollaborationAggregate
from auraclaw.domain.skill_execution import RUN_TERMINAL_EVENTS, pending_skill_invocations
from auraclaw.projection.collaboration.projector import COLLABORATION_EVENTS
from auraclaw.session.ports import ClaimedOutboxRecord

logger = logging.getLogger(__name__)

COLLABORATION_CONTROL_EVENTS = {
    "run.requested",
    "dependency.changed",
    "child.result_published",
    "review.completed",
    "run.failed",
    "run.cancelled",
}

APPROVAL_RESUME_EVENTS = {"approval.approved", "approval.rejected"}


class ControlFeedSource(Protocol):
    async def load(
        self, tenant_id: str, session_id: str, *, from_version: int = 1
    ) -> list[CanonicalEvent]: ...

    async def load_root(
        self,
        tenant_id: str,
        root_session_id: str,
        *,
        event_types: Sequence[str] | None = None,
        limit: int | None = None,
    ) -> list[CanonicalEvent]: ...

    async def claim_outbox(
        self,
        destination: str,
        worker_id: str,
        *,
        limit: int,
        claim_ttl: timedelta,
        wait_seconds: float = 0,
    ) -> list[ClaimedOutboxRecord]: ...

    async def disposition_outbox(
        self,
        destination: str,
        worker_id: str,
        outbox_id: str,
        claim_token: str,
        disposition: str,
        reason: str | None = None,
    ) -> bool: ...


class RunnableFeedConsumer:
    """Converts Session-owned runnable facts into an idempotent Control queue."""

    def __init__(
        self,
        source: ControlFeedSource,
        store: ControlStateStore,
        *,
        worker_id: str,
        wait_seconds: float = 0,
        waiting_recovery_interval: timedelta = timedelta(seconds=5),
    ) -> None:
        self._source = source
        self._store = store
        self._worker_id = worker_id
        self._wait_seconds = max(0.0, wait_seconds)
        self._waiting_recovery_interval = max(0.0, waiting_recovery_interval.total_seconds())
        self._next_waiting_recovery_at = 0.0
        self._ack_tasks: set[asyncio.Task[None]] = set()

    async def run_once(self, *, limit: int = 100) -> int:
        records = await self._source.claim_outbox(
            "control",
            self._worker_id,
            limit=limit,
            claim_ttl=timedelta(seconds=30),
            wait_seconds=self._wait_seconds,
        )
        enqueued = 0
        for record in records:
            try:
                if (
                    record.event.type in RUN_TERMINAL_EVENTS
                    or record.event.type == "skill.invocation.requested"
                ):
                    observed = await self._source.load(
                        record.event.tenant_id, record.event.session_id
                    )
                    terminal = next(
                        (
                            e
                            for e in observed
                            if e.run_id == record.event.run_id and e.type in RUN_TERMINAL_EVENTS
                        ),
                        None,
                    )
                    if terminal is not None:
                        task_id = (
                            f"{record.event.tenant_id}:{record.event.session_id}:"
                            f"{record.event.run_id}"
                        )
                        if pending_skill_invocations(observed, run_id=record.event.run_id):
                            await self._store.suspend_assignment(task_id, "waiting_for_tool")
                        else:
                            await self._store.finish_assignment(task_id, terminal.type[4:])
                item = self._derive_from_record(record)
                if item is None:
                    events = await self._source.load(
                        record.event.tenant_id, record.event.session_id
                    )
                    item = self._derive(events, record.event.aggregate_version)
                if (
                    item is not None
                    and item.root_session_id != item.session_id
                    and (item.user_id is None or item.dept_id is None)
                ):
                    root_events = await self._source.load(item.tenant_id, item.root_session_id)
                    item = replace(
                        item,
                        user_id=item.user_id or self._owner_user_id(root_events),
                        dept_id=item.dept_id or self._owner_dept_id(root_events),
                    )
                if item is not None:
                    inserted = await self._store.enqueue(item)
                    enqueued += int(inserted)
                    if not inserted and record.event.type in APPROVAL_RESUME_EVENTS:
                        enqueued += int(await self._store.wake_assignment(item.task_id))
                if (
                    record.event.root_session_id != record.event.session_id
                    and record.event.type in COLLABORATION_CONTROL_EVENTS
                ):
                    root_events = await self._source.load_root(
                        record.event.tenant_id,
                        record.event.root_session_id,
                        event_types=tuple(COLLABORATION_EVENTS),
                    )
                    graph = CollaborationAggregate.from_events(
                        record.event.tenant_id,
                        record.event.root_session_id,
                        root_events,
                    )
                    for collaboration_item in self._derive_collaboration(root_events, graph=graph):
                        enqueued += int(await self._store.enqueue(collaboration_item))
                    await self._wake_waiting_coordinator(graph)
                # Ack off the schedule critical path; enqueue is idempotent so
                # a redelivered outbox row after crash is safe.
                self._schedule_ack(record)
            except Exception as exc:
                await self._source.disposition_outbox(
                    "control",
                    self._worker_id,
                    record.outbox_id,
                    record.claim_token,
                    "nack",
                    str(exc),
                )
        now = time.monotonic()
        if now >= self._next_waiting_recovery_at:
            self._next_waiting_recovery_at = now + self._waiting_recovery_interval
            enqueued += await self._recover_waiting_coordinators()
            enqueued += await self._recover_waiting_tools()
        return enqueued

    async def _recover_waiting_tools(self, *, limit: int = 100) -> int:
        # Schedule recovery work only. Runtime owns the original invocation result semantics.
        recovered = 0
        for assignment in await self._store.list_waiting_assignments(
            limit=limit, status="waiting_for_tool"
        ):
            task_id = f"{assignment.tenant_id}:{assignment.session_id}:{assignment.run_id}"
            recovered += int(await self._store.wake_assignment(task_id))
        return recovered

    async def _recover_waiting_coordinators(self, *, limit: int = 100) -> int:
        recovered = 0
        for assignment in await self._store.list_waiting_assignments(limit=limit):
            root_events = await self._source.load_root(
                assignment.tenant_id,
                assignment.root_session_id,
                event_types=tuple(COLLABORATION_EVENTS),
            )
            if not root_events:
                continue
            graph = CollaborationAggregate.from_events(
                assignment.tenant_id,
                assignment.root_session_id,
                root_events,
            )
            recovered += int(await self._wake_waiting_coordinator(graph))
        return recovered

    def _schedule_ack(self, record: ClaimedOutboxRecord) -> None:
        async def _ack() -> None:
            try:
                accepted = await self._source.disposition_outbox(
                    "control",
                    self._worker_id,
                    record.outbox_id,
                    record.claim_token,
                    "ack",
                )
                if not accepted:
                    logger.warning(
                        "Session rejected control outbox acknowledgement outbox_id=%s",
                        record.outbox_id,
                    )
            except Exception:
                logger.exception("failed to ack control outbox outbox_id=%s", record.outbox_id)

        task = asyncio.create_task(_ack(), name=f"control-outbox-ack-{record.outbox_id}")
        self._ack_tasks.add(task)
        task.add_done_callback(self._ack_tasks.discard)

    @staticmethod
    def _derive_from_record(record: ClaimedOutboxRecord) -> RunnableItem | None:
        """Hot path for first-run scheduling without a full Session feed reload."""
        event = record.event
        if event.type != "run.requested":
            return None
        # Only a user-originated root request carries a stable identity on the hot
        # path. Coordinator/runtime requests fall back to canonical feed recovery.
        if event.actor.type != "user":
            return None
        run_id = event.payload.get("run_id")
        if run_id is None:
            return None
        role = str(event.payload.get("role", "root"))
        configured = event.payload.get("budget")
        budget = RuntimeBudget()
        if isinstance(configured, dict):
            budget = RuntimeBudget(
                max_steps=int(configured.get("max_steps", DEFAULT_RUNTIME_MAX_STEPS)),
                max_output_tokens=int(configured.get("max_output_tokens", 8192)),
                policy_version=str(configured.get("policy_version", "1")),
                max_cost=(
                    float(configured["max_cost"])
                    if configured.get("max_cost") is not None
                    else None
                ),
            )
        return RunnableItem(
            task_id=f"{event.tenant_id}:{event.session_id}:{run_id}",
            tenant_id=event.tenant_id,
            root_session_id=event.root_session_id,
            session_id=event.session_id,
            run_id=str(run_id),
            source_version=event.aggregate_version,
            queue_partition=event.tenant_id,
            role=role,
            budget=budget,
            user_id=event.actor.id,
            dept_id=_optional_str(event.payload.get("dept_id")),
        )

    @staticmethod
    def _derive(events: Sequence[CanonicalEvent], source_version: int) -> RunnableItem | None:
        if not events:
            return None
        role = "root"
        dependencies: list[str] = []
        run_id: str | None = None
        budget = RuntimeBudget()
        terminal_runs: set[str] = set()
        owner_user_id = RunnableFeedConsumer._owner_user_id(events)
        owner_dept_id = RunnableFeedConsumer._owner_dept_id(events)
        for event in events:
            if event.type in {"session.created", "child.created"}:
                role = str(event.payload.get("role", role))
                dependencies = list(event.payload.get("dependency_ids", dependencies))
                configured = event.payload.get("budget")
                if isinstance(configured, dict):
                    budget = RuntimeBudget(
                        max_steps=int(configured.get("max_steps", DEFAULT_RUNTIME_MAX_STEPS)),
                        max_output_tokens=int(configured.get("max_output_tokens", 8192)),
                        policy_version=str(configured.get("policy_version", "1")),
                        max_cost=(
                            float(configured["max_cost"])
                            if configured.get("max_cost") is not None
                            else None
                        ),
                    )
            elif event.type == "dependency.changed":
                dependencies = list(event.payload.get("dependency_ids", ()))
            elif event.type in {"run.requested", "session.resumed"}:
                run_id = str(event.payload["run_id"])
                configured = event.payload.get("budget")
                if isinstance(configured, dict):
                    budget = RuntimeBudget(
                        max_steps=int(configured.get("max_steps", DEFAULT_RUNTIME_MAX_STEPS)),
                        max_output_tokens=int(configured.get("max_output_tokens", 8192)),
                        max_cost=(float(configured["max_cost"])
                                  if configured.get("max_cost") is not None else None),
                        policy_version=str(configured.get("policy_version", "1")),
                    )
            elif event.type in {"run.completed", "run.failed", "run.cancelled"}:
                if event.run_id is not None:
                    terminal_runs.add(event.run_id)
        latest = events[-1]
        recovering = bool(pending_skill_invocations(events, run_id=run_id))
        if (
            run_id is None
            or (run_id in terminal_runs and not recovering)
            or (dependencies and not recovering)
        ):
            return None
        return RunnableItem(
            task_id=f"{latest.tenant_id}:{latest.session_id}:{run_id}",
            tenant_id=latest.tenant_id,
            root_session_id=latest.root_session_id,
            session_id=latest.session_id,
            run_id=run_id,
            source_version=source_version,
            queue_partition=latest.tenant_id,
            role=role,
            budget=budget,
            user_id=owner_user_id,
            dept_id=owner_dept_id,
        )

    @staticmethod
    def _derive_collaboration(
        events: Sequence[CanonicalEvent],
        *,
        graph: CollaborationAggregate | None = None,
    ) -> list[RunnableItem]:
        if not events:
            return []
        root = events[0].root_session_id
        tenant_id = events[0].tenant_id
        selected_graph = graph or CollaborationAggregate.from_events(tenant_id, root, events)
        latest_versions: dict[str, int] = {}
        runtime_budgets: dict[str, RuntimeBudget] = {}
        for event in events:
            latest_versions[event.session_id] = max(
                latest_versions.get(event.session_id, 0), event.aggregate_version
            )
            if event.type != "child.created":
                continue
            configured = event.payload.get("runtime_budget")
            if isinstance(configured, dict):
                runtime_budgets[event.session_id] = RuntimeBudget(
                    max_steps=int(configured.get("max_steps", DEFAULT_RUNTIME_MAX_STEPS)),
                    max_output_tokens=int(configured.get("max_output_tokens", 8192)),
                    policy_version=str(configured.get("policy_version", "1")),
                    max_cost=(
                        float(configured["max_cost"])
                        if configured.get("max_cost") is not None
                        else None
                    ),
                )
        owner_user_id = RunnableFeedConsumer._owner_user_id(events)
        owner_dept_id = RunnableFeedConsumer._owner_dept_id(events)
        items: list[RunnableItem] = []
        for node in selected_graph.runnable():
            if node.run_id is None:
                continue
            items.append(
                RunnableItem(
                    task_id=f"{tenant_id}:{node.session_id}:{node.run_id}",
                    tenant_id=tenant_id,
                    root_session_id=root,
                    session_id=node.session_id,
                    run_id=node.run_id,
                    source_version=latest_versions[node.session_id],
                    queue_partition=tenant_id,
                    role=node.role.value,
                    budget=runtime_budgets.get(node.session_id, RuntimeBudget()),
                    user_id=owner_user_id,
                    dept_id=owner_dept_id,
                )
            )
        return items

    async def _wake_waiting_coordinator(self, graph: CollaborationAggregate) -> bool:
        root = graph.nodes.get(graph.root_session_id)
        terminal = {"completed", "failed", "cancelled"}
        if root is None or root.run_id is None or root.status in terminal:
            return False
        checkpoint = await self._store.load_checkpoint(
            graph.tenant_id, graph.root_session_id, root.run_id
        )
        if checkpoint is None or checkpoint.phase not in {
            "agent.waiting_children",
            "collaboration.waiting_children",
        }:
            return False
        waiting = tuple(str(item) for item in checkpoint.state.get("waiting_child_ids", ()))
        if not waiting:
            children = tuple(
                node for node in graph.nodes.values() if node.parent_session_id is not None
            )
            active = tuple(node for node in children if node.status not in terminal)
            if not children or active:
                logger.warning(
                    "collaboration.wait_set_missing root=%s active_children=%s",
                    graph.root_session_id,
                    [node.session_id for node in active],
                )
                return False
            waiting = tuple(node.session_id for node in children)
            logger.warning(
                "collaboration.wait_set_recovered root=%s terminal_children=%s",
                graph.root_session_id,
                list(waiting),
            )
        if not all(
            child_id in graph.nodes and graph.nodes[child_id].status in terminal
            for child_id in waiting
        ):
            return False
        return await self._store.wake_assignment(
            f"{graph.tenant_id}:{graph.root_session_id}:{root.run_id}"
        )

    @staticmethod
    def _owner_user_id(events: Sequence[CanonicalEvent]) -> str | None:
        """Resolve the stable task owner from the canonical creation fact."""
        for event in events:
            if event.type == "session.created" and event.actor.type == "user":
                return event.actor.id
        return None

    @staticmethod
    def _owner_dept_id(events: Sequence[CanonicalEvent]) -> str | None:
        """Resolve the frozen department from the canonical creation fact."""
        for event in events:
            if event.type == "session.created":
                return _optional_str(event.payload.get("dept_id"))
        return None


def _optional_str(value: object) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None
