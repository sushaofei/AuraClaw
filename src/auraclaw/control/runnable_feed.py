from __future__ import annotations

import asyncio
import logging
from collections.abc import Sequence
from dataclasses import replace
from datetime import timedelta
from typing import Protocol

from auraclaw.contracts.events import CanonicalEvent
from auraclaw.control.ports import ControlStateStore, RunnableItem, RuntimeBudget
from auraclaw.session.ports import ClaimedOutboxRecord

logger = logging.getLogger(__name__)


class ControlFeedSource(Protocol):
    async def load(
        self, tenant_id: str, session_id: str, *, from_version: int = 1
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
    ) -> None:
        self._source = source
        self._store = store
        self._worker_id = worker_id
        self._wait_seconds = max(0.0, wait_seconds)
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
                item = self._derive_from_record(record)
                if item is None:
                    events = await self._source.load(
                        record.event.tenant_id, record.event.session_id
                    )
                    item = self._derive(events, record.event.aggregate_version)
                if (
                    item is not None
                    and item.user_id is None
                    and item.root_session_id != item.session_id
                ):
                    root_events = await self._source.load(
                        item.tenant_id, item.root_session_id
                    )
                    item = replace(item, user_id=self._owner_user_id(root_events))
                if item is not None:
                    enqueued += int(await self._store.enqueue(item))
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
        return enqueued

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
                logger.exception(
                    "failed to ack control outbox outbox_id=%s", record.outbox_id
                )

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
                max_steps=int(configured.get("max_steps", 16)),
                max_output_tokens=int(configured.get("max_output_tokens", 8192)),
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
        )

    @staticmethod
    def _derive(
        events: Sequence[CanonicalEvent], source_version: int
    ) -> RunnableItem | None:
        if not events:
            return None
        role = "root"
        dependencies: list[str] = []
        run_id: str | None = None
        budget = RuntimeBudget()
        terminal_runs: set[str] = set()
        owner_user_id = RunnableFeedConsumer._owner_user_id(events)
        for event in events:
            if event.type in {"session.created", "child.created"}:
                role = str(event.payload.get("role", role))
                dependencies = list(event.payload.get("dependency_ids", dependencies))
                configured = event.payload.get("budget")
                if isinstance(configured, dict):
                    budget = RuntimeBudget(
                        max_steps=int(configured.get("max_steps", 16)),
                        max_output_tokens=int(configured.get("max_output_tokens", 8192)),
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
            elif event.type in {"run.completed", "run.failed", "run.cancelled"}:
                if event.run_id is not None:
                    terminal_runs.add(event.run_id)
        latest = events[-1]
        if run_id is None or run_id in terminal_runs or dependencies:
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
        )

    @staticmethod
    def _owner_user_id(events: Sequence[CanonicalEvent]) -> str | None:
        """Resolve the stable task owner from the canonical creation fact."""
        for event in events:
            if event.type == "session.created" and event.actor.type == "user":
                return event.actor.id
        return None
