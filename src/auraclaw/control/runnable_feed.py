from __future__ import annotations

from collections.abc import Sequence
from datetime import timedelta
from typing import Protocol

from auraclaw.contracts.events import CanonicalEvent
from auraclaw.control.ports import ControlStateStore, RunnableItem, RuntimeBudget
from auraclaw.session.ports import ClaimedOutboxRecord


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
                events = await self._source.load(
                    record.event.tenant_id, record.event.session_id
                )
                item = self._derive(events, record.event.aggregate_version)
                if item is not None:
                    enqueued += int(await self._store.enqueue(item))
                accepted = await self._source.disposition_outbox(
                    "control",
                    self._worker_id,
                    record.outbox_id,
                    record.claim_token,
                    "ack",
                )
                if not accepted:
                    raise RuntimeError("Session rejected control outbox acknowledgement")
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
        )
