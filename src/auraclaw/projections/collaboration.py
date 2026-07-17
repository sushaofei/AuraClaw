from __future__ import annotations

import asyncio
from collections.abc import Sequence
from typing import Any

from auraclaw.contracts.events import CanonicalEvent
from auraclaw.domain.collaboration import CollaborationAggregate

COLLABORATION_EVENTS = {
    "session.created",
    "child.created",
    "dependency.changed",
    "child.delegated",
    "session.handed_off",
    "run.requested",
    "run.started",
    "child.result_published",
    "review.completed",
    "join.completed",
    "run.completed",
    "run.failed",
    "run.cancelled",
}


class InMemoryCollaborationProjection:
    """Disposable root graph view with dependency-derived runnable state."""

    def __init__(self) -> None:
        self._events: dict[tuple[str, str], list[CanonicalEvent]] = {}
        self._event_ids: set[str] = set()
        self._views: dict[tuple[str, str], dict[str, Any]] = {}
        self._lock = asyncio.Lock()

    async def project(self, events: Sequence[CanonicalEvent]) -> None:
        async with self._lock:
            changed_roots: set[tuple[str, str]] = set()
            for event in events:
                if event.event_id in self._event_ids:
                    continue
                self._event_ids.add(event.event_id)
                if event.type not in COLLABORATION_EVENTS:
                    continue
                root_key = (event.tenant_id, event.root_session_id)
                self._events.setdefault(root_key, []).append(event)
                changed_roots.add(root_key)
            for tenant_id, root_session_id in changed_roots:
                self._rebuild_root(tenant_id, root_session_id)

    async def get(
        self, tenant_id: str, session_id: str
    ) -> dict[str, Any] | None:
        view = self._views.get((tenant_id, session_id))
        return dict(view) if view is not None else None

    async def list_children(
        self, tenant_id: str, root_session_id: str
    ) -> list[dict[str, Any]]:
        return [
            dict(view)
            for (view_tenant, _), view in self._views.items()
            if view_tenant == tenant_id
            and view["root_session_id"] == root_session_id
            and view["session_id"] != root_session_id
        ]

    async def list_runnable(
        self, tenant_id: str, root_session_id: str
    ) -> list[dict[str, Any]]:
        return [
            view
            for view in await self.list_children(tenant_id, root_session_id)
            if view["runnable"]
        ]

    async def rebuild(
        self, events: Sequence[CanonicalEvent], tenant_id: str | None = None
    ) -> int:
        async with self._lock:
            if tenant_id is None:
                self._events.clear()
                self._event_ids.clear()
                self._views.clear()
            else:
                self._events = {
                    key: value for key, value in self._events.items() if key[0] != tenant_id
                }
                self._views = {
                    key: value for key, value in self._views.items() if key[0] != tenant_id
                }
                self._event_ids.difference_update(
                    event.event_id for event in events if event.tenant_id == tenant_id
                )
        selected = [event for event in events if tenant_id is None or event.tenant_id == tenant_id]
        await self.project(selected)
        return len(selected)

    def _rebuild_root(self, tenant_id: str, root_session_id: str) -> None:
        graph = CollaborationAggregate.from_events(
            tenant_id,
            root_session_id,
            self._events[(tenant_id, root_session_id)],
        )
        for key in [
            key
            for key, view in self._views.items()
            if key[0] == tenant_id and view["root_session_id"] == root_session_id
        ]:
            del self._views[key]
        for node in graph.nodes.values():
            result = node.result or {}
            self._views[(tenant_id, node.session_id)] = {
                "tenant_id": tenant_id,
                "root_session_id": root_session_id,
                "session_id": node.session_id,
                "parent_session_id": node.parent_session_id,
                "role": node.role.value,
                "task_key": node.task_key,
                "goal": node.goal,
                "dependency_ids": list(node.dependency_ids),
                "owner": node.owner,
                "status": node.status,
                "runnable": node.status == "runnable",
                "output_contract": node.output_contract.as_dict(),
                "budget": node.budget,
                "result_ref": result.get("result_ref"),
                "artifact_refs": list(result.get("artifact_refs", [])),
                "target_session_id": node.target_session_id,
                "run_id": node.run_id,
                "review_decision": result.get("decision"),
                "evidence_refs": list(result.get("evidence_refs", [])),
            }
