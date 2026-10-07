from __future__ import annotations

from collections.abc import Sequence
from typing import Any, Protocol

from auraclaw.contracts.events import CanonicalEvent
from auraclaw.projection.activity_view import (
    activity_node_id,
    fold_activity_event,
    page_activity,
)


class ActivityReader(Protocol):
    async def get_activity_page(
        self,
        tenant_id: str,
        session_id: str,
        *,
        after_version: int,
        limit: int,
        min_source_version: int,
    ) -> dict[str, Any] | None: ...


class InMemoryActivityProjection:
    """Disposable incremental Activity cache derived only from Canonical Events."""

    def __init__(self) -> None:
        self._nodes: dict[tuple[str, str], dict[str, dict[str, Any]]] = {}
        self._versions: dict[tuple[str, str], int] = {}
        self._complete: dict[tuple[str, str], bool] = {}
        self._event_ids: set[str] = set()

    async def project(self, events: Sequence[CanonicalEvent]) -> None:
        for event in events:
            if event.event_id in self._event_ids:
                continue
            key = (event.tenant_id, event.session_id)
            current = self._versions.get(key, 0)
            complete = self._complete.get(key, event.aggregate_version == 1)
            if complete and event.aggregate_version != current + 1:
                raise ValueError(
                    f"activity projection gap for {event.session_id}: "
                    f"expected {current + 1}, got {event.aggregate_version}"
                )
            self._event_ids.add(event.event_id)
            nodes = self._nodes.setdefault(key, {})
            node_id = activity_node_id(event)
            node = (
                fold_activity_event(event, nodes.get(node_id))
                if node_id is not None
                else None
            )
            if node_id is not None and node is not None:
                nodes[node_id] = node
            self._versions[key] = max(current, event.aggregate_version)
            self._complete[key] = complete

    async def get_activity_page(
        self,
        tenant_id: str,
        session_id: str,
        *,
        after_version: int,
        limit: int,
        min_source_version: int,
    ) -> dict[str, Any] | None:
        key = (tenant_id, session_id)
        source_version = self._versions.get(key, 0)
        if not self._complete.get(key, False) or source_version < min_source_version:
            return None
        return {
            "source_version": source_version,
            **page_activity(
                tuple(self._nodes.get(key, {}).values()),
                after_version=after_version,
                limit=limit,
            ),
        }

    async def rebuild(
        self, events: Sequence[CanonicalEvent], tenant_id: str | None = None
    ) -> int:
        if tenant_id is None:
            self._nodes.clear()
            self._versions.clear()
            self._complete.clear()
            self._event_ids.clear()
        else:
            keys = [key for key in self._nodes if key[0] == tenant_id]
            for key in keys:
                self._nodes.pop(key, None)
                self._versions.pop(key, None)
                self._complete.pop(key, None)
            selected_ids = {event.event_id for event in events if event.tenant_id == tenant_id}
            self._event_ids.difference_update(selected_ids)
        selected = [event for event in events if tenant_id is None or event.tenant_id == tenant_id]
        await self.project(selected)
        return len(selected)
