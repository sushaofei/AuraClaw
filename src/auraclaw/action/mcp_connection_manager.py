from __future__ import annotations

import asyncio
from collections.abc import Callable, MutableMapping
from datetime import UTC, datetime
from typing import Any

from auraclaw.action.capability_catalog import CapabilityCatalog
from auraclaw.action.catalog_reconciler import CapabilityCatalogReconciler
from auraclaw.action.mcp_registry import McpServerRegistryService
from auraclaw.action.ports import CapabilityConnector
from auraclaw.contracts.capabilities import CapabilityStatus, McpServerDefinition
from auraclaw.contracts.mcp_registry import (
    McpActiveSnapshotEntry,
    McpDesiredState,
    McpObservedState,
    McpServerRuntimeRecord,
)

McpConnectorFactory = Callable[[McpServerDefinition], CapabilityConnector]


class McpConnectionManager:
    """Loads active MCP revisions into Action Hands without process restart."""

    def __init__(
        self,
        *,
        registry: McpServerRegistryService,
        connectors: MutableMapping[str, CapabilityConnector],
        factory: McpConnectorFactory,
        catalog: CapabilityCatalog | None = None,
        reconciler: CapabilityCatalogReconciler | None = None,
        drain_seconds: float = 5.0,
    ) -> None:
        self._registry = registry
        self._connectors = connectors
        self._factory = factory
        self._catalog = catalog
        self._reconciler = reconciler
        self._drain_seconds = drain_seconds
        self._generations: dict[str, int] = {}
        self._draining: list[CapabilityConnector] = []

    async def restore(self) -> int:
        snapshot = await self._registry.active_snapshot()
        for entry in snapshot:
            await self.apply(entry, restore=True)
        return len(snapshot)

    async def test(self, entry: McpActiveSnapshotEntry) -> None:
        connector = self._factory(self._definition(entry, enabled=True))
        try:
            await connector.snapshot(_probe_context(entry))
        finally:
            await connector.aclose()

    async def apply(
        self,
        entry: McpActiveSnapshotEntry,
        *,
        restore: bool = False,
        tested: bool = False,
    ) -> None:
        if not restore and not tested:
            await self.test(entry)
        previous = self._connectors.get(entry.server_id)
        definition = self._definition(entry, enabled=True)
        connector = self._factory(definition)
        if self._reconciler is not None:
            setter = getattr(connector, "set_notification_handler", None)
            if setter is not None:
                setter(self._reconciler.handle_notification)
        self._connectors[entry.server_id] = connector
        self._generations[entry.server_id] = entry.revision
        if self._catalog is not None:
            await self._catalog.register_server(definition)
        await self._registry.record_runtime(
            McpServerRuntimeRecord(
                server_id=entry.server_id,
                loaded_revision=entry.revision,
                observed_state=(
                    McpObservedState.LOADING if restore else McpObservedState.ACTIVE
                ),
                updated_at=datetime.now(UTC),
            )
        )
        if previous is not None and previous is not connector:
            self._draining.append(previous)
            asyncio.create_task(self._drain(previous))
        if self._reconciler is not None:
            result = await self._reconciler.reconcile_server(definition)
            observed = {
                CapabilityStatus.ACTIVE: McpObservedState.ACTIVE,
                CapabilityStatus.DEGRADED: McpObservedState.DEGRADED,
            }.get(result.status, McpObservedState.QUARANTINED)
            await self._registry.record_runtime(
                McpServerRuntimeRecord(
                    server_id=entry.server_id,
                    loaded_revision=entry.revision,
                    observed_state=observed,
                    last_sync_at=datetime.now(UTC),
                    consecutive_failures=0 if result.error is None else 1,
                    safe_error_code=(
                        None if result.error is None else "mcp_catalog_degraded"
                    ),
                    updated_at=datetime.now(UTC),
                )
            )

    async def revoke(self, server_id: str) -> None:
        previous = self._connectors.pop(server_id, None)
        self._generations.pop(server_id, None)
        if self._reconciler is not None:
            await self._reconciler.drop_server(server_id)
        await self._registry.record_runtime(
            McpServerRuntimeRecord(
                server_id=server_id,
                loaded_revision=None,
                observed_state=McpObservedState.DISABLED,
                updated_at=datetime.now(UTC),
            )
        )
        if previous is not None:
            self._draining.append(previous)
            asyncio.create_task(self._drain(previous))

    async def reconcile_loaded(self) -> int:
        snapshot = {
            entry.server_id: entry
            for entry in await self._registry.active_snapshot()
        }
        changed = 0
        for server_id, revision in list(self._generations.items()):
            desired = snapshot.get(server_id)
            if desired is None:
                await self.revoke(server_id)
                changed += 1
                continue
            if desired.revision != revision:
                await self.apply(desired)
                changed += 1
        for server_id, entry in snapshot.items():
            if self._generations.get(server_id) != entry.revision:
                await self.apply(entry)
                changed += 1
        return changed

    def _definition(
        self, entry: McpActiveSnapshotEntry, *, enabled: bool
    ) -> McpServerDefinition:
        return entry.config.materialize(
            revision=entry.revision,
            desired_state=(
                McpDesiredState.ENABLED if enabled else McpDesiredState.DISABLED
            ),
            observed_state=entry.observed_state,
        ).model_copy(update={"enabled": enabled, "status": CapabilityStatus.ACTIVE})

    async def _drain(self, connector: Any) -> None:
        await asyncio.sleep(self._drain_seconds)
        close = getattr(connector, "aclose", None)
        if close is not None:
            await close()
        if connector in self._draining:
            self._draining.remove(connector)


def _probe_context(entry: McpActiveSnapshotEntry) -> Any:
    from auraclaw.contracts.hands import HandsTrustedContext

    return HandsTrustedContext(
        tenant_id=entry.tenant_id or "platform",
        root_session_id="mcp-test",
        session_id="mcp-test",
        run_id="mcp-test",
        runtime_id="mcp-test",
        lease_id="mcp-test",
        fencing_token=1,
        deadline=None,
        user_id="mcp-admin",
    )
