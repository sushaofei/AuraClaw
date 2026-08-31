from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable, MutableMapping
from datetime import UTC, datetime
from typing import Any, Protocol

from auraclaw.action.capability_catalog import CapabilityCatalog
from auraclaw.action.catalog_reconciler import (
    CapabilityCatalogReconciler,
    McpReconcileResult,
)
from auraclaw.action.mcp_registry import McpServerRegistryService
from auraclaw.action.ports import CapabilityConnector
from auraclaw.contracts.capabilities import CapabilityStatus, McpServerDefinition
from auraclaw.contracts.errors import AuraClawError
from auraclaw.contracts.mcp_registry import (
    McpActiveSnapshotEntry,
    McpDesiredState,
    McpObservedState,
    McpServerRuntimeRecord,
)

McpConnectorFactory = Callable[[McpServerDefinition], CapabilityConnector]
logger = logging.getLogger(__name__)


class McpEgressLoader(Protocol):
    async def apply(self, entry: McpActiveSnapshotEntry) -> None: ...

    async def revoke(self, server_id: str) -> None: ...


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
        egress: McpEgressLoader | None = None,
        drain_seconds: float = 5.0,
        instance_id: str = "legacy",
    ) -> None:
        self._registry = registry
        self._connectors = connectors
        self._factory = factory
        self._catalog = catalog
        self._reconciler = reconciler
        self._egress = egress
        self._drain_seconds = drain_seconds
        self._instance_id = instance_id
        self._generations: dict[str, int] = {}
        self._draining: list[CapabilityConnector] = []

    async def restore(self) -> int:
        snapshot = await self._registry.active_snapshot()
        loaded = 0
        for entry in snapshot:
            if await self._apply_isolated(entry, restore=True):
                loaded += 1
        return loaded

    async def test(
        self, entry: McpActiveSnapshotEntry, *, persist_egress: bool = False
    ) -> None:
        keep_egress = persist_egress or entry.server_id in self._generations
        if self._egress is not None:
            await self._egress.apply(entry)
        connector = self._factory(self._definition(entry, enabled=True))
        try:
            await connector.snapshot(_probe_context(entry))
        except BaseException:
            if self._egress is not None and not keep_egress:
                await self._egress.revoke(entry.server_id)
            raise
        finally:
            await connector.aclose()
        if self._egress is not None and not keep_egress:
            await self._egress.revoke(entry.server_id)
        await self._record_tested(entry)

    async def apply(
        self,
        entry: McpActiveSnapshotEntry,
        *,
        restore: bool = False,
        tested: bool = False,
    ) -> None:
        if not restore and not tested:
            await self.test(entry, persist_egress=True)
        elif self._egress is not None:
            await self._egress.apply(entry)
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
        now = datetime.now(UTC)
        tested_at = None if restore else now
        await self._registry.record_runtime(
            McpServerRuntimeRecord(
                server_id=entry.server_id,
                instance_id=self._instance_id,
                loaded_revision=entry.revision,
                observed_state=(
                    McpObservedState.LOADING if restore else McpObservedState.ACTIVE
                ),
                last_test_at=tested_at,
                updated_at=now,
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
                CapabilityStatus.QUARANTINED: McpObservedState.QUARANTINED,
            }.get(result.status, McpObservedState.QUARANTINED)
            await self._registry.record_runtime(
                McpServerRuntimeRecord(
                    server_id=entry.server_id,
                    instance_id=self._instance_id,
                    loaded_revision=entry.revision,
                    observed_state=observed,
                    last_test_at=tested_at,
                    last_sync_at=datetime.now(UTC),
                    consecutive_failures=result.consecutive_failures,
                    safe_error_code=(
                        None if result.error is None else "mcp_catalog_degraded"
                    ),
                    updated_at=datetime.now(UTC),
                )
            )

    async def revoke(self, server_id: str) -> None:
        previous = self._connectors.pop(server_id, None)
        self._generations.pop(server_id, None)
        if self._egress is not None:
            await self._egress.revoke(server_id)
        if self._reconciler is not None:
            await self._reconciler.drop_server(server_id)
        await self._registry.record_runtime(
            McpServerRuntimeRecord(
                server_id=server_id,
                instance_id=self._instance_id,
                loaded_revision=None,
                observed_state=McpObservedState.DISABLED,
                updated_at=datetime.now(UTC),
            )
        )
        if previous is not None:
            self._draining.append(previous)
            asyncio.create_task(self._drain(previous))

    async def purge(self, server_id: str) -> None:
        await self.revoke(server_id)
        if self._catalog is not None:
            await self._catalog.remove_server(server_id)

    async def reconcile_loaded(self) -> int:
        snapshot = {
            entry.server_id: entry
            for entry in await self._registry.active_snapshot()
        }
        changed = 0
        for server_id, revision in list(self._generations.items()):
            desired = snapshot.get(server_id)
            if desired is None:
                if await self._revoke_isolated(server_id):
                    changed += 1
                continue
            if desired.revision != revision:
                if await self._apply_isolated(desired):
                    changed += 1
        for server_id, entry in snapshot.items():
            if self._generations.get(server_id) != entry.revision:
                if await self._apply_isolated(entry):
                    changed += 1
        return changed

    async def record_reconcile_results(
        self, results: tuple[McpReconcileResult, ...]
    ) -> None:
        """Heartbeat this instance's catalog load state without global last-writer state."""
        now = datetime.now(UTC)
        for result in results:
            revision = self._generations.get(result.server_id)
            if revision is None:
                continue
            await self._registry.record_runtime(
                McpServerRuntimeRecord(
                    server_id=result.server_id,
                    instance_id=self._instance_id,
                    loaded_revision=revision,
                    observed_state=(
                        McpObservedState.ACTIVE
                        if result.status is CapabilityStatus.ACTIVE
                        else McpObservedState.QUARANTINED
                        if result.status is CapabilityStatus.QUARANTINED
                        else McpObservedState.DEGRADED
                    ),
                    last_sync_at=now,
                    consecutive_failures=result.consecutive_failures,
                    safe_error_code=(
                        None if result.error is None else "mcp_catalog_degraded"
                    ),
                    updated_at=now,
                )
            )

    async def _apply_isolated(
        self,
        entry: McpActiveSnapshotEntry,
        *,
        restore: bool = False,
    ) -> bool:
        try:
            await self.apply(entry, restore=restore)
            return True
        except Exception as exc:
            logger.warning(
                "MCP server %s is unreachable; continuing with other servers (%s)",
                entry.server_id,
                type(exc).__name__,
            )
            await self._record_unavailable(entry, exc)
            return False

    async def _revoke_isolated(self, server_id: str) -> bool:
        try:
            await self.revoke(server_id)
            return True
        except Exception as exc:
            logger.warning(
                "MCP server %s revoke failed; continuing with other servers (%s)",
                server_id,
                type(exc).__name__,
            )
            return False

    async def _record_unavailable(
        self, entry: McpActiveSnapshotEntry, exc: BaseException
    ) -> None:
        safe_error_code = (
            exc.code
            if isinstance(exc, AuraClawError) and isinstance(exc.code, str)
            else "mcp_connection_test_failed"
        )
        try:
            await self._registry.record_runtime(
                McpServerRuntimeRecord(
                    server_id=entry.server_id,
                    instance_id=self._instance_id,
                    loaded_revision=self._generations.get(entry.server_id),
                    observed_state=McpObservedState.UNAVAILABLE,
                    consecutive_failures=1,
                    safe_error_code=safe_error_code,
                    updated_at=datetime.now(UTC),
                )
            )
        except Exception:
            logger.warning(
                "failed to persist unavailable state for MCP server %s",
                entry.server_id,
            )

    async def _record_tested(self, entry: McpActiveSnapshotEntry) -> None:
        now = datetime.now(UTC)
        record = await self._registry.get_server(
            tenant_id=entry.tenant_id or "platform",
            server_id=entry.server_id,
            actor_id="mcp-admin",
        )
        previous = record.runtime
        await self._registry.record_runtime(
            McpServerRuntimeRecord(
                server_id=entry.server_id,
                instance_id=self._instance_id,
                loaded_revision=(
                    previous.loaded_revision if previous is not None else None
                ),
                observed_state=(
                    previous.observed_state
                    if previous is not None
                    else McpObservedState.PENDING
                ),
                last_test_at=now,
                last_sync_at=previous.last_sync_at if previous is not None else None,
                consecutive_failures=0,
                safe_error_code=None,
                updated_at=now,
            )
        )

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
