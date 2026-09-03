from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable, MutableMapping
from datetime import UTC, datetime, timedelta
from time import monotonic
from typing import Any

from auraclaw.contracts.capabilities import McpAuthStrategy, McpServerDefinition
from auraclaw.contracts.errors import CredentialAccessError
from auraclaw.contracts.mcp_registry import (
    McpActiveSnapshotEntry,
    McpDesiredState,
    none_credential_ref,
)
from auraclaw.contracts.tools import CredentialReference
from auraclaw.infrastructure.credentials.mcp_egress import ManagedMcpEgressAdapter
from auraclaw.infrastructure.credentials.proxy import CredentialProxy

logger = logging.getLogger(__name__)


class McpEgressManager:
    """Loads Credential Proxy adapters from the Hands registry snapshot."""

    def __init__(
        self,
        *,
        adapters: MutableMapping[str, Any],
        proxy: CredentialProxy,
        drain_seconds: float = 5.0,
        snapshot_provider: (
            Callable[[], Awaitable[tuple[McpActiveSnapshotEntry, ...]]] | None
        ) = None,
        probe_ttl_seconds: float = 60.0,
    ) -> None:
        self._adapters = adapters
        self._proxy = proxy
        self._drain_seconds = drain_seconds
        self._generations: dict[str, int] = {}
        self._snapshot_provider = snapshot_provider
        self._probe_ttl_seconds = probe_ttl_seconds
        self._probes: dict[tuple[str, int], float] = {}
        self._locks: dict[str, asyncio.Lock] = {}
        self._closing: dict[str, list[Any]] = {}

    async def restore(self, snapshot: tuple[McpActiveSnapshotEntry, ...]) -> int:
        for entry in snapshot:
            await self.apply(entry)
        return len(snapshot)

    async def apply(self, entry: McpActiveSnapshotEntry) -> None:
        async with self._locks.setdefault(entry.server_id, asyncio.Lock()):
            is_probe = entry.desired_state is not McpDesiredState.ENABLED
            current = self._generations.get(entry.server_id, 0)
            if not is_probe and current > entry.revision:
                return
            if self._snapshot_provider is not None and not is_probe:
                desired = {item.server_id: item for item in await self._snapshot_provider()}
                authoritative = desired.get(entry.server_id)
                if authoritative is None or authoritative.revision != entry.revision:
                    raise CredentialAccessError("stale MCP egress apply rejected by authority")
            if current == entry.revision:
                adapter = self._adapters.get(f"mcp:{entry.server_id}")
                if adapter is not None and not is_probe:
                    adapter.set_admission(True)
                return
            key = (
                f"mcp:{entry.server_id}:probe:{entry.revision}"
                if is_probe
                else f"mcp:{entry.server_id}"
            )
            if is_probe and key in self._adapters:
                self._probes[(entry.server_id, entry.revision)] = (
                    monotonic() + self._probe_ttl_seconds
                )
                return
            definition = entry.config.materialize(
                revision=entry.revision,
                desired_state=McpDesiredState.ENABLED,
                observed_state=entry.observed_state,
            ).model_copy(update={"enabled": True})
            adapter = ManagedMcpEgressAdapter(definition, probe_only=is_probe)
            try:
                active = self._adapters.get(f"mcp:{entry.server_id}")
                if (
                    is_probe
                    and active is not None
                    and active.credential_scope != adapter.credential_scope
                    and active.credential_ref == definition.credential_ref
                    and adapter.secret_required
                ):
                    raise CredentialAccessError(
                        "Testing a new MCP credential scope requires "
                        "a distinct credential reference"
                    )
                await self._ensure_reference(definition, adapter.credential_scope)
            except BaseException:
                await adapter.aclose()
                raise
            previous = self._adapters.get(key)
            self._adapters[key] = adapter
            if is_probe:
                self._probes[(entry.server_id, entry.revision)] = (
                    monotonic() + self._probe_ttl_seconds
                )
            else:
                self._generations[entry.server_id] = entry.revision
                self._remove_probes(entry.server_id)
            if previous is not None:
                previous.set_admission(False)
                self._closing.setdefault(entry.server_id, []).append(previous)
            await self._close_pending(entry.server_id)

    async def revoke(self, server_id: str, *, expected_revision: int | None = None) -> None:
        async with self._locks.setdefault(server_id, asyncio.Lock()):
            self._remove_probes(server_id, expected_revision=expected_revision)
            current = self._generations.get(server_id)
            if expected_revision is not None and current not in {None, expected_revision}:
                await self._close_pending(server_id)
                return
            installed = self._adapters.get(f"mcp:{server_id}")
            if installed is not None:
                installed.set_admission(False)
            if self._snapshot_provider is not None:
                desired = {item.server_id: item for item in await self._snapshot_provider()}
                # A late old revoke cannot remove a re-enabled target, even when
                # disable/enable reused the same configuration revision.
                if server_id in desired:
                    if installed is not None and desired[server_id].revision == current:
                        installed.set_admission(True)
                    await self._close_pending(server_id)
                    return
            previous = self._adapters.pop(f"mcp:{server_id}", None)
            if previous is not None:
                previous.set_admission(False)
                self._closing.setdefault(server_id, []).append(previous)
            self._generations.pop(server_id, None)
            await self._close_pending(server_id)

    def _remove_probes(self, server_id: str, *, expected_revision: int | None = None) -> None:
        for probe_server, revision in tuple(self._probes):
            if probe_server != server_id or expected_revision not in {None, revision}:
                continue
            adapter = self._adapters.pop(f"mcp:{server_id}:probe:{revision}", None)
            if adapter is not None:
                adapter.set_admission(False)
                self._closing.setdefault(server_id, []).append(adapter)
            self._probes.pop((server_id, revision), None)

    async def _close_pending(self, server_id: str) -> None:
        pending = self._closing.get(server_id, [])
        for adapter in tuple(pending):
            if self._drain_seconds:
                await asyncio.sleep(self._drain_seconds)
            await adapter.aclose()
            pending.remove(adapter)
        if not pending:
            self._closing.pop(server_id, None)

    async def reconcile(self, snapshot: tuple[McpActiveSnapshotEntry, ...]) -> int:
        desired = {entry.server_id: entry for entry in snapshot}
        server_ids = (
            set(desired)
            | set(self._generations)
            | set(self._closing)
            | {server_id for server_id, _ in self._probes}
        )
        semaphore = asyncio.Semaphore(8)

        async def reconcile_one(server_id: str) -> int:
            async with semaphore:
                try:
                    async with self._locks.setdefault(server_id, asyncio.Lock()):
                        for (probe_server, revision), expires_at in tuple(self._probes.items()):
                            if probe_server == server_id and expires_at <= monotonic():
                                self._remove_probes(server_id, expected_revision=revision)
                    entry = desired.get(server_id)
                    if entry is not None:
                        if self._generations.get(server_id) != entry.revision:
                            await self.apply(entry)
                            return 1
                    elif server_id in self._generations:
                        await self.revoke(server_id, expected_revision=self._generations[server_id])
                        return 1
                    async with self._locks.setdefault(server_id, asyncio.Lock()):
                        await self._close_pending(server_id)
                except Exception:
                    logger.warning("MCP egress reconciliation pending; retry on next tick")
                return 0

        return sum(await asyncio.gather(*(reconcile_one(server_id) for server_id in server_ids)))

    def loaded_revision(self, server_id: str) -> int | None:
        return self._generations.get(server_id)

    async def _ensure_reference(self, definition: McpServerDefinition, account_scope: str) -> None:
        if definition.resolved_auth_strategy is McpAuthStrategy.NONE:
            credential_ref = none_credential_ref(definition.server_id)
        elif definition.credential_ref is None:
            return
        else:
            credential_ref = definition.credential_ref
        reference = CredentialReference(
            credential_ref=credential_ref,
            provider=definition.server_id,
            account_scope=account_scope,
            allowed_operations=("mcp.invoke",),
            expires_at=datetime.now(UTC) + timedelta(days=365),
        )
        tenants = {definition.tenant_id or "platform", "development"}
        for tenant_id in tenants:
            save = getattr(self._proxy, "save_reference", None)
            if save is not None:
                await save(tenant_id, reference)
            else:
                self._proxy.register_reference(tenant_id, reference)
