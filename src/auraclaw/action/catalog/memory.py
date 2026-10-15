from __future__ import annotations

import asyncio
import hashlib
import json
from datetime import UTC, datetime, timedelta

from auraclaw.action.ports import (
    CatalogCommitResult,
    CatalogReconcileLease,
    CatalogSyncHealth,
    CommittedCatalogSnapshot,
)
from auraclaw.contracts.capabilities import (
    CapabilityDescriptor,
    CapabilityStatus,
    McpServerDefinition,
)
from auraclaw.contracts.errors import StaleCapabilitySnapshotError


class InMemoryCapabilityCatalogStore:
    def __init__(self) -> None:
        self._servers: dict[str, McpServerDefinition] = {}
        self._capabilities: dict[str, CapabilityDescriptor] = {}
        self._generations: dict[str, int] = {}
        self._sync_failures: dict[str, int] = {}
        self._reconcile_leases: dict[str, CatalogReconcileLease] = {}
        self._snapshot_digests: dict[str, str] = {}
        self._source_revisions: dict[str, str | None] = {}
        self._lock = asyncio.Lock()

    async def upsert_server(
        self, server: McpServerDefinition, *, allow_rollback: bool = False
    ) -> None:
        async with self._lock:
            current = self._servers.get(server.server_id)
            if (
                not allow_rollback
                and current is not None
                and current.config_revision is not None
                and server.config_revision is not None
                and server.config_revision < current.config_revision
            ):
                return
            self._servers[server.server_id] = server

    async def get_server(self, server_id: str) -> McpServerDefinition | None:
        return self._servers.get(server_id)

    async def list_servers(self, tenant_id: str) -> tuple[McpServerDefinition, ...]:
        return tuple(
            server
            for server in sorted(self._servers.values(), key=lambda item: item.server_id)
            if server.tenant_id is None or server.tenant_id == tenant_id
        )

    async def replace_capabilities(
        self,
        server_id: str,
        capabilities: tuple[CapabilityDescriptor, ...],
        *,
        lease: CatalogReconcileLease,
        snapshot_digest: str,
        source_revision: str | None,
    ) -> CatalogCommitResult:
        current_lease = self._reconcile_leases.get(server_id)
        server = self._servers.get(server_id)
        if (
            current_lease != lease
            or lease.expires_at <= datetime.now(UTC)
            or server is None
            or int(server.config_revision or 0) != lease.config_revision
            or self._generations.get(server_id, 0) != lease.previous_generation
        ):
            raise StaleCapabilitySnapshotError("Capability snapshot ownership is stale")
        if self._snapshot_digests.get(server_id) == snapshot_digest:
            return CatalogCommitResult(
                generation=self._generations.get(server_id, 0),
                committed=False,
                snapshot_digest=snapshot_digest,
            )
        generation = lease.previous_generation + 1
        published = tuple(
            capability.model_copy(
                update={
                    "metadata": {
                        **capability.metadata,
                        "catalog_generation": generation,
                    }
                }
            )
            for capability in capabilities
        )
        self._capabilities = {
            capability_id: capability
            for capability_id, capability in self._capabilities.items()
            if capability.server_id != server_id
        }
        self._capabilities.update(
            {capability.capability_id: capability for capability in published}
        )
        self._generations[server_id] = generation
        self._snapshot_digests[server_id] = snapshot_digest
        self._source_revisions[server_id] = source_revision
        return CatalogCommitResult(generation, True, snapshot_digest)

    async def claim_catalog_reconcile(
        self, *, server_id: str, owner: str, ttl: timedelta
    ) -> CatalogReconcileLease | None:
        now = datetime.now(UTC)
        async with self._lock:
            current = self._reconcile_leases.get(server_id)
            if current is not None and current.expires_at > now:
                return None
            server = self._servers.get(server_id)
            if server is None:
                return None
            lease = CatalogReconcileLease(
                server_id=server_id,
                owner=owner,
                fencing_token=(0 if current is None else current.fencing_token) + 1,
                config_revision=int(server.config_revision or 0),
                previous_generation=self._generations.get(server_id, 0),
                expires_at=now + ttl,
            )
            self._reconcile_leases[server_id] = lease
            return lease

    async def release_catalog_reconcile(self, lease: CatalogReconcileLease) -> None:
        async with self._lock:
            if self._reconcile_leases.get(lease.server_id) == lease:
                self._reconcile_leases.pop(lease.server_id, None)

    async def get_active_generation(self, server_id: str) -> int | None:
        return self._generations.get(server_id)

    async def read_committed_snapshot(
        self, tenant_id: str, server_id: str
    ) -> CommittedCatalogSnapshot | None:
        async with self._lock:
            server = self._servers.get(server_id)
            generation = self._generations.get(server_id, 0)
            if server is None or server.tenant_id not in {None, tenant_id} or generation < 1:
                return None
            return CommittedCatalogSnapshot(
                server,
                generation,
                self._snapshot_digests.get(server_id, ""),
                self._source_revisions.get(server_id),
                tuple(item for item in self._capabilities.values() if item.server_id == server_id),
            )

    async def record_catalog_sync(
        self,
        server_id: str,
        *,
        succeeded: bool,
        attempted_at: datetime,
        safe_error_code: str | None,
        quarantine_after_failures: int,
    ) -> CatalogSyncHealth:
        server = self._servers.get(server_id)
        if server is None:
            raise ValueError(f"MCP server is not registered: {server_id}")
        failures = 0 if succeeded else self._sync_failures.get(server_id, 0) + 1
        self._sync_failures[server_id] = failures
        quarantined = not succeeded and failures >= quarantine_after_failures
        self._servers[server_id] = server.model_copy(
            update={
                "status": (
                    CapabilityStatus.ACTIVE
                    if succeeded
                    else CapabilityStatus.QUARANTINED
                    if quarantined
                    else server.status
                ),
                "metadata": {
                    **server.metadata,
                    "last_sync_at": attempted_at.isoformat(),
                    "last_sync_error": safe_error_code,
                    "consecutive_sync_failures": failures,
                    "catalog_quarantined_at": (attempted_at.isoformat() if quarantined else None),
                },
            }
        )
        return CatalogSyncHealth(failures, quarantined)

    async def remove_server(self, server_id: str) -> None:
        self._servers.pop(server_id, None)
        self._generations.pop(server_id, None)
        self._sync_failures.pop(server_id, None)
        self._reconcile_leases.pop(server_id, None)
        self._snapshot_digests.pop(server_id, None)
        self._source_revisions.pop(server_id, None)
        self._capabilities = {
            capability_id: capability
            for capability_id, capability in self._capabilities.items()
            if capability.server_id != server_id
        }

    async def list_capabilities(self, tenant_id: str) -> tuple[CapabilityDescriptor, ...]:
        return tuple(
            capability
            for capability in sorted(
                self._capabilities.values(),
                key=lambda item: (item.canonical_name, item.version),
            )
            if (
                (server := self._servers.get(capability.server_id)) is not None
                and server.enabled
                and server.status in {CapabilityStatus.ACTIVE, CapabilityStatus.DEGRADED}
            )
            if capability.tenant_id is None or capability.tenant_id == tenant_id
        )

    async def catalog_revision(self, tenant_id: str) -> str:
        encoded = json.dumps(
            [
                {
                    "server_id": server.server_id,
                    "config_revision": server.config_revision,
                    "generation": self._generations.get(server.server_id, 0),
                    "enabled": server.enabled,
                    "status": server.status.value,
                }
                for server in sorted(self._servers.values(), key=lambda item: item.server_id)
                if server.tenant_id in {None, tenant_id}
            ],
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
        return f"sha256:{hashlib.sha256(encoded).hexdigest()}"

    async def find_capabilities_by_canonical_name(
        self, tenant_id: str, canonical_name: str
    ) -> tuple[CapabilityDescriptor, ...]:
        return tuple(
            capability
            for capability in await self.list_capabilities(tenant_id)
            if capability.canonical_name == canonical_name
        )

    async def list_server_capabilities(
        self, tenant_id: str, server_id: str
    ) -> tuple[CapabilityDescriptor, ...]:
        return tuple(
            capability
            for capability in sorted(
                self._capabilities.values(),
                key=lambda item: (item.canonical_name, item.version),
            )
            if capability.server_id == server_id
            if capability.tenant_id is None or capability.tenant_id == tenant_id
        )

    async def get_capability(
        self, tenant_id: str, capability_id: str
    ) -> CapabilityDescriptor | None:
        capability = self._capabilities.get(capability_id)
        if capability is None:
            return None
        server = self._servers.get(capability.server_id)
        if (
            server is None
            or not server.enabled
            or server.status not in {CapabilityStatus.ACTIVE, CapabilityStatus.DEGRADED}
            or capability.tenant_id not in {None, tenant_id}
        ):
            return None
        return capability
