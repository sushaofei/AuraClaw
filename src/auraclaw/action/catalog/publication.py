from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import time
from datetime import timedelta

from auraclaw.action.capability_search import (
    SEARCH_POLICY_VERSION,
    CapabilityEmbeddingProvider,
    capability_source_digest,
    index_capabilities,
    read_search_index,
    search_index_digest,
)
from auraclaw.action.catalog.telemetry import CatalogTelemetry
from auraclaw.action.ports import (
    CapabilityCatalogStore,
    CatalogCommitResult,
    CatalogReconcileLease,
)
from auraclaw.contracts.capabilities import (
    CapabilityDescriptor,
    McpServerDefinition,
)
from auraclaw.contracts.errors import StaleCapabilitySnapshotError

logger = logging.getLogger(__name__)


class CatalogPublisher:
    """Own index construction and leased generation publication."""

    def __init__(
        self,
        store: CapabilityCatalogStore,
        *,
        embedding_provider: CapabilityEmbeddingProvider | None,
        telemetry: CatalogTelemetry,
        index_embedding_timeout_seconds: float,
    ) -> None:
        self._store = store
        self._embedding_provider = embedding_provider
        self._telemetry = telemetry
        self._index_embedding_timeout_seconds = index_embedding_timeout_seconds

    async def get_server_definition(self, server_id: str) -> McpServerDefinition | None:
        return await self._store.get_server(server_id)

    async def register_server(
        self, server: McpServerDefinition, *, allow_rollback: bool = False
    ) -> None:
        await self._store.upsert_server(server, allow_rollback=allow_rollback)

    async def remove_server(self, server_id: str) -> None:
        await self._store.remove_server(server_id)

    async def replace_server_capabilities(
        self,
        server_id: str,
        capabilities: tuple[CapabilityDescriptor, ...],
        *,
        lease: CatalogReconcileLease | None = None,
        snapshot_digest: str | None = None,
        source_revision: str | None = None,
    ) -> CatalogCommitResult:
        server = await self._store.get_server(server_id)
        if server is None:
            raise ValueError(f"MCP server is not registered: {server_id}")
        for capability in capabilities:
            if capability.server_id != server_id:
                raise ValueError("Capability server_id does not match the publication")
            if capability.tenant_id != server.tenant_id:
                raise ValueError("Capability tenant does not match the MCP server")
        source_snapshot_digest = snapshot_digest or capability_source_digest(capabilities)
        owned_lease = lease is None
        if lease is None:
            lease_seconds = max(30.0, self._index_embedding_timeout_seconds + 30.0)
            wait_deadline = time.monotonic() + lease_seconds
            while lease is None:
                matching = await self._matching_committed_index(
                    tenant_id=server.tenant_id or "__platform__",
                    server_id=server_id,
                    capabilities=capabilities,
                    source_snapshot_digest=source_snapshot_digest,
                    source_revision=source_revision,
                )
                if matching is not None:
                    return matching
                lease = await self._store.claim_catalog_reconcile(
                    server_id=server_id,
                    owner=f"catalog-direct-{id(self)}",
                    ttl=timedelta(seconds=lease_seconds),
                )
                if lease is not None:
                    break
                if time.monotonic() >= wait_deadline:
                    raise StaleCapabilitySnapshotError(
                        "Capability catalog reconcile remained owned past its deadline"
                    )
                await asyncio.sleep(0.25)
        try:
            matching = await self._matching_committed_index(
                tenant_id=server.tenant_id or "__platform__",
                server_id=server_id,
                capabilities=capabilities,
                source_snapshot_digest=source_snapshot_digest,
                source_revision=source_revision,
            )
            if matching is not None:
                return matching
            published_capabilities = capabilities
            if self._embedding_provider is not None:
                index_started = time.monotonic()
                try:
                    published_capabilities = await index_capabilities(
                        capabilities,
                        generation=lease.previous_generation + 1,
                        provider=self._embedding_provider,
                        source_snapshot_digest=source_snapshot_digest,
                        timeout_seconds=self._index_embedding_timeout_seconds,
                    )
                except Exception:
                    await self._telemetry.emit(
                        "capability_search_index_build_total",
                        1.0,
                        server.tenant_id,
                        outcome="failed",
                        server_id=server_id,
                        model_version=self._embedding_provider.model_version,
                        policy_version=SEARCH_POLICY_VERSION,
                    )
                    logger.warning(
                        "capability_search_index_build server=%s tenant=%s generation=%s "
                        "model=%s policy=%s outcome=failed",
                        server_id,
                        server.tenant_id,
                        lease.previous_generation + 1,
                        self._embedding_provider.model_version,
                        SEARCH_POLICY_VERSION,
                    )
                    raise
                await self._telemetry.emit(
                    "capability_search_index_build_latency_seconds",
                    time.monotonic() - index_started,
                    server.tenant_id,
                    outcome="succeeded",
                    server_id=server_id,
                    model_version=self._embedding_provider.model_version,
                    policy_version=SEARCH_POLICY_VERSION,
                )
            if self._embedding_provider is not None:
                encoded = json.dumps(
                    {
                        "source_snapshot_digest": source_snapshot_digest,
                        "search_index_digest": search_index_digest(published_capabilities),
                    },
                    sort_keys=True,
                    separators=(",", ":"),
                ).encode()
                snapshot_digest = f"sha256:{hashlib.sha256(encoded).hexdigest()}"
            else:
                snapshot_digest = source_snapshot_digest
            result = await self._store.replace_capabilities(
                server_id,
                published_capabilities,
                lease=lease,
                snapshot_digest=snapshot_digest,
                source_revision=source_revision,
            )
            await self._telemetry.emit(
                "capability_search_index_publish_total",
                1.0,
                server.tenant_id,
                outcome="published" if result.committed else "unchanged",
                server_id=server_id,
                policy_version=SEARCH_POLICY_VERSION,
            )
            logger.info(
                "capability_search_index_publish server=%s tenant=%s generation=%s "
                "snapshot_digest=%s policy=%s committed=%s",
                server_id,
                server.tenant_id,
                result.generation,
                result.snapshot_digest,
                SEARCH_POLICY_VERSION,
                result.committed,
            )
            return result
        finally:
            if owned_lease:
                await self._store.release_catalog_reconcile(lease)

    async def _matching_committed_index(
        self,
        *,
        tenant_id: str,
        server_id: str,
        capabilities: tuple[CapabilityDescriptor, ...],
        source_snapshot_digest: str,
        source_revision: str | None,
    ) -> CatalogCommitResult | None:
        if self._embedding_provider is None:
            return None
        current = await self._store.read_committed_snapshot(tenant_id, server_id)
        if current is None:
            return None
        entries = tuple(
            read_search_index(item, provider=self._embedding_provider)
            for item in current.capabilities
        )
        if (
            current.source_revision != source_revision
            or capability_source_digest(current.capabilities)
            != capability_source_digest(capabilities)
            or any(entry is None for entry in entries)
            or any(
                entry.source_snapshot_digest != source_snapshot_digest
                for entry in entries
                if entry is not None
            )
        ):
            return None
        return CatalogCommitResult(
            generation=current.generation,
            committed=False,
            snapshot_digest=current.snapshot_digest,
        )

    async def publication_status(
        self, *, tenant_id: str, server_id: str
    ) -> dict[str, object] | None:
        server = await self._store.get_server(server_id)
        if server is None or server.tenant_id not in {None, tenant_id}:
            return None
        return {
            "active_generation": await self._store.get_active_generation(server_id),
            "status": server.status.value,
            "stale": bool(server.metadata.get("catalog_stale", False)),
            "last_sync_at": server.metadata.get("last_sync_at"),
            "last_good_at": server.metadata.get("last_good_catalog_at"),
            "last_sync_error": server.metadata.get("last_sync_error"),
        }
