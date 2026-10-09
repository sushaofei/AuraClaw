from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import math
import re
import time
from collections import OrderedDict
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Protocol

from auraclaw.action.capability_search import (
    SEARCH_POLICY_VERSION,
    CapabilityEmbeddingProvider,
    SearchMatch,
    SearchOutcome,
    capability_source_digest,
    cosine_similarity,
    exact_reasons,
    governed_search_aliases,
    index_capabilities,
    lexical_scores,
    normalize_vector,
    read_search_index,
    reciprocal_rank_fusion,
    search_index_digest,
)
from auraclaw.action.ports import (
    CapabilityCatalogStore,
    CatalogCommitResult,
    CatalogReconcileLease,
    CatalogSyncHealth,
    CommittedCatalogSnapshot,
    HandsExecutor,
)
from auraclaw.contracts.capabilities import (
    CapabilityDescriptor,
    CapabilityInvocationRef,
    CapabilityKind,
    CapabilityStatus,
    McpServerDefinition,
)
from auraclaw.contracts.errors import AuthorizationError, StaleCapabilitySnapshotError
from auraclaw.contracts.observability import MetricPoint
from auraclaw.contracts.skills import (
    SkillBinding,
    SkillInstallationRecord,
    SkillPackageRecord,
    SkillPublicationRecord,
    SkillPublicationStatus,
    SkillPublisherKeyRecord,
    SkillPublisherKeyStatus,
    SkillPublisherRecord,
    SkillPublisherStatus,
    SkillRevocationAction,
    effective_skill_role,
)
from auraclaw.contracts.tools import (
    RiskLevel,
    ToolCapability,
    ToolInvocation,
    ToolPermission,
)

CAPABILITY_SEARCH_TOOL_NAME = "auraclaw.capabilities.search"
CAPABILITY_LOAD_TOOL_NAME = "auraclaw.capabilities.load"
SKILL_RESOLVE_TOOL_NAME = "auraclaw.skills.resolve"
SKILL_BINDING_STATUS_TOOL_NAME = "auraclaw.skills.binding-status"
_LATIN_TOKEN_PATTERN = re.compile(r"[A-Za-z0-9_.-]+")
_CJK_RUN_PATTERN = re.compile(r"[\u3400-\u9FFF\uF900-\uFAFF]+")
logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class _SearchCacheEntry:
    outcome: SearchOutcome
    expires_at: float


class CapabilitySearchMetricWriter(Protocol):
    async def write_metric(self, metric: MetricPoint) -> None: ...


class SkillResolverPort(Protocol):
    async def resolve(
        self,
        *,
        tenant_id: str,
        name: str,
        version: str = "*",
        publisher: str | None = None,
        role: str,
        policy_version: str,
        assignment_role: str | None = None,
        subject: str = "agent-runtime",
        correlation_id: str = "skill.resolve",
        active_skill_names: tuple[str, ...] = (),
    ) -> SkillBinding: ...


class SkillPublicationReader(Protocol):
    async def get_publication(
        self, tenant_id: str, publisher: str, name: str, version: str
    ) -> SkillPublicationRecord | None: ...

    async def get_installation(
        self, tenant_id: str, publisher: str, name: str
    ) -> SkillInstallationRecord | None: ...

    async def get_package(
        self, tenant_id: str, publisher: str, name: str, version: str
    ) -> SkillPackageRecord | None: ...


class SkillPublisherSecurityReader(Protocol):
    async def get_publisher(
        self, tenant_id: str, publisher: str
    ) -> SkillPublisherRecord | None: ...

    async def get_key(
        self, tenant_id: str, publisher: str, key_id: str
    ) -> SkillPublisherKeyRecord | None: ...


class CapabilityAvailability(Protocol):
    async def is_available(self, tenant_id: str, capability: CapabilityDescriptor) -> bool: ...


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


class CapabilityCatalog:
    def __init__(
        self,
        store: CapabilityCatalogStore,
        *,
        availability: CapabilityAvailability | None = None,
        embedding_provider: CapabilityEmbeddingProvider | None = None,
        metric_writer: CapabilitySearchMetricWriter | None = None,
        environment: str | None = None,
        semantic_min_similarity: float = 0.50,
        lexical_min_score: float = 0.1,
        index_embedding_timeout_seconds: float = 300.0,
        search_cache_max_entries: int = 2048,
        search_cache_ttl_seconds: float = 15.0,
    ) -> None:
        if (
            not -1.0 <= semantic_min_similarity <= 1.0
            or lexical_min_score < 0
            or index_embedding_timeout_seconds <= 0
            or search_cache_max_entries < 0
            or search_cache_ttl_seconds < 0
        ):
            raise ValueError("Capability search confidence thresholds are invalid")
        self._store = store
        self._availability = availability
        self._embedding_provider = embedding_provider
        self._metric_writer = metric_writer
        self._environment = environment
        self._semantic_min_similarity = semantic_min_similarity
        self._lexical_min_score = lexical_min_score
        self._index_embedding_timeout_seconds = index_embedding_timeout_seconds
        self._search_cache_max_entries = search_cache_max_entries
        self._search_cache_ttl_seconds = search_cache_ttl_seconds
        self._search_cache: OrderedDict[tuple[object, ...], _SearchCacheEntry] = OrderedDict()
        self._search_loads: dict[tuple[object, ...], asyncio.Task[SearchOutcome]] = {}
        self._search_cache_lock = asyncio.Lock()

    def set_availability(self, availability: CapabilityAvailability) -> None:
        self._availability = availability

    async def _is_available(self, tenant_id: str, capability: CapabilityDescriptor) -> bool:
        return self._availability is None or await self._availability.is_available(
            tenant_id, capability
        )

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
                    await self._emit_metric(
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
                await self._emit_metric(
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
            await self._emit_metric(
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

    async def search(
        self,
        *,
        tenant_id: str,
        query: str = "",
        kinds: tuple[CapabilityKind, ...] = (),
        required_permissions: tuple[str, ...] = (),
        capability_id: str | None = None,
        canonical_name: str | None = None,
        server_id: str | None = None,
        limit: int = 10,
        actor_role: str | None = None,
        deadline: datetime | None = None,
    ) -> tuple[CapabilityDescriptor, ...]:
        outcome = await self.search_with_evidence(
            tenant_id=tenant_id,
            query=query,
            kinds=kinds,
            required_permissions=required_permissions,
            capability_id=capability_id,
            canonical_name=canonical_name,
            server_id=server_id,
            limit=limit,
            actor_role=actor_role,
            deadline=deadline,
        )
        return tuple(item.capability for item in outcome.matches)

    async def search_with_evidence(
        self,
        *,
        tenant_id: str,
        query: str = "",
        kinds: tuple[CapabilityKind, ...] = (),
        required_permissions: tuple[str, ...] = (),
        capability_id: str | None = None,
        canonical_name: str | None = None,
        server_id: str | None = None,
        limit: int = 10,
        actor_role: str | None = None,
        deadline: datetime | None = None,
    ) -> SearchOutcome:
        if limit < 1 or limit > 50:
            raise ValueError("Capability search limit must be between 1 and 50")
        if self._search_cache_max_entries == 0 or self._search_cache_ttl_seconds == 0:
            return await self._search_uncached(
                tenant_id=tenant_id,
                query=query,
                kinds=kinds,
                required_permissions=required_permissions,
                capability_id=capability_id,
                canonical_name=canonical_name,
                server_id=server_id,
                limit=limit,
                actor_role=actor_role,
                deadline=deadline,
            )
        revision = await self._store.catalog_revision(tenant_id)
        key = (
            tenant_id,
            revision,
            SEARCH_POLICY_VERSION,
            self._environment,
            actor_role,
            query.strip(),
            tuple(sorted(kind.value for kind in kinds)),
            tuple(sorted(required_permissions)),
            capability_id,
            canonical_name,
            server_id,
            limit,
            _deadline_bucket(deadline),
        )
        now = time.monotonic()
        async with self._search_cache_lock:
            entry = self._search_cache.get(key)
            if entry is not None and entry.expires_at > now:
                self._search_cache.move_to_end(key)
                cache_outcome = "hit"
                task: asyncio.Task[SearchOutcome] | None = None
                cached = entry.outcome
            else:
                if entry is not None:
                    self._search_cache.pop(key, None)
                cached = None
                task = self._search_loads.get(key)
                cache_outcome = "coalesced" if task is not None else "miss"
                if task is None:
                    task = asyncio.create_task(
                        self._populate_search_cache(
                            key,
                            tenant_id=tenant_id,
                            query=query,
                            kinds=kinds,
                            required_permissions=required_permissions,
                            capability_id=capability_id,
                            canonical_name=canonical_name,
                            server_id=server_id,
                            limit=limit,
                            actor_role=actor_role,
                            deadline=deadline,
                        )
                    )
                    self._search_loads[key] = task
        await self._emit_metric(
            "capability_search_cache_requests_total",
            1.0,
            tenant_id,
            outcome=cache_outcome,
            policy_version=SEARCH_POLICY_VERSION,
        )
        if cached is not None:
            return cached
        assert task is not None
        return await asyncio.shield(task)

    async def _populate_search_cache(
        self,
        key: tuple[object, ...],
        **arguments: Any,
    ) -> SearchOutcome:
        try:
            outcome = await self._search_uncached(**arguments)
            async with self._search_cache_lock:
                self._search_cache[key] = _SearchCacheEntry(
                    outcome=outcome,
                    expires_at=time.monotonic() + self._search_cache_ttl_seconds,
                )
                self._search_cache.move_to_end(key)
                while len(self._search_cache) > self._search_cache_max_entries:
                    self._search_cache.popitem(last=False)
            return outcome
        finally:
            async with self._search_cache_lock:
                if self._search_loads.get(key) is asyncio.current_task():
                    self._search_loads.pop(key, None)

    async def _search_uncached(
        self,
        *,
        tenant_id: str,
        query: str = "",
        kinds: tuple[CapabilityKind, ...] = (),
        required_permissions: tuple[str, ...] = (),
        capability_id: str | None = None,
        canonical_name: str | None = None,
        server_id: str | None = None,
        limit: int = 10,
        actor_role: str | None = None,
        deadline: datetime | None = None,
    ) -> SearchOutcome:
        started = time.monotonic()
        kind_filter = set(kinds)
        permission_filter = set(required_permissions)
        visible: list[CapabilityDescriptor] = []
        candidates: tuple[CapabilityDescriptor, ...]
        if capability_id is not None:
            exact = await self._store.get_capability(tenant_id, capability_id)
            candidates = () if exact is None else (exact,)
        elif canonical_name is not None:
            candidates = await self._store.find_capabilities_by_canonical_name(
                tenant_id, canonical_name
            )
        elif server_id is not None:
            candidates = await self._store.list_server_capabilities(tenant_id, server_id)
        else:
            candidates = await self._store.list_capabilities(tenant_id)
        for capability in candidates:
            if capability.status not in {
                CapabilityStatus.ACTIVE,
                CapabilityStatus.DEGRADED,
            }:
                continue
            if kind_filter and capability.kind not in kind_filter:
                continue
            if permission_filter and capability.permission not in permission_filter:
                continue
            if capability_id is not None and capability.capability_id != capability_id:
                continue
            if canonical_name is not None and capability.canonical_name != canonical_name:
                continue
            if server_id is not None and capability.server_id != server_id:
                continue
            allowed_roles = capability.metadata.get("allowed_roles")
            if isinstance(allowed_roles, (list, tuple)) and (
                actor_role is None or actor_role not in {str(value) for value in allowed_roles}
            ):
                continue
            environments = capability.metadata.get("environments")
            if (
                self._environment is not None
                and isinstance(environments, (list, tuple))
                and self._environment not in {str(value) for value in environments}
            ):
                continue
            if not await self._is_available(tenant_id, capability):
                continue
            visible.append(capability)
        # Multiple MCP publications can mirror the same Skill package.  Collapse
        # only byte-identical tenant identities; conflicting digests remain
        # visible and therefore ambiguous.  Tool identities stay server-owned.
        equivalent_skills: dict[
            tuple[str | None, str, str, str], CapabilityDescriptor
        ] = {}
        distinct: list[CapabilityDescriptor] = []
        for capability in visible:
            if capability.kind is not CapabilityKind.SKILL:
                distinct.append(capability)
                continue
            key = (
                capability.tenant_id,
                capability.canonical_name,
                capability.version,
                capability.content_digest,
            )
            existing = equivalent_skills.get(key)
            if existing is None or capability.capability_id < existing.capability_id:
                equivalent_skills[key] = capability
        visible = [*distinct, *equivalent_skills.values()]
        visible.sort(key=lambda item: (item.canonical_name, item.version, item.capability_id))

        if (
            not query.strip()
            and capability_id is None
            and canonical_name is None
            and server_id is None
        ):
            browse_matches = tuple(
                SearchMatch(
                    capability=item,
                    match_reasons=("browse",),
                    exact_rank=None,
                    lexical_rank=None,
                    semantic_rank=None,
                    lexical_score=0.0,
                    semantic_score=None,
                    fused_score=0.0,
                )
                for item in visible[:limit]
            )
            outcome = SearchOutcome(
                matches=browse_matches,
                semantic_degraded=False,
                degraded_reason=None,
                search_policy_version=SEARCH_POLICY_VERSION,
                candidate_counts={
                    "authorized": len(visible),
                    "exact": 0,
                    "lexical": 0,
                    "semantic": 0,
                },
                generation_lag=0,
            )
            await self._emit_search_metrics(
                tenant_id, outcome, "browse", time.monotonic() - started
            )
            return outcome

        exact_by_id: dict[str, tuple[str, ...]] = {}
        for item in visible:
            reasons = list(exact_reasons(query, item))
            if capability_id is not None:
                reasons.append("exact:capability_id_filter")
            if canonical_name is not None:
                reasons.append("exact:canonical_name_filter")
            if server_id is not None:
                reasons.append("exact:server_id_filter")
            if reasons:
                exact_by_id[item.capability_id] = tuple(reasons)
        exact_priority = {
            "exact:capability_id": 0,
            "exact:capability_id_filter": 0,
            "exact:canonical_name": 1,
            "exact:canonical_name_filter": 1,
            "exact:server_id": 2,
            "exact:server_id_filter": 2,
        }
        exact_order = sorted(
            exact_by_id,
            key=lambda item_id: (
                min(exact_priority[reason] for reason in exact_by_id[item_id]),
                item_id,
            ),
        )
        lexical = lexical_scores(query, visible)
        by_id = {item.capability_id: item for item in visible}
        lexical_order = sorted(
            lexical,
            key=lambda item_id: (
                -lexical[item_id],
                by_id[item_id].canonical_name,
                by_id[item_id].version,
                item_id,
            ),
        )

        semantic: dict[str, float] = {}
        semantic_degraded = False
        degraded_reason: str | None = None
        generation_lag = 0
        if not query.strip():
            pass
        elif self._embedding_provider is None:
            semantic_degraded = True
            degraded_reason = "embedding_unconfigured"
        else:
            entries = {}
            for item in visible:
                entry = read_search_index(item, provider=self._embedding_provider)
                if entry is None:
                    generation_lag += 1
                else:
                    entries[item.capability_id] = entry
            if generation_lag:
                semantic_degraded = True
                degraded_reason = "index_generation_lag"
            if entries:
                try:
                    query_vector = normalize_vector(
                        (await self._embedding_provider.embed((query,)))[0],
                        dimensions=self._embedding_provider.dimensions,
                    )
                    semantic = {
                        item_id: similarity
                        for item_id, entry in entries.items()
                        if (similarity := cosine_similarity(query_vector, entry.vector))
                        >= self._semantic_min_similarity
                    }
                except asyncio.CancelledError:
                    raise
                except Exception:
                    semantic_degraded = True
                    degraded_reason = "embedding_query_failed"
                    semantic = {}
            elif visible:
                semantic_degraded = True
                degraded_reason = degraded_reason or "index_unavailable"
        semantic_order = sorted(
            semantic,
            key=lambda item_id: (
                -semantic[item_id],
                by_id[item_id].canonical_name,
                by_id[item_id].version,
                item_id,
            ),
        )
        fused = reciprocal_rank_fusion(
            exact=exact_order,
            lexical=lexical_order,
            semantic=semantic_order,
        )
        ordered = sorted(
            fused,
            key=lambda item_id: (
                item_id not in exact_by_id,
                *_execution_rerank(by_id[item_id], deadline),
                -fused[item_id],
                by_id[item_id].status == CapabilityStatus.DEGRADED,
                by_id[item_id].canonical_name,
                by_id[item_id].version,
            ),
        )
        exact_ranks = {item_id: rank for rank, item_id in enumerate(exact_order, start=1)}
        lexical_ranks = {item_id: rank for rank, item_id in enumerate(lexical_order, start=1)}
        semantic_ranks = {item_id: rank for rank, item_id in enumerate(semantic_order, start=1)}
        ranked_matches: list[SearchMatch] = []
        for item_id in ordered:
            lexical_score = lexical.get(item_id, 0.0)
            semantic_score = semantic.get(item_id)
            if (
                item_id not in exact_by_id
                and lexical_score < self._lexical_min_score
                and (semantic_score is None or semantic_score < self._semantic_min_similarity)
            ):
                continue
            reasons = list(exact_by_id.get(item_id, ()))
            if item_id in lexical:
                reasons.append("lexical:bm25")
            if item_id in semantic:
                reasons.append("semantic:cosine")
            ranked_matches.append(
                SearchMatch(
                    capability=by_id[item_id],
                    match_reasons=tuple(reasons),
                    exact_rank=exact_ranks.get(item_id),
                    lexical_rank=lexical_ranks.get(item_id),
                    semantic_rank=semantic_ranks.get(item_id),
                    lexical_score=lexical_score,
                    semantic_score=semantic_score,
                    fused_score=fused[item_id],
                )
            )
            if len(ranked_matches) >= limit:
                break
        outcome = SearchOutcome(
            matches=tuple(ranked_matches),
            semantic_degraded=semantic_degraded,
            degraded_reason=degraded_reason,
            search_policy_version=SEARCH_POLICY_VERSION,
            candidate_counts={
                "authorized": len(visible),
                "exact": len(exact_order),
                "lexical": len(lexical_order),
                "semantic": len(semantic_order),
            },
            generation_lag=generation_lag,
        )
        mode = "hybrid" if semantic and not semantic_degraded else "lexical_degraded"
        if exact_order:
            mode = "exact"
        await self._emit_search_metrics(tenant_id, outcome, mode, time.monotonic() - started)
        return outcome

    async def _emit_search_metrics(
        self,
        tenant_id: str,
        outcome: SearchOutcome,
        mode: str,
        latency: float,
    ) -> None:
        if self._metric_writer is None:
            return
        metric_writer = self._metric_writer
        points = [
            (
                "capability_search_requests_total",
                1.0,
                {"mode": mode, "outcome": "hit" if outcome.matches else "empty"},
            ),
            ("capability_search_latency_seconds", latency, {"mode": mode}),
            ("capability_search_index_generation_lag", float(outcome.generation_lag), {}),
            *[
                ("capability_search_candidate_count", float(count), {"channel": channel})
                for channel, count in outcome.candidate_counts.items()
            ],
        ]
        if not outcome.matches:
            points.append(("capability_search_zero_result_total", 1.0, {}))
        if outcome.semantic_degraded:
            points.append(
                (
                    "capability_search_semantic_degraded_total",
                    1.0,
                    {"reason": outcome.degraded_reason or "unknown"},
                )
            )

        async def write(name: str, value: float, labels: dict[str, str]) -> None:
            try:
                await asyncio.wait_for(
                    metric_writer.write_metric(
                        MetricPoint(
                            name=name,
                            value=value,
                            observed_at=datetime.now(UTC),
                            tenant_id=tenant_id,
                            labels=labels,
                        )
                    ),
                    timeout=0.1,
                )
            except Exception:
                return

        await asyncio.gather(*(write(name, value, labels) for name, value, labels in points))

    async def _emit_metric(
        self,
        name: str,
        value: float,
        tenant_id: str | None,
        **labels: str,
    ) -> None:
        if self._metric_writer is None:
            return
        try:
            await asyncio.wait_for(
                self._metric_writer.write_metric(
                    MetricPoint(
                        name=name,
                        value=value,
                        observed_at=datetime.now(UTC),
                        tenant_id=tenant_id,
                        labels=labels,
                    )
                ),
                timeout=0.1,
            )
        except Exception:
            return

    async def list_server_tools(
        self, *, tenant_id: str, server_id: str
    ) -> tuple[CapabilityDescriptor, ...]:
        return tuple(
            capability
            for capability in await self.list_server_capabilities(
                tenant_id=tenant_id, server_id=server_id
            )
            if capability.kind is CapabilityKind.TOOL
        )

    async def list_server_capabilities(
        self, *, tenant_id: str, server_id: str
    ) -> tuple[CapabilityDescriptor, ...]:
        return tuple(await self._store.list_server_capabilities(tenant_id, server_id))

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

    async def get(self, *, tenant_id: str, capability_id: str) -> CapabilityDescriptor | None:
        capability = await self._store.get_capability(tenant_id, capability_id)
        if capability is None or capability.status not in {
            CapabilityStatus.ACTIVE,
            CapabilityStatus.DEGRADED,
        }:
            return None
        if not await self._is_available(tenant_id, capability):
            return None
        return capability


@dataclass(frozen=True)
class CapabilitySearchExecutor:
    catalog: CapabilityCatalog

    async def execute(
        self,
        invocation: ToolInvocation,
        capability: ToolCapability,
    ) -> dict[str, object]:
        del capability
        arguments = invocation.arguments
        kinds = tuple(CapabilityKind(str(value)) for value in arguments.get("kinds", ()))
        permissions = tuple(str(value) for value in arguments.get("required_permissions", ()))
        query = str(arguments.get("query", ""))
        limit = int(arguments.get("limit", 10))
        outcome = await self.catalog.search_with_evidence(
            tenant_id=invocation.tenant_id,
            query=query,
            kinds=kinds,
            required_permissions=permissions,
            capability_id=_optional(arguments.get("capability_id")),
            canonical_name=_optional(arguments.get("canonical_name")),
            server_id=_optional(arguments.get("server_id")),
            limit=limit,
            actor_role=invocation.actor_role,
            deadline=invocation.deadline,
        )
        page: list[dict[str, Any]] = []
        for match in outcome.matches:
            item = match.capability.as_search_result()
            item.update(
                {
                    "match_reasons": list(match.match_reasons),
                    "search_policy_version": outcome.search_policy_version,
                    "semantic_degraded": outcome.semantic_degraded,
                }
            )
            page.append(item)
        payload: dict[str, object] = {
            "capabilities": page,
            "truncated": len(outcome.matches) >= limit,
            "search_policy_version": outcome.search_policy_version,
            "semantic_degraded": outcome.semantic_degraded,
        }
        if outcome.degraded_reason is not None:
            payload["semantic_degraded_reason"] = outcome.degraded_reason
        if not page:
            browse = await self.catalog.search(tenant_id=invocation.tenant_id, limit=50)
            domains = sorted(
                {item.canonical_name.split(".", 1)[0] for item in browse if item.canonical_name}
            )[:12]
            payload["empty_reason"] = "no_capability_matched_filters"
            payload["available_domains"] = domains
            payload["hint"] = (
                "No matching capabilities were found. Retry once with a broader query, "
                "an exact capability_id/canonical_name/server_id, or an empty query to browse. "
                "For MCP queries/actions include kinds=[\"tool\"] or omit kinds; "
                "a Skill/Resource filter excludes Tools. "
                "To list Skills use kinds=[\"skill\"] and query=\"\". "
                "No executable match does not mean no Skill is installed or registered; "
                "an installation may be disabled, version-mismatched, or missing dependencies. "
                "An administrator can inspect Skill availability in the management catalog."
            )
        logger.info(
            "capability_search tenant=%s query_digest=%s query_length=%s kinds=%s "
            "permissions=%s hits=%s policy=%s semantic_degraded=%s degraded_reason=%s "
            "generations=%s empty_reason=%s",
            invocation.tenant_id,
            hashlib.sha256(query.encode()).hexdigest()[:16],
            len(query),
            tuple(kind.value for kind in kinds),
            permissions,
            tuple(item["capability_id"] for item in page),
            outcome.search_policy_version,
            outcome.semantic_degraded,
            outcome.degraded_reason,
            tuple(
                sorted(
                    {
                        int(item["catalog_generation"])
                        for item in page
                        if isinstance(item.get("catalog_generation"), int)
                    }
                )
            ),
            payload.get("empty_reason"),
        )
        return payload


@dataclass(frozen=True)
class CapabilityLoadExecutor:
    catalog: CapabilityCatalog

    async def execute(
        self,
        invocation: ToolInvocation,
        capability: ToolCapability,
    ) -> dict[str, object]:
        del capability
        loaded: list[dict[str, Any]] = []
        raw_ids = tuple(invocation.arguments.get("capability_ids", ()))
        if len(raw_ids) > 24:
            raise ValueError("Capability load limit is 24")
        for raw_id in raw_ids:
            capability_id = str(raw_id)
            descriptor = await self.catalog.get(
                tenant_id=invocation.tenant_id,
                capability_id=capability_id,
            )
            if descriptor is not None:
                loaded.append(_load_result(descriptor))
        return {"capabilities": loaded}


@dataclass(frozen=True)
class SkillResolveExecutor:
    resolver: SkillResolverPort

    async def execute(
        self,
        invocation: ToolInvocation,
        capability: ToolCapability,
    ) -> dict[str, object]:
        del capability
        arguments = invocation.arguments
        requested_role = str(arguments["role"])
        assignment_role = invocation.actor_role or requested_role
        policy_role = effective_skill_role(assignment_role)
        if invocation.actor_role is not None and requested_role not in {
            assignment_role,
            policy_role,
        }:
            raise AuthorizationError(
                "Skill resolver role does not match the trusted Runtime assignment"
            )
        binding = await self.resolver.resolve(
            tenant_id=invocation.tenant_id,
            name=str(arguments["name"]),
            version=str(arguments.get("version", "*")),
            publisher=_optional(arguments.get("publisher")),
            role=policy_role,
            assignment_role=assignment_role,
            policy_version=str(arguments.get("policy_version", "runtime")),
            subject=invocation.actor_id,
            correlation_id=invocation.run_id,
            active_skill_names=tuple(
                str(value) for value in arguments.get("active_skill_names", ())
            ),
        )
        dump = getattr(binding, "model_dump", None)
        if not callable(dump):
            raise TypeError("Skill resolver returned an invalid binding")
        return {"binding": dump(mode="json")}


@dataclass(frozen=True)
class SkillBindingStatusExecutor:
    publications: SkillPublicationReader
    publisher_security: SkillPublisherSecurityReader | None = None

    async def execute(
        self,
        invocation: ToolInvocation,
        capability: ToolCapability,
    ) -> dict[str, object]:
        del capability
        arguments = invocation.arguments
        publication = await self.publications.get_publication(
            invocation.tenant_id,
            str(arguments["publisher"]),
            str(arguments["name"]),
            str(arguments["version"]),
        )
        expected_digest = str(arguments["package_digest"])
        if publication is None or publication.package_digest != expected_digest:
            return {
                "publication_status": "unavailable",
                "action": SkillRevocationAction.CANCEL.value,
                "reason_code": "binding_authority_unavailable",
                "policy_version": "skill-revocation-v1",
            }
        decisions: list[dict[str, object]] = []
        if publication.status is SkillPublicationStatus.REVOKED:
            decisions.append(
                {
                    "publication_status": publication.status.value,
                    "action": (publication.revocation_action or SkillRevocationAction.CANCEL).value,
                    "reason_code": publication.reason_code,
                    "policy_version": publication.revocation_policy_version,
                    "policy_decision_id": (publication.revocation_policy_decision_id),
                }
            )
        if self.publisher_security is not None:
            package = await self.publications.get_package(
                invocation.tenant_id,
                publication.publisher,
                publication.name,
                publication.version,
            )
            if package is None:
                decisions.append(
                    {
                        "publication_status": publication.status.value,
                        "action": SkillRevocationAction.CANCEL.value,
                        "reason_code": "package_authority_unavailable",
                        "policy_version": "skill-revocation-v1",
                    }
                )
            elif package.signature_key_id is not None:
                publisher = await self.publisher_security.get_publisher(
                    invocation.tenant_id,
                    publication.publisher,
                )
                key = await self.publisher_security.get_key(
                    invocation.tenant_id,
                    publication.publisher,
                    package.signature_key_id,
                )
                if publisher is None or publisher.status in {
                    SkillPublisherStatus.SUSPENDED,
                    SkillPublisherStatus.REVOKED,
                }:
                    decisions.append(
                        {
                            "publication_status": publication.status.value,
                            "publisher_status": (
                                publisher.status.value if publisher is not None else "unavailable"
                            ),
                            "action": (
                                publisher.security_action or SkillRevocationAction.CANCEL
                                if publisher is not None
                                else SkillRevocationAction.CANCEL
                            ).value,
                            "reason_code": (
                                publisher.status_reason_code
                                if publisher is not None
                                else "publisher_authority_unavailable"
                            ),
                            "policy_version": (
                                publisher.security_policy_version
                                if publisher is not None
                                else "skill-revocation-v1"
                            ),
                            "policy_decision_id": (
                                publisher.security_policy_decision_id
                                if publisher is not None
                                else None
                            ),
                        }
                    )
                if key is None:
                    decisions.append(
                        {
                            "publication_status": publication.status.value,
                            "publisher_status": (
                                publisher.status.value if publisher is not None else "unavailable"
                            ),
                            "key_status": "unavailable",
                            "action": SkillRevocationAction.CANCEL.value,
                            "reason_code": "publisher_key_authority_unavailable",
                            "policy_version": "skill-revocation-v1",
                        }
                    )
                elif key.status is SkillPublisherKeyStatus.REVOKED:
                    decisions.append(
                        {
                            "publication_status": publication.status.value,
                            "publisher_status": (
                                publisher.status.value if publisher is not None else "unavailable"
                            ),
                            "key_status": key.status.value,
                            "action": (key.revocation_action or SkillRevocationAction.CANCEL).value,
                            "reason_code": key.reason_code,
                            "policy_version": key.revocation_policy_version,
                            "policy_decision_id": key.revocation_policy_decision_id,
                        }
                    )
        installation = await self.publications.get_installation(
            invocation.tenant_id,
            str(arguments["publisher"]),
            str(arguments["name"]),
        )
        if (
            installation is not None
            and installation.uninstall_action is SkillRevocationAction.CANCEL
        ):
            decisions.append(
                {
                    "publication_status": publication.status.value,
                    "installation_status": installation.status.value,
                    "action": SkillRevocationAction.CANCEL.value,
                    "reason_code": installation.reason_code,
                    "policy_version": installation.uninstall_policy_version,
                    "policy_decision_id": installation.uninstall_policy_decision_id,
                }
            )
        if decisions:
            priority = {
                SkillRevocationAction.CANCEL.value: 0,
                SkillRevocationAction.PAUSE.value: 1,
                SkillRevocationAction.CONTINUE.value: 2,
            }
            decision = min(decisions, key=lambda item: priority[str(item["action"])])
            decision.setdefault(
                "installation_status",
                installation.status.value if installation is not None else "unmanaged",
            )
            return decision
        return {
            "publication_status": publication.status.value,
            "installation_status": (
                installation.status.value if installation is not None else "unmanaged"
            ),
            "action": (
                SkillRevocationAction.CONTINUE.value
                if publication.status
                in {
                    SkillPublicationStatus.ACTIVE,
                    SkillPublicationStatus.RESTORING,
                    SkillPublicationStatus.RETIRED,
                }
                else SkillRevocationAction.CANCEL.value
            ),
            "reason_code": None,
            "policy_version": "skill-revocation-v1",
        }


class RoutedHandsExecutor:
    def __init__(
        self,
        default: HandsExecutor,
        routes: Mapping[str, HandsExecutor],
    ) -> None:
        self._default = default
        self._routes = dict(routes)

    async def execute(
        self,
        invocation: ToolInvocation,
        capability: ToolCapability,
    ) -> object:
        target = capability.invocation_ref
        route = target.model_name if target is not None else invocation.tool_name
        executor = self._routes.get(route, self._default)
        if target is not None and route not in self._routes:
            raise AuthorizationError("stale_capability: execution route is unavailable")
        return await executor.execute(invocation, capability)

    def replace_owner_routes(
        self,
        owner: str,
        routes: Mapping[str, HandsExecutor],
    ) -> None:
        prefix = f"{owner}:"
        self._routes = {
            name: executor
            for name, executor in self._routes.items()
            if not getattr(executor, "route_owner", "").startswith(prefix)
        }
        self._routes.update(routes)


def capability_search_tool() -> ToolCapability:
    return ToolCapability(
        name=CAPABILITY_SEARCH_TOOL_NAME,
        version="1",
        description=(
            "Search the policy-visible AuraClaw capability catalog without loading "
            "full Resource or Skill content."
        ),
        input_schema={
            "type": "object",
            "properties": {
                "query": {"type": "string", "maxLength": 1024},
                "capability_id": {"type": "string", "maxLength": 256},
                "canonical_name": {"type": "string", "maxLength": 256},
                "server_id": {"type": "string", "maxLength": 128},
                "kinds": {
                    "type": "array",
                    "items": {
                        "type": "string",
                        "enum": [kind.value for kind in CapabilityKind],
                    },
                },
                "required_permissions": {
                    "type": "array",
                    "items": {"type": "string"},
                },
                "limit": {"type": "integer", "minimum": 1, "maximum": 50},
            },
            "additionalProperties": False,
        },
        output_schema={
            "type": "object",
            "properties": {
                "capabilities": {
                    "type": "array",
                    "items": {"type": "object"},
                },
                "hint": {"type": "string"},
                "empty_reason": {"type": "string"},
                "truncated": {"type": "boolean"},
                "search_policy_version": {"type": "string"},
                "semantic_degraded": {"type": "boolean"},
                "semantic_degraded_reason": {"type": "string"},
                "available_domains": {
                    "type": "array",
                    "items": {"type": "string"},
                },
            },
            "required": [
                "capabilities",
                "search_policy_version",
                "semantic_degraded",
            ],
            "additionalProperties": False,
        },
        permission=ToolPermission.READ_ONLY,
        risk_level=RiskLevel.LOW,
        runtime_location="hands",
        cache_result=False,
        owner="platform",
    )


def capability_load_tool() -> ToolCapability:
    return ToolCapability(
        name=CAPABILITY_LOAD_TOOL_NAME,
        version="1",
        description=(
            "Load authoritative contracts for a bounded set of capability ids "
            "returned by capability search."
        ),
        input_schema={
            "type": "object",
            "properties": {
                "capability_ids": {
                    "type": "array",
                    "items": {"type": "string", "maxLength": 256},
                    "maxItems": 8,
                }
            },
            "required": ["capability_ids"],
            "additionalProperties": False,
        },
        output_schema={"type": "object"},
        permission=ToolPermission.READ_ONLY,
        risk_level=RiskLevel.LOW,
        runtime_location="hands",
        cache_result=False,
        owner="platform",
    )


def skill_resolve_tool() -> ToolCapability:
    return ToolCapability(
        name=SKILL_RESOLVE_TOOL_NAME,
        version="1",
        description="Resolve an exact Skill binding for the trusted Agent Runtime.",
        input_schema={
            "type": "object",
            "properties": {
                "name": {"type": "string", "minLength": 1, "maxLength": 256},
                "version": {"type": "string", "maxLength": 128},
                "publisher": {"type": "string", "maxLength": 128},
                "role": {"type": "string", "minLength": 1, "maxLength": 64},
                "policy_version": {"type": "string", "maxLength": 128},
                "active_skill_names": {
                    "type": "array",
                    "items": {"type": "string", "maxLength": 256},
                },
            },
            "required": ["name", "role"],
            "additionalProperties": False,
        },
        output_schema={"type": "object"},
        permission=ToolPermission.READ_ONLY,
        risk_level=RiskLevel.LOW,
        runtime_location="hands",
        cache_result=False,
        owner="platform-internal",
    )


def skill_binding_status_tool() -> ToolCapability:
    return ToolCapability(
        name=SKILL_BINDING_STATUS_TOOL_NAME,
        version="1",
        description=(
            "Evaluate the current governed disposition of an already-fixed Skill binding."
        ),
        input_schema={
            "type": "object",
            "properties": {
                "publisher": {"type": "string", "minLength": 1, "maxLength": 128},
                "name": {"type": "string", "minLength": 1, "maxLength": 256},
                "version": {"type": "string", "minLength": 1, "maxLength": 128},
                "package_digest": {
                    "type": "string",
                    "pattern": "^sha256:[0-9a-f]{64}$",
                },
            },
            "required": ["publisher", "name", "version", "package_digest"],
            "additionalProperties": False,
        },
        output_schema={"type": "object"},
        permission=ToolPermission.READ_ONLY,
        risk_level=RiskLevel.LOW,
        runtime_location="hands",
        cache_result=False,
        owner="platform-internal",
    )


def _load_result(descriptor: CapabilityDescriptor) -> dict[str, Any]:
    result = descriptor.as_search_result()
    raw_source = descriptor.metadata.get("source", {})
    source = dict(raw_source) if isinstance(raw_source, dict) else {}
    if descriptor.kind == CapabilityKind.TOOL:
        ref = (
            CapabilityInvocationRef.from_descriptor(descriptor)
            if descriptor.metadata.get("source_type") == "mcp"
            else None
        )
        if ref is not None:
            result["invocation_ref"] = ref.model_dump(mode="json")
        result["model_tool"] = {
            "type": "function",
            "function": {
                "name": ref.model_name if ref is not None else descriptor.canonical_name,
                "description": descriptor.description,
                "parameters": source.get("inputSchema", {"type": "object"}),
            },
        }
    elif descriptor.kind == CapabilityKind.RESOURCE:
        result["resource"] = {"uri": source.get("uri")}
    elif descriptor.kind == CapabilityKind.RESOURCE_TEMPLATE:
        result["resource"] = {
            "uri_template": (descriptor.metadata.get("uri_template") or source.get("uriTemplate"))
        }
    elif descriptor.kind == CapabilityKind.SKILL:
        raw_contract = descriptor.metadata.get("model_contract", {})
        result["skill"] = dict(raw_contract) if isinstance(raw_contract, dict) else {}
    return result


def _optional(value: object) -> str | None:
    parsed = "" if value is None else str(value).strip()
    return parsed or None


def _deadline_bucket(deadline: datetime | None) -> str:
    if deadline is None:
        return "none"
    remaining = (deadline - datetime.now(UTC)).total_seconds()
    if remaining <= 5:
        return "le-5s"
    if remaining <= 30:
        return "le-30s"
    if remaining <= 120:
        return "le-120s"
    return "gt-120s"


def _tokens(value: str) -> tuple[str, ...]:
    tokens: list[str] = []
    seen: set[str] = set()

    def add(token: str) -> None:
        folded = token.casefold().strip()
        if not folded or folded in seen:
            return
        seen.add(folded)
        tokens.append(folded)

    for token in _LATIN_TOKEN_PATTERN.findall(value):
        add(token)
        for part in re.split(r"[_.-]+", token):
            add(part)
    for run in _CJK_RUN_PATTERN.findall(value):
        add(run)
        if len(run) >= 2:
            for index in range(len(run) - 1):
                add(run[index : index + 2])
    return tuple(tokens)


def _score(
    capability: CapabilityDescriptor,
    query_tokens: tuple[str, ...],
) -> int:
    if not query_tokens:
        return 0
    name = capability.canonical_name.casefold()
    capability_id = capability.capability_id.casefold()
    server_id = capability.server_id.casefold()
    title = capability.title.casefold()
    tags = tuple(tag.casefold() for tag in capability.tags)
    tag_haystack = " ".join(tags)
    description = capability.description.casefold()
    metadata_terms = _capability_metadata_terms(capability)
    score = 0
    for token in query_tokens:
        if token == capability_id:
            score += 100
        if token == name:
            score += 80
        if token == server_id:
            score += 60
        if token in name:
            score += 8
        if token in title:
            score += 5
        if token in tags or token in tag_haystack:
            score += 3
        if token in description:
            score += 1
        if any(token in value for value in metadata_terms.values()):
            score += 4
        if token in {"mcp", "mcp工具"} and metadata_terms.get("source_type") == "mcp":
            score += 6
        if token in {"工具", "tool", "tools"} and capability.kind is CapabilityKind.TOOL:
            score += 5
    return score


def _capability_metadata_terms(capability: CapabilityDescriptor) -> dict[str, str]:
    metadata = capability.metadata
    aliases = governed_search_aliases(capability)
    return {
        "server_id": capability.server_id.casefold(),
        "server_title": str(metadata.get("server_title", "")).casefold(),
        "endpoint": str(metadata.get("endpoint", "")).casefold(),
        "source_type": str(metadata.get("source_type", "")).casefold(),
        "aliases": " ".join(str(item).casefold() for item in aliases),
    }


def _execution_rerank(
    capability: CapabilityDescriptor,
    deadline: datetime | None,
) -> tuple[int, int, int, float]:
    metadata = capability.metadata
    deprecated = metadata.get("deprecated") is True
    drifted = metadata.get("schema_drift") is True
    timeout_infeasible = False
    estimate = metadata.get("estimated_timeout_seconds")
    if deadline is not None and isinstance(estimate, (int, float)) and estimate >= 0:
        timeout_infeasible = float(estimate) > max(
            0.0,
            (deadline - datetime.now(UTC)).total_seconds(),
        )
    raw_quality = metadata.get("historical_quality_score", 1.0)
    quality = (
        float(raw_quality)
        if isinstance(raw_quality, (int, float)) and math.isfinite(float(raw_quality))
        else 0.0
    )
    return (int(deprecated), int(drifted), int(timeout_infeasible), -quality)
