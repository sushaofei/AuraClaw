from __future__ import annotations

import asyncio
import math
import time
from collections import OrderedDict
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from auraclaw.action.capability_search import (
    SEARCH_POLICY_VERSION,
    CapabilityEmbeddingProvider,
    SearchMatch,
    SearchOutcome,
    cosine_similarity,
    exact_reasons,
    lexical_scores,
    normalize_vector,
    read_search_index,
    reciprocal_rank_fusion,
)
from auraclaw.action.catalog.contracts import CapabilityAvailability
from auraclaw.action.catalog.telemetry import CatalogTelemetry
from auraclaw.action.ports import (
    CapabilityCatalogStore,
)
from auraclaw.contracts.capabilities import (
    CapabilityDescriptor,
    CapabilityKind,
    CapabilityStatus,
)


@dataclass(frozen=True)
class _SearchCacheEntry:
    outcome: SearchOutcome
    expires_at: float


class CatalogSearchService:
    """Own authorized search, live descriptor reads, and revision-keyed cache state."""

    def __init__(
        self,
        store: CapabilityCatalogStore,
        *,
        availability: CapabilityAvailability | None,
        embedding_provider: CapabilityEmbeddingProvider | None,
        telemetry: CatalogTelemetry,
        environment: str | None,
        semantic_min_similarity: float,
        lexical_min_score: float,
        search_cache_max_entries: int,
        search_cache_ttl_seconds: float,
    ) -> None:
        self._store = store
        self._availability = availability
        self._embedding_provider = embedding_provider
        self._telemetry = telemetry
        self._environment = environment
        self._semantic_min_similarity = semantic_min_similarity
        self._lexical_min_score = lexical_min_score
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
        await self._telemetry.emit(
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
        equivalent_skills: dict[tuple[str | None, str, str, str], CapabilityDescriptor] = {}
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
            await self._telemetry.record_search(
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
        await self._telemetry.record_search(tenant_id, outcome, mode, time.monotonic() - started)
        return outcome

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
