from __future__ import annotations

from datetime import datetime

from auraclaw.action.capability_search import (
    CapabilityEmbeddingProvider,
    SearchOutcome,
)
from auraclaw.action.catalog.contracts import (
    CAPABILITY_LOAD_TOOL_NAME,
    CAPABILITY_SEARCH_TOOL_NAME,
    SKILL_BINDING_STATUS_TOOL_NAME,
    SKILL_RESOLVE_TOOL_NAME,
    CapabilityAvailability,
    CapabilitySearchMetricWriter,
    SkillPublicationReader,
    SkillPublisherSecurityReader,
    SkillResolverPort,
)
from auraclaw.action.catalog.executors import (
    CapabilityLoadExecutor,
    CapabilitySearchExecutor,
    RoutedHandsExecutor,
    SkillBindingStatusExecutor,
    SkillResolveExecutor,
)
from auraclaw.action.catalog.memory import InMemoryCapabilityCatalogStore
from auraclaw.action.catalog.publication import CatalogPublisher
from auraclaw.action.catalog.search import CatalogSearchService
from auraclaw.action.catalog.telemetry import CatalogTelemetry
from auraclaw.action.catalog.tools import (
    capability_load_tool,
    capability_search_tool,
    skill_binding_status_tool,
    skill_resolve_tool,
)
from auraclaw.action.ports import (
    CapabilityCatalogStore,
    CatalogCommitResult,
    CatalogReconcileLease,
)
from auraclaw.contracts.capabilities import (
    CapabilityDescriptor,
    CapabilityKind,
    McpServerDefinition,
)

__all__ = [
    "CapabilityCatalog",
    "InMemoryCapabilityCatalogStore",
    "CapabilitySearchMetricWriter",
    "SkillResolverPort",
    "SkillPublicationReader",
    "SkillPublisherSecurityReader",
    "CapabilityAvailability",
    "CapabilitySearchExecutor",
    "CapabilityLoadExecutor",
    "SkillResolveExecutor",
    "SkillBindingStatusExecutor",
    "RoutedHandsExecutor",
    "capability_search_tool",
    "capability_load_tool",
    "skill_resolve_tool",
    "skill_binding_status_tool",
    "CAPABILITY_SEARCH_TOOL_NAME",
    "CAPABILITY_LOAD_TOOL_NAME",
    "SKILL_RESOLVE_TOOL_NAME",
    "SKILL_BINDING_STATUS_TOOL_NAME",
]


class CapabilityCatalog:
    """Stable catalog facade over independently owned publication and search services."""

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
        telemetry = CatalogTelemetry(metric_writer)
        self._publisher = CatalogPublisher(
            store,
            embedding_provider=embedding_provider,
            telemetry=telemetry,
            index_embedding_timeout_seconds=index_embedding_timeout_seconds,
        )
        self._search = CatalogSearchService(
            store,
            availability=availability,
            embedding_provider=embedding_provider,
            telemetry=telemetry,
            environment=environment,
            semantic_min_similarity=semantic_min_similarity,
            lexical_min_score=lexical_min_score,
            search_cache_max_entries=search_cache_max_entries,
            search_cache_ttl_seconds=search_cache_ttl_seconds,
        )

    def set_availability(self, availability: CapabilityAvailability) -> None:
        return self._search.set_availability(availability)

    async def get_server_definition(self, server_id: str) -> McpServerDefinition | None:
        return await self._publisher.get_server_definition(server_id)

    async def register_server(
        self, server: McpServerDefinition, *, allow_rollback: bool = False
    ) -> None:
        return await self._publisher.register_server(server, allow_rollback=allow_rollback)

    async def remove_server(self, server_id: str) -> None:
        return await self._publisher.remove_server(server_id)

    async def replace_server_capabilities(
        self,
        server_id: str,
        capabilities: tuple[CapabilityDescriptor, ...],
        *,
        lease: CatalogReconcileLease | None = None,
        snapshot_digest: str | None = None,
        source_revision: str | None = None,
    ) -> CatalogCommitResult:
        return await self._publisher.replace_server_capabilities(
            server_id,
            capabilities,
            lease=lease,
            snapshot_digest=snapshot_digest,
            source_revision=source_revision,
        )

    async def publication_status(
        self, *, tenant_id: str, server_id: str
    ) -> dict[str, object] | None:
        return await self._publisher.publication_status(tenant_id=tenant_id, server_id=server_id)

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
        return await self._search.search(
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
        return await self._search.search_with_evidence(
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

    async def list_server_tools(
        self, *, tenant_id: str, server_id: str
    ) -> tuple[CapabilityDescriptor, ...]:
        return await self._search.list_server_tools(tenant_id=tenant_id, server_id=server_id)

    async def list_server_capabilities(
        self, *, tenant_id: str, server_id: str
    ) -> tuple[CapabilityDescriptor, ...]:
        return await self._search.list_server_capabilities(tenant_id=tenant_id, server_id=server_id)

    async def get(self, *, tenant_id: str, capability_id: str) -> CapabilityDescriptor | None:
        return await self._search.get(tenant_id=tenant_id, capability_id=capability_id)
