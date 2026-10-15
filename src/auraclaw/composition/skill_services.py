"""Skill and MCP registry, catalog storage and publication service assembly."""

from __future__ import annotations

from auraclaw.action.capability_catalog import (
    InMemoryCapabilityCatalogStore,
)
from auraclaw.action.mcp_primitives import HandsResourceRegistry
from auraclaw.action.mcp_registry import (
    InMemoryMcpServerRegistryStore,
    McpServerRegistryService,
)
from auraclaw.action.ports import (
    ArtifactContentReader,
    ArtifactWriter,
    SkillArtifactLifecycle,
)
from auraclaw.action.skill_admin_catalog import SkillCapabilityAvailability
from auraclaw.action.skill_lifecycle import (
    InMemorySkillLifecycleStore,
    SkillLifecycleStore,
)
from auraclaw.action.skill_packages import (
    HmacSkillSignatureVerifier,
    SkillPackageRegistry,
)
from auraclaw.action.skill_publication import SkillPublicationService
from auraclaw.action.skill_publishers import (
    SkillPublisherTrustService,
)
from auraclaw.config import Settings
from auraclaw.infrastructure.artifacts.store import ArtifactStore, InMemoryObjectStorage
from auraclaw.infrastructure.persistence.postgres_capability_catalog import (
    PostgresCapabilityCatalogStore,
)
from auraclaw.infrastructure.persistence.postgres_mcp_registry import (
    PostgresMcpServerRegistryStore,
)
from auraclaw.infrastructure.persistence.postgres_skill_lifecycle import (
    PostgresSkillLifecycleStore,
)

_SKILL_PACKAGE_REGISTRY: SkillPackageRegistry | None = None


def mcp_registry_service(
    settings: Settings,
) -> tuple[
    McpServerRegistryService,
    InMemoryMcpServerRegistryStore | PostgresMcpServerRegistryStore,
]:
    store: InMemoryMcpServerRegistryStore | PostgresMcpServerRegistryStore
    if settings.sql_storage_enabled:
        store = PostgresMcpServerRegistryStore(settings.resolved_database_url)
    else:
        store = InMemoryMcpServerRegistryStore()
    allow_private_none = (
        settings.mcp_allow_private_auth_none
        if settings.mcp_allow_private_auth_none is not None
        else settings.deployment_profile == "development"
    )
    return (
        McpServerRegistryService(store, allow_private_auth_none=allow_private_none),
        store,
    )


def build_capability_catalog_store(
    settings: Settings,
) -> InMemoryCapabilityCatalogStore | PostgresCapabilityCatalogStore:
    if settings.sql_storage_enabled:
        return PostgresCapabilityCatalogStore(settings.resolved_database_url)
    return InMemoryCapabilityCatalogStore()


def skill_registry_service(
    settings: Settings,
    *,
    artifacts: ArtifactWriter | None = None,
) -> SkillPackageRegistry:
    global _SKILL_PACKAGE_REGISTRY
    if artifacts is None and _SKILL_PACKAGE_REGISTRY is not None:
        return _SKILL_PACKAGE_REGISTRY
    configured_signing_key = (
        settings.skill_signing_key.get_secret_value().encode()
        if settings.skill_signing_key is not None
        else None
    )
    signing_key = configured_signing_key or b"auraclaw-development-platform-skill-key"
    registry = SkillPackageRegistry(
        artifacts=(artifacts or ArtifactStore(InMemoryObjectStorage(), signing_key=signing_key)),
        signature_verifier=HmacSkillSignatureVerifier({"platform": signing_key}),
        resources=HandsResourceRegistry(),
    )
    if artifacts is None:
        _SKILL_PACKAGE_REGISTRY = registry
    return registry


def skill_publication_service(
    settings: Settings,
    registry: SkillPackageRegistry,
    lifecycle: SkillLifecycleStore | None = None,
    artifact_reader: ArtifactContentReader | None = None,
    artifact_lifecycle: SkillArtifactLifecycle | None = None,
    publisher_trust: SkillPublisherTrustService | None = None,
    dependency_availability: SkillCapabilityAvailability | None = None,
) -> tuple[SkillPublicationService, SkillLifecycleStore]:
    selected_lifecycle = lifecycle
    if selected_lifecycle is None:
        if settings.sql_storage_enabled:
            selected_lifecycle = PostgresSkillLifecycleStore(
                settings.resolved_database_url,
                transaction_retry_attempts=settings.skill_transaction_retry_attempts,
                transaction_retry_base_delay=(
                    settings.skill_transaction_retry_base_delay_seconds
                ),
            )
        else:
            selected_lifecycle = InMemorySkillLifecycleStore()
    return (
        SkillPublicationService(
            registry=registry,
            lifecycle=selected_lifecycle,
            artifacts=artifact_reader,
            artifact_lifecycle=artifact_lifecycle,
            publisher_trust=publisher_trust,
            dependency_availability=dependency_availability,
        ),
        selected_lifecycle,
    )


class EmptyApprovalReader:
    async def get(self, tenant_id: str, approval_id: str) -> None:
        del tenant_id, approval_id
        return None

    async def find_approved(
        self,
        tenant_id: str,
        session_id: str,
        digest: str,
        policy_version: str,
        run_id: str | None = None,
    ) -> None:
        del tenant_id, session_id, digest, policy_version
        return None
