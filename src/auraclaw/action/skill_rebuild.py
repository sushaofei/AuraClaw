from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import UTC, datetime

from auraclaw.action.capability_catalog import CapabilityCatalog
from auraclaw.action.ports import ArtifactContentReader
from auraclaw.action.skill_lifecycle import SkillLifecycleStore
from auraclaw.action.skill_packages import (
    SkillPackage,
    SkillPackageRegistry,
    skill_capability_descriptor,
    skill_package_digest,
    skill_package_from_archive,
    version_satisfies,
)
from auraclaw.action.skill_publishers import SkillPublisherTrustService
from auraclaw.contracts.capabilities import (
    CapabilityStatus,
    CapabilityTrustLevel,
    McpServerDefinition,
)
from auraclaw.contracts.skills import (
    PublishedSkill,
    SkillInstallationRecord,
    SkillInstallationStatus,
    SkillPackageRetentionStatus,
    SkillPublicationStatus,
)

_SKILL_MEDIA_TYPE = "application/vnd.auraclaw.skill-package+json"


@dataclass(frozen=True)
class SkillRebuildResult:
    tenant_count: int
    publication_count: int
    failure_count: int
    safe_failure_codes: tuple[str, ...] = ()


class SkillStateRebuilder:
    def __init__(
        self,
        *,
        lifecycle: SkillLifecycleStore,
        artifacts: ArtifactContentReader,
        registry: SkillPackageRegistry,
        catalog: CapabilityCatalog,
        publisher_trust: SkillPublisherTrustService | None = None,
    ) -> None:
        self._lifecycle = lifecycle
        self._artifacts = artifacts
        self._registry = registry
        self._catalog = catalog
        self._publisher_trust = publisher_trust

    async def rebuild_all(self) -> SkillRebuildResult:
        tenants = await self._lifecycle.list_tenants()
        publication_count = 0
        failures: list[str] = []
        for tenant_id in tenants:
            count, tenant_failures = await self.rebuild_tenant(tenant_id)
            publication_count += count
            failures.extend(tenant_failures)
        return SkillRebuildResult(
            tenant_count=len(tenants),
            publication_count=publication_count,
            failure_count=len(failures),
            safe_failure_codes=tuple(failures),
        )

    async def rebuild_tenant(self, tenant_id: str) -> tuple[int, tuple[str, ...]]:
        installations = {
            (item.publisher, item.name): item
            for item in await self._lifecycle.list_installations(tenant_id)
            if item.status is SkillInstallationStatus.ACTIVE
        }
        entries: list[tuple[SkillPackage, PublishedSkill]] = []
        discoverable: set[tuple[str, str, str]] = set()
        failures: list[str] = []
        for record in await self._lifecycle.list_publications(tenant_id):
            if record.status is not SkillPublicationStatus.ACTIVE:
                continue
            package_record = await self._lifecycle.get_package(
                tenant_id, record.publisher, record.name, record.version
            )
            if (
                package_record is None
                or package_record.retention_status
                is not SkillPackageRetentionStatus.RETAINED
            ):
                failures.append("package_record_unavailable")
                continue
            if package_record.artifact_ref.media_type != _SKILL_MEDIA_TYPE:
                failures.append("artifact_media_type_invalid")
                continue
            try:
                content = await self._artifacts.read(
                    tenant_id=tenant_id,
                    artifact_ref=package_record.artifact_ref,
                    actor_id="action-hands-skill-rebuilder",
                    correlation_id=f"skill-rebuild:{tenant_id}",
                )
                package = skill_package_from_archive(content)
                if package.manifest.signature.startswith("ed25519:"):
                    if self._publisher_trust is None:
                        raise ValueError("publisher registry unavailable")
                    package = self._registry.validate_content(package)
                    key_id = await self._publisher_trust.verify_for_restore(
                        tenant_id, package
                    )
                    if key_id != package_record.signature_key_id:
                        raise ValueError("signature key mismatch")
                else:
                    package = self._registry.validate(package)
                if package.manifest != package_record.manifest:
                    raise ValueError("manifest mismatch")
                if skill_package_digest(package) != record.package_digest:
                    raise ValueError("package digest mismatch")
                entries.append(
                    (
                        package,
                        PublishedSkill(
                            tenant_id=tenant_id,
                            manifest=package_record.manifest,
                            package_digest=package_record.package_digest,
                            artifact_ref=package_record.artifact_ref,
                            status=record.status,
                        ),
                    )
                )
                installation = installations.get((record.publisher, record.name))
                if installation is not None and _installation_allows(
                    installation, record.version, record.package_digest
                ):
                    discoverable.add(
                        (record.publisher, record.name, record.version)
                    )
            except Exception as exc:
                failures.append(f"package_restore_{type(exc).__name__}")
        self._registry.replace_tenant(
            tenant_id,
            tuple(entries),
            discoverable=frozenset(discoverable),
            signatures_verified=True,
        )
        server = _skill_server(tenant_id)
        await self._catalog.register_server(server)
        await self._catalog.replace_server_capabilities(
            server.server_id,
            tuple(
                skill_capability_descriptor(
                    publication,
                    server_id=server.server_id,
                ).model_copy(update={"updated_at": datetime.now(UTC)})
                for _package, publication in entries
                if (
                    publication.manifest.publisher,
                    publication.manifest.name,
                    publication.manifest.version,
                )
                in discoverable
            ),
        )
        return len(entries), tuple(failures)


def _installation_allows(
    installation: SkillInstallationRecord,
    version: str,
    package_digest: str,
) -> bool:
    if (
        installation.pinned_package_digest is not None
        and installation.pinned_package_digest != package_digest
    ):
        return False
    return version_satisfies(version, installation.version_constraint)


def _skill_server(tenant_id: str) -> McpServerDefinition:
    suffix = hashlib.sha256(tenant_id.encode()).hexdigest()[:24]
    return McpServerDefinition(
        server_id=f"skill-registry-{suffix}",
        tenant_id=tenant_id,
        title="AuraClaw Skill Registry",
        endpoint="https://skill-registry.auraclaw.invalid/mcp",
        credential_ref="internal://action-hands/skill-registry",
        trust_level=CapabilityTrustLevel.TENANT_VERIFIED,
        status=CapabilityStatus.ACTIVE,
        enabled=True,
        metadata={"managed_source": "skill-lifecycle"},
    )
