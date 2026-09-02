from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from auraclaw.action.skill_packages import skill_capability_descriptor
from auraclaw.contracts.capabilities import CapabilityDescriptor
from auraclaw.contracts.errors import VersionConflictError
from auraclaw.contracts.skills import (
    PublishedSkill,
    SkillInstallationRecord,
    SkillPackageRecord,
    SkillPackageRetentionStatus,
    SkillPublicationRecord,
)


class SkillAdminSnapshotReader(Protocol):
    async def get_admin_snapshot(
        self, tenant_id: str
    ) -> tuple[
        tuple[SkillPackageRecord, ...],
        tuple[SkillPublicationRecord, ...],
        tuple[SkillInstallationRecord, ...],
    ]: ...


class SkillCapabilityAvailability(Protocol):
    async def is_available(
        self, tenant_id: str, capability: CapabilityDescriptor
    ) -> bool: ...


@dataclass(frozen=True, slots=True)
class SkillCatalogQuery:
    text: str | None = None
    publisher: str | None = None
    risk_level: str | None = None
    publication_status: str | None = None
    installation_status: str | None = None


@dataclass(frozen=True, slots=True)
class SkillCatalogItem:
    publication: PublishedSkill
    publication_state: SkillPublicationRecord
    installation: SkillInstallationRecord | None
    availability: str


class SkillAdminCatalogQueryService:
    """Own Skill catalog selection, filtering and dependency availability semantics."""

    def __init__(
        self,
        snapshot: SkillAdminSnapshotReader,
        capability_availability: SkillCapabilityAvailability | None = None,
    ) -> None:
        self._snapshot = snapshot
        self._capability_availability = capability_availability

    async def list_latest(
        self, tenant_id: str, query: SkillCatalogQuery
    ) -> tuple[SkillCatalogItem, ...]:
        packages, publication_states, installation_records = (
            await self._snapshot.get_admin_snapshot(tenant_id)
        )
        package_by_version = {
            (item.manifest.publisher, item.manifest.name, item.manifest.version): item
            for item in packages
            if item.retention_status is not SkillPackageRetentionStatus.PURGED
        }
        state_by_version = {
            (item.publisher, item.name, item.version): item
            for item in publication_states
        }
        latest: dict[tuple[str, str], tuple[PublishedSkill, SkillPublicationRecord]] = {}
        for version_key, publication_state in state_by_version.items():
            package = package_by_version.get(version_key)
            if package is None:
                continue
            publication = published_skill(package, publication_state)
            skill_key = (publication.manifest.publisher, publication.manifest.name)
            current = latest.get(skill_key)
            if current is None or semver_key(publication.manifest.version) > semver_key(
                current[0].manifest.version
            ):
                latest[skill_key] = (publication, publication_state)
        installations = {
            (item.publisher, item.name): item for item in installation_records
        }
        normalized_query = (query.text or "").casefold()
        result: list[SkillCatalogItem] = []
        for publication, publication_state in latest.values():
            manifest = publication.manifest
            installation = installations.get((manifest.publisher, manifest.name))
            if query.publisher and manifest.publisher != query.publisher:
                continue
            if query.risk_level and manifest.risk_level != query.risk_level:
                continue
            if (
                query.publication_status
                and publication.status.value != query.publication_status
            ):
                continue
            if query.installation_status and (
                installation is None
                or installation.status.value != query.installation_status
            ):
                continue
            haystack = " ".join(
                (manifest.publisher, manifest.name, manifest.description)
            ).casefold()
            if normalized_query and normalized_query not in haystack:
                continue
            availability = skill_availability(publication, installation)
            if (
                availability == "available"
                and self._capability_availability is not None
                and not await self._capability_availability.is_available(
                    tenant_id,
                    skill_capability_descriptor(publication),
                )
            ):
                availability = "dependencies_unavailable"
            result.append(
                SkillCatalogItem(
                    publication=publication,
                    publication_state=publication_state,
                    installation=installation,
                    availability=availability,
                )
            )
        return tuple(
            sorted(
                result,
                key=lambda item: (
                    item.publication.manifest.publisher,
                    item.publication.manifest.name,
                ),
            )
        )


def published_skill(
    package: SkillPackageRecord,
    publication: SkillPublicationRecord,
) -> PublishedSkill:
    if package.package_digest != publication.package_digest:
        raise VersionConflictError("Skill package and publication digests do not match")
    return PublishedSkill(
        tenant_id=package.tenant_id,
        manifest=package.manifest,
        package_digest=package.package_digest,
        artifact_ref=package.artifact_ref,
        status=publication.status,
        revocation_action=publication.revocation_action,
    )


def skill_availability(
    publication: PublishedSkill,
    installation: SkillInstallationRecord | None,
) -> str:
    if publication.status.value != "active":
        return "publication_unavailable"
    if installation is None:
        return "not_installed"
    if installation.status.value != "active":
        return f"installation_{installation.status.value}"
    return "available"


def semver_key(version: str) -> tuple[int, int, int]:
    core = version.split("-", 1)[0]
    major, minor, patch = core.split(".")
    return int(major), int(minor), int(patch)
