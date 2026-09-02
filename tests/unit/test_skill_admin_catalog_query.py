from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

from auraclaw.action.skill_admin_catalog import (
    SkillAdminCatalogQueryService,
    SkillCatalogQuery,
)
from auraclaw.contracts.capabilities import CapabilityDescriptor
from auraclaw.contracts.skills import (
    SkillInstallationRecord,
    SkillInstallationStatus,
    SkillManifest,
    SkillPackageRecord,
    SkillPublicationRecord,
    SkillPublicationStatus,
)
from auraclaw.contracts.tools import ArtifactRef


class _Snapshot:
    def __init__(
        self,
        packages: tuple[SkillPackageRecord, ...],
        publications: tuple[SkillPublicationRecord, ...],
        installations: tuple[SkillInstallationRecord, ...],
    ) -> None:
        self._value = packages, publications, installations

    async def get_admin_snapshot(
        self, tenant_id: str
    ) -> tuple[
        tuple[SkillPackageRecord, ...],
        tuple[SkillPublicationRecord, ...],
        tuple[SkillInstallationRecord, ...],
    ]:
        assert tenant_id == "tenant-a"
        return self._value


class _UnavailableDependencies:
    async def is_available(
        self, tenant_id: str, capability: CapabilityDescriptor
    ) -> bool:
        assert tenant_id == "tenant-a"
        assert capability.metadata["model_contract"]["publisher"] == "platform"
        assert capability.canonical_name == "release.prepare"
        return False


def _package(version: str, digest_character: str) -> SkillPackageRecord:
    now = datetime.now(UTC)
    return SkillPackageRecord(
        tenant_id="tenant-a",
        manifest=SkillManifest(
            name="release.prepare",
            version=version,
            description="Prepare a production release",
            publisher="platform",
            risk_level="medium",
            signature="hmac-sha256:abc",
        ),
        package_digest=f"sha256:{digest_character * 64}",
        artifact_ref=ArtifactRef(
            artifact_id=f"artifact-{version}",
            version=1,
            content_hash=digest_character * 64,
            media_type="application/vnd.auraclaw.skill-package+json",
            size=128,
        ),
        retention_until=now + timedelta(days=90),
        retention_updated_by="admin",
        retention_updated_at=now,
        created_at=now,
    )


def _publication(package: SkillPackageRecord) -> SkillPublicationRecord:
    now = datetime.now(UTC)
    return SkillPublicationRecord(
        publication_id=f"skp_release_{package.manifest.version.replace('.', '_')}",
        tenant_id="tenant-a",
        publisher=package.manifest.publisher,
        name=package.manifest.name,
        version=package.manifest.version,
        package_digest=package.package_digest,
        status=SkillPublicationStatus.ACTIVE,
        created_by="admin",
        updated_by="admin",
        created_at=now,
        updated_at=now,
    )


def test_catalog_query_selects_latest_and_reports_dependency_availability() -> None:
    async def scenario() -> None:
        now = datetime.now(UTC)
        old_package = _package("1.9.0", "a")
        latest_package = _package("1.10.0", "b")
        installation = SkillInstallationRecord(
            installation_id="ski_release_prepare",
            tenant_id="tenant-a",
            publisher="platform",
            name="release.prepare",
            status=SkillInstallationStatus.ACTIVE,
            created_by="admin",
            updated_by="admin",
            created_at=now,
            updated_at=now,
        )
        service = SkillAdminCatalogQueryService(
            _Snapshot(
                (old_package, latest_package),
                (_publication(old_package), _publication(latest_package)),
                (installation,),
            ),
            _UnavailableDependencies(),
        )

        items = await service.list_latest(
            "tenant-a",
            SkillCatalogQuery(
                text="PRODUCTION",
                publisher="platform",
                risk_level="medium",
                publication_status="active",
                installation_status="active",
            ),
        )

        assert len(items) == 1
        assert items[0].publication.manifest.version == "1.10.0"
        assert items[0].availability == "dependencies_unavailable"

    asyncio.run(scenario())


def test_catalog_query_excludes_skills_that_do_not_match_filters() -> None:
    async def scenario() -> None:
        package = _package("1.0.0", "a")
        service = SkillAdminCatalogQueryService(
            _Snapshot((package,), (_publication(package),), ()),
        )

        items = await service.list_latest(
            "tenant-a", SkillCatalogQuery(installation_status="active")
        )

        assert items == ()

    asyncio.run(scenario())
