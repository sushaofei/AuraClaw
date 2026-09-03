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
    async def is_available(self, tenant_id: str, capability: CapabilityDescriptor) -> bool:
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


def test_catalog_and_runtime_reject_active_new_publication_pinned_to_old_package() -> None:
    from auraclaw.action.skill_admin_catalog import published_skill, skill_availability
    from auraclaw.action.skill_rebuild import _installation_allows

    async def scenario() -> None:
        old, new = _package("1.0.0", "a"), _package("2.0.0", "b")
        now = datetime.now(UTC)
        installation = SkillInstallationRecord(
            installation_id="ski_release_prepare",
            tenant_id="tenant-a",
            publisher="platform",
            name="release.prepare",
            status=SkillInstallationStatus.ACTIVE,
            version_constraint="==1.0.0",
            auto_upgrade=False,
            pinned_package_digest=old.package_digest,
            created_by="admin",
            updated_by="admin",
            created_at=now,
            updated_at=now,
        )
        service = SkillAdminCatalogQueryService(
            _Snapshot(
                (old, new),
                (
                    _publication(old).model_copy(update={"status": SkillPublicationStatus.REVOKED}),
                    _publication(new),
                ),
                (installation,),
            )
        )
        assert (await service.list_latest("tenant-a", SkillCatalogQuery()))[0].availability == (
            "installation_version_mismatch"
        )
        publication = published_skill(new, _publication(new))
        assert not _installation_allows(installation, "2.0.0", new.package_digest)
        digest_mismatch = installation.model_copy(update={"version_constraint": "==2.0.0"})
        assert skill_availability(publication, digest_mismatch) == "installation_digest_mismatch"
        assert not _installation_allows(digest_mismatch, "2.0.0", new.package_digest)
        upgraded = digest_mismatch.model_copy(update={"pinned_package_digest": new.package_digest})
        assert skill_availability(publication, upgraded) == "available"
        assert _installation_allows(upgraded, "2.0.0", new.package_digest)

    asyncio.run(scenario())


def test_staged_candidate_does_not_replace_current_skill_or_upgrade_status() -> None:
    from auraclaw.contracts.skills import SkillUpgradeState

    async def scenario() -> None:
        current, candidate = _package("2.0.0", "b"), _package("3.0.0", "c")
        state = SkillUpgradeState(
            tenant_id="tenant-a",
            publisher="platform",
            name="release.prepare",
            operation_id="upgrade",
            command_id="upgrade",
            current_version="2.0.0",
            package_digest=current.package_digest,
            generation=2,
            phase="deleting",
            actor_id="admin",
            correlation_id="upgrade",
            causation_id="upgrade",
            updated_at=datetime.now(UTC),
        )

        class Snapshot(_Snapshot):
            async def list_upgrade_states(self, tenant_id):
                return (state,)

        snapshot = Snapshot(
            (current, candidate),
            (
                _publication(current),
                _publication(candidate).model_copy(
                    update={"status": SkillPublicationStatus.STAGED}
                ),
            ),
            (),
        )
        items = await SkillAdminCatalogQueryService(snapshot).list_latest(
            "tenant-a", SkillCatalogQuery()
        )
        assert items[0].publication.manifest.version == "2.0.0"
        assert items[0].publication.upgrade == state

    asyncio.run(scenario())
