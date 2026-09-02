from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

import pytest
from pydantic import ValidationError
from tests.skill_lifecycle_contract import assert_skill_lifecycle_core_contract

from auraclaw.action.skill_lifecycle import InMemorySkillLifecycleStore
from auraclaw.contracts.errors import VersionConflictError
from auraclaw.contracts.skills import (
    SkillInstallationRecord,
    SkillInstallationStatus,
    SkillManifest,
    SkillPackageRecord,
    SkillPackageRetentionStatus,
    SkillPublicationRecord,
    SkillPublicationStatus,
)
from auraclaw.contracts.tools import ArtifactRef

DIGEST_A = f"sha256:{'a' * 64}"
DIGEST_B = f"sha256:{'b' * 64}"


def test_memory_store_satisfies_shared_lifecycle_contract() -> None:
    asyncio.run(
        assert_skill_lifecycle_core_contract(
            InMemorySkillLifecycleStore(),
            tenant_id="tenant-memory-contract",
            identity_suffix="memory_contract",
        )
    )


def _manifest(version: str = "1.0.0") -> SkillManifest:
    return SkillManifest(
        name="release.prepare",
        version=version,
        description="Prepare a release",
        publisher="platform",
        signature="hmac-sha256:abc",
    )


def _package(*, digest: str = DIGEST_A) -> SkillPackageRecord:
    now = datetime.now(UTC)
    return SkillPackageRecord(
        tenant_id="tenant-a",
        manifest=_manifest(),
        package_digest=digest,
        artifact_ref=ArtifactRef(
            artifact_id="art-skill",
            version=1,
            content_hash="a" * 64,
            media_type="application/vnd.auraclaw.skill-package+json",
            size=128,
        ),
        signature_key_id="platform-2026",
        retention_until=now + timedelta(days=90),
        retention_updated_by="admin",
        retention_updated_at=now,
        created_at=now,
    )


def _publication(
    *, revision: int = 1, status: SkillPublicationStatus = SkillPublicationStatus.ACTIVE
) -> SkillPublicationRecord:
    now = datetime.now(UTC)
    return SkillPublicationRecord(
        publication_id="skp_release_prepare",
        tenant_id="tenant-a",
        publisher="platform",
        name="release.prepare",
        version="1.0.0",
        package_digest=DIGEST_A,
        status=status,
        revision=revision,
        created_by="admin",
        updated_by="admin",
        created_at=now,
        updated_at=now,
    )


def test_skill_lifecycle_store_enforces_immutability_and_revisions() -> None:
    async def scenario() -> None:
        store = InMemorySkillLifecycleStore()
        package = await store.put_package(_package())
        assert await store.put_package(_package()) == package
        with pytest.raises(VersionConflictError, match="immutable"):
            await store.put_package(_package(digest=DIGEST_B))

        publication = await store.put_publication(
            _publication(), expected_revision=0
        )
        assert publication.status is SkillPublicationStatus.ACTIVE
        disabled = publication.model_copy(
            update={
                "status": SkillPublicationStatus.QUARANTINED,
                "revision": 2,
                "updated_at": datetime.now(UTC),
                "reason_code": "manual_review",
            }
        )
        await store.put_publication(disabled, expected_revision=1)
        with pytest.raises(VersionConflictError, match="revision conflict"):
            await store.put_publication(
                disabled.model_copy(update={"revision": 3}), expected_revision=1
            )
        assert (await store.list_publications("tenant-a"))[0].revision == 2

    asyncio.run(scenario())


def test_installation_state_is_independent_from_publication() -> None:
    async def scenario() -> None:
        now = datetime.now(UTC)
        store = InMemorySkillLifecycleStore()
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
        await store.put_installation(installation, expected_revision=0)
        assert await store.list_installations("tenant-a") == (installation,)
        assert await store.list_tenants() == ("tenant-a",)
        uninstalled = installation.model_copy(
            update={
                "status": SkillInstallationStatus.UNINSTALLED,
                "revision": 2,
                "reason_code": "tenant_request",
                "updated_at": datetime.now(UTC),
            }
        )
        await store.put_installation(uninstalled, expected_revision=1)
        assert (
            await store.get_installation("tenant-a", "platform", "release.prepare")
        ) == uninstalled

    asyncio.run(scenario())


def test_purge_is_package_retention_not_publication_status() -> None:
    assert "purged" not in {item.value for item in SkillPublicationStatus}
    with pytest.raises(ValidationError, match="purged_at"):
        SkillPackageRecord(
            **{
                **_package().model_dump(),
                "retention_status": SkillPackageRetentionStatus.PURGED,
                "purged_at": None,
            }
        )


def test_installation_contracts_fail_closed() -> None:
    now = datetime.now(UTC)
    with pytest.raises(ValidationError, match="cannot auto-upgrade"):
        SkillInstallationRecord(
            installation_id="ski_release_prepare",
            tenant_id="tenant-a",
            publisher="platform",
            name="release.prepare",
            pinned_package_digest=DIGEST_A,
            auto_upgrade=True,
            created_by="admin",
            updated_by="admin",
            created_at=now,
            updated_at=now,
        )
    with pytest.raises(ValidationError, match="uninstall policy evidence"):
        SkillInstallationRecord(
            installation_id="ski_draining",
            tenant_id="tenant-a",
            publisher="platform",
            name="release.prepare",
            status=SkillInstallationStatus.DRAINING,
            reason_code="tenant_uninstalled",
            created_by="admin",
            updated_by="admin",
            created_at=now,
            updated_at=now,
        )
    with pytest.raises(ValidationError, match="requires a reason"):
        SkillPublicationRecord(
            publication_id="skp_revoked",
            tenant_id="tenant-a",
            publisher="platform",
            name="release.prepare",
            version="1.0.0",
            package_digest=DIGEST_A,
            status=SkillPublicationStatus.REVOKED,
            created_by="admin",
            updated_by="admin",
            created_at=now,
            updated_at=now,
        )
