from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

import pytest
from pydantic import ValidationError

from auraclaw.action.skill_lifecycle import InMemorySkillLifecycleStore
from auraclaw.contracts.errors import NotFoundError, VersionConflictError
from auraclaw.contracts.skills import (
    SkillInstallationRecord,
    SkillInstallationStatus,
    SkillManifest,
    SkillPackageRecord,
    SkillPackageRetentionStatus,
    SkillPublicationRecord,
    SkillPublicationStatus,
    SkillSourceDesiredState,
    SkillSourceKind,
    SkillSourceRecord,
    SkillSourceSyncState,
)
from auraclaw.contracts.tools import ArtifactRef

DIGEST_A = f"sha256:{'a' * 64}"
DIGEST_B = f"sha256:{'b' * 64}"


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


def test_installation_and_source_state_are_independent_from_publication() -> None:
    async def scenario() -> None:
        now = datetime.now(UTC)
        store = InMemorySkillLifecycleStore()
        source = SkillSourceRecord(
            source_id="sks_builtin",
            tenant_id="tenant-a",
            kind=SkillSourceKind.BUILTIN,
            desired_state=SkillSourceDesiredState.ENABLED,
            publisher_allowlist=("platform",),
            created_by="admin",
            updated_by="admin",
            created_at=now,
            updated_at=now,
        )
        await store.put_source(source, expected_revision=0)
        with pytest.raises(VersionConflictError, match="kind is immutable"):
            await store.put_source(
                source.model_copy(
                    update={"kind": SkillSourceKind.GIT, "revision": 2}
                ),
                expected_revision=1,
            )
        installation = SkillInstallationRecord(
            installation_id="ski_release_prepare",
            tenant_id="tenant-a",
            publisher="platform",
            name="release.prepare",
            status=SkillInstallationStatus.ACTIVE,
            source_id="sks_builtin",
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

        await store.put_sync_state(
            SkillSourceSyncState(
                source_id=source.source_id,
                tenant_id=source.tenant_id,
                generation=2,
                complete_snapshot=True,
                last_success_at=now,
                last_attempt_at=now,
            )
        )
        with pytest.raises(VersionConflictError, match="move backwards"):
            await store.put_sync_state(
                SkillSourceSyncState(
                    source_id=source.source_id,
                    tenant_id=source.tenant_id,
                    generation=1,
                )
            )
        with pytest.raises(NotFoundError, match="Source"):
            await store.put_sync_state(
                SkillSourceSyncState(
                    source_id="sks_missing",
                    tenant_id="tenant-a",
                )
            )

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


def test_source_lease_is_exclusive_and_rejects_stale_fencing() -> None:
    async def scenario() -> None:
        store = InMemorySkillLifecycleStore()
        now = datetime.now(UTC)
        source = SkillSourceRecord(
            source_id="sks_external",
            tenant_id="tenant-a",
            kind=SkillSourceKind.MCP,
            desired_state=SkillSourceDesiredState.ENABLED,
            publisher_allowlist=("platform",),
            created_by="admin",
            updated_by="admin",
            created_at=now,
            updated_at=now,
        )
        await store.put_source(source, expected_revision=0)
        first = await store.claim_source_lease(
            tenant_id="tenant-a",
            source_id=source.source_id,
            owner="hands-a",
            ttl=timedelta(minutes=1),
        )
        assert first is not None and first.fencing_token == 1
        assert (
            await store.claim_source_lease(
                tenant_id="tenant-a",
                source_id=source.source_id,
                owner="hands-b",
                ttl=timedelta(minutes=1),
            )
            is None
        )

        await store.release_source_lease(first)
        second = await store.claim_source_lease(
            tenant_id="tenant-a",
            source_id=source.source_id,
            owner="hands-b",
            ttl=timedelta(minutes=1),
        )
        assert second is not None and second.fencing_token == 2
        stale_state = SkillSourceSyncState(
            source_id=source.source_id,
            tenant_id="tenant-a",
            generation=first.fencing_token,
            last_attempt_at=now,
        )
        with pytest.raises(VersionConflictError, match="lease is stale"):
            await store.put_sync_state_fenced(stale_state, lease=first)

        current_state = stale_state.model_copy(
            update={
                "generation": second.fencing_token,
                "complete_snapshot": True,
                "last_success_at": now,
            }
        )
        await store.put_sync_state_fenced(current_state, lease=second)
        assert await store.get_sync_state("tenant-a", source.source_id) == current_state

    asyncio.run(scenario())


def test_source_and_installation_contracts_fail_closed() -> None:
    now = datetime.now(UTC)
    with pytest.raises(ValidationError, match="publisher allowlist"):
        SkillSourceRecord(
            source_id="sks_external",
            tenant_id="tenant-a",
            kind=SkillSourceKind.MCP,
            desired_state=SkillSourceDesiredState.ENABLED,
            created_by="admin",
            updated_by="admin",
            created_at=now,
            updated_at=now,
        )
    with pytest.raises(ValidationError, match="credential_ref"):
        SkillSourceRecord(
            source_id="sks_external",
            tenant_id="tenant-a",
            kind=SkillSourceKind.MCP,
            desired_state=SkillSourceDesiredState.ENABLED,
            publisher_allowlist=("acme",),
            config_metadata={"nested": {"access_token": "do-not-store"}},
            created_by="admin",
            updated_by="admin",
            created_at=now,
            updated_at=now,
        )
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
    with pytest.raises(ValidationError, match="last_success_at"):
        SkillSourceSyncState(
            source_id="sks_external",
            tenant_id="tenant-a",
            complete_snapshot=True,
        )
