from __future__ import annotations

from datetime import UTC, datetime, timedelta

from auraclaw.action.skill_lifecycle import SkillLifecycleStore
from auraclaw.contracts.skills import (
    SkillInstallationRecord,
    SkillInstallationStatus,
    SkillManifest,
    SkillPackageRecord,
    SkillPublicationRecord,
    SkillPublicationStatus,
)
from auraclaw.contracts.tools import ArtifactRef


async def assert_skill_lifecycle_core_contract(
    store: SkillLifecycleStore,
    *,
    tenant_id: str,
    identity_suffix: str,
) -> None:
    """Exercise the lifecycle semantics shared by memory and PostgreSQL adapters."""
    now = datetime.now(UTC)
    digest = f"sha256:{'c' * 64}"
    manifest = SkillManifest(
        name="contract.release",
        version="1.0.0",
        description="Shared lifecycle adapter contract",
        publisher="platform",
        signature="hmac-sha256:contract",
    )
    package = SkillPackageRecord(
        tenant_id=tenant_id,
        manifest=manifest,
        package_digest=digest,
        artifact_ref=ArtifactRef(
            artifact_id=f"artifact-{identity_suffix}",
            version=1,
            content_hash="c" * 64,
            media_type="application/vnd.auraclaw.skill-package+json",
            size=128,
        ),
        retention_until=now + timedelta(days=90),
        retention_updated_by="contract-test",
        retention_updated_at=now,
        created_at=now,
    )
    publication = SkillPublicationRecord(
        publication_id=f"skp_{identity_suffix}",
        tenant_id=tenant_id,
        publisher=manifest.publisher,
        name=manifest.name,
        version=manifest.version,
        package_digest=digest,
        status=SkillPublicationStatus.ACTIVE,
        created_by="contract-test",
        updated_by="contract-test",
        created_at=now,
        updated_at=now,
    )
    installation = SkillInstallationRecord(
        installation_id=f"ski_{identity_suffix}",
        tenant_id=tenant_id,
        publisher=manifest.publisher,
        name=manifest.name,
        status=SkillInstallationStatus.ACTIVE,
        created_by="contract-test",
        updated_by="contract-test",
        created_at=now,
        updated_at=now,
    )

    assert await store.put_package(package) == package
    assert await store.put_package(package) == package
    assert await store.get_package(
        tenant_id, manifest.publisher, manifest.name, manifest.version
    ) == package
    assert await store.list_packages(tenant_id) == (package,)

    assert await store.put_publication(publication, expected_revision=0) == publication
    assert await store.get_publication(
        tenant_id, manifest.publisher, manifest.name, manifest.version
    ) == publication
    assert await store.list_publications(tenant_id) == (publication,)

    assert await store.put_installation(installation, expected_revision=0) == installation
    assert await store.get_installation(
        tenant_id, manifest.publisher, manifest.name
    ) == installation
    assert await store.list_installations(tenant_id) == (installation,)

    assert await store.list_packages(f"other-{tenant_id}") == ()
    assert await store.list_publications(f"other-{tenant_id}") == ()
    assert await store.list_installations(f"other-{tenant_id}") == ()
