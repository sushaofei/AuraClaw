from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from pathlib import Path
from uuid import uuid4

import asyncpg
import pytest

from auraclaw.config import get_settings
from auraclaw.contracts.errors import VersionConflictError
from auraclaw.contracts.skills import (
    SkillInstallationRecord,
    SkillInstallationStatus,
    SkillManifest,
    SkillPackageRecord,
    SkillPublicationRecord,
    SkillPublicationStatus,
    SkillSourceDesiredState,
    SkillSourceKind,
    SkillSourceRecord,
    SkillSourceSyncState,
)
from auraclaw.contracts.tools import ArtifactRef
from auraclaw.infrastructure.persistence.postgres_common import asyncpg_url
from auraclaw.infrastructure.persistence.postgres_skill_lifecycle import (
    PostgresSkillLifecycleStore,
)

SETTINGS = get_settings()
DATABASE_URL = asyncpg_url(SETTINGS.resolved_database_url) if SETTINGS.postgres_enabled else None
ROOT = Path(__file__).resolve().parents[2]
MIGRATION = (ROOT / "migrations/0023_skill_lifecycle.sql").read_text()
pytestmark = pytest.mark.skipif(DATABASE_URL is None, reason="PostgreSQL test URL not configured")


def test_postgres_skill_lifecycle_is_persistent_and_tenant_scoped() -> None:
    async def scenario() -> None:
        assert DATABASE_URL is not None
        connection = await asyncpg.connect(DATABASE_URL)
        await connection.execute(MIGRATION)
        suffix = uuid4().hex
        tenant_id = f"tenant-skill-{suffix}"
        digest = f"sha256:{suffix.ljust(64, '0')}"
        store_a = PostgresSkillLifecycleStore(DATABASE_URL)
        store_b = PostgresSkillLifecycleStore(DATABASE_URL)
        now = datetime.now(UTC)
        try:
            package = SkillPackageRecord(
                tenant_id=tenant_id,
                manifest=SkillManifest(
                    name="release.prepare",
                    version="1.0.0",
                    description="Prepare release",
                    publisher="platform",
                    signature="hmac-sha256:abc",
                ),
                package_digest=digest,
                artifact_ref=ArtifactRef(
                    artifact_id=f"art-{suffix}",
                    version=1,
                    content_hash=suffix,
                    media_type="application/vnd.auraclaw.skill-package+json",
                    size=100,
                ),
                created_at=now,
            )
            await store_a.put_package(package)
            publication = SkillPublicationRecord(
                publication_id=f"skp_{suffix}",
                tenant_id=tenant_id,
                publisher="platform",
                name="release.prepare",
                version="1.0.0",
                package_digest=digest,
                status=SkillPublicationStatus.ACTIVE,
                revision=1,
                created_by="integration-test",
                created_at=now,
                updated_at=now,
            )
            await store_a.put_publication(publication, expected_revision=0)
            installation = SkillInstallationRecord(
                installation_id=f"ski_{suffix}",
                tenant_id=tenant_id,
                publisher="platform",
                name="release.prepare",
                status=SkillInstallationStatus.ACTIVE,
                revision=1,
                created_by="integration-test",
                updated_by="integration-test",
                created_at=now,
                updated_at=now,
            )
            await store_a.put_installation(installation, expected_revision=0)
            source = SkillSourceRecord(
                source_id=f"sks_source-{suffix}",
                tenant_id=tenant_id,
                kind=SkillSourceKind.MCP,
                desired_state=SkillSourceDesiredState.ENABLED,
                publisher_allowlist=("platform",),
                credential_ref=f"vault/{suffix}#mcp",
                config_metadata={"server_id": f"server-{suffix}"},
                revision=1,
                created_by="integration-test",
                updated_by="integration-test",
                created_at=now,
                updated_at=now,
            )
            await store_a.put_source(source, expected_revision=0)
            with pytest.raises(VersionConflictError, match="revision conflict"):
                await store_a.put_source(
                    source.model_copy(
                        update={"kind": SkillSourceKind.GIT, "revision": 2}
                    ),
                    expected_revision=1,
                )
            sync_state = SkillSourceSyncState(
                source_id=source.source_id,
                tenant_id=tenant_id,
                generation=1,
                cursor="cursor-1",
                complete_snapshot=True,
                last_success_at=now,
                last_attempt_at=now,
            )
            await store_a.put_sync_state(sync_state)

            assert await store_b.get_package(
                tenant_id, "platform", "release.prepare", "1.0.0"
            ) == package
            assert await store_b.get_installation(
                tenant_id, "platform", "release.prepare"
            ) == installation
            assert await store_b.list_installations(tenant_id) == (installation,)
            assert tenant_id in await store_b.list_tenants()
            assert await store_b.list_publications(f"other-{tenant_id}") == ()
            assert await store_b.get_source(tenant_id, source.source_id) == source
            assert await store_b.get_sync_state(tenant_id, source.source_id) == sync_state

            with pytest.raises(VersionConflictError, match="revision conflict"):
                await store_b.put_installation(
                    installation,
                    expected_revision=0,
                )
        finally:
            await store_a.close()
            await store_b.close()
            await connection.execute(
                "DELETE FROM hands.skill_source_sync_state WHERE tenant_id=$1",
                tenant_id,
            )
            await connection.execute(
                "DELETE FROM hands.skill_source WHERE tenant_id=$1", tenant_id
            )
            await connection.execute(
                "DELETE FROM hands.skill_installation WHERE tenant_id=$1", tenant_id
            )
            await connection.execute(
                "DELETE FROM hands.skill_publication WHERE tenant_id=$1", tenant_id
            )
            await connection.execute(
                "DELETE FROM hands.skill_package WHERE tenant_id=$1", tenant_id
            )
            await connection.close()

    asyncio.run(scenario())
