import asyncio
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

import asyncpg
import pytest

from auraclaw.action.capability_catalog import CapabilityCatalog
from auraclaw.config import get_settings
from auraclaw.contracts.capabilities import (
    CapabilityDescriptor,
    CapabilityKind,
    CapabilityStatus,
    CapabilityTrustLevel,
    McpOAuthConfiguration,
    McpServerDefinition,
)
from auraclaw.contracts.errors import StaleCapabilitySnapshotError
from auraclaw.infrastructure.persistence.postgres_capability_catalog import (
    PostgresCapabilityCatalogStore,
)
from auraclaw.infrastructure.persistence.postgres_common import asyncpg_url

SETTINGS = get_settings()
DATABASE_URL = asyncpg_url(SETTINGS.resolved_database_url) if SETTINGS.postgres_enabled else None
ROOT = Path(__file__).resolve().parents[2]
OWNER_MIGRATION = (ROOT / "migrations/0009_s3_owner_boundaries.sql").read_text()
MIGRATION = (ROOT / "migrations/0015_m9_capability_catalog.sql").read_text()
REGISTRY_MIGRATION = (ROOT / "migrations/0020_mcp_server_registry.sql").read_text()
CONSISTENCY_MIGRATION = (
    ROOT / "migrations/0041_capability_catalog_consistency.sql"
).read_text()
HEALTH_MIGRATION = (ROOT / "migrations/0045_mcp_catalog_sync_health.sql").read_text()
FENCING_MIGRATION = (
    ROOT / "migrations/0051_mcp_catalog_reconcile_fencing.sql"
).read_text()
pytestmark = pytest.mark.skipif(DATABASE_URL is None, reason="PostgreSQL test URL not configured")


def test_postgres_capability_catalog_is_shared_and_tenant_scoped() -> None:
    async def scenario() -> None:
        assert DATABASE_URL is not None
        connection = await asyncpg.connect(DATABASE_URL)
        await connection.execute(OWNER_MIGRATION)
        await connection.execute(MIGRATION)
        await connection.execute(REGISTRY_MIGRATION)
        await connection.execute(CONSISTENCY_MIGRATION)
        await connection.execute(HEALTH_MIGRATION)
        await connection.execute(FENCING_MIGRATION)
        suffix = uuid4().hex
        tenant_id = f"tenant-capability-{suffix}"
        server_id = f"server-{suffix}"
        capability_id = f"capability-{suffix}"
        store_a = PostgresCapabilityCatalogStore(DATABASE_URL)
        store_b = PostgresCapabilityCatalogStore(DATABASE_URL)
        catalog_a = CapabilityCatalog(store_a)
        catalog_b = CapabilityCatalog(store_b)
        try:
            await catalog_a.register_server(
                McpServerDefinition(
                    server_id=server_id,
                    tenant_id=tenant_id,
                    title="Tenant MCP",
                    endpoint="https://tenant.example/mcp",
                    credential_ref=f"vault/{server_id}#client_secret",
                    oauth=McpOAuthConfiguration(
                        protected_resource_metadata_url=(
                            "https://tenant.example/.well-known/"
                            "oauth-protected-resource"
                        ),
                        authorization_server_metadata_url=(
                            "https://auth.tenant.example/.well-known/"
                            "oauth-authorization-server"
                        ),
                        issuer="https://auth.tenant.example",
                        token_endpoint="https://auth.tenant.example/oauth/token",
                        client_id="auraclaw",
                        resource="https://tenant.example/mcp",
                    ),
                    trust_level=CapabilityTrustLevel.TENANT_VERIFIED,
                    status=CapabilityStatus.ACTIVE,
                    enabled=True,
                    config_revision=1,
                )
            )
            await catalog_a.replace_server_capabilities(
                server_id,
                (
                    CapabilityDescriptor(
                        capability_id=capability_id,
                        kind=CapabilityKind.RESOURCE,
                        server_id=server_id,
                        canonical_name="tenant.docs.release",
                        version="1",
                        content_digest=f"sha256:{suffix}",
                        title="Release documentation",
                        description="Tenant release policy",
                        tags=("release", "docs"),
                        tenant_id=tenant_id,
                        trust_level=CapabilityTrustLevel.TENANT_VERIFIED,
                        status=CapabilityStatus.ACTIVE,
                        updated_at=datetime.now(UTC),
                    ),
                ),
            )
            assert [
                item.capability_id
                for item in await catalog_b.search(
                    tenant_id=tenant_id,
                    query="release docs",
                )
            ] == [capability_id]
            assert await store_b.get_active_generation(server_id) == 1
            loaded = await catalog_b.get(
                tenant_id=tenant_id, capability_id=capability_id
            )
            assert loaded is not None
            assert loaded.metadata["catalog_generation"] == 1
            assert await catalog_b.search(
                tenant_id=f"other-{suffix}",
                query="release",
            ) == ()
            loaded_server = await store_b.get_server(server_id)
            assert loaded_server is not None
            assert loaded_server.oauth is not None
            assert loaded_server.oauth.resource == "https://tenant.example/mcp"

            old_lease = await store_a.claim_catalog_reconcile(
                server_id=server_id,
                owner="reconciler-old",
                ttl=timedelta(milliseconds=20),
            )
            assert old_lease is not None
            await asyncio.sleep(0.03)
            new_lease = await store_b.claim_catalog_reconcile(
                server_id=server_id,
                owner="reconciler-new",
                ttl=timedelta(seconds=30),
            )
            assert new_lease is not None
            new_descriptor = loaded.model_copy(
                update={"content_digest": f"sha256:new-{suffix}"}
            )
            committed = await catalog_b.replace_server_capabilities(
                server_id,
                (new_descriptor,),
                lease=new_lease,
                snapshot_digest=f"sha256:snapshot-new-{suffix}",
                source_revision="2",
            )
            assert committed.committed and committed.generation == 2
            with pytest.raises(StaleCapabilitySnapshotError):
                await catalog_a.replace_server_capabilities(
                    server_id,
                    (loaded,),
                    lease=old_lease,
                    snapshot_digest=f"sha256:snapshot-old-{suffix}",
                    source_revision="1",
                )
            await store_b.release_catalog_reconcile(new_lease)
            replay_lease = await store_a.claim_catalog_reconcile(
                server_id=server_id,
                owner="reconciler-replay",
                ttl=timedelta(seconds=30),
            )
            assert replay_lease is not None
            replayed = await catalog_a.replace_server_capabilities(
                server_id,
                (new_descriptor,),
                lease=replay_lease,
                snapshot_digest=f"sha256:snapshot-new-{suffix}",
                source_revision="2",
            )
            assert not replayed.committed and replayed.generation == 2
            await store_a.release_catalog_reconcile(replay_lease)
            config_lease = await store_a.claim_catalog_reconcile(
                server_id=server_id,
                owner="reconciler-old-config",
                ttl=timedelta(seconds=30),
            )
            assert config_lease is not None
            await catalog_b.register_server(
                loaded_server.model_copy(update={"config_revision": 2})
            )
            with pytest.raises(StaleCapabilitySnapshotError):
                await catalog_a.replace_server_capabilities(
                    server_id,
                    (loaded,),
                    lease=config_lease,
                    snapshot_digest=f"sha256:old-config-{suffix}",
                    source_revision="1",
                )
            await store_a.release_catalog_reconcile(config_lease)
            assert await store_b.get_active_generation(server_id) == 2

            first_failure = await store_a.record_catalog_sync(
                server_id,
                succeeded=False,
                attempted_at=datetime.now(UTC),
                safe_error_code="snapshot_failed",
                quarantine_after_failures=3,
            )
            assert first_failure.consecutive_failures == 1
            assert not first_failure.quarantined
            concurrent = await asyncio.gather(
                store_b.record_catalog_sync(
                    server_id,
                    succeeded=False,
                    attempted_at=datetime.now(UTC),
                    safe_error_code="snapshot_failed",
                    quarantine_after_failures=3,
                ),
                store_a.record_catalog_sync(
                    server_id,
                    succeeded=False,
                    attempted_at=datetime.now(UTC),
                    safe_error_code="snapshot_failed",
                    quarantine_after_failures=3,
                ),
            )
            assert all(item.consecutive_failures >= 2 for item in concurrent)
            third_failure = await store_a.record_catalog_sync(
                server_id,
                succeeded=False,
                attempted_at=datetime.now(UTC) + timedelta(seconds=1),
                safe_error_code="snapshot_failed",
                quarantine_after_failures=3,
            )
            assert third_failure.consecutive_failures == 3
            persisted = await store_b.get_server(server_id)
            assert persisted is not None
            assert persisted.status is CapabilityStatus.QUARANTINED
            assert persisted.metadata["consecutive_sync_failures"] == 3
            assert not await catalog_b.search(tenant_id=tenant_id, query="release")

            recovered = await store_b.record_catalog_sync(
                server_id,
                succeeded=True,
                attempted_at=datetime.now(UTC) + timedelta(seconds=2),
                safe_error_code=None,
                quarantine_after_failures=3,
            )
            assert recovered.consecutive_failures == 0
            assert not recovered.quarantined
            assert [
                item.capability_id
                for item in await catalog_b.search(tenant_id=tenant_id, query="release")
            ] == [capability_id]
        finally:
            await store_a.close()
            await store_b.close()
            await connection.execute(
                "DELETE FROM hands.capability_catalog WHERE server_id=$1",
                server_id,
            )
            await connection.execute(
                "DELETE FROM hands.downstream_mcp_server WHERE server_id=$1",
                server_id,
            )
            await connection.close()

    asyncio.run(scenario())
