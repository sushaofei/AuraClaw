from __future__ import annotations

import asyncio
from pathlib import Path
from uuid import uuid4

import asyncpg
import pytest
from tests.unit.test_skill_publication import _KEY, _command, _package
from tests.unit.test_skill_upgrade_cleanup import _Artifacts, _Projector, _References

from auraclaw.action.skill_packages import HmacSkillSignatureVerifier, SkillPackageRegistry
from auraclaw.action.skill_publication import SkillPublicationService
from auraclaw.action.skill_upgrade_cleanup import SkillUpgradeCleanupWorker
from auraclaw.config import get_settings
from auraclaw.infrastructure.artifacts.store import ArtifactStore, InMemoryObjectStorage
from auraclaw.infrastructure.persistence.postgres_common import asyncpg_url
from auraclaw.infrastructure.persistence.postgres_skill_lifecycle import PostgresSkillLifecycleStore

SETTINGS = get_settings()
DATABASE_URL = asyncpg_url(SETTINGS.resolved_database_url) if SETTINGS.postgres_enabled else None
MIGRATION = (
    Path(__file__).resolve().parents[2] / "migrations/0063_skill_upgrade_cleanup.sql"
).read_text()
pytestmark = pytest.mark.skipif(DATABASE_URL is None, reason="Explicit PostgreSQL required")


def test_two_replicas_remove_replaced_package_without_recoverable_history() -> None:
    async def scenario() -> None:
        assert DATABASE_URL is not None
        connection = await asyncpg.connect(DATABASE_URL)
        await connection.execute(MIGRATION)
        tenant = "cleanup-" + uuid4().hex
        a, b = PostgresSkillLifecycleStore(DATABASE_URL), PostgresSkillLifecycleStore(DATABASE_URL)
        service = SkillPublicationService(
            lifecycle=a,
            registry=SkillPackageRegistry(
                artifacts=ArtifactStore(InMemoryObjectStorage(), signing_key=_KEY),
                signature_verifier=HmacSkillSignatureVerifier({"acme": _KEY}),
            ),
        )
        artifacts, refs = _Artifacts(), _References()
        refs.active = False
        worker_a = SkillUpgradeCleanupWorker(
            lifecycle=a, artifacts=artifacts, references=refs, projector=_Projector()
        )
        worker_b = SkillUpgradeCleanupWorker(
            lifecycle=b, artifacts=artifacts, references=refs, projector=_Projector()
        )
        try:
            old = await service.publish(
                _command().model_copy(update={"tenant_id": tenant}), _package()
            )
            new = await service.publish(
                _command(command_id="upgrade").model_copy(update={"tenant_id": tenant}),
                _package(version="2.0.0"),
            )
            state = new.upgrade
            assert state is not None
            artifacts.failed = True
            assert not await worker_a._process(state)
            assert (await b.get_upgrade(tenant, "acme", "release.prepare")).phase == "blocked"
            artifacts.failed = False
            outcomes = await asyncio.gather(worker_a._process(state), worker_b._process(state))
            assert sum(outcomes) == 1
            assert await b.get_package(tenant, "acme", "release.prepare", "1.0.0") is None
            assert await b.get_publication(tenant, "acme", "release.prepare", "1.0.0") is None
            assert await b.get_package(tenant, "acme", "release.prepare", "2.0.0") is not None
            assert await b.list_package_tombstones(tenant, "acme", "release.prepare") == ()
            command = await connection.fetchrow(
                "SELECT version,package_digest FROM hands.skill_command WHERE tenant_id=$1 "
                "AND command_id='publish-1'",
                tenant,
            )
            assert (
                command is not None
                and command["version"] is None
                and command["package_digest"] is None
            )
            assert not await connection.fetchval(
                "SELECT EXISTS(SELECT 1 FROM hands.skill_outbox WHERE tenant_id=$1 "
                "AND payload->>'package_digest'=$2)",
                tenant,
                old.package_digest,
            )
            assert not await connection.fetchval(
                "SELECT EXISTS(SELECT 1 FROM hands.skill_admission_audit WHERE tenant_id=$1 "
                "AND package_digest=$2)",
                tenant,
                old.package_digest,
            )
            assert (await a.get_upgrade(tenant, "acme", "release.prepare")).phase == "completed"
        finally:
            await a.close()
            await b.close()
            for table in (
                "skill_outbox",
                "skill_command",
                "skill_publication_restore_command",
                "skill_installation_command",
                "skill_installation",
                "skill_publication",
                "skill_package_tombstone",
                "skill_package",
                "skill_admission_audit",
                "skill_upgrade_current",
            ):
                await connection.execute(f"DELETE FROM hands.{table} WHERE tenant_id=$1", tenant)
            await connection.close()

    asyncio.run(scenario())
