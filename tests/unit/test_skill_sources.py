from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

import pytest

from auraclaw.action.skill_lifecycle import (
    InMemorySkillLifecycleStore,
    SkillPublishCommit,
)
from auraclaw.action.skill_sources import SkillSourceService
from auraclaw.contracts.errors import PolicyDeniedError, VersionConflictError
from auraclaw.contracts.skills import (
    ConfigureSkillSourceCommand,
    RetireSkillSourceCommand,
    SkillManifest,
    SkillPackageRecord,
    SkillPublicationRecord,
    SkillPublicationStatus,
    SkillSourceDesiredState,
    SkillSourceKind,
)
from auraclaw.contracts.tools import ArtifactRef

_DIGEST = f"sha256:{'a' * 64}"


def _configure(
    source_id: str,
    *,
    command_id: str,
    priority: int,
    expected_revision: int = 0,
    desired_state: SkillSourceDesiredState = SkillSourceDesiredState.ENABLED,
) -> ConfigureSkillSourceCommand:
    return ConfigureSkillSourceCommand(
        tenant_id="tenant-a",
        actor_id="admin-a",
        source_id=source_id,
        kind=SkillSourceKind.MCP,
        desired_state=desired_state,
        publisher_allowlist=("acme",),
        credential_ref=f"vault/tenant-a/{source_id}",
        config_metadata={"server_id": source_id.removeprefix("sks_")},
        priority=priority,
        command_id=command_id,
        expected_revision=expected_revision,
        correlation_id="corr-a",
        causation_id=command_id,
    )


def _retire(
    source_id: str, *, command_id: str, expected_revision: int
) -> RetireSkillSourceCommand:
    return RetireSkillSourceCommand(
        tenant_id="tenant-a",
        actor_id="admin-a",
        source_id=source_id,
        reason_code="source_decommissioned",
        command_id=command_id,
        expected_revision=expected_revision,
        correlation_id="corr-a",
        causation_id=command_id,
    )


def _publish_commit(
    source_id: str,
    *,
    command_id: str,
    expected_revision: int,
) -> SkillPublishCommit:
    now = datetime.now(UTC)
    package = SkillPackageRecord(
        tenant_id="tenant-a",
        manifest=SkillManifest(
            name="release.prepare",
            version="1.0.0",
            description="Prepare a release",
            publisher="acme",
            signature="hmac-sha256:test",
        ),
        package_digest=_DIGEST,
        artifact_ref=ArtifactRef(
            artifact_id="art-source-test",
            version=1,
            content_hash="a" * 64,
            media_type="application/vnd.auraclaw.skill-package+json",
            size=128,
        ),
        retention_until=now + timedelta(days=90),
        retention_updated_by="admin-a",
        retention_updated_at=now,
        created_at=now,
    )
    publication = SkillPublicationRecord(
        publication_id="skp_source_test",
        tenant_id="tenant-a",
        publisher="acme",
        name="release.prepare",
        version="1.0.0",
        package_digest=_DIGEST,
        source_id=source_id,
        status=SkillPublicationStatus.ACTIVE,
        revision=max(1, expected_revision),
        created_by="admin-a",
        updated_by="admin-a",
        created_at=now,
        updated_at=now,
    )
    return SkillPublishCommit(
        command_id=command_id,
        request_digest=f"sha256:{command_id.encode().hex().ljust(64, '0')[:64]}",
        actor_id="admin-a",
        source_id=source_id,
        correlation_id="corr-a",
        causation_id=command_id,
        expected_publication_revision=expected_revision,
        package=package,
        publication=publication,
        installation=None,
        occurred_at=now,
    )


def test_source_configuration_is_idempotent_and_revision_guarded() -> None:
    async def scenario() -> None:
        store = InMemorySkillLifecycleStore()
        service = SkillSourceService(store)
        command = _configure("sks_primary", command_id="source-create", priority=10)

        created = await service.configure(command)
        replayed = await service.configure(command)
        assert replayed == created
        assert await service.list_sources("tenant-a") == (created,)

        with pytest.raises(VersionConflictError, match="command id was reused"):
            await service.configure(command.model_copy(update={"priority": 20}))
        with pytest.raises(VersionConflictError, match="revision conflict"):
            await service.configure(
                _configure(
                    "sks_primary",
                    command_id="source-stale-update",
                    priority=20,
                    expected_revision=0,
                )
            )

    asyncio.run(scenario())


def test_multi_source_priority_and_retirement_preserve_available_publication() -> None:
    async def scenario() -> None:
        store = InMemorySkillLifecycleStore()
        service = SkillSourceService(store)
        await service.configure(
            _configure("sks_low", command_id="source-low", priority=0)
        )
        await service.configure(
            _configure("sks_high", command_id="source-high", priority=10)
        )

        first = await store.commit_publish(
            _publish_commit("sks_low", command_id="publish-low", expected_revision=0)
        )
        assert first.publication.source_id == "sks_low"
        second = await store.commit_publish(
            _publish_commit("sks_high", command_id="publish-high", expected_revision=1)
        )
        assert second.publication.source_id == "sks_high"
        assert second.publication.revision == 2

        await service.configure(
            _configure(
                "sks_low",
                command_id="source-low-priority",
                priority=20,
                expected_revision=1,
            )
        )
        selected = await store.get_publication(
            "tenant-a", "acme", "release.prepare", "1.0.0"
        )
        assert selected is not None
        assert selected.source_id == "sks_low"
        assert selected.status is SkillPublicationStatus.ACTIVE

        await service.retire(
            _retire("sks_low", command_id="source-low-retire", expected_revision=2)
        )
        fallback = await store.get_publication(
            "tenant-a", "acme", "release.prepare", "1.0.0"
        )
        assert fallback is not None
        assert fallback.source_id == "sks_high"
        assert fallback.status is SkillPublicationStatus.ACTIVE

        await service.retire(
            _retire("sks_high", command_id="source-high-retire", expected_revision=1)
        )
        unavailable = await store.get_publication(
            "tenant-a", "acme", "release.prepare", "1.0.0"
        )
        assert unavailable is not None
        assert unavailable.status is SkillPublicationStatus.RETIRED
        assert unavailable.reason_code == "source_decommissioned"

    asyncio.run(scenario())


def test_source_sync_requires_an_enabled_mcp_synchronizer() -> None:
    class Synchronizer:
        async def reconcile_source(self, tenant_id: str, source_id: str) -> object:
            return {"tenant_id": tenant_id, "source_id": source_id}

    async def scenario() -> None:
        store = InMemorySkillLifecycleStore()
        service = SkillSourceService(store, synchronizer=Synchronizer())
        await service.configure(
            _configure("sks_sync", command_id="source-sync", priority=0)
        )
        assert await service.sync("tenant-a", "sks_sync") == {
            "tenant_id": "tenant-a",
            "source_id": "sks_sync",
        }

        await service.configure(
            _configure(
                "sks_sync",
                command_id="source-disable",
                priority=0,
                expected_revision=1,
                desired_state=SkillSourceDesiredState.DISABLED,
            )
        )
        with pytest.raises(PolicyDeniedError, match="not enabled"):
            await service.sync("tenant-a", "sks_sync")

    asyncio.run(scenario())
