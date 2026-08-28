from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

import pytest

from auraclaw.action.skill_lifecycle import InMemorySkillLifecycleStore
from auraclaw.action.skill_management import SkillManagementService
from auraclaw.contracts.errors import (
    InvalidTransitionError,
    PolicyDeniedError,
    VersionConflictError,
)
from auraclaw.contracts.skills import (
    ChangeSkillInstallationCommand,
    PurgeSkillPackageCommand,
    RevokeSkillPublicationCommand,
    SkillInstallationOperation,
    SkillInstallationRecord,
    SkillInstallationStatus,
    SkillManifest,
    SkillPackageRecord,
    SkillPublicationRecord,
    SkillPublicationStatus,
)
from auraclaw.contracts.tools import ArtifactRef

_DIGEST = f"sha256:{'a' * 64}"


class _Projector:
    def __init__(self) -> None:
        self.tenants: list[str] = []

    async def rebuild_tenant(self, tenant_id: str) -> None:
        self.tenants.append(tenant_id)


class _Artifacts:
    def __init__(self) -> None:
        self.deleted: list[str] = []

    async def delete(self, *, artifact_ref: ArtifactRef, **kwargs: object) -> None:
        del kwargs
        self.deleted.append(artifact_ref.artifact_id)


class _BindingReferences:
    def __init__(self, referenced: bool = False) -> None:
        self.referenced = referenced

    async def has_reference(self, **kwargs: object) -> bool:
        del kwargs
        return self.referenced


async def _service() -> tuple[
    SkillManagementService,
    InMemorySkillLifecycleStore,
    _Projector,
]:
    now = datetime.now(UTC)
    lifecycle = InMemorySkillLifecycleStore()
    manifest = SkillManifest(
        name="release.prepare",
        version="1.0.0",
        description="Prepare a release",
        publisher="platform",
        signature="hmac-sha256:abc",
    )
    await lifecycle.put_package(
        SkillPackageRecord(
            tenant_id="tenant-a",
            manifest=manifest,
            package_digest=_DIGEST,
            artifact_ref=ArtifactRef(
                artifact_id="art_skill",
                version=1,
                content_hash="a" * 64,
                media_type="application/vnd.auraclaw.skill-package+json",
                size=10,
            ),
            retention_until=now + timedelta(days=90),
            retention_updated_by="publisher",
            retention_updated_at=now,
            created_at=now,
        )
    )
    await lifecycle.put_publication(
        SkillPublicationRecord(
            publication_id="skp_release",
            tenant_id="tenant-a",
            publisher="platform",
            name="release.prepare",
            version="1.0.0",
            package_digest=_DIGEST,
            status=SkillPublicationStatus.ACTIVE,
            revision=1,
            created_by="publisher",
            updated_by="publisher",
            created_at=now,
            updated_at=now,
        ),
        expected_revision=0,
    )
    await lifecycle.put_installation(
        SkillInstallationRecord(
            installation_id="ski_release",
            tenant_id="tenant-a",
            publisher="platform",
            name="release.prepare",
            pinned_package_digest=_DIGEST,
            auto_upgrade=False,
            status=SkillInstallationStatus.ACTIVE,
            revision=1,
            created_by="publisher",
            updated_by="publisher",
            created_at=now,
            updated_at=now,
        ),
        expected_revision=0,
    )
    projector = _Projector()
    return (
        SkillManagementService(lifecycle=lifecycle, projector=projector),
        lifecycle,
        projector,
    )


def _installation_command(
    operation: SkillInstallationOperation,
    *,
    revision: int,
    reason: str | None = None,
) -> ChangeSkillInstallationCommand:
    return ChangeSkillInstallationCommand(
        tenant_id="tenant-a",
        actor_id="admin-a",
        publisher="platform",
        name="release.prepare",
        operation=operation,
        reason_code=reason,
        command_id=f"command-{operation.value}",
        expected_revision=revision,
        correlation_id="corr-a",
        causation_id=f"command-{operation.value}",
    )


def test_disable_uninstall_and_reinstall_change_installation_only() -> None:
    async def scenario() -> None:
        service, lifecycle, projector = await _service()

        disabled = await service.change_installation(
            _installation_command(
                SkillInstallationOperation.DISABLE,
                revision=1,
                reason="tenant_disabled",
            )
        )
        assert disabled.status is SkillInstallationStatus.DISABLED
        assert disabled.updated_by == "admin-a"
        assert disabled.revision == 2
        publication = await lifecycle.get_publication(
            "tenant-a", "platform", "release.prepare", "1.0.0"
        )
        assert publication is not None
        assert publication.status is SkillPublicationStatus.ACTIVE

        uninstalled = await service.change_installation(
            _installation_command(
                SkillInstallationOperation.UNINSTALL,
                revision=2,
                reason="tenant_uninstalled",
            )
        )
        assert uninstalled.status is SkillInstallationStatus.UNINSTALLED
        with pytest.raises(InvalidTransitionError):
            await service.change_installation(
                _installation_command(
                    SkillInstallationOperation.ENABLE,
                    revision=3,
                )
            )
        installed = await service.change_installation(
            _installation_command(
                SkillInstallationOperation.INSTALL,
                revision=3,
            )
        )
        assert installed.status is SkillInstallationStatus.ACTIVE
        assert installed.reason_code is None
        assert projector.tenants == ["tenant-a", "tenant-a", "tenant-a"]

    asyncio.run(scenario())


def test_management_enforces_revision_and_revoke_is_separate() -> None:
    async def scenario() -> None:
        service, lifecycle, projector = await _service()
        with pytest.raises(VersionConflictError, match="revision conflict"):
            await service.change_installation(
                _installation_command(
                    SkillInstallationOperation.DISABLE,
                    revision=9,
                    reason="tenant_disabled",
                )
            )

        revoked = await service.revoke_publication(
            RevokeSkillPublicationCommand(
                tenant_id="tenant-a",
                actor_id="security-a",
                publisher="platform",
                name="release.prepare",
                version="1.0.0",
                reason_code="publisher_key_compromised",
                command_id="revoke-1",
                expected_revision=1,
                correlation_id="corr-revoke",
                causation_id="revoke-1",
            )
        )
        assert revoked.status is SkillPublicationStatus.REVOKED
        assert revoked.updated_by == "security-a"
        assert revoked.reason_code == "publisher_key_compromised"
        package = await lifecycle.get_package(
            "tenant-a", "platform", "release.prepare", "1.0.0"
        )
        assert package is not None
        assert package.retention_status.value == "retained"
        assert projector.tenants == ["tenant-a"]

    asyncio.run(scenario())


def test_security_revoke_can_override_ordinary_retirement() -> None:
    async def scenario() -> None:
        service, lifecycle, _projector = await _service()
        current = await service.get_publication(
            "tenant-a", "platform", "release.prepare", "1.0.0"
        )
        await lifecycle.put_publication(
            current.model_copy(
                update={
                    "status": SkillPublicationStatus.RETIRED,
                    "revision": 2,
                    "updated_by": "source-reconciler",
                    "updated_at": datetime.now(UTC),
                    "reason_code": "source_missing_confirmed",
                }
            ),
            expected_revision=1,
        )

        revoked = await service.revoke_publication(
            RevokeSkillPublicationCommand(
                tenant_id="tenant-a",
                actor_id="security-a",
                publisher="platform",
                name="release.prepare",
                version="1.0.0",
                reason_code="publisher_key_compromised",
                command_id="revoke-retired",
                expected_revision=2,
                correlation_id="corr-revoke-retired",
                causation_id="revoke-retired",
            )
        )
        assert revoked.status is SkillPublicationStatus.REVOKED
        assert revoked.revision == 3
        assert revoked.reason_code == "publisher_key_compromised"

    asyncio.run(scenario())


def test_purge_requires_expired_uninstalled_revoked_unreferenced_package() -> None:
    async def scenario() -> None:
        _unused, lifecycle, projector = await _service()
        artifacts = _Artifacts()
        references = _BindingReferences(referenced=True)
        service = SkillManagementService(
            lifecycle=lifecycle,
            projector=projector,
            artifacts=artifacts,
            binding_references=references,
            purge_quiescence=timedelta(0),
        )
        await service.change_installation(
            _installation_command(
                SkillInstallationOperation.DISABLE,
                revision=1,
                reason="retiring",
            )
        )
        await service.change_installation(
            _installation_command(
                SkillInstallationOperation.UNINSTALL,
                revision=2,
                reason="retiring",
            )
        )
        await service.revoke_publication(
            RevokeSkillPublicationCommand(
                tenant_id="tenant-a",
                actor_id="security-a",
                publisher="platform",
                name="release.prepare",
                version="1.0.0",
                reason_code="retiring",
                command_id="revoke-purge",
                expected_revision=1,
                correlation_id="corr-purge",
                causation_id="revoke-purge",
            )
        )
        package = await service.get_package(
            "tenant-a", "platform", "release.prepare", "1.0.0"
        )
        package = await lifecycle.update_package_retention(
            package.model_copy(
                update={
                    "retention_until": datetime.now(UTC) - timedelta(seconds=1),
                    "retention_revision": 2,
                    "retention_updated_by": "retention-worker",
                    "retention_updated_at": datetime.now(UTC),
                }
            ),
            expected_revision=1,
        )
        command = PurgeSkillPackageCommand(
            tenant_id="tenant-a",
            actor_id="admin-a",
            publisher="platform",
            name="release.prepare",
            version="1.0.0",
            reason_code="retention_elapsed",
            command_id="purge-1",
            expected_revision=package.retention_revision,
            correlation_id="corr-purge",
            causation_id="purge-1",
        )
        with pytest.raises(PolicyDeniedError, match="Session binding"):
            await service.purge_package(command)
        assert artifacts.deleted == []

        references.referenced = False
        purged = await service.purge_package(command)
        assert purged.retention_status.value == "purged"
        assert purged.retention_revision == 3
        assert purged.retention_updated_by == "admin-a"
        assert purged.purged_at is not None
        assert artifacts.deleted == ["art_skill"]
        assert await service.purge_package(command) == purged
        assert artifacts.deleted == ["art_skill"]

    asyncio.run(scenario())


def test_purge_rejects_retention_period_and_legal_hold() -> None:
    async def scenario() -> None:
        _unused, lifecycle, projector = await _service()
        artifacts = _Artifacts()
        service = SkillManagementService(
            lifecycle=lifecycle,
            projector=projector,
            artifacts=artifacts,
            binding_references=_BindingReferences(),
            purge_quiescence=timedelta(0),
        )
        await service.change_installation(
            _installation_command(
                SkillInstallationOperation.DISABLE,
                revision=1,
                reason="retiring",
            )
        )
        await service.change_installation(
            _installation_command(
                SkillInstallationOperation.UNINSTALL,
                revision=2,
                reason="retiring",
            )
        )
        await service.revoke_publication(
            RevokeSkillPublicationCommand(
                tenant_id="tenant-a",
                actor_id="security-a",
                publisher="platform",
                name="release.prepare",
                version="1.0.0",
                reason_code="retiring",
                command_id="revoke-retention",
                expected_revision=1,
                correlation_id="corr-retention",
                causation_id="revoke-retention",
            )
        )
        command = PurgeSkillPackageCommand(
            tenant_id="tenant-a",
            actor_id="admin-a",
            publisher="platform",
            name="release.prepare",
            version="1.0.0",
            reason_code="retention_elapsed",
            command_id="purge-retention",
            expected_revision=1,
            correlation_id="corr-retention",
            causation_id="purge-retention",
        )
        with pytest.raises(PolicyDeniedError, match="retention period"):
            await service.purge_package(command)
        package = await service.get_package(
            "tenant-a", "platform", "release.prepare", "1.0.0"
        )
        package = await lifecycle.update_package_retention(
            package.model_copy(
                update={
                    "retention_until": datetime.now(UTC) - timedelta(seconds=1),
                    "legal_hold": True,
                    "retention_revision": 2,
                    "retention_updated_by": "legal",
                    "retention_updated_at": datetime.now(UTC),
                }
            ),
            expected_revision=1,
        )
        with pytest.raises(PolicyDeniedError, match="legal hold"):
            await service.purge_package(
                command.model_copy(
                    update={"expected_revision": package.retention_revision}
                )
            )
        assert artifacts.deleted == []

    asyncio.run(scenario())
