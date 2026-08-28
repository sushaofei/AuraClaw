from __future__ import annotations

import asyncio
from datetime import UTC, datetime

import pytest

from auraclaw.action.skill_lifecycle import InMemorySkillLifecycleStore
from auraclaw.action.skill_management import SkillManagementService
from auraclaw.contracts.errors import InvalidTransitionError, VersionConflictError
from auraclaw.contracts.skills import (
    ChangeSkillInstallationCommand,
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
