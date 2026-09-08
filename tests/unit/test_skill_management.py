from __future__ import annotations

import asyncio
import hashlib
from datetime import UTC, datetime, timedelta

import pytest

from auraclaw.action.capability_catalog import (
    SkillBindingStatusExecutor,
    skill_binding_status_tool,
)
from auraclaw.action.mcp_primitives import HandsResourceRegistry
from auraclaw.action.skill_lifecycle import InMemorySkillLifecycleStore
from auraclaw.action.skill_management import SkillManagementService
from auraclaw.action.skill_packages import (
    HmacSkillSignatureVerifier,
    SkillPackage,
    SkillPackageRegistry,
)
from auraclaw.action.skill_publication import SkillPublicationService
from auraclaw.contracts.errors import (
    InvalidTransitionError,
    PolicyDeniedError,
    VersionConflictError,
)
from auraclaw.contracts.skills import (
    ChangeSkillInstallationCommand,
    PublishedSkill,
    PublishSkillCommand,
    PurgeSkillPackageCommand,
    RestoreSkillPublicationCommand,
    RevokeSkillPublicationCommand,
    SkillInstallationOperation,
    SkillInstallationRecord,
    SkillInstallationStatus,
    SkillManifest,
    SkillPackageRecord,
    SkillPublicationRecord,
    SkillPublicationStatus,
    SkillRevocationAction,
)
from auraclaw.contracts.tools import ArtifactRef, ToolInvocation

_DIGEST = f"sha256:{'a' * 64}"


class _Projector:
    def __init__(self) -> None:
        self.tenants: list[str] = []

    async def rebuild_tenant(self, tenant_id: str) -> None:
        self.tenants.append(tenant_id)


class _RetiredActivator:
    def __init__(self, lifecycle: InMemorySkillLifecycleStore) -> None:
        self.lifecycle = lifecycle
        self.calls: list[tuple[PublishSkillCommand, ArtifactRef, str]] = []
        self.failure: Exception | None = None

    async def publish_artifact(
        self,
        command: PublishSkillCommand,
        artifact_ref: ArtifactRef,
        expected_digest: str,
    ) -> PublishedSkill:
        self.calls.append((command, artifact_ref, expected_digest))
        if self.failure is not None:
            raise self.failure
        publication = await self.lifecycle.get_publication(
            command.tenant_id, "platform", "release.prepare", "1.0.0"
        )
        package = await self.lifecycle.get_package(
            command.tenant_id, "platform", "release.prepare", "1.0.0"
        )
        assert publication is not None
        assert package is not None
        await self.lifecycle.put_publication(
            publication.model_copy(
                update={
                    "status": SkillPublicationStatus.ACTIVE,
                    "revision": publication.revision + 1,
                    "updated_by": command.actor_id,
                    "updated_at": datetime.now(UTC),
                    "reason_code": None,
                }
            ),
            expected_revision=command.expected_revision,
        )
        return PublishedSkill(
            tenant_id=command.tenant_id,
            manifest=package.manifest,
            package_digest=package.package_digest,
            artifact_ref=package.artifact_ref,
        )


class _Artifacts:
    def __init__(self) -> None:
        self.deleted: list[str] = []
        self.written: dict[str, bytes] = {}

    async def put(self, **kwargs: object) -> ArtifactRef:
        content = kwargs["content"]
        assert isinstance(content, bytes)
        artifact_id = f"art_republished_{len(self.written) + 1}"
        self.written[artifact_id] = content
        return ArtifactRef(
            artifact_id=artifact_id,
            version=1,
            content_hash=hashlib.sha256(content).hexdigest(),
            media_type="application/vnd.auraclaw.skill-package+json",
            size=len(content),
        )

    async def delete(self, *, artifact_ref: ArtifactRef, **kwargs: object) -> None:
        del kwargs
        self.deleted.append(artifact_ref.artifact_id)


class _BindingReferences:
    def __init__(self, referenced: bool = False) -> None:
        self.referenced = referenced
        self.active = referenced
        self.active_queries: list[dict[str, object]] = []

    async def has_reference(self, **kwargs: object) -> bool:
        del kwargs
        return self.referenced

    async def has_active_skill_reference(self, **kwargs: object) -> bool:
        self.active_queries.append(kwargs)
        return self.active


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
    force: bool = False,
) -> ChangeSkillInstallationCommand:
    return ChangeSkillInstallationCommand(
        tenant_id="tenant-a",
        actor_id="admin-a",
        publisher="platform",
        name="release.prepare",
        operation=operation,
        force=force,
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
                force=True,
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


def test_uninstall_drains_active_bindings_before_finalizing() -> None:
    async def scenario() -> None:
        _unused, lifecycle, projector = await _service()
        references = _BindingReferences(referenced=True)
        service = SkillManagementService(
            lifecycle=lifecycle,
            projector=projector,
            binding_references=references,
        )
        command = _installation_command(
            SkillInstallationOperation.UNINSTALL,
            revision=1,
            reason="tenant_uninstalled",
        )

        draining = await service.change_installation(command)
        assert draining.status is SkillInstallationStatus.DRAINING
        assert draining.uninstall_action is SkillRevocationAction.CONTINUE
        assert draining.uninstall_policy_version == "skill-uninstall-v1"
        assert await service.change_installation(command) == draining
        assert await service.reconcile_draining() == 0

        disposition = await SkillBindingStatusExecutor(lifecycle).execute(
            ToolInvocation(
                tool_invocation_id="binding-status-draining",
                tenant_id="tenant-a",
                root_session_id="root-1",
                session_id="session-1",
                run_id="run-1",
                tool_name="auraclaw.skills.binding-status",
                tool_version="1",
                arguments={
                    "publisher": "platform",
                    "name": "release.prepare",
                    "version": "1.0.0",
                    "package_digest": _DIGEST,
                },
                expected_side_effect="read",
                idempotency_key="binding-status-draining",
                deadline=None,
                fencing_token=1,
                actor_id="runtime-1",
            ),
            skill_binding_status_tool(),
        )
        assert disposition["action"] == "continue"
        assert disposition["installation_status"] == "draining"

        references.active = False
        assert await service.reconcile_draining() == 1
        completed = await service.get_installation(
            "tenant-a", "platform", "release.prepare"
        )
        assert completed.status is SkillInstallationStatus.UNINSTALLED
        assert completed.revision == 3
        assert await service.reconcile_draining() == 0

    asyncio.run(scenario())


def test_force_uninstall_cancels_active_bindings_and_is_command_idempotent() -> None:
    async def scenario() -> None:
        service, lifecycle, _projector = await _service()
        command = _installation_command(
            SkillInstallationOperation.UNINSTALL,
            revision=1,
            reason="security_force_uninstall",
            force=True,
        )
        uninstalled = await service.change_installation(command)
        assert uninstalled.status is SkillInstallationStatus.UNINSTALLED
        assert uninstalled.uninstall_action is SkillRevocationAction.CANCEL
        assert await service.change_installation(command) == uninstalled

        with pytest.raises(VersionConflictError, match="command id was reused"):
            await service.change_installation(
                command.model_copy(update={"reason_code": "different_reason"})
            )

        disposition = await SkillBindingStatusExecutor(lifecycle).execute(
            ToolInvocation(
                tool_invocation_id="binding-status-force-uninstall",
                tenant_id="tenant-a",
                root_session_id="root-1",
                session_id="session-1",
                run_id="run-1",
                tool_name="auraclaw.skills.binding-status",
                tool_version="1",
                arguments={
                    "publisher": "platform",
                    "name": "release.prepare",
                    "version": "1.0.0",
                    "package_digest": _DIGEST,
                },
                expected_side_effect="read",
                idempotency_key="binding-status-force-uninstall",
                deadline=None,
                fencing_token=1,
                actor_id="runtime-1",
            ),
            skill_binding_status_tool(),
        )
        assert disposition["action"] == "cancel"
        assert disposition["reason_code"] == "security_force_uninstall"
        assert disposition["policy_version"] == "skill-uninstall-v1"

    asyncio.run(scenario())


def _restore_command(
    *, command_id: str = "restore-1", reason: str = "source_reviewed"
) -> RestoreSkillPublicationCommand:
    return RestoreSkillPublicationCommand(
        tenant_id="tenant-a",
        actor_id="reviewer-a",
        publisher="platform",
        name="release.prepare",
        version="1.0.0",
        reason_code=reason,
        command_id=command_id,
        expected_revision=2,
        correlation_id="corr-restore",
        causation_id=command_id,
    )


async def _retire(lifecycle: InMemorySkillLifecycleStore) -> None:
    current = await lifecycle.get_publication("tenant-a", "platform", "release.prepare", "1.0.0")
    assert current is not None
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


def test_restore_is_reviewed_idempotent_and_revalidates_retired_artifact() -> None:
    async def scenario() -> None:
        _unused, lifecycle, projector = await _service()
        await _retire(lifecycle)
        activator = _RetiredActivator(lifecycle)
        service = SkillManagementService(
            lifecycle=lifecycle,
            projector=projector,
            retired_activator=activator,
        )

        restored = await service.restore_publication(_restore_command())

        assert restored.status is SkillPublicationStatus.ACTIVE
        assert restored.revision == 4
        assert restored.reason_code is None
        assert len(activator.calls) == 1
        activation, artifact_ref, digest = activator.calls[0]
        assert activation.expected_revision == 3
        assert activation.causation_id == "restore-1"
        assert artifact_ref.artifact_id == "art_skill"
        assert digest == _DIGEST

        assert await service.restore_publication(_restore_command()) == restored
        assert len(activator.calls) == 1
        assert projector.tenants == ["tenant-a", "tenant-a"]

    asyncio.run(scenario())


def test_failed_restore_stays_non_discoverable_and_same_command_can_retry() -> None:
    async def scenario() -> None:
        _unused, lifecycle, projector = await _service()
        await _retire(lifecycle)
        activator = _RetiredActivator(lifecycle)
        activator.failure = PolicyDeniedError("publisher suspended")
        service = SkillManagementService(
            lifecycle=lifecycle,
            projector=projector,
            retired_activator=activator,
        )

        with pytest.raises(PolicyDeniedError, match="publisher suspended"):
            await service.restore_publication(_restore_command())
        restoring = await service.get_publication(
            "tenant-a", "platform", "release.prepare", "1.0.0"
        )
        assert restoring.status is SkillPublicationStatus.RESTORING
        assert restoring.revision == 3

        with pytest.raises(VersionConflictError, match="revision conflict"):
            await service.restore_publication(_restore_command(command_id="restore-2"))
        with pytest.raises(VersionConflictError, match="command id was reused"):
            await service.restore_publication(_restore_command(reason="different_review"))

        activator.failure = None
        restored = await service.restore_publication(_restore_command())
        assert restored.status is SkillPublicationStatus.ACTIVE
        assert restored.revision == 4

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
                revocation_action=SkillRevocationAction.PAUSE,
                policy_decision_id="decision-1",
                command_id="revoke-1",
                expected_revision=1,
                correlation_id="corr-revoke",
                causation_id="revoke-1",
            )
        )
        assert revoked.status is SkillPublicationStatus.REVOKED
        assert revoked.updated_by == "security-a"
        assert revoked.reason_code == "publisher_key_compromised"
        assert revoked.revocation_action is SkillRevocationAction.PAUSE
        assert revoked.revocation_policy_decision_id == "decision-1"
        disposition = await SkillBindingStatusExecutor(lifecycle).execute(
            ToolInvocation(
                tool_invocation_id="binding-status-1",
                tenant_id="tenant-a",
                root_session_id="root-1",
                session_id="session-1",
                run_id="run-1",
                tool_name="auraclaw.skills.binding-status",
                tool_version="1",
                arguments={
                    "publisher": "platform",
                    "name": "release.prepare",
                    "version": "1.0.0",
                    "package_digest": _DIGEST,
                },
                expected_side_effect="read",
                idempotency_key="binding-status-1",
                deadline=None,
                fencing_token=1,
                actor_id="runtime-1",
            ),
            skill_binding_status_tool(),
        )
        assert disposition["action"] == "pause"
        assert disposition["policy_decision_id"] == "decision-1"
        unavailable = await SkillBindingStatusExecutor(lifecycle).execute(
            ToolInvocation(
                tool_invocation_id="binding-status-cross-tenant",
                tenant_id="tenant-b",
                root_session_id="root-1",
                session_id="session-1",
                run_id="run-1",
                tool_name="auraclaw.skills.binding-status",
                tool_version="1",
                arguments={
                    "publisher": "platform",
                    "name": "release.prepare",
                    "version": "1.0.0",
                    "package_digest": _DIGEST,
                },
                expected_side_effect="read",
                idempotency_key="binding-status-cross-tenant",
                deadline=None,
                fencing_token=1,
                actor_id="runtime-1",
            ),
            skill_binding_status_tool(),
        )
        assert unavailable["action"] == "cancel"
        assert unavailable["reason_code"] == "binding_authority_unavailable"

        await service.change_installation(
            _installation_command(
                SkillInstallationOperation.UNINSTALL,
                revision=1,
                reason="security_force_uninstall",
                force=True,
            )
        )
        stronger_disposition = await SkillBindingStatusExecutor(lifecycle).execute(
            ToolInvocation(
                tool_invocation_id="binding-status-strongest-policy",
                tenant_id="tenant-a",
                root_session_id="root-1",
                session_id="session-1",
                run_id="run-1",
                tool_name="auraclaw.skills.binding-status",
                tool_version="1",
                arguments={
                    "publisher": "platform",
                    "name": "release.prepare",
                    "version": "1.0.0",
                    "package_digest": _DIGEST,
                },
                expected_side_effect="read",
                idempotency_key="binding-status-strongest-policy",
                deadline=None,
                fencing_token=1,
                actor_id="runtime-1",
            ),
            skill_binding_status_tool(),
        )
        assert stronger_disposition["action"] == "cancel"
        assert stronger_disposition["reason_code"] == "security_force_uninstall"
        package = await lifecycle.get_package("tenant-a", "platform", "release.prepare", "1.0.0")
        assert package is not None
        assert package.retention_status.value == "retained"
        assert projector.tenants == ["tenant-a", "tenant-a"]

    asyncio.run(scenario())


def test_security_revoke_can_override_ordinary_retirement() -> None:
    async def scenario() -> None:
        service, lifecycle, _projector = await _service()
        current = await service.get_publication("tenant-a", "platform", "release.prepare", "1.0.0")
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


def test_purge_ignores_retention_and_history_but_rejects_active_bindings() -> None:
    async def scenario() -> None:
        _unused, lifecycle, projector = await _service()
        artifacts = _Artifacts()
        references = _BindingReferences(referenced=True)
        service = SkillManagementService(
            lifecycle=lifecycle,
            projector=projector,
            artifacts=artifacts,
            binding_references=references,
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
                force=True,
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
        package = await service.get_package("tenant-a", "platform", "release.prepare", "1.0.0")
        assert package.retention_until > datetime.now(UTC)
        command = PurgeSkillPackageCommand(
            tenant_id="tenant-a",
            actor_id="admin-a",
            publisher="platform",
            name="release.prepare",
            version="1.0.0",
            reason_code="operator_purge",
            command_id="purge-1",
            expected_revision=package.retention_revision,
            correlation_id="corr-purge",
            causation_id="purge-1",
        )
        with pytest.raises(PolicyDeniedError, match="active Session binding"):
            await service.purge_package(command)
        assert artifacts.deleted == []
        assert references.active_queries[-1]["package_digest"] == package.package_digest

        references.active = False
        assert references.referenced
        purged = await service.purge_package(command)
        assert purged.retention_status.value == "purged"
        assert purged.retention_revision == 2
        assert purged.retention_updated_by == "admin-a"
        assert purged.purged_at is not None
        assert artifacts.deleted == ["art_skill"]
        assert await service.purge_package(command) == purged
        assert artifacts.deleted == ["art_skill"]

        verifier = HmacSkillSignatureVerifier(
            {"platform": b"republish-signing-key"}
        )
        unsigned = SkillManifest(
            name="release.prepare",
            version="1.0.0",
            description="Republished release preparation",
            publisher="platform",
            signature_payload_version="v2",
            signature="hmac-sha256:unsigned",
        )
        files = {"SKILL.md": b"# Republished Release\n"}
        manifest = unsigned.model_copy(
            update={"signature": verifier.sign(unsigned, files)}
        )
        republished = await SkillPublicationService(
            registry=SkillPackageRegistry(
                artifacts=artifacts,
                signature_verifier=verifier,
                resources=HandsResourceRegistry(),
            ),
            lifecycle=lifecycle,
            artifacts=artifacts,
        ).publish(
            PublishSkillCommand(
                tenant_id="tenant-a",
                actor_id="publisher-a",
                command_id="republish-1",
                correlation_id="corr-republish",
                causation_id="republish-1",
            ),
            SkillPackage(
                manifest=manifest,
                files={"manifest.json": manifest.model_dump_json().encode(), **files},
            ),
        )
        assert republished.package_digest != purged.package_digest
        current = await lifecycle.get_package(
            "tenant-a", "platform", "release.prepare", "1.0.0"
        )
        assert current is not None and current.retention_status.value == "retained"
        assert current.artifact_ref.artifact_id == "art_republished_1"
        current_publication = await lifecycle.get_publication(
            "tenant-a", "platform", "release.prepare", "1.0.0"
        )
        assert current_publication is not None
        assert current_publication.status is SkillPublicationStatus.ACTIVE
        assert current_publication.revision == 3
        current_installation = await lifecycle.get_installation(
            "tenant-a", "platform", "release.prepare"
        )
        assert current_installation is not None
        assert current_installation.status is SkillInstallationStatus.ACTIVE
        assert current_installation.revision == 4
        tombstones = await lifecycle.list_package_tombstones(
            "tenant-a", "platform", "release.prepare"
        )
        assert tombstones == (purged,)  # Transient cleanup input, erased by the upgrade worker.
        from tests.unit.test_skill_upgrade_cleanup import _Artifacts as CleanupArtifacts

        from auraclaw.action.skill_upgrade_cleanup import SkillUpgradeCleanupWorker

        cleanup_artifacts = CleanupArtifacts()
        worker = SkillUpgradeCleanupWorker(lifecycle=lifecycle, artifacts=cleanup_artifacts,
            references=references, projector=projector)
        assert republished.upgrade is not None
        assert await worker.run_once() == 1
        assert await lifecycle.list_package_tombstones(
            "tenant-a", "platform", "release.prepare"
        ) == ()
        assert cleanup_artifacts.calls == [purged.artifact_ref]
        assert (await lifecycle.get_package(
            "tenant-a", "platform", "release.prepare", "1.0.0"
        )).package_digest == republished.package_digest

    asyncio.run(scenario())


def test_purge_rejects_legal_hold() -> None:
    async def scenario() -> None:
        _unused, lifecycle, projector = await _service()
        artifacts = _Artifacts()
        service = SkillManagementService(
            lifecycle=lifecycle,
            projector=projector,
            artifacts=artifacts,
            binding_references=_BindingReferences(),
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
                force=True,
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
        package = await service.get_package("tenant-a", "platform", "release.prepare", "1.0.0")
        package = await lifecycle.update_package_retention(
            package.model_copy(
                update={
                    "legal_hold": True,
                    "retention_revision": 2,
                    "retention_updated_by": "legal",
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
            reason_code="operator_purge",
            command_id="purge-legal-hold",
            expected_revision=package.retention_revision,
            correlation_id="corr-legal-hold",
            causation_id="purge-legal-hold",
        )
        with pytest.raises(PolicyDeniedError, match="legal hold"):
            await service.purge_package(command)
        assert artifacts.deleted == []

    asyncio.run(scenario())
