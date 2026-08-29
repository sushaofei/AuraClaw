from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime, timedelta
from typing import Protocol

from auraclaw.action.ports import ArtifactDeleter, SkillBindingReferenceReader
from auraclaw.action.skill_lifecycle import (
    SkillInstallationCommit,
    SkillLifecycleStore,
    SkillRestoreCommit,
)
from auraclaw.action.skill_packages import SkillPackageRegistry
from auraclaw.contracts.errors import (
    InvalidTransitionError,
    NotFoundError,
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
    SkillPackageRecord,
    SkillPackageRetentionStatus,
    SkillPublicationRecord,
    SkillPublicationStatus,
    SkillRevocationAction,
)
from auraclaw.contracts.tools import ArtifactRef


class SkillStateProjector(Protocol):
    async def rebuild_tenant(self, tenant_id: str) -> object: ...


class SkillRetiredActivator(Protocol):
    async def publish_artifact(
        self,
        command: PublishSkillCommand,
        artifact_ref: ArtifactRef,
        expected_digest: str,
    ) -> PublishedSkill: ...


class SkillManagementService:
    """Govern installation visibility and security revocation independently."""

    def __init__(
        self,
        *,
        lifecycle: SkillLifecycleStore,
        projector: SkillStateProjector,
        artifacts: ArtifactDeleter | None = None,
        binding_references: SkillBindingReferenceReader | None = None,
        retired_activator: SkillRetiredActivator | None = None,
        purge_quiescence: timedelta = timedelta(minutes=5),
    ) -> None:
        self._lifecycle = lifecycle
        self._projector = projector
        self._artifacts = artifacts
        self._binding_references = binding_references
        self._retired_activator = retired_activator
        self._purge_quiescence = purge_quiescence

    async def get_package(
        self,
        tenant_id: str,
        publisher: str,
        name: str,
        version: str,
    ) -> SkillPackageRecord:
        record = await self._lifecycle.get_package(tenant_id, publisher, name, version)
        if record is None:
            raise NotFoundError("Skill package not found")
        return record

    async def get_installation(
        self,
        tenant_id: str,
        publisher: str,
        name: str,
    ) -> SkillInstallationRecord:
        record = await self._lifecycle.get_installation(tenant_id, publisher, name)
        if record is None:
            raise NotFoundError("Skill installation not found")
        return record

    async def get_publication(
        self,
        tenant_id: str,
        publisher: str,
        name: str,
        version: str,
    ) -> SkillPublicationRecord:
        record = await self._lifecycle.get_publication(tenant_id, publisher, name, version)
        if record is None:
            raise NotFoundError("Skill publication not found")
        return record

    async def change_installation(
        self,
        command: ChangeSkillInstallationCommand,
    ) -> SkillInstallationRecord:
        current = await self._lifecycle.get_installation(
            command.tenant_id,
            command.publisher,
            command.name,
        )
        if current is None:
            raise NotFoundError("Skill installation not found")
        target = _installation_target(command)
        if current.status is not target:
            _validate_installation_transition(current.status, command)
        now = datetime.now(UTC)
        proposed = current
        if current.status is not target:
            uninstall_action = current.uninstall_action
            uninstall_policy_version = current.uninstall_policy_version
            uninstall_policy_decision_id = current.uninstall_policy_decision_id
            if command.operation is SkillInstallationOperation.UNINSTALL:
                uninstall_action = (
                    SkillRevocationAction.CANCEL
                    if command.force
                    else SkillRevocationAction.CONTINUE
                )
                uninstall_policy_version = "skill-uninstall-v1"
                uninstall_policy_decision_id = command.command_id
            elif target is SkillInstallationStatus.ACTIVE:
                uninstall_action = None
                uninstall_policy_version = None
                uninstall_policy_decision_id = None
            proposed = current.model_copy(
                update={
                    "status": target,
                    "revision": current.revision + 1,
                    "updated_by": command.actor_id,
                    "updated_at": now,
                    "reason_code": command.reason_code,
                    "uninstall_action": uninstall_action,
                    "uninstall_policy_version": uninstall_policy_version,
                    "uninstall_policy_decision_id": uninstall_policy_decision_id,
                }
            )
        updated = await self._lifecycle.commit_installation_change(
            SkillInstallationCommit(
                command_id=command.command_id,
                request_digest=_installation_request_digest(command),
                operation=command.operation.value,
                force_uninstall=command.force,
                actor_id=command.actor_id,
                correlation_id=command.correlation_id,
                causation_id=command.causation_id,
                reason_code=command.reason_code,
                expected_revision=command.expected_revision,
                installation=proposed,
                occurred_at=now,
            )
        )
        await self._projector.rebuild_tenant(command.tenant_id)
        return updated

    async def reconcile_draining(self) -> int:
        if self._binding_references is None:
            return 0
        completed = 0
        for tenant_id in await self._lifecycle.list_tenants():
            for installation in await self._lifecycle.list_installations(tenant_id):
                if installation.status is not SkillInstallationStatus.DRAINING:
                    continue
                correlation_id = f"skill-uninstall-drain:{installation.installation_id}"
                if await self._binding_references.has_active_skill_reference(
                    tenant_id=tenant_id,
                    publisher=installation.publisher,
                    name=installation.name,
                    correlation_id=correlation_id,
                ):
                    continue
                now = datetime.now(UTC)
                try:
                    await self._lifecycle.put_installation(
                        installation.model_copy(
                            update={
                                "status": SkillInstallationStatus.UNINSTALLED,
                                "revision": installation.revision + 1,
                                "updated_by": "action-hands-skill-drainer",
                                "updated_at": now,
                            }
                        ),
                        expected_revision=installation.revision,
                    )
                except VersionConflictError:
                    continue
                await self._projector.rebuild_tenant(tenant_id)
                completed += 1
        return completed

    async def revoke_publication(
        self,
        command: RevokeSkillPublicationCommand,
    ) -> SkillPublicationRecord:
        current = await self._lifecycle.get_publication(
            command.tenant_id,
            command.publisher,
            command.name,
            command.version,
        )
        if current is None:
            raise NotFoundError("Skill publication not found")
        if current.status is SkillPublicationStatus.REVOKED:
            await self._projector.rebuild_tenant(command.tenant_id)
            return current
        if current.status not in {
            SkillPublicationStatus.STAGED,
            SkillPublicationStatus.VALIDATING,
            SkillPublicationStatus.ACTIVE,
            SkillPublicationStatus.QUARANTINED,
            SkillPublicationStatus.RESTORING,
            SkillPublicationStatus.RETIRED,
        }:
            raise InvalidTransitionError("Skill publication cannot be revoked")
        updated = await self._lifecycle.put_publication(
            current.model_copy(
                update={
                    "status": SkillPublicationStatus.REVOKED,
                    "revision": current.revision + 1,
                    "updated_by": command.actor_id,
                    "updated_at": datetime.now(UTC),
                    "reason_code": command.reason_code,
                    "revocation_action": command.revocation_action,
                    "revocation_policy_version": command.policy_version,
                    "revocation_policy_decision_id": command.policy_decision_id,
                }
            ),
            expected_revision=command.expected_revision,
        )
        await self._projector.rebuild_tenant(command.tenant_id)
        return updated

    async def restore_publication(
        self,
        command: RestoreSkillPublicationCommand,
    ) -> SkillPublicationRecord:
        if self._retired_activator is None:
            raise PolicyDeniedError("Skill publication restore is not configured")
        current = await self.get_publication(
            command.tenant_id,
            command.publisher,
            command.name,
            command.version,
        )
        now = datetime.now(UTC)
        proposed = current
        if current.status is SkillPublicationStatus.RETIRED:
            proposed = current.model_copy(
                update={
                    "status": SkillPublicationStatus.RESTORING,
                    "revision": current.revision + 1,
                    "updated_by": command.actor_id,
                    "updated_at": now,
                    "reason_code": command.reason_code,
                }
            )
        request_digest = _restore_request_digest(command)
        restoring = await self._lifecycle.commit_restore(
            SkillRestoreCommit(
                command_id=command.command_id,
                request_digest=request_digest,
                actor_id=command.actor_id,
                reason_code=command.reason_code,
                correlation_id=command.correlation_id,
                causation_id=command.causation_id,
                expected_revision=command.expected_revision,
                publication=proposed,
                occurred_at=now,
            )
        )
        if restoring.status is SkillPublicationStatus.ACTIVE:
            await self._projector.rebuild_tenant(command.tenant_id)
            return restoring
        if restoring.status is not SkillPublicationStatus.RESTORING:
            raise VersionConflictError("Skill publication restore is incomplete")
        if restoring.source_id is None:
            raise PolicyDeniedError("Skill publication has no governed Source")
        package = await self.get_package(
            command.tenant_id,
            command.publisher,
            command.name,
            command.version,
        )
        activation_command_id = _restore_activation_command_id(command.command_id)
        await self._retired_activator.publish_artifact(
            PublishSkillCommand(
                tenant_id=command.tenant_id,
                actor_id=command.actor_id,
                source_id=restoring.source_id,
                activate=True,
                command_id=activation_command_id,
                expected_revision=restoring.revision,
                correlation_id=command.correlation_id,
                causation_id=command.command_id,
            ),
            package.artifact_ref,
            package.package_digest,
        )
        restored = await self.get_publication(
            command.tenant_id,
            command.publisher,
            command.name,
            command.version,
        )
        await self._projector.rebuild_tenant(command.tenant_id)
        return restored

    async def purge_package(
        self,
        command: PurgeSkillPackageCommand,
    ) -> SkillPackageRecord:
        if self._artifacts is None or self._binding_references is None:
            raise PolicyDeniedError("Skill package purge is not configured")
        package = await self.get_package(
            command.tenant_id,
            command.publisher,
            command.name,
            command.version,
        )
        if package.retention_status is SkillPackageRetentionStatus.PURGED:
            await self._projector.rebuild_tenant(command.tenant_id)
            return package
        if package.retention_revision != command.expected_revision:
            raise VersionConflictError("Skill package retention revision conflict")
        publication = await self.get_publication(
            command.tenant_id,
            command.publisher,
            command.name,
            command.version,
        )
        if publication.status is not SkillPublicationStatus.REVOKED:
            raise PolicyDeniedError("Skill publication must be revoked before purge")
        now = datetime.now(UTC)
        if publication.updated_at + self._purge_quiescence > now:
            raise PolicyDeniedError("Skill publication revocation is not yet quiescent")
        installation = await self._lifecycle.get_installation(
            command.tenant_id, command.publisher, command.name
        )
        if installation is None or installation.status is not SkillInstallationStatus.UNINSTALLED:
            raise PolicyDeniedError("Skill must be uninstalled before package purge")
        if package.legal_hold:
            raise PolicyDeniedError("Skill package is under legal hold")
        if package.retention_until > now:
            raise PolicyDeniedError("Skill package retention period has not elapsed")
        if await self._binding_references.has_reference(
            tenant_id=command.tenant_id,
            package_digest=package.package_digest,
            correlation_id=command.correlation_id,
        ):
            raise PolicyDeniedError("Skill package is referenced by a Session binding")
        await self._artifacts.delete(
            tenant_id=command.tenant_id,
            artifact_ref=package.artifact_ref,
            actor_id=command.actor_id,
            reason_code=command.reason_code,
            correlation_id=command.correlation_id,
        )
        updated = await self._lifecycle.update_package_retention(
            package.model_copy(
                update={
                    "retention_status": SkillPackageRetentionStatus.PURGED,
                    "retention_revision": package.retention_revision + 1,
                    "retention_updated_by": command.actor_id,
                    "retention_updated_at": now,
                    "purged_at": now,
                }
            ),
            expected_revision=command.expected_revision,
        )
        await self._projector.rebuild_tenant(command.tenant_id)
        return updated


class InProcessSkillStateProjector:
    """Memory-profile compatibility projector; production uses full rebuild."""

    def __init__(
        self,
        *,
        lifecycle: SkillLifecycleStore,
        registry: SkillPackageRegistry,
    ) -> None:
        self._lifecycle = lifecycle
        self._registry = registry

    async def rebuild_tenant(self, tenant_id: str) -> None:
        installations = {
            (item.publisher, item.name): item
            for item in await self._lifecycle.list_installations(tenant_id)
        }
        for publication in await self._lifecycle.list_publications(tenant_id):
            if publication.status is SkillPublicationStatus.REVOKED:
                cached = self._registry.get_publication(
                    tenant_id,
                    publication.publisher,
                    publication.name,
                    publication.version,
                )
                if cached.status is not SkillPublicationStatus.REVOKED:
                    self._registry.revoke(
                        tenant_id,
                        publication.publisher,
                        publication.name,
                        publication.version,
                        action=(publication.revocation_action or SkillRevocationAction.CANCEL),
                    )
            installation = installations.get((publication.publisher, publication.name))
            self._registry.set_skill_discoverable(
                tenant_id,
                publication.publisher,
                publication.name,
                discoverable=(
                    publication.status is SkillPublicationStatus.ACTIVE
                    and installation is not None
                    and installation.status is SkillInstallationStatus.ACTIVE
                ),
            )


def _restore_request_digest(command: RestoreSkillPublicationCommand) -> str:
    value = "\0".join(
        (
            command.tenant_id,
            command.publisher,
            command.name,
            command.version,
            command.actor_id,
            command.reason_code,
            str(command.expected_revision),
        )
    )
    return f"sha256:{hashlib.sha256(value.encode()).hexdigest()}"


def _restore_activation_command_id(command_id: str) -> str:
    digest = hashlib.sha256(command_id.encode()).hexdigest()
    return f"skill-restore-activate:{digest}"


def _installation_request_digest(command: ChangeSkillInstallationCommand) -> str:
    encoded = json.dumps(
        command.model_dump(mode="json"),
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    return f"sha256:{hashlib.sha256(encoded).hexdigest()}"


def _installation_target(
    command: ChangeSkillInstallationCommand,
) -> SkillInstallationStatus:
    if command.operation in {
        SkillInstallationOperation.INSTALL,
        SkillInstallationOperation.ENABLE,
    }:
        return SkillInstallationStatus.ACTIVE
    if command.operation is SkillInstallationOperation.DISABLE:
        return SkillInstallationStatus.DISABLED
    return (
        SkillInstallationStatus.UNINSTALLED
        if command.force
        else SkillInstallationStatus.DRAINING
    )


def _validate_installation_transition(
    current: SkillInstallationStatus,
    command: ChangeSkillInstallationCommand,
) -> None:
    allowed = {
        SkillInstallationStatus.ACTIVE: {
            SkillInstallationOperation.DISABLE,
            SkillInstallationOperation.UNINSTALL,
        },
        SkillInstallationStatus.DISABLED: {
            SkillInstallationOperation.ENABLE,
            SkillInstallationOperation.UNINSTALL,
        },
        SkillInstallationStatus.DRAINING: {
            SkillInstallationOperation.UNINSTALL,
        },
        SkillInstallationStatus.UNINSTALLED: {
            SkillInstallationOperation.INSTALL,
        },
    }
    if command.operation not in allowed[current] or (
        current is SkillInstallationStatus.DRAINING and not command.force
    ):
        raise InvalidTransitionError(
            f"Cannot {command.operation.value} a {current.value} Skill installation"
        )
