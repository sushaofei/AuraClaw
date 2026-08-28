from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Protocol

from auraclaw.action.ports import ArtifactDeleter, SkillBindingReferenceReader
from auraclaw.action.skill_lifecycle import SkillLifecycleStore
from auraclaw.action.skill_packages import SkillPackageRegistry
from auraclaw.contracts.errors import (
    InvalidTransitionError,
    NotFoundError,
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
    SkillPackageRecord,
    SkillPackageRetentionStatus,
    SkillPublicationRecord,
    SkillPublicationStatus,
)


class SkillStateProjector(Protocol):
    async def rebuild_tenant(self, tenant_id: str) -> object: ...


class SkillManagementService:
    """Govern installation visibility and security revocation independently."""

    def __init__(
        self,
        *,
        lifecycle: SkillLifecycleStore,
        projector: SkillStateProjector,
        artifacts: ArtifactDeleter | None = None,
        binding_references: SkillBindingReferenceReader | None = None,
        purge_quiescence: timedelta = timedelta(minutes=5),
    ) -> None:
        self._lifecycle = lifecycle
        self._projector = projector
        self._artifacts = artifacts
        self._binding_references = binding_references
        self._purge_quiescence = purge_quiescence

    async def get_package(
        self,
        tenant_id: str,
        publisher: str,
        name: str,
        version: str,
    ) -> SkillPackageRecord:
        record = await self._lifecycle.get_package(
            tenant_id, publisher, name, version
        )
        if record is None:
            raise NotFoundError("Skill package not found")
        return record

    async def get_installation(
        self,
        tenant_id: str,
        publisher: str,
        name: str,
    ) -> SkillInstallationRecord:
        record = await self._lifecycle.get_installation(
            tenant_id, publisher, name
        )
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
        record = await self._lifecycle.get_publication(
            tenant_id, publisher, name, version
        )
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
        target = _installation_target(command.operation)
        if current.status is target:
            await self._projector.rebuild_tenant(command.tenant_id)
            return current
        _validate_installation_transition(current.status, command.operation)
        now = datetime.now(UTC)
        updated = await self._lifecycle.put_installation(
            current.model_copy(
                update={
                    "status": target,
                    "revision": current.revision + 1,
                    "updated_by": command.actor_id,
                    "updated_at": now,
                    "reason_code": command.reason_code,
                }
            ),
            expected_revision=command.expected_revision,
        )
        await self._projector.rebuild_tenant(command.tenant_id)
        return updated

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
                }
            ),
            expected_revision=command.expected_revision,
        )
        await self._projector.rebuild_tenant(command.tenant_id)
        return updated

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
        if (
            installation is None
            or installation.status is not SkillInstallationStatus.UNINSTALLED
        ):
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
                    )
            installation = installations.get(
                (publication.publisher, publication.name)
            )
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

def _installation_target(
    operation: SkillInstallationOperation,
) -> SkillInstallationStatus:
    if operation in {
        SkillInstallationOperation.INSTALL,
        SkillInstallationOperation.ENABLE,
    }:
        return SkillInstallationStatus.ACTIVE
    if operation is SkillInstallationOperation.DISABLE:
        return SkillInstallationStatus.DISABLED
    return SkillInstallationStatus.UNINSTALLED


def _validate_installation_transition(
    current: SkillInstallationStatus,
    operation: SkillInstallationOperation,
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
        SkillInstallationStatus.UNINSTALLED: {
            SkillInstallationOperation.INSTALL,
        },
    }
    if operation not in allowed[current]:
        raise InvalidTransitionError(
            f"Cannot {operation.value} a {current.value} Skill installation"
        )
