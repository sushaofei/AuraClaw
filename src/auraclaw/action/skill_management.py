from __future__ import annotations

from datetime import UTC, datetime
from typing import Protocol

from auraclaw.action.skill_lifecycle import SkillLifecycleStore
from auraclaw.action.skill_packages import SkillPackageRegistry
from auraclaw.contracts.errors import InvalidTransitionError, NotFoundError
from auraclaw.contracts.skills import (
    ChangeSkillInstallationCommand,
    RevokeSkillPublicationCommand,
    SkillInstallationOperation,
    SkillInstallationRecord,
    SkillInstallationStatus,
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
    ) -> None:
        self._lifecycle = lifecycle
        self._projector = projector

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
