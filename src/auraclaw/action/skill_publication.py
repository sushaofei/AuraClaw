from __future__ import annotations

import hashlib
from datetime import UTC, datetime

from auraclaw.action.skill_lifecycle import SkillLifecycleStore
from auraclaw.action.skill_packages import (
    SkillPackage,
    SkillPackageRegistry,
    skill_package_digest,
)
from auraclaw.contracts.errors import (
    InvalidTransitionError,
    PolicyDeniedError,
    VersionConflictError,
)
from auraclaw.contracts.skills import (
    PublishedSkill,
    PublishSkillCommand,
    SkillInstallationRecord,
    SkillInstallationStatus,
    SkillPackageRecord,
    SkillPublicationRecord,
    SkillPublicationStatus,
    SkillSourceDesiredState,
    SkillSourceRecord,
)


class SkillPublicationService:
    """Single governed entry point for publishing a validated Skill package."""

    def __init__(
        self,
        *,
        registry: SkillPackageRegistry,
        lifecycle: SkillLifecycleStore,
        bootstrap_sources: tuple[SkillSourceRecord, ...] = (),
    ) -> None:
        self._registry = registry
        self._lifecycle = lifecycle
        self._bootstrap_sources = {
            (source.tenant_id, source.source_id): source for source in bootstrap_sources
        }

    async def publish(self, command: PublishSkillCommand, package: SkillPackage) -> PublishedSkill:
        source = await self._authorized_source(command, package)
        digest = skill_package_digest(package)
        manifest = package.manifest
        existing = await self._lifecycle.get_publication(
            command.tenant_id,
            manifest.publisher,
            manifest.name,
            manifest.version,
        )
        desired_status = (
            SkillPublicationStatus.ACTIVE if command.activate else SkillPublicationStatus.STAGED
        )
        if existing is not None and existing.package_digest != digest:
            raise VersionConflictError("Skill version is immutable")
        if (
            existing is not None
            and existing.status is not desired_status
            and not (
                existing.status is SkillPublicationStatus.STAGED
                and desired_status is SkillPublicationStatus.ACTIVE
            )
        ):
            raise InvalidTransitionError("Skill publish only permits staged to active transition")
        if (
            existing is not None
            and existing.status is not desired_status
            and existing.revision != command.expected_revision
        ):
            raise VersionConflictError("Skill publication revision conflict")
        if existing is None and command.expected_revision != 0:
            raise VersionConflictError("Skill publication revision conflict")

        now = datetime.now(UTC)
        if existing is None:
            publication = await self._registry.publish(
                command.tenant_id,
                package,
                status=desired_status,
            )
            await self._lifecycle.put_package(
                SkillPackageRecord(
                    tenant_id=command.tenant_id,
                    manifest=publication.manifest,
                    package_digest=publication.package_digest,
                    artifact_ref=publication.artifact_ref,
                    created_at=now,
                )
            )
        if existing is None:
            record = SkillPublicationRecord(
                publication_id=_stable_id(
                    "skp",
                    command.tenant_id,
                    manifest.publisher,
                    manifest.name,
                    manifest.version,
                ),
                tenant_id=command.tenant_id,
                publisher=manifest.publisher,
                name=manifest.name,
                version=manifest.version,
                package_digest=publication.package_digest,
                status=desired_status,
                source_id=source.source_id,
                revision=1,
                created_by=command.actor_id,
                created_at=now,
                updated_at=now,
            )
            committed = await self._put_publication_idempotently(record, expected_revision=0)
        elif existing.status is not desired_status:
            committed = await self._put_publication_idempotently(
                existing.model_copy(
                    update={
                        "status": desired_status,
                        "source_id": source.source_id,
                        "revision": existing.revision + 1,
                        "updated_at": now,
                        "reason_code": None,
                    }
                ),
                expected_revision=command.expected_revision,
            )
        else:
            committed = existing

        persisted_package = await self._lifecycle.get_package(
            command.tenant_id,
            manifest.publisher,
            manifest.name,
            manifest.version,
        )
        if persisted_package is None:
            raise VersionConflictError("Skill publication references a missing package")
        publication = self._registry.restore(
            command.tenant_id,
            package,
            PublishedSkill(
                tenant_id=command.tenant_id,
                manifest=persisted_package.manifest,
                package_digest=persisted_package.package_digest,
                artifact_ref=persisted_package.artifact_ref,
                status=committed.status,
            ),
        )

        if command.activate:
            await self._ensure_installation(command, source, publication, now)
        return publication

    async def _put_publication_idempotently(
        self,
        record: SkillPublicationRecord,
        *,
        expected_revision: int,
    ) -> SkillPublicationRecord:
        try:
            return await self._lifecycle.put_publication(
                record, expected_revision=expected_revision
            )
        except VersionConflictError:
            concurrent = await self._lifecycle.get_publication(
                record.tenant_id,
                record.publisher,
                record.name,
                record.version,
            )
            if (
                concurrent is not None
                and concurrent.package_digest == record.package_digest
                and concurrent.status is record.status
            ):
                return concurrent
            raise

    async def _authorized_source(
        self, command: PublishSkillCommand, package: SkillPackage
    ) -> SkillSourceRecord:
        source = await self._lifecycle.get_source(command.tenant_id, command.source_id)
        if source is None:
            source = self._bootstrap_sources.get((command.tenant_id, command.source_id))
        if source is None:
            template = self._bootstrap_sources.get(("*", command.source_id))
            if template is not None:
                source = template.model_copy(update={"tenant_id": command.tenant_id})
        if (
            source is not None
            and await self._lifecycle.get_source(command.tenant_id, command.source_id) is None
        ):
            try:
                source = await self._lifecycle.put_source(source, expected_revision=0)
            except VersionConflictError:
                source = await self._lifecycle.get_source(command.tenant_id, command.source_id)
        if source is None:
            raise PolicyDeniedError("Skill Source is not configured")
        if source.desired_state is not SkillSourceDesiredState.ENABLED:
            raise PolicyDeniedError("Skill Source is not enabled")
        if package.manifest.publisher not in source.publisher_allowlist:
            raise PolicyDeniedError("Skill publisher is not allowed by the Source")
        return source

    async def _ensure_installation(
        self,
        command: PublishSkillCommand,
        source: SkillSourceRecord,
        publication: PublishedSkill,
        now: datetime,
    ) -> None:
        manifest = publication.manifest
        existing = await self._lifecycle.get_installation(
            command.tenant_id,
            manifest.publisher,
            manifest.name,
        )
        if existing is not None:
            return
        record = SkillInstallationRecord(
            installation_id=_stable_id(
                "ski",
                command.tenant_id,
                manifest.publisher,
                manifest.name,
            ),
            tenant_id=command.tenant_id,
            publisher=manifest.publisher,
            name=manifest.name,
            version_constraint=f"={manifest.version}",
            pinned_package_digest=publication.package_digest,
            status=SkillInstallationStatus.ACTIVE,
            source_id=source.source_id,
            auto_upgrade=False,
            revision=1,
            created_by=command.actor_id,
            updated_by=command.actor_id,
            created_at=now,
            updated_at=now,
        )
        try:
            await self._lifecycle.put_installation(record, expected_revision=0)
        except VersionConflictError:
            concurrent = await self._lifecycle.get_installation(
                command.tenant_id, manifest.publisher, manifest.name
            )
            if (
                concurrent is None
                or concurrent.status is not SkillInstallationStatus.ACTIVE
                or concurrent.pinned_package_digest != publication.package_digest
            ):
                raise


def _stable_id(prefix: str, *parts: str) -> str:
    digest = hashlib.sha256("\0".join(parts).encode()).hexdigest()[:32]
    return f"{prefix}_{digest}"
