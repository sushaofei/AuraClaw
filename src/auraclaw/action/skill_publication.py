from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime, timedelta

from auraclaw.action.ports import ArtifactContentReader, SkillArtifactLifecycle
from auraclaw.action.skill_lifecycle import SkillLifecycleStore, SkillPublishCommit
from auraclaw.action.skill_packages import (
    SkillPackage,
    SkillPackageRegistry,
    skill_package_digest,
    skill_package_from_archive,
)
from auraclaw.action.skill_publishers import SkillPublisherTrustService
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
    SkillSourceKind,
    SkillSourceRecord,
)
from auraclaw.contracts.tools import ArtifactRef


class SkillPublicationService:
    """Single governed entry point for publishing a validated Skill package."""

    def __init__(
        self,
        *,
        registry: SkillPackageRegistry,
        lifecycle: SkillLifecycleStore,
        artifacts: ArtifactContentReader | None = None,
        artifact_lifecycle: SkillArtifactLifecycle | None = None,
        publisher_trust: SkillPublisherTrustService | None = None,
        bootstrap_sources: tuple[SkillSourceRecord, ...] = (),
    ) -> None:
        self._registry = registry
        self._lifecycle = lifecycle
        self._artifacts = artifacts
        self._artifact_lifecycle = artifact_lifecycle
        self._publisher_trust = publisher_trust
        self._bootstrap_sources = {
            (source.tenant_id, source.source_id): source for source in bootstrap_sources
        }

    async def publish(self, command: PublishSkillCommand, package: SkillPackage) -> PublishedSkill:
        package, key_id, externally_verified = await self._validate_signature(
            command.tenant_id, package
        )
        return await self._publish(
            command,
            package,
            signature_key_id=key_id,
            signature_verified=externally_verified,
        )

    async def publish_artifact(
        self,
        command: PublishSkillCommand,
        artifact_ref: ArtifactRef,
        expected_digest: str,
    ) -> PublishedSkill:
        if self._artifacts is None:
            raise PolicyDeniedError("Staged Skill publication is not configured")
        if artifact_ref.media_type != "application/vnd.auraclaw.skill-package+json":
            raise PolicyDeniedError("Staged Artifact is not a Skill package")
        if expected_digest != f"sha256:{artifact_ref.content_hash}":
            raise VersionConflictError("Staged Artifact digest does not match")
        if self._artifact_lifecycle is not None:
            await self._artifact_lifecycle.claim_publication(
                tenant_id=command.tenant_id,
                artifact_ref=artifact_ref,
                command_id=command.command_id,
                correlation_id=command.correlation_id,
            )
        content = await self._artifacts.read(
            tenant_id=command.tenant_id,
            artifact_ref=artifact_ref,
            actor_id=command.actor_id,
            correlation_id=command.correlation_id,
        )
        package, key_id, externally_verified = await self._validate_signature(
            command.tenant_id, skill_package_from_archive(content)
        )
        if skill_package_digest(package) != expected_digest:
            raise VersionConflictError("Skill package digest does not match")
        return await self._publish(
            command,
            package,
            artifact_ref=artifact_ref,
            artifact_claimed=True,
            signature_key_id=key_id,
            signature_verified=externally_verified,
        )

    async def _publish(
        self,
        command: PublishSkillCommand,
        package: SkillPackage,
        *,
        artifact_ref: ArtifactRef | None = None,
        artifact_claimed: bool = False,
        signature_key_id: str | None = None,
        signature_verified: bool = False,
    ) -> PublishedSkill:
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
            if artifact_ref is None:
                publication = await self._registry.publish(
                    command.tenant_id,
                    package,
                    status=desired_status,
                    signature_verified=signature_verified,
                )
            else:
                publication = self._registry.restore(
                    command.tenant_id,
                    package,
                    PublishedSkill(
                        tenant_id=command.tenant_id,
                        manifest=manifest,
                        package_digest=digest,
                        artifact_ref=artifact_ref,
                        status=desired_status,
                    ),
                    signature_verified=signature_verified,
                )
            package_record = SkillPackageRecord(
                tenant_id=command.tenant_id,
                manifest=publication.manifest,
                package_digest=publication.package_digest,
                artifact_ref=publication.artifact_ref,
                signature_key_id=signature_key_id,
                retention_until=now + timedelta(days=90),
                retention_updated_by=command.actor_id,
                retention_updated_at=now,
                created_at=now,
            )
        else:
            persisted_package = await self._lifecycle.get_package(
                command.tenant_id,
                manifest.publisher,
                manifest.name,
                manifest.version,
            )
            if persisted_package is None:
                raise VersionConflictError(
                    "Skill publication references a missing package"
                )
            package_record = persisted_package

        if self._artifact_lifecycle is not None and not artifact_claimed:
            await self._artifact_lifecycle.claim_publication(
                tenant_id=command.tenant_id,
                artifact_ref=package_record.artifact_ref,
                command_id=command.command_id,
                correlation_id=command.correlation_id,
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
                updated_by=command.actor_id,
                created_at=now,
                updated_at=now,
            )
            expected_publication_revision = 0
        elif existing.status is not desired_status:
            record = existing.model_copy(
                update={
                    "status": desired_status,
                    "source_id": source.source_id,
                    "revision": existing.revision + 1,
                    "updated_by": command.actor_id,
                    "updated_at": now,
                    "reason_code": None,
                }
            )
            expected_publication_revision = command.expected_revision
        else:
            record = existing
            expected_publication_revision = existing.revision

        installation = await self._new_installation(
            command,
            source,
            package_record,
            now,
        )
        committed = await self._lifecycle.commit_publish(
            SkillPublishCommit(
                command_id=command.command_id,
                request_digest=_publish_request_digest(
                    command, digest, artifact_ref
                ),
                actor_id=command.actor_id,
                source_id=source.source_id,
                correlation_id=command.correlation_id,
                causation_id=command.causation_id,
                expected_publication_revision=expected_publication_revision,
                package=package_record,
                publication=record,
                installation=installation,
                occurred_at=now,
            )
        )
        if self._artifact_lifecycle is not None:
            await self._artifact_lifecycle.bind_publication(
                tenant_id=command.tenant_id,
                artifact_ref=committed.package.artifact_ref,
                command_id=command.command_id,
                package_digest=committed.package.package_digest,
                correlation_id=command.correlation_id,
            )
        publication = self._registry.restore(
            command.tenant_id,
            package,
            PublishedSkill(
                tenant_id=command.tenant_id,
                manifest=committed.package.manifest,
                package_digest=committed.package.package_digest,
                artifact_ref=committed.package.artifact_ref,
                status=committed.publication.status,
            ),
            signature_verified=signature_verified,
        )
        return publication

    async def _validate_signature(
        self, tenant_id: str, package: SkillPackage
    ) -> tuple[SkillPackage, str | None, bool]:
        if not package.manifest.signature.startswith("ed25519:"):
            return self._registry.validate(package), None, False
        if self._publisher_trust is None:
            raise PolicyDeniedError("External Skill Publisher Registry is unavailable")
        normalized = self._registry.validate_content(package)
        key_id = await self._publisher_trust.verify_for_admission(
            tenant_id, normalized
        )
        return normalized, key_id, True

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
        registry_authorized_admin = (
            source.kind is SkillSourceKind.ADMIN_UPLOAD
            and package.manifest.signature.startswith("ed25519:")
            and self._publisher_trust is not None
        )
        if (
            package.manifest.publisher not in source.publisher_allowlist
            and not registry_authorized_admin
        ):
            raise PolicyDeniedError("Skill publisher is not allowed by the Source")
        return source

    async def _new_installation(
        self,
        command: PublishSkillCommand,
        source: SkillSourceRecord,
        package: SkillPackageRecord,
        now: datetime,
    ) -> SkillInstallationRecord | None:
        if not command.activate:
            return None
        manifest = package.manifest
        existing = await self._lifecycle.get_installation(
            command.tenant_id,
            manifest.publisher,
            manifest.name,
        )
        if existing is not None:
            return None
        return SkillInstallationRecord(
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
            pinned_package_digest=package.package_digest,
            status=SkillInstallationStatus.ACTIVE,
            source_id=source.source_id,
            auto_upgrade=False,
            revision=1,
            created_by=command.actor_id,
            updated_by=command.actor_id,
            created_at=now,
            updated_at=now,
        )


def _stable_id(prefix: str, *parts: str) -> str:
    digest = hashlib.sha256("\0".join(parts).encode()).hexdigest()[:32]
    return f"{prefix}_{digest}"


def _publish_request_digest(
    command: PublishSkillCommand,
    package_digest: str,
    artifact_ref: ArtifactRef | None,
) -> str:
    payload = {
        "tenant_id": command.tenant_id,
        "actor_id": command.actor_id,
        "source_id": command.source_id,
        "activate": command.activate,
        "expected_revision": command.expected_revision,
        "package_digest": package_digest,
        "artifact_ref": None if artifact_ref is None else artifact_ref.as_dict(),
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    return f"sha256:{hashlib.sha256(encoded).hexdigest()}"
