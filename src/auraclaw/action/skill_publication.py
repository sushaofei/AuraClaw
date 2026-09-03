from __future__ import annotations

import hashlib
import json
import re
import time
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from uuid import uuid4

from auraclaw.action.ports import ArtifactContentReader, SkillArtifactLifecycle
from auraclaw.action.skill_admin_catalog import SkillCapabilityAvailability
from auraclaw.action.skill_lifecycle import (
    SkillAdmissionAuditRecord,
    SkillLifecycleStore,
    SkillPublishCommit,
    skill_version_key,
)
from auraclaw.action.skill_packages import (
    DefaultSkillPackageContentScanner,
    SkillPackage,
    SkillPackageContentScanner,
    SkillPackageRegistry,
    skill_capability_descriptor,
    skill_package_digest,
    skill_package_from_archive,
    version_satisfies,
)
from auraclaw.action.skill_publishers import SkillPublisherTrustService
from auraclaw.contracts.errors import (
    AuraClawError,
    InvalidTransitionError,
    PolicyDeniedError,
    SkillContentRejectedError,
    VersionConflictError,
)
from auraclaw.contracts.skills import (
    PublishedSkill,
    PublishSkillCommand,
    SkillInstallationRecord,
    SkillInstallationStatus,
    SkillPackageRecord,
    SkillPackageRetentionStatus,
    SkillPublicationRecord,
    SkillPublicationStatus,
    SkillUpgradeState,
)
from auraclaw.contracts.tools import ArtifactRef

_CONTENT_POLICY_VERSION = re.compile(r"^[a-z0-9][a-z0-9._-]{0,127}$")


@dataclass
class _AdmissionTrace:
    operation: str
    stage: str = "request_validation"
    publisher: str | None = None
    name: str | None = None
    version: str | None = None
    package_digest: str | None = None
    artifact_id: str | None = None

    @classmethod
    def from_package(cls, operation: str, package: SkillPackage) -> _AdmissionTrace:
        trace = cls(operation=operation)
        trace.set_identity(package)
        return trace

    def set_identity(self, package: SkillPackage) -> None:
        self.publisher = package.manifest.publisher
        self.name = package.manifest.name
        self.version = package.manifest.version


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
        content_scanner: SkillPackageContentScanner | None = None,
        dependency_availability: SkillCapabilityAvailability | None = None,
    ) -> None:
        self._dependency_availability = dependency_availability
        self._registry = registry
        self._lifecycle = lifecycle
        self._artifacts = artifacts
        self._artifact_lifecycle = artifact_lifecycle
        self._publisher_trust = publisher_trust
        self._content_scanner = content_scanner or DefaultSkillPackageContentScanner()
        self._content_policy_version = self._content_scanner.policy_version
        if _CONTENT_POLICY_VERSION.fullmatch(self._content_policy_version) is None:
            raise ValueError("Skill content policy version is invalid")

    async def publish(
        self,
        command: PublishSkillCommand,
        package: SkillPackage,
    ) -> PublishedSkill:
        started = time.monotonic()
        trace = _AdmissionTrace.from_package("publish", package)
        try:
            trace.stage = "signature_validation"
            package, key_id, externally_verified = await self._validate_signature(
                command.tenant_id, package
            )
            trace.package_digest = skill_package_digest(package)
            trace.stage = "content_scan"
            self._scan_content(package)
            trace.stage = "lifecycle_commit"
            result = await self._publish(
                command,
                package,
                signature_key_id=key_id,
                signature_verified=externally_verified,
            )
        except Exception as exc:
            await self._record_admission(command, trace, started, error=exc)
            raise
        await self._record_admission(command, trace, started)
        return result

    async def publish_artifact(
        self,
        command: PublishSkillCommand,
        artifact_ref: ArtifactRef,
        expected_digest: str,
    ) -> PublishedSkill:
        started = time.monotonic()
        trace = _AdmissionTrace(
            operation="publish_artifact",
            artifact_id=artifact_ref.artifact_id,
            package_digest=expected_digest,
        )
        try:
            trace.stage = "artifact_validation"
            if self._artifacts is None:
                raise PolicyDeniedError("Staged Skill publication is not configured")
            if artifact_ref.media_type != "application/vnd.auraclaw.skill-package+json":
                raise PolicyDeniedError("Staged Artifact is not a Skill package")
            if expected_digest != f"sha256:{artifact_ref.content_hash}":
                raise VersionConflictError("Staged Artifact digest does not match")
            if self._artifact_lifecycle is not None:
                trace.stage = "artifact_claim"
                await self._artifact_lifecycle.claim_publication(
                    tenant_id=command.tenant_id,
                    artifact_ref=artifact_ref,
                    command_id=command.command_id,
                    correlation_id=command.correlation_id,
                )
            trace.stage = "artifact_read"
            content = await self._artifacts.read(
                tenant_id=command.tenant_id,
                artifact_ref=artifact_ref,
                actor_id=command.actor_id,
                correlation_id=command.correlation_id,
            )
            trace.stage = "archive_validation"
            package = skill_package_from_archive(content)
            trace.set_identity(package)
            trace.stage = "signature_validation"
            package, key_id, externally_verified = await self._validate_signature(
                command.tenant_id, package
            )
            if skill_package_digest(package) != expected_digest:
                raise VersionConflictError("Skill package digest does not match")
            trace.stage = "content_scan"
            self._scan_content(package)
            trace.stage = "lifecycle_commit"
            result = await self._publish(
                command,
                package,
                artifact_ref=artifact_ref,
                artifact_claimed=True,
                signature_key_id=key_id,
                signature_verified=externally_verified,
            )
        except Exception as exc:
            await self._record_admission(command, trace, started, error=exc)
            raise
        await self._record_admission(command, trace, started)
        return result

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
        persisted_package = (
            None
            if existing is None
            else await self._lifecycle.get_package(
                command.tenant_id,
                manifest.publisher,
                manifest.name,
                manifest.version,
            )
        )
        existing_installation = await self._lifecycle.get_installation(
            command.tenant_id, manifest.publisher, manifest.name
        )
        if command.activate:
            installation_revision = existing_installation.revision if existing_installation else 0
            if (
                command.expected_installation_revision is not None
                and command.expected_installation_revision != installation_revision
            ):
                replay_digest = await self._lifecycle.get_publish_command_digest(
                    command.tenant_id, command.command_id
                )
                if replay_digest != _publish_request_digest(command, digest, artifact_ref):
                    raise VersionConflictError("Skill installation revision conflict")
            for current in await self._lifecycle.list_publications(command.tenant_id):
                if (current.publisher, current.name) == (manifest.publisher, manifest.name) and (
                    current.status is not SkillPublicationStatus.STAGED
                    and skill_version_key(current.version) > skill_version_key(manifest.version)
                ):
                    raise VersionConflictError("Skill upgrade cannot downgrade the current version")
        replace_purged = (
            existing is not None
            and persisted_package is not None
            and persisted_package.retention_status is SkillPackageRetentionStatus.PURGED
            and not persisted_package.legal_hold
            and existing.status is SkillPublicationStatus.REVOKED
            and existing_installation is not None
            and existing_installation.status is SkillInstallationStatus.UNINSTALLED
            and desired_status is SkillPublicationStatus.ACTIVE
        )
        if existing is not None and existing.package_digest != digest and not replace_purged:
            raise VersionConflictError("Skill version is immutable")
        if (
            existing is not None
            and not replace_purged
            and existing.status is not desired_status
            and not (
                existing.status
                in {
                    SkillPublicationStatus.STAGED,
                    SkillPublicationStatus.RESTORING,
                }
                and desired_status is SkillPublicationStatus.ACTIVE
            )
        ):
            raise InvalidTransitionError(
                "Skill publish only permits staged or restoring to active transition"
            )
        if (
            existing is not None
            and not replace_purged
            and existing.status is not desired_status
            and existing.revision != command.expected_revision
        ):
            raise VersionConflictError("Skill publication revision conflict")
        if existing is None and command.expected_revision != 0:
            raise VersionConflictError("Skill publication revision conflict")

        now = datetime.now(UTC)
        if existing is None or replace_purged:
            if replace_purged:
                self._registry.forget_package(
                    command.tenant_id,
                    manifest.publisher,
                    manifest.name,
                    manifest.version,
                )
            if artifact_ref is None:
                publication = await self._registry.publish(
                    command.tenant_id,
                    package,
                    status=SkillPublicationStatus.STAGED,
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
                        status=SkillPublicationStatus.STAGED,
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
            if persisted_package is None:
                raise VersionConflictError("Skill publication references a missing package")
            package_record = persisted_package

        if (
            command.activate
            and existing_installation is not None
            and (existing_installation.pinned_package_digest != package_record.package_digest)
        ):
            candidate = PublishedSkill(
                tenant_id=command.tenant_id,
                manifest=manifest,
                package_digest=package_record.package_digest,
                artifact_ref=package_record.artifact_ref,
            )
            if manifest.required_tools or manifest.required_resources or manifest.required_skills:
                if (
                    self._dependency_availability is None
                    or not await self._dependency_availability.is_available(
                        command.tenant_id, skill_capability_descriptor(candidate)
                    )
                ):
                    raise PolicyDeniedError("Skill upgrade dependencies are unavailable")
            installations = {
                (item.publisher, item.name): item
                for item in await self._lifecycle.list_installations(command.tenant_id)
            }
            for other in await self._lifecycle.list_packages(command.tenant_id):
                selected = installations.get((other.manifest.publisher, other.manifest.name))
                if selected is None or selected.status is not SkillInstallationStatus.ACTIVE:
                    continue
                if selected.pinned_package_digest not in {None, other.package_digest}:
                    continue
                for requirement in other.manifest.required_skills:
                    if requirement.name == manifest.name and requirement.publisher in {
                        None,
                        manifest.publisher,
                    }:
                        if not version_satisfies(manifest.version, requirement.version):
                            raise PolicyDeniedError(
                                "Skill upgrade conflicts with an installed dependent Skill"
                            )
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
                revision=1,
                created_by=command.actor_id,
                updated_by=command.actor_id,
                created_at=now,
                updated_at=now,
            )
            expected_publication_revision = 0
        elif replace_purged:
            record = existing.model_copy(
                update={
                    "package_digest": package_record.package_digest,
                    "status": SkillPublicationStatus.ACTIVE,
                    "revision": existing.revision + 1,
                    "updated_by": command.actor_id,
                    "updated_at": now,
                    "reason_code": None,
                    "revocation_action": None,
                    "revocation_policy_version": None,
                    "revocation_policy_decision_id": None,
                }
            )
            expected_publication_revision = existing.revision
        elif existing.status is not desired_status:
            record = existing.model_copy(
                update={
                    "status": desired_status,
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

        installation: SkillInstallationRecord | None
        if replace_purged:
            assert existing_installation is not None
            installation = existing_installation.model_copy(
                update={
                    "version_constraint": f"={manifest.version}",
                    "pinned_package_digest": package_record.package_digest,
                    "status": SkillInstallationStatus.ACTIVE,
                    "auto_upgrade": False,
                    "revision": existing_installation.revision + 1,
                    "updated_by": command.actor_id,
                    "updated_at": now,
                    "reason_code": None,
                    "uninstall_action": None,
                    "uninstall_policy_version": None,
                    "uninstall_policy_decision_id": None,
                }
            )
        else:
            installation = await self._new_installation(
                command,
                package_record,
                now,
            )
        upgrade = None
        if installation is not None and existing_installation is not None:
            upgrade = SkillUpgradeState(
                tenant_id=command.tenant_id,
                publisher=manifest.publisher,
                name=manifest.name,
                operation_id=_stable_id("sku", command.tenant_id, command.command_id),
                command_id=command.command_id,
                current_version=manifest.version,
                package_digest=package_record.package_digest,
                generation=installation.revision,
                actor_id=command.actor_id,
                correlation_id=command.correlation_id,
                causation_id=command.causation_id,
                updated_at=now,
            )
        committed = await self._lifecycle.commit_publish(
            SkillPublishCommit(
                command_id=command.command_id,
                request_digest=_publish_request_digest(command, digest, artifact_ref),
                actor_id=command.actor_id,
                correlation_id=command.correlation_id,
                causation_id=command.causation_id,
                expected_publication_revision=expected_publication_revision,
                package=package_record,
                publication=record,
                installation=installation,
                occurred_at=now,
                replace_purged=replace_purged,
                upgrade=upgrade,
                expected_installation_revision=(
                    existing_installation.revision
                    if (replace_purged or upgrade is not None) and existing_installation is not None
                    else None
                ),
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
                upgrade=await self._lifecycle.get_upgrade(
                    command.tenant_id, manifest.publisher, manifest.name
                ),
            ),
            signature_verified=signature_verified,
        )
        if upgrade is not None:
            for old in await self._lifecycle.list_publications(command.tenant_id):
                if (old.publisher, old.name) == (manifest.publisher, manifest.name):
                    self._registry.set_skill_discoverable(
                        command.tenant_id,
                        old.publisher,
                        old.name,
                        version=old.version,
                        discoverable=(
                            old.version == manifest.version
                            and installation is not None
                            and installation.status is SkillInstallationStatus.ACTIVE
                        ),
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
        key_id = await self._publisher_trust.verify_for_admission(tenant_id, normalized)
        return normalized, key_id, True

    def _scan_content(self, package: SkillPackage) -> None:
        findings = self._content_scanner.scan(package)
        if findings:
            raise SkillContentRejectedError(findings[0])

    async def _record_admission(
        self,
        command: PublishSkillCommand,
        trace: _AdmissionTrace,
        started: float,
        *,
        error: Exception | None = None,
    ) -> None:
        safe_error_code = None
        if error is not None:
            safe_error_code = error.code if isinstance(error, AuraClawError) else "internal_error"
        await self._lifecycle.record_admission(
            SkillAdmissionAuditRecord(
                admission_id=f"skad_{uuid4().hex}",
                tenant_id=command.tenant_id,
                command_id=command.command_id,
                operation=trace.operation,
                actor_id=command.actor_id,
                correlation_id=command.correlation_id,
                causation_id=command.causation_id,
                publisher=trace.publisher,
                name=trace.name,
                version=trace.version,
                package_digest=trace.package_digest,
                artifact_id=trace.artifact_id,
                outcome=(
                    "quarantined"
                    if isinstance(error, SkillContentRejectedError)
                    else "rejected"
                    if error is not None
                    else "accepted"
                ),
                stage=trace.stage if error is not None else "completed",
                safe_error_code=safe_error_code,
                duration_ms=max(0, int((time.monotonic() - started) * 1000)),
                occurred_at=datetime.now(UTC),
                content_policy_version=self._content_policy_version,
            )
        )

    async def _new_installation(
        self,
        command: PublishSkillCommand,
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
            if (
                existing.pinned_package_digest == package.package_digest
                and existing.version_constraint == f"={manifest.version}"
            ):
                return None
            return existing.model_copy(
                update={
                    "version_constraint": f"={manifest.version}",
                    "pinned_package_digest": package.package_digest,
                    "auto_upgrade": False,
                    "revision": existing.revision + 1,
                    "updated_by": command.actor_id,
                    "updated_at": now,
                }
            )
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
        "activate": command.activate,
        "expected_revision": command.expected_revision,
        "package_digest": package_digest,
        "artifact_ref": None if artifact_ref is None else artifact_ref.as_dict(),
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    return f"sha256:{hashlib.sha256(encoded).hexdigest()}"
