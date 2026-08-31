from __future__ import annotations

import asyncio
import base64
import binascii
import hashlib
import json
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Protocol

from auraclaw.contracts.errors import (
    NotFoundError,
    SchemaValidationError,
    VersionConflictError,
)
from auraclaw.contracts.skills import (
    SkillInstallationRecord,
    SkillPackageRecord,
    SkillPublicationRecord,
    SkillPublicationStatus,
    SkillSourceDesiredState,
    SkillSourceRecord,
    SkillSourceSyncState,
)

PackageKey = tuple[str, str, str, str]
PublicationKey = tuple[str, str, str, str]
InstallationKey = tuple[str, str, str]
SourceKey = tuple[str, str]
PublicationSourceKey = tuple[str, str, str, str, str]
CommandKey = tuple[str, str]


@dataclass(frozen=True)
class SkillPublishCommit:
    command_id: str
    request_digest: str
    actor_id: str
    source_id: str
    correlation_id: str
    causation_id: str
    expected_publication_revision: int
    package: SkillPackageRecord
    publication: SkillPublicationRecord
    installation: SkillInstallationRecord | None
    occurred_at: datetime
    source_lease: SkillSourceLease | None = None


@dataclass(frozen=True)
class SkillSourceLease:
    tenant_id: str
    source_id: str
    owner: str
    fencing_token: int
    expires_at: datetime


@dataclass(frozen=True, order=True)
class SkillSourcePackageIdentity:
    publisher: str
    name: str
    version: str


@dataclass(frozen=True)
class SkillSourceSnapshotCommit:
    state: SkillSourceSyncState
    lease: SkillSourceLease
    observed: tuple[SkillSourcePackageIdentity, ...]
    missing_snapshot_threshold: int
    actor_id: str
    command_prefix: str
    correlation_id: str
    causation_id: str
    occurred_at: datetime


@dataclass(frozen=True)
class SkillSourceSnapshotResult:
    retired: tuple[SkillPublicationRecord, ...]


@dataclass(frozen=True)
class SkillRestoreCommit:
    command_id: str
    request_digest: str
    actor_id: str
    reason_code: str
    correlation_id: str
    causation_id: str
    expected_revision: int
    publication: SkillPublicationRecord
    occurred_at: datetime


@dataclass(frozen=True)
class SkillSourceConfigCommit:
    command_id: str
    request_digest: str
    operation: str
    actor_id: str
    correlation_id: str
    causation_id: str
    reason_code: str | None
    expected_revision: int
    source: SkillSourceRecord
    occurred_at: datetime


@dataclass(frozen=True)
class SkillInstallationCommit:
    command_id: str
    request_digest: str
    operation: str
    force_uninstall: bool
    actor_id: str
    correlation_id: str
    causation_id: str
    reason_code: str | None
    expected_revision: int
    installation: SkillInstallationRecord
    occurred_at: datetime


@dataclass(frozen=True)
class SkillAdmissionAuditRecord:
    admission_id: str
    tenant_id: str
    command_id: str
    operation: str
    actor_id: str
    source_id: str
    correlation_id: str
    causation_id: str
    publisher: str | None
    name: str | None
    version: str | None
    package_digest: str | None
    artifact_id: str | None
    outcome: str
    stage: str
    safe_error_code: str | None
    duration_ms: int
    occurred_at: datetime
    content_policy_version: str = "unknown"


@dataclass(frozen=True)
class SkillAdmissionMetricRecord:
    outcome: str
    content_policy_version: str
    count: int
    average_duration_ms: float


@dataclass(frozen=True)
class SkillAdmissionPage:
    admissions: tuple[SkillAdmissionAuditRecord, ...]
    next_cursor: str | None = None


def encode_skill_admission_cursor(record: SkillAdmissionAuditRecord) -> str:
    raw = json.dumps(
        (record.occurred_at.isoformat(), record.admission_id),
        separators=(",", ":"),
    ).encode()
    return base64.urlsafe_b64encode(raw).decode().rstrip("=")


def decode_skill_admission_cursor(cursor: str) -> tuple[datetime, str]:
    try:
        padding = "=" * (-len(cursor) % 4)
        raw = base64.b64decode(
            cursor + padding, altchars=b"-_", validate=True
        ).decode()
        payload = json.loads(raw)
        if not isinstance(payload, list) or len(payload) != 2:
            raise ValueError
        occurred_at_value, admission_id = payload
        if not isinstance(occurred_at_value, str) or not isinstance(admission_id, str):
            raise ValueError
        occurred_at = datetime.fromisoformat(occurred_at_value)
        if occurred_at.tzinfo is None or not 1 <= len(admission_id) <= 128:
            raise ValueError
    except (
        ValueError,
        TypeError,
        UnicodeDecodeError,
        json.JSONDecodeError,
        binascii.Error,
    ) as exc:
        raise SchemaValidationError("Skill admission cursor is invalid") from exc
    return occurred_at, str(admission_id)


@dataclass(frozen=True)
class SkillPublishCommitResult:
    package: SkillPackageRecord
    publication: SkillPublicationRecord
    installation: SkillInstallationRecord | None
    replayed: bool = False


@dataclass(frozen=True)
class SkillOutboxRecord:
    outbox_id: str
    tenant_id: str
    command_id: str
    event_type: str
    payload: dict[str, object]
    attempt: int = 0


class SkillLifecycleStore(Protocol):
    async def record_admission(self, record: SkillAdmissionAuditRecord) -> None: ...

    async def list_admissions(
        self,
        tenant_id: str,
        *,
        outcome: str | None = None,
        stage: str | None = None,
        content_policy_version: str | None = None,
        limit: int = 100,
    ) -> tuple[SkillAdmissionAuditRecord, ...]: ...

    async def admission_metrics(
        self, tenant_id: str, *, since: datetime | None = None
    ) -> tuple[SkillAdmissionMetricRecord, ...]: ...

    async def page_admissions(
        self,
        tenant_id: str,
        *,
        outcome: str | None = None,
        stage: str | None = None,
        content_policy_version: str | None = None,
        since: datetime | None = None,
        cursor: str | None = None,
        limit: int = 100,
    ) -> SkillAdmissionPage: ...

    async def delete_admissions_before(
        self, cutoff: datetime, *, limit: int = 1000
    ) -> int: ...

    async def commit_publish(
        self, commit: SkillPublishCommit
    ) -> SkillPublishCommitResult: ...

    async def claim_outbox(
        self, *, owner: str, limit: int = 100
    ) -> tuple[SkillOutboxRecord, ...]: ...

    async def complete_outbox(self, *, outbox_id: str, owner: str) -> None: ...

    async def fail_outbox(
        self, *, outbox_id: str, owner: str, safe_error_code: str
    ) -> None: ...

    async def has_artifact_reference(
        self, tenant_id: str, artifact_id: str, version: int
    ) -> bool: ...

    async def put_package(self, record: SkillPackageRecord) -> SkillPackageRecord: ...

    async def get_package(
        self, tenant_id: str, publisher: str, name: str, version: str
    ) -> SkillPackageRecord | None: ...

    async def list_packages(self, tenant_id: str) -> tuple[SkillPackageRecord, ...]: ...

    async def update_package_retention(
        self, record: SkillPackageRecord, *, expected_revision: int
    ) -> SkillPackageRecord: ...

    async def put_publication(
        self, record: SkillPublicationRecord, *, expected_revision: int
    ) -> SkillPublicationRecord: ...

    async def get_publication(
        self, tenant_id: str, publisher: str, name: str, version: str
    ) -> SkillPublicationRecord | None: ...

    async def list_publications(
        self, tenant_id: str
    ) -> tuple[SkillPublicationRecord, ...]: ...

    async def list_tenants(self) -> tuple[str, ...]: ...

    async def put_installation(
        self, record: SkillInstallationRecord, *, expected_revision: int
    ) -> SkillInstallationRecord: ...

    async def commit_installation_change(
        self, commit: SkillInstallationCommit
    ) -> SkillInstallationRecord: ...

    async def get_installation(
        self, tenant_id: str, publisher: str, name: str
    ) -> SkillInstallationRecord | None: ...

    async def list_installations(
        self, tenant_id: str
    ) -> tuple[SkillInstallationRecord, ...]: ...

    async def put_source(
        self, record: SkillSourceRecord, *, expected_revision: int
    ) -> SkillSourceRecord: ...

    async def commit_source_config(
        self, commit: SkillSourceConfigCommit
    ) -> SkillSourceRecord: ...

    async def get_source(
        self, tenant_id: str, source_id: str
    ) -> SkillSourceRecord | None: ...

    async def list_sources(self, tenant_id: str) -> tuple[SkillSourceRecord, ...]: ...

    async def put_sync_state(self, state: SkillSourceSyncState) -> None: ...

    async def get_sync_state(
        self, tenant_id: str, source_id: str
    ) -> SkillSourceSyncState | None: ...

    async def claim_source_lease(
        self, *, tenant_id: str, source_id: str, owner: str, ttl: timedelta
    ) -> SkillSourceLease | None: ...

    async def renew_source_lease(
        self, lease: SkillSourceLease, *, ttl: timedelta
    ) -> SkillSourceLease | None: ...

    async def release_source_lease(self, lease: SkillSourceLease) -> None: ...

    async def put_sync_state_fenced(
        self, state: SkillSourceSyncState, *, lease: SkillSourceLease
    ) -> None: ...

    async def commit_source_snapshot(
        self, commit: SkillSourceSnapshotCommit
    ) -> SkillSourceSnapshotResult: ...

    async def commit_restore(
        self, commit: SkillRestoreCommit
    ) -> SkillPublicationRecord: ...


@dataclass
class InMemorySkillLifecycleStore:
    _admission_audits: list[SkillAdmissionAuditRecord] = field(default_factory=list)
    _packages: dict[PackageKey, SkillPackageRecord] = field(default_factory=dict)
    _publications: dict[PublicationKey, SkillPublicationRecord] = field(
        default_factory=dict
    )
    _installations: dict[InstallationKey, SkillInstallationRecord] = field(
        default_factory=dict
    )
    _sources: dict[SourceKey, SkillSourceRecord] = field(default_factory=dict)
    _sync_states: dict[SourceKey, SkillSourceSyncState] = field(default_factory=dict)
    _source_leases: dict[SourceKey, SkillSourceLease] = field(default_factory=dict)
    _source_inventory: dict[
        tuple[str, str, str, str, str], tuple[int, int]
    ] = field(default_factory=dict)
    _publication_sources: dict[PublicationSourceKey, bool] = field(
        default_factory=dict
    )
    _source_retirement_commands: set[tuple[str, str]] = field(default_factory=set)
    _restore_commands: dict[CommandKey, str] = field(default_factory=dict)
    _source_config_commands: dict[CommandKey, tuple[str, str]] = field(
        default_factory=dict
    )
    _installation_commands: dict[CommandKey, tuple[str, InstallationKey]] = field(
        default_factory=dict
    )
    _commands: dict[CommandKey, tuple[str, SkillPublishCommitResult]] = field(
        default_factory=dict
    )
    _outbox: dict[str, SkillOutboxRecord] = field(default_factory=dict)
    _claimed_outbox: dict[str, str] = field(default_factory=dict)
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    async def record_admission(self, record: SkillAdmissionAuditRecord) -> None:
        async with self._lock:
            self._admission_audits.append(record)

    async def list_admissions(
        self,
        tenant_id: str,
        *,
        outcome: str | None = None,
        stage: str | None = None,
        content_policy_version: str | None = None,
        limit: int = 100,
    ) -> tuple[SkillAdmissionAuditRecord, ...]:
        return tuple(
            record
            for record in reversed(self._admission_audits)
            if record.tenant_id == tenant_id
            and (outcome is None or record.outcome == outcome)
            and (stage is None or record.stage == stage)
            and (
                content_policy_version is None
                or record.content_policy_version == content_policy_version
            )
        )[:limit]

    async def admission_metrics(
        self, tenant_id: str, *, since: datetime | None = None
    ) -> tuple[SkillAdmissionMetricRecord, ...]:
        grouped: dict[tuple[str, str], list[int]] = {}
        for record in self._admission_audits:
            if record.tenant_id != tenant_id or (
                since is not None and record.occurred_at < since
            ):
                continue
            grouped.setdefault(
                (record.outcome, record.content_policy_version), []
            ).append(record.duration_ms)
        return tuple(
            SkillAdmissionMetricRecord(
                outcome=outcome,
                content_policy_version=policy_version,
                count=len(durations),
                average_duration_ms=sum(durations) / len(durations),
            )
            for (outcome, policy_version), durations in sorted(grouped.items())
        )

    async def page_admissions(
        self,
        tenant_id: str,
        *,
        outcome: str | None = None,
        stage: str | None = None,
        content_policy_version: str | None = None,
        since: datetime | None = None,
        cursor: str | None = None,
        limit: int = 100,
    ) -> SkillAdmissionPage:
        cursor_key = decode_skill_admission_cursor(cursor) if cursor else None
        records = sorted(
            (
                record
                for record in self._admission_audits
                if record.tenant_id == tenant_id
                and (outcome is None or record.outcome == outcome)
                and (stage is None or record.stage == stage)
                and (
                    content_policy_version is None
                    or record.content_policy_version == content_policy_version
                )
                and (since is None or record.occurred_at >= since)
                and (
                    cursor_key is None
                    or (record.occurred_at, record.admission_id) < cursor_key
                )
            ),
            key=lambda record: (record.occurred_at, record.admission_id),
            reverse=True,
        )
        page = records[: limit + 1]
        admissions = tuple(page[:limit])
        next_cursor = (
            encode_skill_admission_cursor(admissions[-1])
            if len(page) > limit and admissions
            else None
        )
        return SkillAdmissionPage(admissions=admissions, next_cursor=next_cursor)

    async def delete_admissions_before(
        self, cutoff: datetime, *, limit: int = 1000
    ) -> int:
        async with self._lock:
            candidates = sorted(
                (
                    record
                    for record in self._admission_audits
                    if record.occurred_at < cutoff
                ),
                key=lambda record: (record.occurred_at, record.admission_id),
            )[:limit]
            deleted = {record.admission_id for record in candidates}
            if deleted:
                self._admission_audits = [
                    record
                    for record in self._admission_audits
                    if record.admission_id not in deleted
                ]
            return len(deleted)

    async def commit_publish(
        self, commit: SkillPublishCommit
    ) -> SkillPublishCommitResult:
        key = (commit.package.tenant_id, commit.command_id)
        async with self._lock:
            if commit.source_lease is not None:
                if (
                    commit.source_lease.tenant_id != commit.package.tenant_id
                    or commit.source_lease.source_id != commit.source_id
                ):
                    raise VersionConflictError("Skill Source lease scope mismatch")
                self._require_active_lease(commit.source_lease)
            existing_command = self._commands.get(key)
            if existing_command is not None:
                request_digest, result = existing_command
                if request_digest != commit.request_digest:
                    raise VersionConflictError("Skill command id was reused")
                current_publication = self._publications.get(
                    _publication_key(result.publication)
                )
                return SkillPublishCommitResult(
                    package=result.package,
                    publication=current_publication or result.publication,
                    installation=result.installation,
                    replayed=True,
                )
            packages = dict(self._packages)
            publications = dict(self._publications)
            installations = dict(self._installations)
            try:
                package = await self.put_package(commit.package)
                publication_key = _publication_key(commit.publication)
                current_publication = self._publications.get(publication_key)
                if (
                    current_publication is not None
                    and commit.publication.revision
                    == commit.expected_publication_revision
                    and current_publication.package_digest
                    == commit.publication.package_digest
                    and current_publication.status is commit.publication.status
                ):
                    publication = current_publication
                else:
                    publication = await self.put_publication(
                        commit.publication,
                        expected_revision=commit.expected_publication_revision,
                    )
                installation = None
                if commit.installation is not None:
                    current = self._installations.get(_installation_key(commit.installation))
                    if current is None:
                        installation = await self.put_installation(
                            commit.installation, expected_revision=0
                        )
                    elif (
                        current.status is not commit.installation.status
                        or current.pinned_package_digest
                        != commit.installation.pinned_package_digest
                    ):
                        raise VersionConflictError(
                            "Skill installation revision conflict"
                        )
                    else:
                        installation = current
            except Exception:
                self._packages = packages
                self._publications = publications
                self._installations = installations
                raise
            result = SkillPublishCommitResult(package, publication, installation)
            reference_key = (
                package.tenant_id,
                publication.publisher,
                publication.name,
                publication.version,
                commit.source_id,
            )
            self._publication_sources[reference_key] = True
            selected_source = self._select_publication_source(publication)
            if selected_source is not None and selected_source != publication.source_id:
                publication = publication.model_copy(
                    update={
                        "source_id": selected_source,
                        "revision": publication.revision + 1,
                        "updated_by": commit.actor_id,
                        "updated_at": commit.occurred_at,
                    }
                )
                self._publications[_publication_key(publication)] = publication
                result = SkillPublishCommitResult(package, publication, installation)
            self._commands[key] = (commit.request_digest, result)
            outbox_id = f"{commit.package.tenant_id}:{commit.command_id}:published"
            self._outbox[outbox_id] = SkillOutboxRecord(
                outbox_id=outbox_id,
                tenant_id=commit.package.tenant_id,
                command_id=commit.command_id,
                event_type="skill.publication.committed",
                payload=_publish_outbox_payload(result),
            )
            return result

    def _select_publication_source(
        self, publication: SkillPublicationRecord
    ) -> str | None:
        candidates: list[tuple[int, str]] = []
        for key, available in self._publication_sources.items():
            tenant_id, publisher, name, version, source_id = key
            if not available or (
                tenant_id,
                publisher,
                name,
                version,
            ) != (
                publication.tenant_id,
                publication.publisher,
                publication.name,
                publication.version,
            ):
                continue
            source = self._sources.get((tenant_id, source_id))
            if (
                source is not None
                and source.desired_state is SkillSourceDesiredState.ENABLED
            ):
                candidates.append((source.priority, source_id))
        if not candidates:
            return None
        return min(candidates, key=lambda item: (-item[0], item[1]))[1]

    async def claim_outbox(
        self, *, owner: str, limit: int = 100
    ) -> tuple[SkillOutboxRecord, ...]:
        claimed: list[SkillOutboxRecord] = []
        async with self._lock:
            for outbox_id, record in self._outbox.items():
                if outbox_id in self._claimed_outbox:
                    continue
                self._claimed_outbox[outbox_id] = owner
                claimed.append(record)
                if len(claimed) >= limit:
                    break
        return tuple(claimed)

    async def complete_outbox(self, *, outbox_id: str, owner: str) -> None:
        async with self._lock:
            if self._claimed_outbox.get(outbox_id) == owner:
                self._outbox.pop(outbox_id, None)
                self._claimed_outbox.pop(outbox_id, None)

    async def fail_outbox(
        self, *, outbox_id: str, owner: str, safe_error_code: str
    ) -> None:
        del safe_error_code
        async with self._lock:
            if self._claimed_outbox.get(outbox_id) != owner:
                return
            record = self._outbox.get(outbox_id)
            if record is not None:
                self._outbox[outbox_id] = SkillOutboxRecord(
                    outbox_id=record.outbox_id,
                    tenant_id=record.tenant_id,
                    command_id=record.command_id,
                    event_type=record.event_type,
                    payload=record.payload,
                    attempt=record.attempt + 1,
                )
            self._claimed_outbox.pop(outbox_id, None)

    async def has_artifact_reference(
        self, tenant_id: str, artifact_id: str, version: int
    ) -> bool:
        return any(
            package.tenant_id == tenant_id
            and package.artifact_ref.artifact_id == artifact_id
            and package.artifact_ref.version == version
            and package.retention_status.value == "retained"
            for package in self._packages.values()
        )

    async def put_package(self, record: SkillPackageRecord) -> SkillPackageRecord:
        key = _package_key(record)
        existing = self._packages.get(key)
        if existing is not None:
            if existing.package_digest != record.package_digest:
                raise VersionConflictError("Skill version is immutable")
            return existing
        digest_owner = next(
            (
                item
                for item in self._packages.values()
                if item.tenant_id == record.tenant_id
                and item.package_digest == record.package_digest
            ),
            None,
        )
        if digest_owner is not None and _package_key(digest_owner) != key:
            raise VersionConflictError("Skill package digest belongs to another version")
        self._packages[key] = record
        return record

    async def get_package(
        self, tenant_id: str, publisher: str, name: str, version: str
    ) -> SkillPackageRecord | None:
        return self._packages.get((tenant_id, publisher, name, version))

    async def list_packages(self, tenant_id: str) -> tuple[SkillPackageRecord, ...]:
        return tuple(
            sorted(
                (
                    record
                    for record in self._packages.values()
                    if record.tenant_id == tenant_id
                ),
                key=lambda item: (
                    item.manifest.publisher,
                    item.manifest.name,
                    item.manifest.version,
                ),
            )
        )

    async def update_package_retention(
        self, record: SkillPackageRecord, *, expected_revision: int
    ) -> SkillPackageRecord:
        key = _package_key(record)
        existing = self._packages.get(key)
        if existing is None:
            raise NotFoundError("Skill package was not found")
        if existing.package_digest != record.package_digest:
            raise VersionConflictError("Skill version is immutable")
        if existing.retention_revision != expected_revision:
            raise VersionConflictError("Skill package retention revision conflict")
        if record.retention_revision != expected_revision + 1:
            raise VersionConflictError("Skill package retention next revision is invalid")
        self._packages[key] = record
        return record

    async def put_publication(
        self, record: SkillPublicationRecord, *, expected_revision: int
    ) -> SkillPublicationRecord:
        key = _publication_key(record)
        existing = self._publications.get(key)
        _validate_revision(existing, record.revision, expected_revision, "publication")
        if existing is not None and existing.publication_id != record.publication_id:
            raise VersionConflictError("Skill publication identity is immutable")
        package = self._packages.get(key)
        if package is None:
            raise NotFoundError("Skill package was not found")
        if (
            record.source_id is not None
            and (record.tenant_id, record.source_id) not in self._sources
        ):
            raise NotFoundError("Skill Source was not found")
        if package.package_digest != record.package_digest:
            raise VersionConflictError("Skill publication package digest mismatch")
        if existing is not None and existing.package_digest != record.package_digest:
            raise VersionConflictError("Published Skill version is immutable")
        self._publications[key] = record
        return record

    async def get_publication(
        self, tenant_id: str, publisher: str, name: str, version: str
    ) -> SkillPublicationRecord | None:
        return self._publications.get((tenant_id, publisher, name, version))

    async def list_publications(
        self, tenant_id: str
    ) -> tuple[SkillPublicationRecord, ...]:
        return tuple(
            sorted(
                (
                    record
                    for record in self._publications.values()
                    if record.tenant_id == tenant_id
                ),
                key=lambda item: (item.publisher, item.name, item.version),
            )
        )

    async def list_tenants(self) -> tuple[str, ...]:
        return tuple(
            sorted(
                {
                    *(
                        record.tenant_id
                        for record in self._publications.values()
                    ),
                    *(
                        record.tenant_id
                        for record in self._installations.values()
                    ),
                }
            )
        )

    async def put_installation(
        self, record: SkillInstallationRecord, *, expected_revision: int
    ) -> SkillInstallationRecord:
        key = _installation_key(record)
        existing = self._installations.get(key)
        _validate_revision(existing, record.revision, expected_revision, "installation")
        if existing is not None and existing.installation_id != record.installation_id:
            raise VersionConflictError("Skill installation identity is immutable")
        if (
            record.source_id is not None
            and (record.tenant_id, record.source_id) not in self._sources
        ):
            raise NotFoundError("Skill Source was not found")
        self._installations[key] = record
        return record

    async def get_installation(
        self, tenant_id: str, publisher: str, name: str
    ) -> SkillInstallationRecord | None:
        return self._installations.get((tenant_id, publisher, name))

    async def commit_installation_change(
        self, commit: SkillInstallationCommit
    ) -> SkillInstallationRecord:
        command_key = (commit.installation.tenant_id, commit.command_id)
        async with self._lock:
            replay = self._installation_commands.get(command_key)
            if replay is not None:
                request_digest, installation_key = replay
                if request_digest != commit.request_digest:
                    raise VersionConflictError("Skill installation command id was reused")
                current = self._installations.get(installation_key)
                if current is None:
                    raise VersionConflictError(
                        "Skill installation command result is incomplete"
                    )
                return current
            result = await self.put_installation(
                commit.installation,
                expected_revision=commit.expected_revision,
            )
            self._installation_commands[command_key] = (
                commit.request_digest,
                _installation_key(result),
            )
            return result

    async def list_installations(
        self, tenant_id: str
    ) -> tuple[SkillInstallationRecord, ...]:
        return tuple(
            sorted(
                (
                    record
                    for record in self._installations.values()
                    if record.tenant_id == tenant_id
                ),
                key=lambda item: (item.publisher, item.name),
            )
        )

    async def put_source(
        self, record: SkillSourceRecord, *, expected_revision: int
    ) -> SkillSourceRecord:
        key = (record.tenant_id, record.source_id)
        existing = self._sources.get(key)
        _validate_revision(existing, record.revision, expected_revision, "source")
        if existing is not None and existing.kind is not record.kind:
            raise VersionConflictError("Skill Source kind is immutable")
        self._sources[key] = record
        return record

    async def commit_source_config(
        self, commit: SkillSourceConfigCommit
    ) -> SkillSourceRecord:
        key = (commit.source.tenant_id, commit.command_id)
        async with self._lock:
            replay = self._source_config_commands.get(key)
            if replay is not None:
                request_digest, source_id = replay
                if request_digest != commit.request_digest:
                    raise VersionConflictError("Skill Source command id was reused")
                current = self._sources.get((commit.source.tenant_id, source_id))
                if current is None:
                    raise VersionConflictError("Skill Source command result is incomplete")
                return current
            result = await self.put_source(
                commit.source, expected_revision=commit.expected_revision
            )
            if result.desired_state is SkillSourceDesiredState.RETIRED:
                for reference_key in tuple(self._publication_sources):
                    if (
                        reference_key[0] == result.tenant_id
                        and reference_key[4] == result.source_id
                    ):
                        self._publication_sources[reference_key] = False
            affected_publications = {
                reference_key[:4]
                for reference_key in self._publication_sources
                if reference_key[0] == result.tenant_id
                and reference_key[4] == result.source_id
            }
            for publication_key in affected_publications:
                publication = self._publications.get(publication_key)
                if publication is None or publication.status in {
                    SkillPublicationStatus.RETIRED,
                    SkillPublicationStatus.REVOKED,
                }:
                    continue
                selected = self._select_publication_source(publication)
                if selected == publication.source_id:
                    continue
                self._publications[publication_key] = publication.model_copy(
                    update={
                        "source_id": selected or publication.source_id,
                        "status": (
                            publication.status
                            if selected is not None
                            else SkillPublicationStatus.RETIRED
                        ),
                        "revision": publication.revision + 1,
                        "updated_by": commit.actor_id,
                        "updated_at": commit.occurred_at,
                        "reason_code": (
                            publication.reason_code
                            if selected is not None
                            else commit.reason_code or "source_unavailable"
                        ),
                    }
                )
            self._source_config_commands[key] = (
                commit.request_digest,
                result.source_id,
            )
            return result

    async def get_source(
        self, tenant_id: str, source_id: str
    ) -> SkillSourceRecord | None:
        return self._sources.get((tenant_id, source_id))

    async def list_sources(self, tenant_id: str) -> tuple[SkillSourceRecord, ...]:
        return tuple(
            source
            for (source_tenant, _source_id), source in sorted(
                self._sources.items(), key=lambda item: item[0]
            )
            if source_tenant == tenant_id
        )

    async def put_sync_state(self, state: SkillSourceSyncState) -> None:
        key = (state.tenant_id, state.source_id)
        if key not in self._sources:
            raise NotFoundError("Skill Source was not found")
        existing = self._sync_states.get(key)
        if existing is not None and state.generation < existing.generation:
            raise VersionConflictError("Skill Source generation cannot move backwards")
        self._sync_states[key] = state

    async def get_sync_state(
        self, tenant_id: str, source_id: str
    ) -> SkillSourceSyncState | None:
        return self._sync_states.get((tenant_id, source_id))

    async def claim_source_lease(
        self, *, tenant_id: str, source_id: str, owner: str, ttl: timedelta
    ) -> SkillSourceLease | None:
        key = (tenant_id, source_id)
        async with self._lock:
            if key not in self._sources:
                raise NotFoundError("Skill Source was not found")
            now = datetime.now(UTC)
            current = self._source_leases.get(key)
            if current is not None and current.expires_at > now:
                return None
            lease = SkillSourceLease(
                tenant_id=tenant_id,
                source_id=source_id,
                owner=owner,
                fencing_token=1 if current is None else current.fencing_token + 1,
                expires_at=now + ttl,
            )
            self._source_leases[key] = lease
            return lease

    async def renew_source_lease(
        self, lease: SkillSourceLease, *, ttl: timedelta
    ) -> SkillSourceLease | None:
        async with self._lock:
            try:
                self._require_active_lease(lease)
            except VersionConflictError:
                return None
            renewed = SkillSourceLease(
                tenant_id=lease.tenant_id,
                source_id=lease.source_id,
                owner=lease.owner,
                fencing_token=lease.fencing_token,
                expires_at=datetime.now(UTC) + ttl,
            )
            self._source_leases[(lease.tenant_id, lease.source_id)] = renewed
            return renewed

    async def release_source_lease(self, lease: SkillSourceLease) -> None:
        async with self._lock:
            key = (lease.tenant_id, lease.source_id)
            current = self._source_leases.get(key)
            if current == lease:
                self._source_leases[key] = lease.__class__(
                    **{**lease.__dict__, "expires_at": datetime.now(UTC)}
                )

    async def put_sync_state_fenced(
        self, state: SkillSourceSyncState, *, lease: SkillSourceLease
    ) -> None:
        async with self._lock:
            self._require_active_lease(lease)
            key = (state.tenant_id, state.source_id)
            if key != (lease.tenant_id, lease.source_id):
                raise VersionConflictError("Skill Source lease scope mismatch")
            existing = self._sync_states.get(key)
            if existing is not None and state.generation < existing.generation:
                raise VersionConflictError("Skill Source generation cannot move backwards")
            self._sync_states[key] = state

    async def commit_source_snapshot(
        self, commit: SkillSourceSnapshotCommit
    ) -> SkillSourceSnapshotResult:
        if commit.missing_snapshot_threshold < 2:
            raise ValueError("Skill Source missing snapshot threshold must be at least two")
        async with self._lock:
            self._require_active_lease(commit.lease)
            state = commit.state
            if (state.tenant_id, state.source_id) != (
                commit.lease.tenant_id,
                commit.lease.source_id,
            ):
                raise VersionConflictError("Skill Source lease scope mismatch")
            existing_state = self._sync_states.get(
                (state.tenant_id, state.source_id)
            )
            if existing_state is not None and state.generation <= existing_state.generation:
                raise VersionConflictError(
                    "Skill Source complete snapshot generation must advance"
                )
            observed = set(commit.observed)
            retired: list[SkillPublicationRecord] = []
            now = commit.occurred_at
            for key, publication in tuple(self._publications.items()):
                if (
                    publication.tenant_id != state.tenant_id
                    or (
                            state.tenant_id,
                            publication.publisher,
                            publication.name,
                            publication.version,
                            state.source_id,
                        ) not in self._publication_sources
                    or publication.status in {
                        SkillPublicationStatus.RETIRED,
                        SkillPublicationStatus.REVOKED,
                    }
                ):
                    continue
                identity = SkillSourcePackageIdentity(
                    publication.publisher,
                    publication.name,
                    publication.version,
                )
                inventory_key = (
                    state.tenant_id,
                    state.source_id,
                    identity.publisher,
                    identity.name,
                    identity.version,
                )
                if identity in observed:
                    self._source_inventory[inventory_key] = (state.generation, 0)
                    self._publication_sources[
                        (
                            state.tenant_id,
                            publication.publisher,
                            publication.name,
                            publication.version,
                            state.source_id,
                        )
                    ] = True
                    continue
                previous = self._source_inventory.get(inventory_key)
                missing = 1 if previous is None else previous[1] + 1
                self._source_inventory[inventory_key] = (state.generation, missing)
                if missing < commit.missing_snapshot_threshold:
                    continue
                self._publication_sources[
                    (
                        state.tenant_id,
                        publication.publisher,
                        publication.name,
                        publication.version,
                        state.source_id,
                    )
                ] = False
                selected_source = self._select_publication_source(publication)
                if selected_source is not None:
                    if selected_source != publication.source_id:
                        self._publications[key] = publication.model_copy(
                            update={
                                "source_id": selected_source,
                                "revision": publication.revision + 1,
                                "updated_by": commit.actor_id,
                                "updated_at": now,
                            }
                        )
                    continue
                command_id = _retirement_command_id(commit.command_prefix, identity)
                command_key = (state.tenant_id, command_id)
                if command_key in self._source_retirement_commands:
                    continue
                updated = publication.model_copy(
                    update={
                        "status": SkillPublicationStatus.RETIRED,
                        "revision": publication.revision + 1,
                        "updated_by": commit.actor_id,
                        "updated_at": now,
                        "reason_code": "source_missing_confirmed",
                    }
                )
                self._publications[key] = updated
                self._source_retirement_commands.add(command_key)
                retired.append(updated)
            self._sync_states[(state.tenant_id, state.source_id)] = state
            return SkillSourceSnapshotResult(tuple(retired))

    async def commit_restore(
        self, commit: SkillRestoreCommit
    ) -> SkillPublicationRecord:
        tenant_id = commit.publication.tenant_id
        command_key = (tenant_id, commit.command_id)
        publication_key = _publication_key(commit.publication)
        async with self._lock:
            replay = self._restore_commands.get(command_key)
            if replay is not None:
                if replay != commit.request_digest:
                    raise VersionConflictError(
                        "Skill restore command id was reused"
                    )
                current = self._publications.get(publication_key)
                if current is None:
                    raise VersionConflictError(
                        "Skill restore command result is incomplete"
                    )
                return current
            current = self._publications.get(publication_key)
            if current is None:
                raise NotFoundError("Skill publication was not found")
            if current.revision != commit.expected_revision:
                raise VersionConflictError("Skill publication revision conflict")
            if current.status is not SkillPublicationStatus.RETIRED:
                raise VersionConflictError("Skill publication is not retired")
            updated = commit.publication
            if (
                updated.status is not SkillPublicationStatus.RESTORING
                or updated.revision != current.revision + 1
                or updated.package_digest != current.package_digest
                or updated.publication_id != current.publication_id
            ):
                raise VersionConflictError("Skill restore transition is invalid")
            self._publications[publication_key] = updated
            self._restore_commands[command_key] = commit.request_digest
            return updated

    def _require_active_lease(self, lease: SkillSourceLease) -> None:
        current = self._source_leases.get((lease.tenant_id, lease.source_id))
        if (
            current != lease
            or current is None
            or current.expires_at <= datetime.now(UTC)
        ):
            raise VersionConflictError("Skill Source lease is stale")


def _package_key(record: SkillPackageRecord) -> PackageKey:
    manifest = record.manifest
    return record.tenant_id, manifest.publisher, manifest.name, manifest.version


def _retirement_command_id(
    prefix: str, identity: SkillSourcePackageIdentity
) -> str:
    value = f"{identity.publisher}\0{identity.name}\0{identity.version}"
    return f"{prefix}:{hashlib.sha256(value.encode()).hexdigest()}"


def _publication_key(record: SkillPublicationRecord) -> PublicationKey:
    return record.tenant_id, record.publisher, record.name, record.version


def _installation_key(record: SkillInstallationRecord) -> InstallationKey:
    return record.tenant_id, record.publisher, record.name


def _validate_revision(
    existing: (
        SkillPublicationRecord | SkillInstallationRecord | SkillSourceRecord | None
    ),
    revision: int,
    expected_revision: int,
    label: str,
) -> None:
    current = 0 if existing is None else existing.revision
    if current != expected_revision:
        raise VersionConflictError(f"Skill {label} revision conflict")
    if revision != expected_revision + 1:
        raise VersionConflictError(f"Skill {label} next revision is invalid")


def _publish_outbox_payload(
    result: SkillPublishCommitResult,
) -> dict[str, object]:
    package = result.package
    publication = result.publication
    return {
        "publisher": publication.publisher,
        "name": publication.name,
        "version": publication.version,
        "package_digest": publication.package_digest,
        "publication_status": publication.status.value,
        "artifact_ref": package.artifact_ref.as_dict(),
    }
