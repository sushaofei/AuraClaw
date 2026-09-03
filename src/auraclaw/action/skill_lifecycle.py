from __future__ import annotations

import asyncio
import base64
import binascii
import json
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any, Protocol
from uuid import uuid4

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
    SkillRevocationAction,
    SkillUpgradeState,
)

PackageKey = tuple[str, str, str, str]
PublicationKey = tuple[str, str, str, str]
InstallationKey = tuple[str, str, str]
CommandKey = tuple[str, str]


@dataclass(frozen=True)
class SkillPublishCommit:
    command_id: str
    request_digest: str
    actor_id: str
    correlation_id: str
    causation_id: str
    expected_publication_revision: int
    package: SkillPackageRecord
    publication: SkillPublicationRecord
    installation: SkillInstallationRecord | None
    occurred_at: datetime
    replace_purged: bool = False
    upgrade: SkillUpgradeState | None = None
    expected_installation_revision: int | None = None


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
        raw = base64.b64decode(cursor + padding, altchars=b"-_", validate=True).decode()
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
    async def claim_upgrade(self, state: SkillUpgradeState, *, ttl: timedelta) -> str | None: ...

    async def renew_upgrade(
        self, state: SkillUpgradeState, token: str, *, ttl: timedelta
    ) -> bool: ...

    async def set_upgrade_phase(
        self, state: SkillUpgradeState, token: str, *, phase: str, reason: str | None = None
    ) -> bool: ...

    async def remove_replaced_package(
        self, state: SkillUpgradeState, token: str, package: SkillPackageRecord
    ) -> bool: ...

    async def get_publish_command_digest(self, tenant_id: str, command_id: str) -> str | None: ...

    async def get_upgrade(
        self, tenant_id: str, publisher: str, name: str
    ) -> SkillUpgradeState | None: ...

    async def list_pending_upgrades(self, *, limit: int = 100) -> tuple[SkillUpgradeState, ...]: ...

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

    async def delete_admissions_before(self, cutoff: datetime, *, limit: int = 1000) -> int: ...

    async def commit_publish(self, commit: SkillPublishCommit) -> SkillPublishCommitResult: ...

    async def claim_outbox(
        self, *, owner: str, limit: int = 100, claim_ttl: timedelta = timedelta(seconds=30)
    ) -> tuple[SkillOutboxRecord, ...]: ...

    async def renew_outbox(self, *, outbox_id: str, owner: str, claim_ttl: timedelta) -> bool: ...

    async def complete_outbox(self, *, outbox_id: str, owner: str) -> bool: ...

    async def fail_outbox(self, *, outbox_id: str, owner: str, safe_error_code: str) -> bool: ...

    async def has_artifact_reference(
        self, tenant_id: str, artifact_id: str, version: int
    ) -> bool: ...

    async def put_package(self, record: SkillPackageRecord) -> SkillPackageRecord: ...

    async def get_package(
        self, tenant_id: str, publisher: str, name: str, version: str
    ) -> SkillPackageRecord | None: ...

    async def list_packages(self, tenant_id: str) -> tuple[SkillPackageRecord, ...]: ...

    async def list_package_tombstones(
        self, tenant_id: str, publisher: str, name: str
    ) -> tuple[SkillPackageRecord, ...]: ...

    async def update_package_retention(
        self, record: SkillPackageRecord, *, expected_revision: int
    ) -> SkillPackageRecord: ...

    async def put_publication(
        self, record: SkillPublicationRecord, *, expected_revision: int
    ) -> SkillPublicationRecord: ...

    async def get_publication(
        self, tenant_id: str, publisher: str, name: str, version: str
    ) -> SkillPublicationRecord | None: ...

    async def list_publications(self, tenant_id: str) -> tuple[SkillPublicationRecord, ...]: ...

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

    async def list_installations(self, tenant_id: str) -> tuple[SkillInstallationRecord, ...]: ...

    async def commit_restore(self, commit: SkillRestoreCommit) -> SkillPublicationRecord: ...


@dataclass
class InMemorySkillLifecycleStore:
    _admission_audits: list[SkillAdmissionAuditRecord] = field(default_factory=list)
    _upgrades: dict[InstallationKey, SkillUpgradeState] = field(default_factory=dict)
    _upgrade_claims: dict[InstallationKey, tuple[str, int, datetime]] = field(default_factory=dict)
    _packages: dict[PackageKey, SkillPackageRecord] = field(default_factory=dict)
    _package_tombstones: list[SkillPackageRecord] = field(default_factory=list)
    _publications: dict[PublicationKey, SkillPublicationRecord] = field(default_factory=dict)
    _installations: dict[InstallationKey, SkillInstallationRecord] = field(default_factory=dict)
    _restore_commands: dict[CommandKey, str] = field(default_factory=dict)
    _installation_commands: dict[CommandKey, tuple[str, InstallationKey]] = field(
        default_factory=dict
    )
    _commands: dict[CommandKey, tuple[str, SkillPublishCommitResult | None]] = field(
        default_factory=dict
    )
    _outbox: dict[str, SkillOutboxRecord] = field(default_factory=dict)
    _claimed_outbox: dict[str, str] = field(default_factory=dict)
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    async def claim_upgrade(self, state: SkillUpgradeState, *, ttl: timedelta) -> str | None:
        async with self._lock:
            key = (state.tenant_id, state.publisher, state.name)
            current = self._upgrades.get(key)
            claim = self._upgrade_claims.get(key)
            now = datetime.now(UTC)
            if (
                current is None
                or current.generation != state.generation
                or current.phase == "completed"
                or (claim is not None and claim[2] > now)
            ):
                return None
            token = uuid4().hex
            self._upgrade_claims[key] = (token, state.generation, now + ttl)
            return token

    def _owns_upgrade(self, state: SkillUpgradeState, token: str) -> bool:
        key = (state.tenant_id, state.publisher, state.name)
        current, claim = self._upgrades.get(key), self._upgrade_claims.get(key)
        return bool(
            current
            and current.generation == state.generation
            and claim
            and claim[:2] == (token, state.generation)
            and claim[2] > datetime.now(UTC)
        )

    async def renew_upgrade(self, state: SkillUpgradeState, token: str, *, ttl: timedelta) -> bool:
        async with self._lock:
            if not self._owns_upgrade(state, token):
                return False
            self._upgrade_claims[(state.tenant_id, state.publisher, state.name)] = (
                token,
                state.generation,
                datetime.now(UTC) + ttl,
            )
            return True

    async def set_upgrade_phase(
        self, state: SkillUpgradeState, token: str, *, phase: str, reason: str | None = None
    ) -> bool:
        if phase not in {"draining", "deleting", "completed", "blocked"}:
            raise ValueError("Invalid Skill upgrade phase")
        async with self._lock:
            if not self._owns_upgrade(state, token):
                return False
            key = (state.tenant_id, state.publisher, state.name)
            self._upgrades[key] = self._upgrades[key].model_copy(
                update={"phase": phase, "reason_code": reason, "updated_at": datetime.now(UTC)}
            )
            if phase != "deleting":
                self._upgrade_claims.pop(key, None)
            return True

    async def remove_replaced_package(
        self, state: SkillUpgradeState, token: str, package: SkillPackageRecord
    ) -> bool:
        async with self._lock:
            if not self._owns_upgrade(state, token) or not is_replaced_package(state, package):
                return False
            if package.legal_hold:
                return False
            key = _package_key(package)
            current = self._packages.get(key)
            installation = self._installations.get(key[:3])
            if installation and installation.pinned_package_digest == package.package_digest:
                return False
            if current and current.package_digest == package.package_digest:
                publication = self._publications.get(key)
                if publication and (
                    publication.status is not SkillPublicationStatus.REVOKED
                    or publication.revocation_action is not SkillRevocationAction.CANCEL
                ):
                    return False
                if current.legal_hold:
                    return False
                self._publications.pop(key, None)
                self._packages.pop(key, None)
            self._package_tombstones = [
                p
                for p in self._package_tombstones
                if not (_package_key(p) == key and p.package_digest == package.package_digest)
            ]
            removed_commands = set()
            for command_key, (digest, result) in tuple(self._commands.items()):
                if (
                    result
                    and _package_key(result.package) == key
                    and result.package.package_digest == package.package_digest
                ):
                    self._commands[command_key] = (digest, None)
                    removed_commands.add(command_key)
            for outbox_id, outbox in tuple(self._outbox.items()):
                if (outbox.tenant_id, outbox.command_id) in removed_commands:
                    self._outbox.pop(outbox_id, None)
                    self._claimed_outbox.pop(outbox_id, None)
            self._admission_audits = [
                a
                for a in self._admission_audits
                if not (
                    a.tenant_id == package.tenant_id
                    and a.publisher == state.publisher
                    and a.name == state.name
                    and a.package_digest == package.package_digest
                )
            ]
            return True

    async def get_publish_command_digest(self, tenant_id: str, command_id: str) -> str | None:
        command = self._commands.get((tenant_id, command_id))
        return command[0] if command is not None else None

    async def get_upgrade(
        self, tenant_id: str, publisher: str, name: str
    ) -> SkillUpgradeState | None:
        return self._upgrades.get((tenant_id, publisher, name))

    async def list_pending_upgrades(self, *, limit: int = 100) -> tuple[SkillUpgradeState, ...]:
        return tuple(item for item in self._upgrades.values() if item.phase != "completed")[:limit]

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
            if record.tenant_id != tenant_id or (since is not None and record.occurred_at < since):
                continue
            grouped.setdefault((record.outcome, record.content_policy_version), []).append(
                record.duration_ms
            )
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
                and (cursor_key is None or (record.occurred_at, record.admission_id) < cursor_key)
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

    async def delete_admissions_before(self, cutoff: datetime, *, limit: int = 1000) -> int:
        async with self._lock:
            candidates = sorted(
                (record for record in self._admission_audits if record.occurred_at < cutoff),
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

    async def commit_publish(self, commit: SkillPublishCommit) -> SkillPublishCommitResult:
        key = (commit.package.tenant_id, commit.command_id)
        async with self._lock:
            existing_command = self._commands.get(key)
            if existing_command is not None:
                request_digest, result = existing_command
                if request_digest != commit.request_digest:
                    raise VersionConflictError("Skill command id was reused")
                if result is None:
                    raise VersionConflictError("Published Skill version was removed")
                current_publication = self._publications.get(_publication_key(result.publication))
                return SkillPublishCommitResult(
                    package=result.package,
                    publication=current_publication or result.publication,
                    installation=result.installation,
                    replayed=True,
                )
            if commit.upgrade is not None:
                validate_upgrade(
                    commit,
                    tuple(self._publications.values()),
                    self._installations.get(
                        (
                            commit.package.tenant_id,
                            commit.package.manifest.publisher,
                            commit.package.manifest.name,
                        )
                    ),
                )
            packages = dict(self._packages)
            package_tombstones = list(self._package_tombstones)
            publications = dict(self._publications)
            installations = dict(self._installations)
            try:
                if commit.replace_purged:
                    package, publication, installation = self._replace_purged(commit)
                else:
                    package = await self.put_package(commit.package)
                    publication_key = _publication_key(commit.publication)
                    current_publication = self._publications.get(publication_key)
                    if (
                        current_publication is not None
                        and commit.publication.revision == commit.expected_publication_revision
                        and current_publication.package_digest == commit.publication.package_digest
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
                        elif commit.upgrade is not None:
                            installation = await self.put_installation(
                                commit.installation,
                                expected_revision=commit.expected_installation_revision or 0,
                            )
                        elif (
                            current.status is not commit.installation.status
                            or current.pinned_package_digest
                            != commit.installation.pinned_package_digest
                        ):
                            raise VersionConflictError("Skill installation revision conflict")
                        else:
                            installation = current
            except Exception:
                self._packages = packages
                self._package_tombstones = package_tombstones
                self._publications = publications
                self._installations = installations
                raise
            if commit.upgrade is not None:
                for old_key, old in tuple(self._publications.items()):
                    if (
                        old_key[:3] == _publication_key(publication)[:3]
                        and old.version != publication.version
                    ):
                        if old.status is SkillPublicationStatus.ACTIVE:
                            self._publications[old_key] = superseded_publication(old, commit)
                self._upgrades[
                    (package.tenant_id, package.manifest.publisher, package.manifest.name)
                ] = commit.upgrade
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

    def _replace_purged(
        self, commit: SkillPublishCommit
    ) -> tuple[
        SkillPackageRecord,
        SkillPublicationRecord,
        SkillInstallationRecord | None,
    ]:
        key = _package_key(commit.package)
        existing_package = self._packages.get(key)
        existing_publication = self._publications.get(key)
        installation_key = (
            commit.package.tenant_id,
            commit.package.manifest.publisher,
            commit.package.manifest.name,
        )
        existing_installation = self._installations.get(installation_key)
        if (
            commit.package.retention_status.value != "retained"
            or commit.publication.status is not SkillPublicationStatus.ACTIVE
            or commit.publication.package_digest != commit.package.package_digest
            or commit.installation is None
            or commit.installation.status.value != "active"
            or commit.installation.pinned_package_digest != commit.package.package_digest
            or existing_package is None
            or existing_package.retention_status.value != "purged"
            or existing_package.legal_hold
            or existing_publication is None
            or existing_publication.status is not SkillPublicationStatus.REVOKED
            or existing_publication.revision != commit.expected_publication_revision
            or existing_installation is None
            or existing_installation.status.value != "uninstalled"
            or existing_installation.revision != commit.expected_installation_revision
        ):
            raise VersionConflictError("Purged Skill replacement preconditions changed")
        if commit.publication.revision != existing_publication.revision + 1:
            raise VersionConflictError("Skill publication next revision is invalid")
        if commit.installation.revision != existing_installation.revision + 1:
            raise VersionConflictError("Skill installation next revision is invalid")
        self._package_tombstones.append(existing_package)
        self._packages[key] = commit.package
        self._publications[key] = commit.publication
        self._installations[installation_key] = commit.installation
        return commit.package, commit.publication, commit.installation

    async def claim_outbox(
        self, *, owner: str, limit: int = 100, claim_ttl: timedelta = timedelta(seconds=30)
    ) -> tuple[SkillOutboxRecord, ...]:
        del claim_ttl
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

    async def renew_outbox(self, *, outbox_id: str, owner: str, claim_ttl: timedelta) -> bool:
        del claim_ttl
        async with self._lock:
            return self._claimed_outbox.get(outbox_id) == owner

    async def complete_outbox(self, *, outbox_id: str, owner: str) -> bool:
        async with self._lock:
            if self._claimed_outbox.get(outbox_id) == owner:
                self._outbox.pop(outbox_id, None)
                self._claimed_outbox.pop(outbox_id, None)
                return True
            return False

    async def fail_outbox(self, *, outbox_id: str, owner: str, safe_error_code: str) -> bool:
        del safe_error_code
        async with self._lock:
            if self._claimed_outbox.get(outbox_id) != owner:
                return False
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
            return True

    async def has_artifact_reference(self, tenant_id: str, artifact_id: str, version: int) -> bool:
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
                (record for record in self._packages.values() if record.tenant_id == tenant_id),
                key=lambda item: (
                    item.manifest.publisher,
                    item.manifest.name,
                    item.manifest.version,
                ),
            )
        )

    async def list_package_tombstones(
        self, tenant_id: str, publisher: str, name: str
    ) -> tuple[SkillPackageRecord, ...]:
        return tuple(
            item
            for item in self._package_tombstones
            if item.tenant_id == tenant_id
            and item.manifest.publisher == publisher
            and item.manifest.name == name
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

    async def list_publications(self, tenant_id: str) -> tuple[SkillPublicationRecord, ...]:
        return tuple(
            sorted(
                (record for record in self._publications.values() if record.tenant_id == tenant_id),
                key=lambda item: (item.publisher, item.name, item.version),
            )
        )

    async def list_tenants(self) -> tuple[str, ...]:
        return tuple(
            sorted(
                {
                    *(record.tenant_id for record in self._publications.values()),
                    *(record.tenant_id for record in self._installations.values()),
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
                    raise VersionConflictError("Skill installation command result is incomplete")
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

    async def list_installations(self, tenant_id: str) -> tuple[SkillInstallationRecord, ...]:
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

    async def commit_restore(self, commit: SkillRestoreCommit) -> SkillPublicationRecord:
        tenant_id = commit.publication.tenant_id
        command_key = (tenant_id, commit.command_id)
        publication_key = _publication_key(commit.publication)
        async with self._lock:
            replay = self._restore_commands.get(command_key)
            if replay is not None:
                if replay != commit.request_digest:
                    raise VersionConflictError("Skill restore command id was reused")
                current = self._publications.get(publication_key)
                if current is None:
                    raise VersionConflictError("Skill restore command result is incomplete")
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


def _package_key(record: SkillPackageRecord) -> PackageKey:
    manifest = record.manifest
    return record.tenant_id, manifest.publisher, manifest.name, manifest.version


def _publication_key(record: SkillPublicationRecord) -> PublicationKey:
    return record.tenant_id, record.publisher, record.name, record.version


def _installation_key(record: SkillInstallationRecord) -> InstallationKey:
    return record.tenant_id, record.publisher, record.name


def _validate_revision(
    existing: (SkillPublicationRecord | SkillInstallationRecord | None),
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


def skill_version_key(version: str) -> tuple[Any, ...]:
    core, _, prerelease = version.partition("-")
    return (
        *tuple(int(part) for part in core.split(".")),
        not bool(prerelease),
        tuple(
            (0, int(part)) if part.isdigit() else (1, part)
            for part in prerelease.split(".")
            if part
        ),
    )


def validate_upgrade(
    commit: SkillPublishCommit,
    publications: tuple[SkillPublicationRecord, ...],
    installation: SkillInstallationRecord | None,
) -> None:
    target = commit.package.manifest
    if installation is None or installation.revision != commit.expected_installation_revision:
        raise VersionConflictError("Skill installation revision conflict")
    if commit.installation is None or commit.installation.revision != installation.revision + 1:
        raise VersionConflictError("Skill installation next revision is invalid")
    for current in publications:
        if (current.tenant_id, current.publisher, current.name) != (
            commit.package.tenant_id,
            target.publisher,
            target.name,
        ):
            continue
        if current.status is not SkillPublicationStatus.STAGED and (
            skill_version_key(current.version) > skill_version_key(target.version)
        ):
            raise VersionConflictError("Skill upgrade cannot downgrade the current version")


def superseded_publication(
    old: SkillPublicationRecord, commit: SkillPublishCommit
) -> SkillPublicationRecord:
    return old.model_copy(
        update={
            "status": SkillPublicationStatus.REVOKED,
            "revocation_action": SkillRevocationAction.CONTINUE,
            "revocation_policy_version": "skill-upgrade-v1",
            "reason_code": "skill_version_replaced",
            "revision": old.revision + 1,
            "updated_by": commit.actor_id,
            "updated_at": commit.occurred_at,
        }
    )


def is_replaced_package(state: SkillUpgradeState, package: SkillPackageRecord) -> bool:
    return (
        package.tenant_id == state.tenant_id
        and package.manifest.publisher == state.publisher
        and package.manifest.name == state.name
        and package.package_digest != state.package_digest
        and skill_version_key(package.manifest.version) <= skill_version_key(state.current_version)
    )
