from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import datetime
from typing import Protocol

from auraclaw.contracts.errors import NotFoundError, VersionConflictError
from auraclaw.contracts.skills import (
    SkillInstallationRecord,
    SkillPackageRecord,
    SkillPublicationRecord,
    SkillSourceRecord,
    SkillSourceSyncState,
)

PackageKey = tuple[str, str, str, str]
PublicationKey = tuple[str, str, str, str]
InstallationKey = tuple[str, str, str]
SourceKey = tuple[str, str]
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

    async def get_installation(
        self, tenant_id: str, publisher: str, name: str
    ) -> SkillInstallationRecord | None: ...

    async def list_installations(
        self, tenant_id: str
    ) -> tuple[SkillInstallationRecord, ...]: ...

    async def put_source(
        self, record: SkillSourceRecord, *, expected_revision: int
    ) -> SkillSourceRecord: ...

    async def get_source(
        self, tenant_id: str, source_id: str
    ) -> SkillSourceRecord | None: ...

    async def put_sync_state(self, state: SkillSourceSyncState) -> None: ...

    async def get_sync_state(
        self, tenant_id: str, source_id: str
    ) -> SkillSourceSyncState | None: ...


@dataclass
class InMemorySkillLifecycleStore:
    _packages: dict[PackageKey, SkillPackageRecord] = field(default_factory=dict)
    _publications: dict[PublicationKey, SkillPublicationRecord] = field(
        default_factory=dict
    )
    _installations: dict[InstallationKey, SkillInstallationRecord] = field(
        default_factory=dict
    )
    _sources: dict[SourceKey, SkillSourceRecord] = field(default_factory=dict)
    _sync_states: dict[SourceKey, SkillSourceSyncState] = field(default_factory=dict)
    _commands: dict[CommandKey, tuple[str, SkillPublishCommitResult]] = field(
        default_factory=dict
    )
    _outbox: dict[str, SkillOutboxRecord] = field(default_factory=dict)
    _claimed_outbox: dict[str, str] = field(default_factory=dict)
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    async def commit_publish(
        self, commit: SkillPublishCommit
    ) -> SkillPublishCommitResult:
        key = (commit.package.tenant_id, commit.command_id)
        async with self._lock:
            existing_command = self._commands.get(key)
            if existing_command is not None:
                request_digest, result = existing_command
                if request_digest != commit.request_digest:
                    raise VersionConflictError("Skill command id was reused")
                return SkillPublishCommitResult(
                    package=result.package,
                    publication=result.publication,
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

    async def get_source(
        self, tenant_id: str, source_id: str
    ) -> SkillSourceRecord | None:
        return self._sources.get((tenant_id, source_id))

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


def _package_key(record: SkillPackageRecord) -> PackageKey:
    manifest = record.manifest
    return record.tenant_id, manifest.publisher, manifest.name, manifest.version


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
