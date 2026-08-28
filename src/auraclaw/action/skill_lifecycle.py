from __future__ import annotations

from dataclasses import dataclass, field
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


class SkillLifecycleStore(Protocol):
    async def put_package(self, record: SkillPackageRecord) -> SkillPackageRecord: ...

    async def get_package(
        self, tenant_id: str, publisher: str, name: str, version: str
    ) -> SkillPackageRecord | None: ...

    async def put_publication(
        self, record: SkillPublicationRecord, *, expected_revision: int
    ) -> SkillPublicationRecord: ...

    async def get_publication(
        self, tenant_id: str, publisher: str, name: str, version: str
    ) -> SkillPublicationRecord | None: ...

    async def list_publications(
        self, tenant_id: str
    ) -> tuple[SkillPublicationRecord, ...]: ...

    async def put_installation(
        self, record: SkillInstallationRecord, *, expected_revision: int
    ) -> SkillInstallationRecord: ...

    async def get_installation(
        self, tenant_id: str, publisher: str, name: str
    ) -> SkillInstallationRecord | None: ...

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
