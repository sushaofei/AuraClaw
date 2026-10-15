from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from auraclaw.action.skill_admin_catalog import (
    SkillAdminCatalogQueryService,
    SkillCatalogQuery,
    semver_key,
)
from auraclaw.contracts.errors import NotFoundError
from auraclaw.contracts.skills import (
    PublishedSkill,
    SkillInstallationRecord,
    SkillPackageRecord,
    SkillPublicationRecord,
    SkillPublisherKeyRecord,
    SkillPublisherRecord,
    SkillUpgradeState,
)


class SkillManagementReader(Protocol):
    async def list_installations(self, tenant_id: str) -> tuple[SkillInstallationRecord, ...]: ...

    async def list_publications(self, tenant_id: str) -> tuple[SkillPublicationRecord, ...]: ...

    async def list_packages(self, tenant_id: str) -> tuple[SkillPackageRecord, ...]: ...

    async def get_installation(
        self, tenant_id: str, publisher: str, name: str
    ) -> SkillInstallationRecord: ...


class SkillPublisherReader(Protocol):
    async def list_publishers(
        self, tenant_id: str
    ) -> tuple[tuple[SkillPublisherRecord, tuple[SkillPublisherKeyRecord, ...]], ...]: ...


@dataclass(frozen=True)
class SkillAdminDetail:
    publication: PublishedSkill
    versions: tuple[str, ...]


@dataclass(frozen=True)
class SkillManagementView:
    installation: SkillInstallationRecord | None
    versions: tuple[tuple[SkillPublicationRecord, SkillPackageRecord | None], ...]
    upgrade: SkillUpgradeState | None


class SkillAdminManagementQueryService:
    """Own management selection, record filters and cross-record aggregation."""

    def __init__(self, reader: SkillManagementReader) -> None:
        self._reader = reader

    async def list_installations(
        self, tenant_id: str, *, status: str | None = None, publisher: str | None = None
    ) -> tuple[SkillInstallationRecord, ...]:
        return tuple(
            item
            for item in await self._reader.list_installations(tenant_id)
            if (not status or item.status.value == status)
            and (not publisher or item.publisher == publisher)
        )

    async def list_publications(
        self,
        tenant_id: str,
        *,
        status: str | None = None,
        publisher: str | None = None,
        name: str | None = None,
    ) -> tuple[SkillPublicationRecord, ...]:
        return tuple(
            item
            for item in await self._reader.list_publications(tenant_id)
            if (not status or item.status.value == status)
            and (not publisher or item.publisher == publisher)
            and (not name or item.name == name)
        )

    async def list_packages(
        self,
        tenant_id: str,
        *,
        retention_status: str | None = None,
        publisher: str | None = None,
        name: str | None = None,
        legal_hold: bool | None = None,
    ) -> tuple[SkillPackageRecord, ...]:
        return tuple(
            item
            for item in await self._reader.list_packages(tenant_id)
            if (not retention_status or item.retention_status.value == retention_status)
            and (not publisher or item.manifest.publisher == publisher)
            and (not name or item.manifest.name == name)
            and (legal_hold is None or item.legal_hold is legal_hold)
        )

    async def detail(
        self, tenant_id: str, publisher: str, name: str, catalog: SkillAdminCatalogQueryService
    ) -> SkillAdminDetail:
        records = [
            item
            for item in await self._reader.list_publications(tenant_id)
            if item.publisher == publisher and item.name == name
        ]
        if not records:
            raise NotFoundError("Skill publication not found")
        selected = next(
            (
                item
                for item in await catalog.list_latest(
                    tenant_id, SkillCatalogQuery(publisher=publisher)
                )
                if item.publication.manifest.name == name
            ),
            None,
        )
        if selected is None:
            raise NotFoundError("Skill publication not found")
        return SkillAdminDetail(
            selected.publication,
            tuple(sorted((item.version for item in records), key=semver_key, reverse=True)),
        )

    async def management_view(
        self, tenant_id: str, publisher: str, name: str
    ) -> SkillManagementView:
        publications = tuple(
            item
            for item in await self._reader.list_publications(tenant_id)
            if item.publisher == publisher and item.name == name
        )
        packages = {
            item.manifest.version: item
            for item in await self._reader.list_packages(tenant_id)
            if item.manifest.publisher == publisher and item.manifest.name == name
        }
        try:
            installation = await self._reader.get_installation(tenant_id, publisher, name)
        except NotFoundError:
            installation = None
        read_upgrade = getattr(self._reader, "get_upgrade_state", None)
        upgrade = await read_upgrade(tenant_id, publisher, name) if callable(read_upgrade) else None
        return SkillManagementView(
            installation,
            tuple((item, packages.get(item.version)) for item in publications),
            upgrade,
        )


class SkillAdminPublisherQueryService:
    def __init__(self, reader: SkillPublisherReader) -> None:
        self._reader = reader

    async def list_publishers(
        self, tenant_id: str, *, status: str | None = None, text: str | None = None
    ) -> tuple[tuple[SkillPublisherRecord, tuple[SkillPublisherKeyRecord, ...]], ...]:
        query = (text or "").casefold()
        return tuple(
            (record, keys)
            for record, keys in await self._reader.list_publishers(tenant_id)
            if (not status or record.status.value == status)
            and (not query or query in f"{record.publisher} {record.display_name}".casefold())
        )
