from __future__ import annotations

from datetime import datetime
from typing import Protocol

from auraclaw.action.capability_search import (
    SearchOutcome,
)
from auraclaw.contracts.capabilities import (
    CapabilityDescriptor,
    CapabilityKind,
)
from auraclaw.contracts.observability import MetricPoint
from auraclaw.contracts.skills import (
    SkillBinding,
    SkillInstallationRecord,
    SkillPackageRecord,
    SkillPublicationRecord,
    SkillPublisherKeyRecord,
    SkillPublisherRecord,
)

CAPABILITY_SEARCH_TOOL_NAME = "auraclaw.capabilities.search"
CAPABILITY_LOAD_TOOL_NAME = "auraclaw.capabilities.load"
SKILL_RESOLVE_TOOL_NAME = "auraclaw.skills.resolve"
SKILL_BINDING_STATUS_TOOL_NAME = "auraclaw.skills.binding-status"


class CapabilitySearchMetricWriter(Protocol):
    async def write_metric(self, metric: MetricPoint) -> None: ...


class SkillResolverPort(Protocol):
    async def resolve(
        self,
        *,
        tenant_id: str,
        name: str,
        version: str = "*",
        publisher: str | None = None,
        role: str,
        policy_version: str,
        assignment_role: str | None = None,
        subject: str = "agent-runtime",
        correlation_id: str = "skill.resolve",
        active_skill_names: tuple[str, ...] = (),
    ) -> SkillBinding: ...


class SkillPublicationReader(Protocol):
    async def get_publication(
        self, tenant_id: str, publisher: str, name: str, version: str
    ) -> SkillPublicationRecord | None: ...

    async def get_installation(
        self, tenant_id: str, publisher: str, name: str
    ) -> SkillInstallationRecord | None: ...

    async def get_package(
        self, tenant_id: str, publisher: str, name: str, version: str
    ) -> SkillPackageRecord | None: ...


class SkillPublisherSecurityReader(Protocol):
    async def get_publisher(
        self, tenant_id: str, publisher: str
    ) -> SkillPublisherRecord | None: ...

    async def get_key(
        self, tenant_id: str, publisher: str, key_id: str
    ) -> SkillPublisherKeyRecord | None: ...


class CapabilityAvailability(Protocol):
    async def is_available(self, tenant_id: str, capability: CapabilityDescriptor) -> bool: ...


class CatalogReader(Protocol):
    async def search(
        self,
        *,
        tenant_id: str,
        query: str = "",
        kinds: tuple[CapabilityKind, ...] = (),
        required_permissions: tuple[str, ...] = (),
        capability_id: str | None = None,
        canonical_name: str | None = None,
        server_id: str | None = None,
        limit: int = 10,
        actor_role: str | None = None,
        deadline: datetime | None = None,
    ) -> tuple[CapabilityDescriptor, ...]: ...

    async def search_with_evidence(
        self,
        *,
        tenant_id: str,
        query: str = "",
        kinds: tuple[CapabilityKind, ...] = (),
        required_permissions: tuple[str, ...] = (),
        capability_id: str | None = None,
        canonical_name: str | None = None,
        server_id: str | None = None,
        limit: int = 10,
        actor_role: str | None = None,
        deadline: datetime | None = None,
    ) -> SearchOutcome: ...

    async def get(self, *, tenant_id: str, capability_id: str) -> CapabilityDescriptor | None: ...
