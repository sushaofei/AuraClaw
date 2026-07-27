from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Protocol

from auraclaw.action.ports import CapabilityCatalogStore, HandsExecutor
from auraclaw.contracts.capabilities import (
    CapabilityDescriptor,
    CapabilityKind,
    CapabilityStatus,
    McpServerDefinition,
)
from auraclaw.contracts.skills import SkillBinding
from auraclaw.contracts.tools import (
    RiskLevel,
    ToolCapability,
    ToolInvocation,
    ToolPermission,
)

CAPABILITY_SEARCH_TOOL_NAME = "auraclaw.capabilities.search"
CAPABILITY_LOAD_TOOL_NAME = "auraclaw.capabilities.load"
SKILL_RESOLVE_TOOL_NAME = "auraclaw.skills.resolve"
_TOKEN_PATTERN = re.compile(r"[A-Za-z0-9_.-]+")


class SkillCatalogSource(Protocol):
    def capability_descriptors(
        self, tenant_id: str
    ) -> tuple[CapabilityDescriptor, ...]: ...

    def get_capability(
        self, tenant_id: str, capability_id: str
    ) -> CapabilityDescriptor | None: ...


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
        subject: str = "agent-runtime",
        correlation_id: str = "skill.resolve",
        active_skill_names: tuple[str, ...] = (),
    ) -> SkillBinding: ...


class InMemoryCapabilityCatalogStore:
    def __init__(self) -> None:
        self._servers: dict[str, McpServerDefinition] = {}
        self._capabilities: dict[str, CapabilityDescriptor] = {}

    async def upsert_server(self, server: McpServerDefinition) -> None:
        self._servers[server.server_id] = server

    async def get_server(self, server_id: str) -> McpServerDefinition | None:
        return self._servers.get(server_id)

    async def list_servers(self, tenant_id: str) -> tuple[McpServerDefinition, ...]:
        return tuple(
            server
            for server in sorted(self._servers.values(), key=lambda item: item.server_id)
            if server.tenant_id is None or server.tenant_id == tenant_id
        )

    async def replace_capabilities(
        self,
        server_id: str,
        capabilities: tuple[CapabilityDescriptor, ...],
    ) -> None:
        self._capabilities = {
            capability_id: capability
            for capability_id, capability in self._capabilities.items()
            if capability.server_id != server_id
        }
        self._capabilities.update(
            {capability.capability_id: capability for capability in capabilities}
        )

    async def list_capabilities(
        self, tenant_id: str
    ) -> tuple[CapabilityDescriptor, ...]:
        return tuple(
            capability
            for capability in sorted(
                self._capabilities.values(),
                key=lambda item: (item.canonical_name, item.version),
            )
            if (
                (server := self._servers.get(capability.server_id)) is not None
                and server.enabled
                and server.status
                in {CapabilityStatus.ACTIVE, CapabilityStatus.DEGRADED}
            )
            if capability.tenant_id is None or capability.tenant_id == tenant_id
        )

    async def get_capability(
        self, tenant_id: str, capability_id: str
    ) -> CapabilityDescriptor | None:
        capability = self._capabilities.get(capability_id)
        if capability is None:
            return None
        server = self._servers.get(capability.server_id)
        if (
            server is None
            or not server.enabled
            or server.status not in {CapabilityStatus.ACTIVE, CapabilityStatus.DEGRADED}
            or capability.tenant_id not in {None, tenant_id}
        ):
            return None
        return capability


class CapabilityCatalog:
    def __init__(self, store: CapabilityCatalogStore) -> None:
        self._store = store

    async def register_server(self, server: McpServerDefinition) -> None:
        await self._store.upsert_server(server)

    async def replace_server_capabilities(
        self,
        server_id: str,
        capabilities: tuple[CapabilityDescriptor, ...],
    ) -> None:
        server = await self._store.get_server(server_id)
        if server is None:
            raise ValueError(f"MCP server is not registered: {server_id}")
        for capability in capabilities:
            if capability.server_id != server_id:
                raise ValueError("Capability server_id does not match the publication")
            if capability.tenant_id != server.tenant_id:
                raise ValueError("Capability tenant does not match the MCP server")
            if capability.trust_level != server.trust_level:
                raise ValueError("Capability trust level does not match the MCP server")
        await self._store.replace_capabilities(server_id, capabilities)

    async def search(
        self,
        *,
        tenant_id: str,
        query: str = "",
        kinds: tuple[CapabilityKind, ...] = (),
        required_permissions: tuple[str, ...] = (),
        limit: int = 10,
    ) -> tuple[CapabilityDescriptor, ...]:
        if limit < 1 or limit > 50:
            raise ValueError("Capability search limit must be between 1 and 50")
        kind_filter = set(kinds)
        permission_filter = set(required_permissions)
        query_tokens = _tokens(query)
        ranked: list[tuple[int, CapabilityDescriptor]] = []
        for capability in await self._store.list_capabilities(tenant_id):
            if capability.status not in {
                CapabilityStatus.ACTIVE,
                CapabilityStatus.DEGRADED,
            }:
                continue
            if kind_filter and capability.kind not in kind_filter:
                continue
            if permission_filter and capability.permission not in permission_filter:
                continue
            score = _score(capability, query_tokens)
            if query_tokens and score == 0:
                continue
            ranked.append((score, capability))
        ranked.sort(
            key=lambda item: (
                -item[0],
                item[1].status == CapabilityStatus.DEGRADED,
                item[1].canonical_name,
                item[1].version,
            )
        )
        return tuple(capability for _score_value, capability in ranked[:limit])

    async def get(
        self, *, tenant_id: str, capability_id: str
    ) -> CapabilityDescriptor | None:
        capability = await self._store.get_capability(tenant_id, capability_id)
        if capability is None or capability.status not in {
            CapabilityStatus.ACTIVE,
            CapabilityStatus.DEGRADED,
        }:
            return None
        return capability


@dataclass(frozen=True)
class CapabilitySearchExecutor:
    catalog: CapabilityCatalog
    skills: SkillCatalogSource | None = None

    async def execute(
        self,
        invocation: ToolInvocation,
        capability: ToolCapability,
    ) -> dict[str, object]:
        del capability
        arguments = invocation.arguments
        kinds = tuple(CapabilityKind(str(value)) for value in arguments.get("kinds", ()))
        permissions = tuple(
            str(value) for value in arguments.get("required_permissions", ())
        )
        query = str(arguments.get("query", ""))
        results = list(
            await self.catalog.search(
                tenant_id=invocation.tenant_id,
                query=query,
                kinds=kinds,
                required_permissions=permissions,
                limit=50,
            )
        )
        if self.skills is not None and (
            not kinds or CapabilityKind.SKILL in kinds
        ):
            query_tokens = _tokens(query)
            for descriptor in self.skills.capability_descriptors(
                invocation.tenant_id
            ):
                if permissions and descriptor.permission not in permissions:
                    continue
                if query_tokens and _score(descriptor, query_tokens) == 0:
                    continue
                results.append(descriptor)
        query_tokens = _tokens(query)
        results.sort(
            key=lambda item: (
                -_score(item, query_tokens),
                item.status == CapabilityStatus.DEGRADED,
                item.canonical_name,
                item.version,
            )
        )
        limit = int(arguments.get("limit", 10))
        page = [descriptor.as_search_result() for descriptor in results[:limit]]
        payload: dict[str, object] = {"capabilities": page}
        if not page:
            payload["hint"] = (
                "No matching capabilities were found. Answer the user directly "
                "without calling capability tools again."
            )
        return payload


@dataclass(frozen=True)
class CapabilityLoadExecutor:
    catalog: CapabilityCatalog
    skills: SkillCatalogSource | None = None

    async def execute(
        self,
        invocation: ToolInvocation,
        capability: ToolCapability,
    ) -> dict[str, object]:
        del capability
        loaded: list[dict[str, Any]] = []
        raw_ids = tuple(invocation.arguments.get("capability_ids", ()))
        if len(raw_ids) > 8:
            raise ValueError("Capability load limit is 8")
        for raw_id in raw_ids:
            capability_id = str(raw_id)
            descriptor = await self.catalog.get(
                tenant_id=invocation.tenant_id,
                capability_id=capability_id,
            )
            if descriptor is None and self.skills is not None:
                descriptor = self.skills.get_capability(
                    invocation.tenant_id, capability_id
                )
            if descriptor is not None:
                loaded.append(_load_result(descriptor))
        return {"capabilities": loaded}


@dataclass(frozen=True)
class SkillResolveExecutor:
    resolver: SkillResolverPort

    async def execute(
        self,
        invocation: ToolInvocation,
        capability: ToolCapability,
    ) -> dict[str, object]:
        del capability
        arguments = invocation.arguments
        binding = await self.resolver.resolve(
            tenant_id=invocation.tenant_id,
            name=str(arguments["name"]),
            version=str(arguments.get("version", "*")),
            publisher=_optional(arguments.get("publisher")),
            role=str(arguments["role"]),
            policy_version=str(arguments.get("policy_version", "runtime")),
            subject=invocation.actor_id,
            correlation_id=invocation.run_id,
            active_skill_names=tuple(
                str(value) for value in arguments.get("active_skill_names", ())
            ),
        )
        dump = getattr(binding, "model_dump", None)
        if not callable(dump):
            raise TypeError("Skill resolver returned an invalid binding")
        return {"binding": dump(mode="json")}


class RoutedHandsExecutor:
    def __init__(
        self,
        default: HandsExecutor,
        routes: Mapping[str, HandsExecutor],
    ) -> None:
        self._default = default
        self._routes = dict(routes)

    async def execute(
        self,
        invocation: ToolInvocation,
        capability: ToolCapability,
    ) -> object:
        executor = self._routes.get(invocation.tool_name, self._default)
        return await executor.execute(invocation, capability)

    def replace_owner_routes(
        self,
        owner: str,
        routes: Mapping[str, HandsExecutor],
    ) -> None:
        prefix = f"{owner}:"
        if not routes:
            self._routes = {
                name: executor
                for name, executor in self._routes.items()
                if not getattr(executor, "route_owner", "").startswith(prefix)
            }
        self._routes.update(routes)


def capability_search_tool() -> ToolCapability:
    return ToolCapability(
        name=CAPABILITY_SEARCH_TOOL_NAME,
        version="1",
        description=(
            "Search the policy-visible AuraClaw capability catalog without loading "
            "full Resource or Skill content."
        ),
        input_schema={
            "type": "object",
            "properties": {
                "query": {"type": "string", "maxLength": 1024},
                "kinds": {
                    "type": "array",
                    "items": {
                        "type": "string",
                        "enum": [kind.value for kind in CapabilityKind],
                    },
                },
                "required_permissions": {
                    "type": "array",
                    "items": {"type": "string"},
                },
                "limit": {"type": "integer", "minimum": 1, "maximum": 50},
            },
            "additionalProperties": False,
        },
        output_schema={
            "type": "object",
            "properties": {
                "capabilities": {
                    "type": "array",
                    "items": {"type": "object"},
                },
                "hint": {"type": "string"},
            },
            "required": ["capabilities"],
            "additionalProperties": False,
        },
        permission=ToolPermission.READ_ONLY,
        risk_level=RiskLevel.LOW,
        runtime_location="hands",
        owner="platform",
    )


def capability_load_tool() -> ToolCapability:
    return ToolCapability(
        name=CAPABILITY_LOAD_TOOL_NAME,
        version="1",
        description=(
            "Load authoritative contracts for a bounded set of capability ids "
            "returned by capability search."
        ),
        input_schema={
            "type": "object",
            "properties": {
                "capability_ids": {
                    "type": "array",
                    "items": {"type": "string", "maxLength": 256},
                    "maxItems": 8,
                }
            },
            "required": ["capability_ids"],
            "additionalProperties": False,
        },
        output_schema={"type": "object"},
        permission=ToolPermission.READ_ONLY,
        risk_level=RiskLevel.LOW,
        runtime_location="hands",
        owner="platform",
    )


def skill_resolve_tool() -> ToolCapability:
    return ToolCapability(
        name=SKILL_RESOLVE_TOOL_NAME,
        version="1",
        description="Resolve an exact Skill binding for the trusted Agent Runtime.",
        input_schema={
            "type": "object",
            "properties": {
                "name": {"type": "string", "minLength": 1, "maxLength": 256},
                "version": {"type": "string", "maxLength": 128},
                "publisher": {"type": "string", "maxLength": 128},
                "role": {"type": "string", "minLength": 1, "maxLength": 64},
                "policy_version": {"type": "string", "maxLength": 128},
                "active_skill_names": {
                    "type": "array",
                    "items": {"type": "string", "maxLength": 256},
                },
            },
            "required": ["name", "role"],
            "additionalProperties": False,
        },
        output_schema={"type": "object"},
        permission=ToolPermission.READ_ONLY,
        risk_level=RiskLevel.LOW,
        runtime_location="hands",
        owner="platform-internal",
    )


def _load_result(descriptor: CapabilityDescriptor) -> dict[str, Any]:
    result = descriptor.as_search_result()
    raw_source = descriptor.metadata.get("source", {})
    source = dict(raw_source) if isinstance(raw_source, dict) else {}
    if descriptor.kind == CapabilityKind.TOOL:
        result["model_tool"] = {
            "type": "function",
            "function": {
                "name": descriptor.canonical_name,
                "description": descriptor.description,
                "parameters": source.get("inputSchema", {"type": "object"}),
            },
        }
    elif descriptor.kind == CapabilityKind.RESOURCE:
        result["resource"] = {"uri": source.get("uri")}
    elif descriptor.kind == CapabilityKind.RESOURCE_TEMPLATE:
        result["resource"] = {
            "uri_template": (
                descriptor.metadata.get("uri_template")
                or source.get("uriTemplate")
            )
        }
    elif descriptor.kind == CapabilityKind.SKILL:
        raw_contract = descriptor.metadata.get("model_contract", {})
        result["skill"] = (
            dict(raw_contract) if isinstance(raw_contract, dict) else {}
        )
    return result


def _optional(value: object) -> str | None:
    return None if value is None else str(value)


def _tokens(value: str) -> tuple[str, ...]:
    return tuple(token.casefold() for token in _TOKEN_PATTERN.findall(value))


def _score(
    capability: CapabilityDescriptor,
    query_tokens: tuple[str, ...],
) -> int:
    if not query_tokens:
        return 0
    name = capability.canonical_name.casefold()
    title = capability.title.casefold()
    tags = {tag.casefold() for tag in capability.tags}
    description = capability.description.casefold()
    score = 0
    for token in query_tokens:
        if token in name:
            score += 8
        if token in title:
            score += 5
        if token in tags:
            score += 3
        if token in description:
            score += 1
    return score
