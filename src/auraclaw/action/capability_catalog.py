from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass

from auraclaw.action.ports import CapabilityCatalogStore, HandsExecutor
from auraclaw.contracts.capabilities import (
    CapabilityDescriptor,
    CapabilityKind,
    CapabilityStatus,
    McpServerDefinition,
)
from auraclaw.contracts.tools import (
    RiskLevel,
    ToolCapability,
    ToolInvocation,
    ToolPermission,
)

CAPABILITY_SEARCH_TOOL_NAME = "auraclaw.capabilities.search"
_TOKEN_PATTERN = re.compile(r"[A-Za-z0-9_.-]+")


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


@dataclass(frozen=True)
class CapabilitySearchExecutor:
    catalog: CapabilityCatalog

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
        results = await self.catalog.search(
            tenant_id=invocation.tenant_id,
            query=str(arguments.get("query", "")),
            kinds=kinds,
            required_permissions=permissions,
            limit=int(arguments.get("limit", 10)),
        )
        return {
            "capabilities": [
                descriptor.as_search_result() for descriptor in results
            ]
        }


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
                }
            },
            "required": ["capabilities"],
            "additionalProperties": False,
        },
        permission=ToolPermission.READ_ONLY,
        risk_level=RiskLevel.LOW,
        runtime_location="hands",
        owner="platform",
    )


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
