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
from auraclaw.contracts.skills import (
    SkillBinding,
    SkillInstallationRecord,
    SkillPackageRecord,
    SkillPublicationRecord,
    SkillPublicationStatus,
    SkillPublisherKeyRecord,
    SkillPublisherKeyStatus,
    SkillPublisherRecord,
    SkillPublisherStatus,
    SkillRevocationAction,
)
from auraclaw.contracts.tools import (
    RiskLevel,
    ToolCapability,
    ToolInvocation,
    ToolPermission,
)

CAPABILITY_SEARCH_TOOL_NAME = "auraclaw.capabilities.search"
CAPABILITY_LOAD_TOOL_NAME = "auraclaw.capabilities.load"
SKILL_RESOLVE_TOOL_NAME = "auraclaw.skills.resolve"
SKILL_BINDING_STATUS_TOOL_NAME = "auraclaw.skills.binding-status"
_LATIN_TOKEN_PATTERN = re.compile(r"[A-Za-z0-9_.-]+")
_CJK_RUN_PATTERN = re.compile(r"[\u3400-\u9FFF\uF900-\uFAFF]+")


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

    async def remove_server(self, server_id: str) -> None:
        self._servers.pop(server_id, None)
        self._capabilities = {
            capability_id: capability
            for capability_id, capability in self._capabilities.items()
            if capability.server_id != server_id
        }

    async def list_capabilities(self, tenant_id: str) -> tuple[CapabilityDescriptor, ...]:
        return tuple(
            capability
            for capability in sorted(
                self._capabilities.values(),
                key=lambda item: (item.canonical_name, item.version),
            )
            if (
                (server := self._servers.get(capability.server_id)) is not None
                and server.enabled
                and server.status in {CapabilityStatus.ACTIVE, CapabilityStatus.DEGRADED}
            )
            if capability.tenant_id is None or capability.tenant_id == tenant_id
        )

    async def list_server_capabilities(
        self, tenant_id: str, server_id: str
    ) -> tuple[CapabilityDescriptor, ...]:
        return tuple(
            capability
            for capability in sorted(
                self._capabilities.values(),
                key=lambda item: (item.canonical_name, item.version),
            )
            if capability.server_id == server_id
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

    async def remove_server(self, server_id: str) -> None:
        await self._store.remove_server(server_id)

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

    async def list_server_tools(
        self, *, tenant_id: str, server_id: str
    ) -> tuple[CapabilityDescriptor, ...]:
        return tuple(
            capability
            for capability in await self._store.list_server_capabilities(tenant_id, server_id)
            if capability.kind is CapabilityKind.TOOL
        )

    async def get(self, *, tenant_id: str, capability_id: str) -> CapabilityDescriptor | None:
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

    async def execute(
        self,
        invocation: ToolInvocation,
        capability: ToolCapability,
    ) -> dict[str, object]:
        del capability
        arguments = invocation.arguments
        kinds = tuple(CapabilityKind(str(value)) for value in arguments.get("kinds", ()))
        permissions = tuple(str(value) for value in arguments.get("required_permissions", ()))
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

    async def execute(
        self,
        invocation: ToolInvocation,
        capability: ToolCapability,
    ) -> dict[str, object]:
        del capability
        loaded: list[dict[str, Any]] = []
        raw_ids = tuple(invocation.arguments.get("capability_ids", ()))
        if len(raw_ids) > 24:
            raise ValueError("Capability load limit is 24")
        for raw_id in raw_ids:
            capability_id = str(raw_id)
            descriptor = await self.catalog.get(
                tenant_id=invocation.tenant_id,
                capability_id=capability_id,
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


@dataclass(frozen=True)
class SkillBindingStatusExecutor:
    publications: SkillPublicationReader
    publisher_security: SkillPublisherSecurityReader | None = None

    async def execute(
        self,
        invocation: ToolInvocation,
        capability: ToolCapability,
    ) -> dict[str, object]:
        del capability
        arguments = invocation.arguments
        publication = await self.publications.get_publication(
            invocation.tenant_id,
            str(arguments["publisher"]),
            str(arguments["name"]),
            str(arguments["version"]),
        )
        expected_digest = str(arguments["package_digest"])
        if publication is None or publication.package_digest != expected_digest:
            return {
                "publication_status": "unavailable",
                "action": SkillRevocationAction.CANCEL.value,
                "reason_code": "binding_authority_unavailable",
                "policy_version": "skill-revocation-v1",
            }
        decisions: list[dict[str, object]] = []
        if publication.status is SkillPublicationStatus.REVOKED:
            decisions.append({
                "publication_status": publication.status.value,
                "action": (
                    publication.revocation_action or SkillRevocationAction.CANCEL
                ).value,
                "reason_code": publication.reason_code,
                "policy_version": publication.revocation_policy_version,
                "policy_decision_id": (publication.revocation_policy_decision_id),
            })
        if self.publisher_security is not None:
            package = await self.publications.get_package(
                invocation.tenant_id,
                publication.publisher,
                publication.name,
                publication.version,
            )
            if package is None:
                decisions.append({
                    "publication_status": publication.status.value,
                    "action": SkillRevocationAction.CANCEL.value,
                    "reason_code": "package_authority_unavailable",
                    "policy_version": "skill-revocation-v1",
                })
            elif package.signature_key_id is not None:
                publisher = await self.publisher_security.get_publisher(
                    invocation.tenant_id,
                    publication.publisher,
                )
                key = await self.publisher_security.get_key(
                    invocation.tenant_id,
                    publication.publisher,
                    package.signature_key_id,
                )
                if publisher is None or publisher.status in {
                    SkillPublisherStatus.SUSPENDED,
                    SkillPublisherStatus.REVOKED,
                }:
                    decisions.append({
                        "publication_status": publication.status.value,
                        "publisher_status": (
                            publisher.status.value
                            if publisher is not None
                            else "unavailable"
                        ),
                        "action": (
                            publisher.security_action or SkillRevocationAction.CANCEL
                            if publisher is not None
                            else SkillRevocationAction.CANCEL
                        ).value,
                        "reason_code": (
                            publisher.status_reason_code
                            if publisher is not None
                            else "publisher_authority_unavailable"
                        ),
                        "policy_version": (
                            publisher.security_policy_version
                            if publisher is not None
                            else "skill-revocation-v1"
                        ),
                        "policy_decision_id": (
                            publisher.security_policy_decision_id
                            if publisher is not None
                            else None
                        ),
                    })
                if key is None:
                    decisions.append({
                        "publication_status": publication.status.value,
                        "publisher_status": (
                            publisher.status.value
                            if publisher is not None
                            else "unavailable"
                        ),
                        "key_status": "unavailable",
                        "action": SkillRevocationAction.CANCEL.value,
                        "reason_code": "publisher_key_authority_unavailable",
                        "policy_version": "skill-revocation-v1",
                    })
                elif key.status is SkillPublisherKeyStatus.REVOKED:
                    decisions.append({
                    "publication_status": publication.status.value,
                    "publisher_status": (
                        publisher.status.value if publisher is not None else "unavailable"
                    ),
                    "key_status": key.status.value,
                    "action": (
                        key.revocation_action or SkillRevocationAction.CANCEL
                    ).value,
                    "reason_code": key.reason_code,
                    "policy_version": key.revocation_policy_version,
                    "policy_decision_id": key.revocation_policy_decision_id,
                    })
        installation = await self.publications.get_installation(
            invocation.tenant_id,
            str(arguments["publisher"]),
            str(arguments["name"]),
        )
        if (
            installation is not None
            and installation.uninstall_action is SkillRevocationAction.CANCEL
        ):
            decisions.append({
                "publication_status": publication.status.value,
                "installation_status": installation.status.value,
                "action": SkillRevocationAction.CANCEL.value,
                "reason_code": installation.reason_code,
                "policy_version": installation.uninstall_policy_version,
                "policy_decision_id": installation.uninstall_policy_decision_id,
            })
        if decisions:
            priority = {
                SkillRevocationAction.CANCEL.value: 0,
                SkillRevocationAction.PAUSE.value: 1,
                SkillRevocationAction.CONTINUE.value: 2,
            }
            decision = min(decisions, key=lambda item: priority[str(item["action"])])
            decision.setdefault(
                "installation_status",
                installation.status.value if installation is not None else "unmanaged",
            )
            return decision
        return {
            "publication_status": publication.status.value,
            "installation_status": (
                installation.status.value if installation is not None else "unmanaged"
            ),
            "action": (
                SkillRevocationAction.CONTINUE.value
                if publication.status
                in {
                    SkillPublicationStatus.ACTIVE,
                    SkillPublicationStatus.RESTORING,
                    SkillPublicationStatus.RETIRED,
                }
                else SkillRevocationAction.CANCEL.value
            ),
            "reason_code": None,
            "policy_version": "skill-revocation-v1",
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

    def replace_owner_routes(
        self,
        owner: str,
        routes: Mapping[str, HandsExecutor],
    ) -> None:
        prefix = f"{owner}:"
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


def skill_binding_status_tool() -> ToolCapability:
    return ToolCapability(
        name=SKILL_BINDING_STATUS_TOOL_NAME,
        version="1",
        description=(
            "Evaluate the current governed disposition of an already-fixed Skill binding."
        ),
        input_schema={
            "type": "object",
            "properties": {
                "publisher": {"type": "string", "minLength": 1, "maxLength": 128},
                "name": {"type": "string", "minLength": 1, "maxLength": 256},
                "version": {"type": "string", "minLength": 1, "maxLength": 128},
                "package_digest": {
                    "type": "string",
                    "pattern": "^sha256:[0-9a-f]{64}$",
                },
            },
            "required": ["publisher", "name", "version", "package_digest"],
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
            "uri_template": (descriptor.metadata.get("uri_template") or source.get("uriTemplate"))
        }
    elif descriptor.kind == CapabilityKind.SKILL:
        raw_contract = descriptor.metadata.get("model_contract", {})
        result["skill"] = dict(raw_contract) if isinstance(raw_contract, dict) else {}
    return result


def _optional(value: object) -> str | None:
    return None if value is None else str(value)


def _tokens(value: str) -> tuple[str, ...]:
    tokens: list[str] = []
    seen: set[str] = set()

    def add(token: str) -> None:
        folded = token.casefold().strip()
        if not folded or folded in seen:
            return
        seen.add(folded)
        tokens.append(folded)

    for token in _LATIN_TOKEN_PATTERN.findall(value):
        add(token)
        for part in re.split(r"[_.-]+", token):
            add(part)
    for run in _CJK_RUN_PATTERN.findall(value):
        add(run)
        if len(run) >= 2:
            for index in range(len(run) - 1):
                add(run[index : index + 2])
    return tuple(tokens)


def _score(
    capability: CapabilityDescriptor,
    query_tokens: tuple[str, ...],
) -> int:
    if not query_tokens:
        return 0
    name = capability.canonical_name.casefold()
    title = capability.title.casefold()
    tags = tuple(tag.casefold() for tag in capability.tags)
    tag_haystack = " ".join(tags)
    description = capability.description.casefold()
    score = 0
    for token in query_tokens:
        if token in name:
            score += 8
        if token in title:
            score += 5
        if token in tags or token in tag_haystack:
            score += 3
        if token in description:
            score += 1
    return score
