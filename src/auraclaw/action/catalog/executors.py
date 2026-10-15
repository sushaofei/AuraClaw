from __future__ import annotations

import hashlib
import logging
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from auraclaw.action.catalog.contracts import (
    CatalogReader,
    SkillPublicationReader,
    SkillPublisherSecurityReader,
    SkillResolverPort,
)
from auraclaw.action.ports import (
    HandsExecutor,
)
from auraclaw.contracts.capabilities import (
    CapabilityDescriptor,
    CapabilityInvocationRef,
    CapabilityKind,
)
from auraclaw.contracts.errors import AuthorizationError
from auraclaw.contracts.skills import (
    SkillPublicationStatus,
    SkillPublisherKeyStatus,
    SkillPublisherStatus,
    SkillRevocationAction,
    effective_skill_role,
)
from auraclaw.contracts.tools import (
    ToolCapability,
    ToolInvocation,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class CapabilitySearchExecutor:
    catalog: CatalogReader

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
        limit = int(arguments.get("limit", 10))
        outcome = await self.catalog.search_with_evidence(
            tenant_id=invocation.tenant_id,
            query=query,
            kinds=kinds,
            required_permissions=permissions,
            capability_id=_optional(arguments.get("capability_id")),
            canonical_name=_optional(arguments.get("canonical_name")),
            server_id=_optional(arguments.get("server_id")),
            limit=limit,
            actor_role=invocation.actor_role,
            deadline=invocation.deadline,
        )
        page: list[dict[str, Any]] = []
        for match in outcome.matches:
            item = match.capability.as_search_result()
            item.update(
                {
                    "match_reasons": list(match.match_reasons),
                    "search_policy_version": outcome.search_policy_version,
                    "semantic_degraded": outcome.semantic_degraded,
                }
            )
            page.append(item)
        payload: dict[str, object] = {
            "capabilities": page,
            "truncated": len(outcome.matches) >= limit,
            "search_policy_version": outcome.search_policy_version,
            "semantic_degraded": outcome.semantic_degraded,
        }
        if outcome.degraded_reason is not None:
            payload["semantic_degraded_reason"] = outcome.degraded_reason
        if not page:
            browse = await self.catalog.search(tenant_id=invocation.tenant_id, limit=50)
            domains = sorted(
                {item.canonical_name.split(".", 1)[0] for item in browse if item.canonical_name}
            )[:12]
            payload["empty_reason"] = "no_capability_matched_filters"
            payload["available_domains"] = domains
            payload["hint"] = (
                "No matching capabilities were found. Retry once with a broader query, "
                "an exact capability_id/canonical_name/server_id, or an empty query to browse. "
                'For MCP queries/actions include kinds=["tool"] or omit kinds; '
                "a Skill/Resource filter excludes Tools. "
                'To list Skills use kinds=["skill"] and query="". '
                "No executable match does not mean no Skill is installed or registered; "
                "an installation may be disabled, version-mismatched, or missing dependencies. "
                "An administrator can inspect Skill availability in the management catalog."
            )
        logger.info(
            "capability_search tenant=%s query_digest=%s query_length=%s kinds=%s "
            "permissions=%s hits=%s policy=%s semantic_degraded=%s degraded_reason=%s "
            "generations=%s empty_reason=%s",
            invocation.tenant_id,
            hashlib.sha256(query.encode()).hexdigest()[:16],
            len(query),
            tuple(kind.value for kind in kinds),
            permissions,
            tuple(item["capability_id"] for item in page),
            outcome.search_policy_version,
            outcome.semantic_degraded,
            outcome.degraded_reason,
            tuple(
                sorted(
                    {
                        int(item["catalog_generation"])
                        for item in page
                        if isinstance(item.get("catalog_generation"), int)
                    }
                )
            ),
            payload.get("empty_reason"),
        )
        return payload


@dataclass(frozen=True)
class CapabilityLoadExecutor:
    catalog: CatalogReader

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
                loaded.append(load_result(descriptor))
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
        requested_role = str(arguments["role"])
        assignment_role = invocation.actor_role or requested_role
        policy_role = effective_skill_role(assignment_role)
        if invocation.actor_role is not None and requested_role not in {
            assignment_role,
            policy_role,
        }:
            raise AuthorizationError(
                "Skill resolver role does not match the trusted Runtime assignment"
            )
        binding = await self.resolver.resolve(
            tenant_id=invocation.tenant_id,
            name=str(arguments["name"]),
            version=str(arguments.get("version", "*")),
            publisher=_optional(arguments.get("publisher")),
            role=policy_role,
            assignment_role=assignment_role,
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
            decisions.append(
                {
                    "publication_status": publication.status.value,
                    "action": (publication.revocation_action or SkillRevocationAction.CANCEL).value,
                    "reason_code": publication.reason_code,
                    "policy_version": publication.revocation_policy_version,
                    "policy_decision_id": (publication.revocation_policy_decision_id),
                }
            )
        if self.publisher_security is not None:
            package = await self.publications.get_package(
                invocation.tenant_id,
                publication.publisher,
                publication.name,
                publication.version,
            )
            if package is None:
                decisions.append(
                    {
                        "publication_status": publication.status.value,
                        "action": SkillRevocationAction.CANCEL.value,
                        "reason_code": "package_authority_unavailable",
                        "policy_version": "skill-revocation-v1",
                    }
                )
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
                    decisions.append(
                        {
                            "publication_status": publication.status.value,
                            "publisher_status": (
                                publisher.status.value if publisher is not None else "unavailable"
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
                        }
                    )
                if key is None:
                    decisions.append(
                        {
                            "publication_status": publication.status.value,
                            "publisher_status": (
                                publisher.status.value if publisher is not None else "unavailable"
                            ),
                            "key_status": "unavailable",
                            "action": SkillRevocationAction.CANCEL.value,
                            "reason_code": "publisher_key_authority_unavailable",
                            "policy_version": "skill-revocation-v1",
                        }
                    )
                elif key.status is SkillPublisherKeyStatus.REVOKED:
                    decisions.append(
                        {
                            "publication_status": publication.status.value,
                            "publisher_status": (
                                publisher.status.value if publisher is not None else "unavailable"
                            ),
                            "key_status": key.status.value,
                            "action": (key.revocation_action or SkillRevocationAction.CANCEL).value,
                            "reason_code": key.reason_code,
                            "policy_version": key.revocation_policy_version,
                            "policy_decision_id": key.revocation_policy_decision_id,
                        }
                    )
        installation = await self.publications.get_installation(
            invocation.tenant_id,
            str(arguments["publisher"]),
            str(arguments["name"]),
        )
        if (
            installation is not None
            and installation.uninstall_action is SkillRevocationAction.CANCEL
        ):
            decisions.append(
                {
                    "publication_status": publication.status.value,
                    "installation_status": installation.status.value,
                    "action": SkillRevocationAction.CANCEL.value,
                    "reason_code": installation.reason_code,
                    "policy_version": installation.uninstall_policy_version,
                    "policy_decision_id": installation.uninstall_policy_decision_id,
                }
            )
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
        target = capability.invocation_ref
        route = target.model_name if target is not None else invocation.tool_name
        executor = self._routes.get(route, self._default)
        if target is not None and route not in self._routes:
            raise AuthorizationError("stale_capability: execution route is unavailable")
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


def load_result(descriptor: CapabilityDescriptor) -> dict[str, Any]:
    result = descriptor.as_search_result()
    raw_source = descriptor.metadata.get("source", {})
    source = dict(raw_source) if isinstance(raw_source, dict) else {}
    if descriptor.kind == CapabilityKind.TOOL:
        ref = (
            CapabilityInvocationRef.from_descriptor(descriptor)
            if descriptor.metadata.get("source_type") == "mcp"
            else None
        )
        if ref is not None:
            result["invocation_ref"] = ref.model_dump(mode="json")
        result["model_tool"] = {
            "type": "function",
            "function": {
                "name": ref.model_name if ref is not None else descriptor.canonical_name,
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
    parsed = "" if value is None else str(value).strip()
    return parsed or None
