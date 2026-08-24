from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Protocol
from urllib.parse import urlsplit

from auraclaw.action.capability_catalog import (
    CapabilityCatalog,
    RoutedHandsExecutor,
)
from auraclaw.action.ports import CapabilityCatalogStore, CapabilityConnector
from auraclaw.action.tool_gateway import ToolRegistry
from auraclaw.contracts.capabilities import (
    CapabilityDescriptor,
    CapabilityKind,
    CapabilityStatus,
    McpServerDefinition,
)
from auraclaw.contracts.hands import (
    CapabilitySnapshot,
    HandsPromptDescriptor,
    HandsResourceDescriptor,
    HandsToolDescriptor,
    HandsToolResult,
    HandsTrustedContext,
)
from auraclaw.contracts.tools import (
    RiskLevel,
    ToolCapability,
    ToolInvocation,
    ToolPermission,
)

_NAME = re.compile(r"^[A-Za-z0-9_.:/{}-]{1,256}$")


class ResourceCacheInvalidator(Protocol):
    async def invalidate(self, uri: str, *, tenant_id: str | None = None) -> int: ...


@dataclass(frozen=True)
class McpReconcileResult:
    server_id: str
    status: CapabilityStatus
    capability_count: int
    error: str | None = None


CapabilityReconcileResult = McpReconcileResult


class ConnectorToolExecutor:
    def __init__(self, connector: CapabilityConnector) -> None:
        self._connector = connector
        self.route_owner = f"{connector.connector_id}:tools"

    async def execute(
        self, invocation: ToolInvocation, capability: ToolCapability
    ) -> object:
        del capability
        result = await self._connector.call_tool(
            HandsTrustedContext(
                tenant_id=invocation.tenant_id,
                root_session_id=invocation.root_session_id,
                session_id=invocation.session_id,
                run_id=invocation.run_id,
                runtime_id=invocation.actor_id,
                lease_id=f"tool:{invocation.tool_invocation_id}",
                fencing_token=invocation.fencing_token,
                deadline=invocation.deadline,
                user_id=invocation.user_id,
            ),
            name=invocation.tool_name,
            arguments=invocation.arguments,
            invocation_id=invocation.tool_invocation_id,
        )
        return _executor_payload(result)


class CapabilityCatalogReconciler:
    """Periodic source-of-truth sync; notifications only make reconciliation sooner."""

    def __init__(
        self,
        *,
        catalog: CapabilityCatalog,
        store: CapabilityCatalogStore,
        connectors: dict[str, CapabilityConnector],
        resource_cache: ResourceCacheInvalidator | None = None,
        tool_registry: ToolRegistry | None = None,
        hands_router: RoutedHandsExecutor | None = None,
        trust_remote_tool_annotations: bool = False,
        quarantine_after_failures: int = 3,
        max_pages: int = 100,
        max_items: int = 10_000,
    ) -> None:
        del max_pages
        self._catalog = catalog
        self._store = store
        self._connectors = connectors
        self._resource_cache = resource_cache
        self._tool_registry = tool_registry
        self._hands_router = hands_router
        self._trust_remote_tool_annotations = trust_remote_tool_annotations
        self._quarantine_after_failures = quarantine_after_failures
        self._max_items = max_items
        self._failures: dict[str, int] = {}
        self._dirty: set[str] = set()

    async def reconcile_all(self) -> int:
        servers = {
            server_id: server
            for server_id in self._connectors
            if (server := await self._store.get_server(server_id)) is not None
        }
        results = [
            await self.reconcile_server(server)
            for server in sorted(servers.values(), key=lambda item: item.server_id)
        ]
        return sum(result.status == CapabilityStatus.ACTIVE for result in results)

    async def reconcile_server(
        self,
        server: McpServerDefinition,
    ) -> McpReconcileResult:
        connector = self._connectors.get(server.server_id)
        if connector is None or not server.enabled:
            return McpReconcileResult(
                server_id=server.server_id,
                status=server.status,
                capability_count=0,
                error="transport_unavailable",
            )
        trusted = _reconcile_context(server)
        try:
            snapshot = await connector.snapshot(trusted)
            descriptors = _normalize_snapshot(server, snapshot, self._max_items)
            ids = [item.capability_id for item in descriptors]
            if len(ids) != len(set(ids)):
                raise ValueError("remote connector returned duplicate capabilities")
            existing = {
                (item.kind, item.canonical_name, item.version): item
                for item in await self._store.list_capabilities(
                    server.tenant_id or "platform"
                )
                if item.server_id == server.server_id
            }
            for item in descriptors:
                previous = existing.get(
                    (item.kind, item.canonical_name, item.version)
                )
                if (
                    previous is not None
                    and previous.content_digest != item.content_digest
                ):
                    raise ValueError(
                        "remote MCP capability changed without version bump"
                    )
            await self._catalog.replace_server_capabilities(
                server.server_id,
                descriptors,
            )
            self._replace_remote_tools(server, descriptors, connector)
            active = server.model_copy(
                update={
                    "status": CapabilityStatus.ACTIVE,
                    "metadata": {
                        **server.metadata,
                        "last_sync_at": datetime.now(UTC).isoformat(),
                        "last_sync_error": None,
                    },
                }
            )
            await self._catalog.register_server(active)
            self._failures.pop(server.server_id, None)
            self._dirty.discard(server.server_id)
            return McpReconcileResult(
                server_id=server.server_id,
                status=CapabilityStatus.ACTIVE,
                capability_count=len(descriptors),
            )
        except Exception as exc:
            failures = self._failures.get(server.server_id, 0) + 1
            self._failures[server.server_id] = failures
            status = (
                CapabilityStatus.QUARANTINED
                if failures >= self._quarantine_after_failures
                else CapabilityStatus.DEGRADED
            )
            await self._catalog.register_server(
                server.model_copy(
                    update={
                        "status": status,
                        "metadata": {
                            **server.metadata,
                            "last_sync_at": datetime.now(UTC).isoformat(),
                            "last_sync_error": type(exc).__name__,
                            "consecutive_sync_failures": failures,
                        },
                    }
                )
            )
            if status == CapabilityStatus.QUARANTINED:
                self._remove_remote_tools(server)
            return McpReconcileResult(
                server_id=server.server_id,
                status=status,
                capability_count=0,
                error=type(exc).__name__,
            )

    async def handle_notification(
        self,
        server_id: str,
        method: str,
        params: dict[str, Any],
    ) -> bool:
        if server_id not in self._connectors:
            return False
        if method in {
            "notifications/tools/list_changed",
            "notifications/resources/list_changed",
            "notifications/prompts/list_changed",
        }:
            self._dirty.add(server_id)
            return True
        if method == "notifications/resources/updated":
            uri = str(params.get("uri", ""))
            if not uri:
                return False
            server = await self._store.get_server(server_id)
            if server is None:
                return False
            if self._resource_cache is not None:
                await self._resource_cache.invalidate(
                    uri,
                    tenant_id=server.tenant_id,
                )
            return True
        return False

    async def reconcile_dirty(self) -> int:
        server_ids = sorted(self._dirty)
        reconciled = 0
        for server_id in server_ids:
            server = await self._store.get_server(server_id)
            if server is None:
                self._dirty.discard(server_id)
                continue
            result = await self.reconcile_server(server)
            reconciled += result.status == CapabilityStatus.ACTIVE
        return reconciled

    def _replace_remote_tools(
        self,
        server: McpServerDefinition,
        snapshot: tuple[CapabilityDescriptor, ...],
        connector: CapabilityConnector,
    ) -> None:
        if self._tool_registry is None or self._hands_router is None:
            return
        owner = connector.connector_id
        capabilities = tuple(
            _tool_capability(
                descriptor,
                owner,
                trust_annotations=self._trust_remote_tool_annotations,
            )
            for descriptor in snapshot
            if descriptor.kind == CapabilityKind.TOOL
        )
        executor = ConnectorToolExecutor(connector)
        self._tool_registry.replace_owner(owner, capabilities)
        self._hands_router.replace_owner_routes(
            owner,
            {capability.name: executor for capability in capabilities},
        )

    def _remove_remote_tools(self, server: McpServerDefinition) -> None:
        if self._tool_registry is None or self._hands_router is None:
            return
        owner = f"mcp:{server.server_id}"
        connector = self._connectors.get(server.server_id)
        if connector is not None:
            owner = connector.connector_id
        self._tool_registry.revoke_owner(owner)
        self._hands_router.replace_owner_routes(owner, {})


McpCatalogReconciler = CapabilityCatalogReconciler


def _normalize_snapshot(
    server: McpServerDefinition,
    snapshot: CapabilitySnapshot,
    max_items: int,
) -> tuple[CapabilityDescriptor, ...]:
    items: list[CapabilityDescriptor] = []
    items.extend(_normalize_tools(server, snapshot.tools))
    items.extend(_normalize_resources(server, snapshot.resources, CapabilityKind.RESOURCE))
    items.extend(
        _normalize_resources(
            server, snapshot.resource_templates, CapabilityKind.RESOURCE_TEMPLATE
        )
    )
    items.extend(_normalize_prompts(server, snapshot.prompts))
    if len(items) > max_items:
        raise ValueError("remote MCP capability count exceeds limit")
    return tuple(items)


def _normalize_tools(
    server: McpServerDefinition,
    tools: tuple[HandsToolDescriptor, ...],
) -> tuple[CapabilityDescriptor, ...]:
    normalized: list[CapabilityDescriptor] = []
    for tool in tools:
        if not _prefix_allowed(tool.name, server.allowed_tool_prefixes):
            continue
        source = {
            "name": tool.name,
            "description": tool.description,
            "inputSchema": tool.input_schema,
            "outputSchema": tool.output_schema,
            "version": tool.version,
        }
        normalized.append(
            _descriptor(
                server,
                kind=CapabilityKind.TOOL,
                canonical_name=tool.name,
                source=source,
                title=tool.name,
                description=tool.description,
                permission="read-only" if tool.read_only else "write-with-approval",
                risk_level=tool.risk_level or "medium",
                version=_capability_semver(tool.version),
                tags=_tool_search_tags(server, tool.name),
            )
        )
    return tuple(normalized)


def _normalize_resources(
    server: McpServerDefinition,
    resources: tuple[HandsResourceDescriptor, ...],
    kind: CapabilityKind,
) -> tuple[CapabilityDescriptor, ...]:
    normalized: list[CapabilityDescriptor] = []
    for resource in resources:
        locator = resource.uri or resource.uri_template or ""
        if urlsplit(locator).scheme not in server.allowed_resource_schemes:
            continue
        canonical_name = f"{server.server_id}.{kind.value}.{resource.name}"
        source = resource.model_dump(mode="json")
        descriptor = _descriptor(
            server,
            kind=kind,
            canonical_name=canonical_name,
            source=source,
            title=resource.title or resource.name,
            description=resource.description or "",
            permission="read-only",
            risk_level="low",
            version="0.0.0",
        )
        if kind == CapabilityKind.RESOURCE_TEMPLATE and resource.uri_template:
            descriptor = descriptor.model_copy(
                update={
                    "metadata": {
                        **descriptor.metadata,
                        "uri_template": resource.uri_template,
                    }
                }
            )
        normalized.append(descriptor)
    return tuple(normalized)


def _normalize_prompts(
    server: McpServerDefinition,
    prompts: tuple[HandsPromptDescriptor, ...],
) -> tuple[CapabilityDescriptor, ...]:
    normalized: list[CapabilityDescriptor] = []
    for prompt in prompts:
        if not _prefix_allowed(prompt.name, server.allowed_prompt_prefixes):
            continue
        normalized.append(
            _descriptor(
                server,
                kind=CapabilityKind.PROMPT,
                canonical_name=prompt.name,
                source=prompt.model_dump(mode="json"),
                title=prompt.title or prompt.name,
                description=prompt.description or "",
                permission="read-only",
                risk_level="low",
                version="0.0.0",
            )
        )
    return tuple(normalized)


def _descriptor(
    server: McpServerDefinition,
    *,
    kind: CapabilityKind,
    canonical_name: str,
    source: dict[str, Any],
    title: str,
    description: str,
    permission: str,
    risk_level: str,
    version: str,
    tags: tuple[str, ...] = (),
) -> CapabilityDescriptor:
    if not _NAME.fullmatch(canonical_name):
        raise ValueError("remote capability name is invalid")
    digest = _digest(source)
    capability_key = f"{server.server_id}:{kind.value}:{canonical_name}"
    return CapabilityDescriptor(
        capability_id=f"cap_{hashlib.sha256(capability_key.encode()).hexdigest()[:32]}",
        kind=kind,
        server_id=server.server_id,
        canonical_name=canonical_name,
        version=version,
        content_digest=digest,
        title=_text(title, 256),
        description=_text(description, 4096),
        tags=tags,
        tenant_id=server.tenant_id,
        trust_level=server.trust_level,
        classification="internal",
        permission=permission,
        risk_level=risk_level,
        status=CapabilityStatus.ACTIVE,
        source_revision=digest,
        updated_at=datetime.now(UTC),
        metadata={"source": source},
    )


def _tool_capability(
    descriptor: CapabilityDescriptor,
    owner: str,
    *,
    trust_annotations: bool = False,
) -> ToolCapability:
    source = descriptor.metadata.get("source", {})
    if not isinstance(source, dict):
        source = {}
    input_schema = source.get("inputSchema", {"type": "object"})
    output_schema = source.get("outputSchema", {"type": "object"})
    if not isinstance(input_schema, dict) or not isinstance(output_schema, dict):
        raise ValueError("remote MCP Tool schemas are invalid")
    permission = ToolPermission.WRITE_WITH_APPROVAL
    risk_level = RiskLevel.HIGH
    if trust_annotations:
        permission = ToolPermission(
            descriptor.permission or ToolPermission.WRITE_WITH_APPROVAL
        )
        risk_level = RiskLevel(descriptor.risk_level or RiskLevel.HIGH)
    return ToolCapability(
        name=descriptor.canonical_name,
        version=descriptor.version,
        description=descriptor.description,
        input_schema=input_schema,
        output_schema=output_schema,
        permission=permission,
        risk_level=risk_level,
        runtime_location="remote-mcp",
        owner=owner,
    )


def _text(value: object, limit: int) -> str:
    return "".join(
        character for character in str(value)[:limit] if character >= " " or character == "\n"
    )


def _semver(value: str) -> bool:
    return bool(re.fullmatch(r"\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?", value))


def _capability_semver(value: str) -> str:
    raw = str(value).strip()
    if _semver(raw):
        return raw
    if re.fullmatch(r"\d+", raw):
        return f"{raw}.0.0"
    if re.fullmatch(r"\d+\.\d+", raw):
        return f"{raw}.0"
    return "1.0.0"


_PRICE_INSIGHT_SEARCH_TAGS = ("价格洞察", "采购价格", "price_insight")


def _tool_search_tags(server: McpServerDefinition, canonical_name: str) -> tuple[str, ...]:
    tags: list[str] = []
    configured = server.metadata.get("search_tags", ())
    if isinstance(configured, (list, tuple)):
        tags.extend(str(item) for item in configured if str(item).strip())
    aliases = server.metadata.get("tool_name_aliases", {})
    if isinstance(aliases, dict):
        for remote_name, mapped in aliases.items():
            if str(mapped) != canonical_name:
                continue
            tags.append(str(remote_name))
            tags.extend(
                part for part in re.split(r"[_.-]+", str(remote_name)) if part
            )
    lowered = canonical_name.casefold()
    if any(
        marker in lowered
        for marker in ("price_insight", "price-insight", "procurement.price")
    ):
        tags.extend(_PRICE_INSIGHT_SEARCH_TAGS)
    tags.extend(part for part in re.split(r"[_.-]+", canonical_name) if part)
    return tuple(dict.fromkeys(tag for tag in tags if tag))


def _prefix_allowed(value: str, prefixes: tuple[str, ...]) -> bool:
    return bool(value) and any(value.startswith(prefix) for prefix in prefixes)


def _digest(value: dict[str, Any]) -> str:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    if len(encoded) > 256 * 1024:
        raise ValueError("remote MCP descriptor exceeds size limit")
    _validate_depth(value, depth=0)
    return f"sha256:{hashlib.sha256(encoded).hexdigest()}"


def _validate_depth(value: Any, *, depth: int) -> None:
    if depth > 16:
        raise ValueError("remote MCP descriptor exceeds recursion limit")
    if isinstance(value, dict):
        for key, item in value.items():
            if not isinstance(key, str) or len(key) > 256:
                raise ValueError("remote MCP descriptor key is invalid")
            _validate_depth(item, depth=depth + 1)
    elif isinstance(value, list):
        for item in value:
            _validate_depth(item, depth=depth + 1)
    elif value is not None and not isinstance(value, (str, int, float, bool)):
        raise ValueError("remote MCP descriptor contains unsupported data")


def _executor_payload(result: HandsToolResult) -> dict[str, object]:
    if result.status in {"error", "denied", "timeout", "cancelled"}:
        raise RuntimeError(result.summary or "remote connector Tool returned an error")
    if isinstance(result.content, dict):
        return dict(result.content)
    return result.as_dict()


def _reconcile_context(server: McpServerDefinition) -> HandsTrustedContext:
    now = datetime.now(UTC)
    return HandsTrustedContext(
        tenant_id=server.tenant_id or "platform",
        root_session_id=f"catalog:{server.server_id}",
        session_id=f"catalog:{server.server_id}",
        run_id=f"catalog:{server.server_id}:{int(now.timestamp())}",
        runtime_id="action-hands-reconciler",
        lease_id=f"catalog:{server.server_id}",
        fencing_token=1,
        deadline=now + timedelta(minutes=5),
    )
