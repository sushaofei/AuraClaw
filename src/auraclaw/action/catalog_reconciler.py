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
from auraclaw.action.ports import CapabilityCatalogStore
from auraclaw.action.remote_mcp import (
    ManagedRemoteMcpTransport,
    RemoteMcpToolExecutor,
)
from auraclaw.action.tool_gateway import ToolRegistry
from auraclaw.contracts.capabilities import (
    CapabilityDescriptor,
    CapabilityKind,
    CapabilityStatus,
    McpServerDefinition,
)
from auraclaw.contracts.mcp import (
    MCP_CLIENT_CAPABILITIES_META_KEY,
    MCP_CLIENT_INFO_META_KEY,
    MCP_LEGACY_PROTOCOL_VERSION,
    MCP_PROTOCOL_VERSION,
    MCP_PROTOCOL_VERSION_META_KEY,
    McpJsonRpcRequest,
    McpTransport,
    McpTrustedContext,
)
from auraclaw.contracts.tools import RiskLevel, ToolCapability, ToolPermission

_NAME = re.compile(r"^[A-Za-z0-9_.:/{}-]{1,256}$")
_LIST_METHODS = (
    ("tools/list", "tools"),
    ("resources/list", "resources"),
    ("resources/templates/list", "resourceTemplates"),
    ("prompts/list", "prompts"),
)


class ResourceCacheInvalidator(Protocol):
    async def invalidate(self, uri: str, *, tenant_id: str | None = None) -> int: ...


@dataclass(frozen=True)
class McpReconcileResult:
    server_id: str
    status: CapabilityStatus
    capability_count: int
    error: str | None = None


class McpCatalogReconciler:
    """Periodic source-of-truth sync; notifications only make reconciliation sooner."""

    def __init__(
        self,
        *,
        catalog: CapabilityCatalog,
        store: CapabilityCatalogStore,
        transports: dict[str, McpTransport],
        resource_cache: ResourceCacheInvalidator | None = None,
        tool_registry: ToolRegistry | None = None,
        hands_router: RoutedHandsExecutor | None = None,
        quarantine_after_failures: int = 3,
        max_pages: int = 100,
        max_items: int = 10_000,
    ) -> None:
        self._catalog = catalog
        self._store = store
        self._transports = transports
        self._resource_cache = resource_cache
        self._tool_registry = tool_registry
        self._hands_router = hands_router
        self._quarantine_after_failures = quarantine_after_failures
        self._max_pages = max_pages
        self._max_items = max_items
        self._failures: dict[str, int] = {}
        self._dirty: set[str] = set()

    async def reconcile_all(self) -> int:
        servers = {
            server_id: server
            for server_id in self._transports
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
        transport = self._transports.get(server.server_id)
        if transport is None or not server.enabled:
            return McpReconcileResult(
                server_id=server.server_id,
                status=server.status,
                capability_count=0,
                error="transport_unavailable",
            )
        trusted = _reconcile_context(server)
        try:
            if server.protocol_revision == MCP_PROTOCOL_VERSION:
                discovery = await _send(
                    transport,
                    trusted,
                    "server/discover",
                    {},
                    protocol_version=server.protocol_revision,
                )
                if MCP_PROTOCOL_VERSION not in discovery.get("supportedVersions", []):
                    raise ValueError("remote MCP protocol version is incompatible")
            elif server.protocol_revision == MCP_LEGACY_PROTOCOL_VERSION:
                discovery = await _send(
                    transport,
                    trusted,
                    "initialize",
                    {
                        "protocolVersion": MCP_LEGACY_PROTOCOL_VERSION,
                        "capabilities": {},
                        "clientInfo": {
                            "name": "auraclaw-capability-reconciler",
                            "version": "1",
                        },
                    },
                    protocol_version=server.protocol_revision,
                )
                if discovery.get("protocolVersion") != MCP_LEGACY_PROTOCOL_VERSION:
                    raise ValueError("remote MCP protocol version is incompatible")
            else:
                raise ValueError("remote MCP protocol version is not supported")
            snapshot: list[CapabilityDescriptor] = []
            listed_resources: list[str] = []
            for method, key in _LIST_METHODS:
                items = await self._list_all(
                    transport,
                    trusted,
                    method,
                    key,
                    protocol_version=server.protocol_revision,
                )
                if key == "resources":
                    listed_resources = [
                        str(item.get("uri", "")) for item in items
                    ]
                snapshot.extend(_normalize_items(server, key, items))
                if len(snapshot) > self._max_items:
                    raise ValueError("remote MCP capability count exceeds limit")
            ids = [item.capability_id for item in snapshot]
            if len(ids) != len(set(ids)):
                raise ValueError("remote MCP returned duplicate capabilities")
            existing = {
                (item.kind, item.canonical_name, item.version): item
                for item in await self._store.list_capabilities(
                    server.tenant_id or "platform"
                )
                if item.server_id == server.server_id
            }
            for item in snapshot:
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
                tuple(snapshot),
            )
            self._replace_remote_tools(server, snapshot, transport)
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
            resources_capability = discovery.get("capabilities", {}).get(
                "resources", {}
            )
            if (
                server.protocol_revision == MCP_LEGACY_PROTOCOL_VERSION
                and resources_capability.get("subscribe") is True
            ):
                for uri in listed_resources[:100]:
                    try:
                        await _send(
                            transport,
                            trusted,
                            "resources/subscribe",
                            {"uri": uri},
                            protocol_version=server.protocol_revision,
                        )
                    except Exception:
                        break
            return McpReconcileResult(
                server_id=server.server_id,
                status=CapabilityStatus.ACTIVE,
                capability_count=len(snapshot),
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
        if server_id not in self._transports:
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

    async def _list_all(
        self,
        transport: McpTransport,
        trusted: McpTrustedContext,
        method: str,
        key: str,
        *,
        protocol_version: str,
    ) -> list[dict[str, Any]]:
        items: list[dict[str, Any]] = []
        cursor: str | None = None
        seen: set[str] = set()
        for _page in range(self._max_pages):
            result = await _send(
                transport,
                trusted,
                method,
                {"cursor": cursor} if cursor is not None else {},
                protocol_version=protocol_version,
            )
            raw_items = result.get(key, [])
            if not isinstance(raw_items, list) or any(
                not isinstance(item, dict) for item in raw_items
            ):
                raise ValueError(f"remote MCP {key} list is invalid")
            items.extend(dict(item) for item in raw_items)
            if len(items) > self._max_items:
                raise ValueError(f"remote MCP {key} list exceeds limit")
            raw_cursor = result.get("nextCursor")
            if raw_cursor is None:
                return items
            cursor = str(raw_cursor)
            if not cursor or cursor in seen:
                raise ValueError("remote MCP pagination cursor did not advance")
            seen.add(cursor)
        raise ValueError("remote MCP pagination exceeded page limit")

    def _replace_remote_tools(
        self,
        server: McpServerDefinition,
        snapshot: list[CapabilityDescriptor],
        transport: McpTransport,
    ) -> None:
        if self._tool_registry is None or self._hands_router is None:
            return
        if not isinstance(transport, ManagedRemoteMcpTransport):
            return
        owner = f"mcp:{server.server_id}"
        capabilities = tuple(
            _tool_capability(descriptor)
            for descriptor in snapshot
            if descriptor.kind == CapabilityKind.TOOL
        )
        executor = RemoteMcpToolExecutor(server, transport)
        self._tool_registry.replace_owner(owner, capabilities)
        self._hands_router.replace_owner_routes(
            owner,
            {capability.name: executor for capability in capabilities},
        )

    def _remove_remote_tools(self, server: McpServerDefinition) -> None:
        if self._tool_registry is None or self._hands_router is None:
            return
        owner = f"mcp:{server.server_id}"
        self._tool_registry.revoke_owner(owner)
        self._hands_router.replace_owner_routes(owner, {})


async def _send(
    transport: McpTransport,
    trusted: McpTrustedContext,
    method: str,
    params: dict[str, Any],
    *,
    protocol_version: str,
) -> dict[str, Any]:
    request_params = dict(params)
    if protocol_version == MCP_PROTOCOL_VERSION:
        request_params["_meta"] = {
            MCP_PROTOCOL_VERSION_META_KEY: MCP_PROTOCOL_VERSION,
            MCP_CLIENT_INFO_META_KEY: {
                "name": "auraclaw-capability-reconciler",
                "version": "1",
            },
            MCP_CLIENT_CAPABILITIES_META_KEY: {},
        }
    response = await transport.send(
        McpJsonRpcRequest(
            id=f"reconcile:{method}", method=method, params=request_params
        ),
        trusted_context=trusted,
    )
    if response.error is not None:
        raise ValueError(f"remote MCP error {response.error.code}")
    return dict(response.result or {})


def _normalize_items(
    server: McpServerDefinition,
    key: str,
    items: list[dict[str, Any]],
) -> tuple[CapabilityDescriptor, ...]:
    normalized: list[CapabilityDescriptor] = []
    for item in items:
        safe = _sanitize_item(item)
        if key == "tools":
            raw_name = str(safe.get("name", ""))
            if not _prefix_allowed(raw_name, server.allowed_tool_prefixes):
                continue
            kind = CapabilityKind.TOOL
            canonical_name = raw_name
            permission = "read-only"
            risk_level = "medium"
        elif key in {"resources", "resourceTemplates"}:
            uri_key = "uri" if key == "resources" else "uriTemplate"
            uri = str(safe.get(uri_key, ""))
            if urlsplit(uri).scheme not in server.allowed_resource_schemes:
                continue
            kind = (
                CapabilityKind.RESOURCE
                if key == "resources"
                else CapabilityKind.RESOURCE_TEMPLATE
            )
            raw_name = str(safe.get("name", "resource"))
            canonical_name = f"{server.server_id}.{kind.value}.{raw_name}"
            permission = "read-only"
            risk_level = "low"
        else:
            raw_name = str(safe.get("name", ""))
            if not _prefix_allowed(raw_name, server.allowed_prompt_prefixes):
                continue
            kind = CapabilityKind.PROMPT
            canonical_name = raw_name
            permission = "read-only"
            risk_level = "low"
        if not _NAME.fullmatch(canonical_name):
            continue
        digest = _digest(safe)
        metadata: dict[str, Any] = {"source": safe}
        if key == "resourceTemplates":
            metadata["uri_template"] = str(safe["uriTemplate"])
        capability_key = f"{server.server_id}:{kind.value}:{canonical_name}"
        normalized.append(
            CapabilityDescriptor(
                capability_id=(
                    f"cap_{hashlib.sha256(capability_key.encode()).hexdigest()[:32]}"
                ),
                kind=kind,
                server_id=server.server_id,
                canonical_name=canonical_name,
                version=_version(safe),
                content_digest=digest,
                title=_text(safe.get("title") or raw_name, 256),
                description=_text(safe.get("description", ""), 4096),
                tenant_id=server.tenant_id,
                trust_level=server.trust_level,
                classification="internal",
                permission=permission,
                risk_level=risk_level,
                status=CapabilityStatus.ACTIVE,
                source_revision=digest,
                updated_at=datetime.now(UTC),
                metadata=metadata,
            )
        )
    return tuple(normalized)


def _tool_capability(descriptor: CapabilityDescriptor) -> ToolCapability:
    source = descriptor.metadata.get("source", {})
    if not isinstance(source, dict):
        source = {}
    input_schema = source.get("inputSchema", {"type": "object"})
    output_schema = source.get("outputSchema", {"type": "object"})
    if not isinstance(input_schema, dict) or not isinstance(output_schema, dict):
        raise ValueError("remote MCP Tool schemas are invalid")
    return ToolCapability(
        name=descriptor.canonical_name,
        version=descriptor.version,
        description=descriptor.description,
        input_schema=input_schema,
        output_schema=output_schema,
        permission=ToolPermission.WRITE_WITH_APPROVAL,
        risk_level=RiskLevel.HIGH,
        runtime_location="remote-mcp",
        owner=f"mcp:{descriptor.server_id}",
    )


def _sanitize_item(item: dict[str, Any]) -> dict[str, Any]:
    encoded = json.dumps(item, sort_keys=True, separators=(",", ":")).encode()
    if len(encoded) > 256 * 1024:
        raise ValueError("remote MCP descriptor exceeds size limit")
    _validate_depth(item, depth=0)
    payload = json.loads(encoded)
    if not isinstance(payload, dict):
        raise ValueError("remote MCP descriptor must be an object")
    return dict(payload)


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


def _text(value: object, limit: int) -> str:
    return "".join(
        character for character in str(value)[:limit] if character >= " " or character == "\n"
    )


def _version(item: dict[str, Any]) -> str:
    meta = item.get("_meta", {})
    if isinstance(meta, dict):
        auraclaw = meta.get("auraclaw", {})
        if isinstance(auraclaw, dict):
            value = str(auraclaw.get("version", ""))
            if re.fullmatch(r"\d+\.\d+\.\d+(?:-[0-9A-Za-z.-]+)?", value):
                return value
    return "0.0.0"


def _prefix_allowed(value: str, prefixes: tuple[str, ...]) -> bool:
    return bool(value) and any(value.startswith(prefix) for prefix in prefixes)


def _digest(value: dict[str, Any]) -> str:
    content = json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    return f"sha256:{hashlib.sha256(content).hexdigest()}"


def _reconcile_context(server: McpServerDefinition) -> McpTrustedContext:
    now = datetime.now(UTC)
    return McpTrustedContext(
        tenant_id=server.tenant_id or "platform",
        root_session_id=f"catalog:{server.server_id}",
        session_id=f"catalog:{server.server_id}",
        run_id=f"catalog:{server.server_id}:{int(now.timestamp())}",
        runtime_id="action-hands-reconciler",
        lease_id=f"catalog:{server.server_id}",
        fencing_token=1,
        deadline=now + timedelta(minutes=5),
    )
