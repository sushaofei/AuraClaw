from __future__ import annotations

import asyncio
import hashlib
import json
import re
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Protocol
from urllib.parse import urlsplit

from auraclaw.action.bounded import bounded_partition_map
from auraclaw.action.capability_catalog import (
    CapabilityCatalog,
    RoutedHandsExecutor,
)
from auraclaw.action.ports import CapabilityCatalogStore, CapabilityConnector
from auraclaw.action.tool_gateway import ToolRegistry
from auraclaw.contracts.capabilities import (
    CapabilityDescriptor,
    CapabilityInvocationRef,
    CapabilityKind,
    CapabilityStatus,
    McpServerDefinition,
)
from auraclaw.contracts.errors import StaleCapabilitySnapshotError
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
MAX_DESCRIPTOR_DEPTH = 64
MAX_DESCRIPTOR_BYTES = 256 * 1024


class ResourceCacheInvalidator(Protocol):
    async def invalidate(self, uri: str, *, tenant_id: str | None = None) -> int: ...


@dataclass(frozen=True)
class McpReconcileResult:
    server_id: str
    status: CapabilityStatus
    capability_count: int
    error: str | None = None
    consecutive_failures: int = 0


CapabilityReconcileResult = McpReconcileResult


class CapabilitySchemaDriftError(ValueError):
    pass


class CapabilityDescriptorDepthError(ValueError):
    pass


class CapabilityDescriptorSizeError(ValueError):
    pass


class CapabilityAllowlistError(ValueError):
    pass


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
                dept_id=invocation.dept_id,
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
        quarantine_after_failures: int = 3,
        max_pages: int = 100,
        max_items: int = 10_000,
        max_concurrent: int = 8,
        max_concurrent_per_tenant: int | None = None,
        max_concurrent_per_host: int | None = None,
        server_timeout_seconds: float = 30.0,
        owner: str | None = None,
    ) -> None:
        del max_pages
        tenant_limit = (
            max_concurrent if max_concurrent_per_tenant is None else max_concurrent_per_tenant
        )
        host_limit = (
            max_concurrent if max_concurrent_per_host is None else max_concurrent_per_host
        )
        if (
            max_concurrent < 1
            or not 1 <= tenant_limit <= max_concurrent
            or not 1 <= host_limit <= max_concurrent
            or server_timeout_seconds <= 0
        ):
            raise ValueError("MCP reconcile capacity and timeout must be positive")
        self._catalog = catalog
        self._store = store
        self._connectors = connectors
        self._resource_cache = resource_cache
        self._tool_registry = tool_registry
        self._hands_router = hands_router
        self._quarantine_after_failures = quarantine_after_failures
        self._max_items = max_items
        self._max_concurrent = max_concurrent
        self._max_concurrent_per_tenant = tenant_limit
        self._max_concurrent_per_host = host_limit
        self._server_timeout_seconds = server_timeout_seconds
        self._lease_ttl = timedelta(seconds=max(3.0, server_timeout_seconds * 2))
        self._owner = owner or f"catalog-reconciler-{uuid.uuid4().hex}"
        self._dirty: set[str] = set()
        self._snapshots: dict[str, CapabilitySnapshot] = {}
        self._pending_invalidations: dict[str, set[str]] = {}

    def snapshot_for(self, server_id: str) -> CapabilitySnapshot | None:
        """Return the exact validated snapshot used for the latest publication."""
        return self._snapshots.get(server_id)

    async def reconcile_all(self) -> int:
        results = await self.reconcile_all_results()
        return sum(result.status == CapabilityStatus.ACTIVE for result in results)

    async def reconcile_all_results(self) -> tuple[McpReconcileResult, ...]:
        servers = {
            server_id: server
            for server_id in self._connectors
            if (server := await self._store.get_server(server_id)) is not None
        }
        ordered = tuple(sorted(servers.values(), key=lambda item: item.server_id))
        return await bounded_partition_map(
            ordered,
            self._reconcile_isolated,
            max_concurrent=self._max_concurrent,
            partitions=self._partitions(),
        )

    async def _reconcile_isolated(
        self, server: McpServerDefinition
    ) -> McpReconcileResult:
        try:
            return await asyncio.wait_for(
                self.reconcile_server(server), timeout=self._server_timeout_seconds
            )
        except TimeoutError:
            health = await self._store.record_catalog_sync(
                server.server_id,
                succeeded=False,
                attempted_at=datetime.now(UTC),
                safe_error_code="TimeoutError",
                quarantine_after_failures=self._quarantine_after_failures,
            )
            status = (CapabilityStatus.QUARANTINED if health.quarantined
                      else CapabilityStatus.DEGRADED)
            if health.quarantined:
                self._remove_remote_tools(server)
                self._snapshots.pop(server.server_id, None)
                await self._catalog.register_server(server.model_copy(update={
                    "status": status,
                    "metadata": {**server.metadata,
                                 "consecutive_sync_failures": health.consecutive_failures,
                                 "last_sync_error": "TimeoutError"},
                }))
            return McpReconcileResult(
                server.server_id,
                status,
                0,
                error="TimeoutError",
                consecutive_failures=health.consecutive_failures,
            )

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
        lease = await self._store.claim_catalog_reconcile(
            server_id=server.server_id,
            owner=self._owner,
            ttl=self._lease_ttl,
        )
        if lease is None:
            return McpReconcileResult(
                server_id=server.server_id,
                status=server.status,
                capability_count=0,
                error="lease_contended",
            )
        trusted = _reconcile_context(server)
        existing_items = await self._store.list_server_capabilities(
            server.tenant_id or "platform", server.server_id
        )
        published_server = await self._store.get_server(server.server_id)
        base_metadata = {
            **server.metadata,
            **({} if published_server is None else published_server.metadata),
        }
        committed = False
        try:
            snapshot = await connector.snapshot(trusted)
            descriptors = _normalize_snapshot(
                server,
                snapshot,
                self._max_items,
            )
            remote_count = (
                len(snapshot.tools)
                + len(snapshot.resources)
                + len(snapshot.resource_templates)
                + len(snapshot.prompts)
            )
            if remote_count and not descriptors:
                raise CapabilityAllowlistError(
                    "MCP allowlists filtered every discovered capability"
                )
            ids = [item.capability_id for item in descriptors]
            if len(ids) != len(set(ids)):
                raise ValueError("remote connector returned duplicate capabilities")
            existing = {
                (item.kind, item.canonical_name, item.version): item
                for item in existing_items
            }
            for item in descriptors:
                previous = existing.get(
                    (item.kind, item.canonical_name, item.version)
                )
                if (
                    previous is not None
                    and previous.content_digest != item.content_digest
                ):
                    raise CapabilitySchemaDriftError(
                        "remote MCP capability changed without version bump"
                    )
            if self._tool_registry is not None:
                self._tool_registry.prepare_owner(connector.connector_id, tuple(
                    _tool_capability(item, connector.connector_id) for item in descriptors
                    if item.kind == CapabilityKind.TOOL
                ))
            snapshot_digest = _catalog_snapshot_digest(server, snapshot, descriptors)
            commit = await self._catalog.replace_server_capabilities(
                server.server_id,
                descriptors,
                lease=lease,
                snapshot_digest=snapshot_digest,
                source_revision=snapshot.source_revision,
            )
            committed = True
            self._replace_remote_tools(server, descriptors, connector)
            self._snapshots[server.server_id] = snapshot.model_copy(
                update={
                    "extra": {
                        **snapshot.extra,
                        "_auraclaw_config_revision": int(server.config_revision or 0),
                        "_auraclaw_catalog_generation": commit.generation,
                        "_auraclaw_snapshot_digest": commit.snapshot_digest,
                    }
                }
            )
            if self._resource_cache is not None:
                for uri in sorted(self._pending_invalidations.pop(server.server_id, set())):
                    await self._resource_cache.invalidate(
                        uri,
                        tenant_id=server.tenant_id,
                    )
            synced_at = datetime.now(UTC).isoformat()
            health = await self._store.record_catalog_sync(
                server.server_id,
                succeeded=True,
                attempted_at=datetime.now(UTC),
                safe_error_code=None,
                quarantine_after_failures=self._quarantine_after_failures,
            )
            active = server.model_copy(
                update={
                    "status": CapabilityStatus.ACTIVE,
                    "metadata": {
                        **base_metadata,
                        "last_sync_at": synced_at,
                        "last_good_catalog_at": synced_at,
                        "last_sync_error": None,
                        "consecutive_sync_failures": health.consecutive_failures,
                        "catalog_quarantined_at": None,
                        "catalog_stale": False,
                        "active_catalog_generation": (
                            commit.generation
                        ),
                    },
                }
            )
            await self._catalog.register_server(active)
            self._dirty.discard(server.server_id)
            return McpReconcileResult(
                server_id=server.server_id,
                status=CapabilityStatus.ACTIVE,
                capability_count=len(descriptors),
            )
        except StaleCapabilitySnapshotError:
            return McpReconcileResult(
                server_id=server.server_id,
                status=CapabilityStatus.ACTIVE if existing_items else server.status,
                capability_count=len(existing_items),
                error="stale_snapshot",
            )
        except Exception as exc:
            attempted_at = datetime.now(UTC)
            health = await self._store.record_catalog_sync(
                server.server_id,
                succeeded=False,
                attempted_at=attempted_at,
                safe_error_code=type(exc).__name__,
                quarantine_after_failures=self._quarantine_after_failures,
            )
            failures = health.consecutive_failures
            status = (
                CapabilityStatus.QUARANTINED
                if health.quarantined
                else CapabilityStatus.DEGRADED
            )
            published_status = (
                CapabilityStatus.QUARANTINED
                if health.quarantined
                else CapabilityStatus.ACTIVE
                if existing_items
                else CapabilityStatus.DEGRADED
            )
            await self._catalog.register_server(
                server.model_copy(
                    update={
                        "status": published_status,
                        "metadata": {
                            **base_metadata,
                            "last_sync_at": attempted_at.isoformat(),
                            "last_sync_error": type(exc).__name__,
                            "consecutive_sync_failures": failures,
                            "catalog_quarantined_at": (
                                attempted_at.isoformat()
                                if health.quarantined
                                else None
                            ),
                            "catalog_stale": bool(existing_items),
                            "active_catalog_generation": (
                                await self._store.get_active_generation(server.server_id)
                            ),
                        },
                    }
                )
            )
            if committed:
                # Shared catalog commit succeeded, but local installation did not.
                # Never attach the old schema to a remotely upgraded server.
                self._remove_remote_tools(server)
                self._snapshots.pop(server.server_id, None)
                self._dirty.add(server.server_id)
            elif existing_items and not health.quarantined:
                self._replace_remote_tools(server, existing_items, connector)
            elif health.quarantined:
                self._remove_remote_tools(server)
            return McpReconcileResult(
                server_id=server.server_id,
                status=status,
                capability_count=len(existing_items),
                error=type(exc).__name__,
                consecutive_failures=failures,
            )
        finally:
            await self._store.release_catalog_reconcile(lease)

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
            self._pending_invalidations.setdefault(server_id, set()).add(uri)
            self._dirty.add(server_id)
            return True
        return False

    async def reconcile_dirty(self) -> int:
        server_ids = sorted(self._dirty)
        servers: list[McpServerDefinition] = []
        for server_id in server_ids:
            server = await self._store.get_server(server_id)
            if server is None:
                self._dirty.discard(server_id)
                continue
            servers.append(server)
        results = await bounded_partition_map(
            servers,
            self._reconcile_isolated,
            max_concurrent=self._max_concurrent,
            partitions=self._partitions(),
        )
        return sum(result.status == CapabilityStatus.ACTIVE for result in results)

    def _partitions(
        self,
    ) -> tuple[tuple[Any, int], ...]:
        return (
            (
                lambda server: server.tenant_id or "platform",
                self._max_concurrent_per_tenant,
            ),
            (
                lambda server: urlsplit(server.endpoint).hostname or server.server_id,
                self._max_concurrent_per_host,
            ),
        )

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
            _tool_capability(descriptor, owner)
            for descriptor in snapshot
            if descriptor.kind == CapabilityKind.TOOL
        )
        executor = ConnectorToolExecutor(connector)
        self._tool_registry.replace_owner(owner, capabilities)
        self._hands_router.replace_owner_routes(
            owner,
            {capability.invocation_ref.model_name: executor for capability in capabilities
             if capability.invocation_ref is not None},
        )
        admission = getattr(connector, "set_admission", None)
        if callable(admission):
            admission(True)

    async def drop_server(self, server_id: str) -> None:
        # Local cleanup must not depend on a shared record another replica deleted.
        self.block_server(server_id)
        self._dirty.discard(server_id)
        self._snapshots.pop(server_id, None)
        self._pending_invalidations.pop(server_id, None)
        server = await self._store.get_server(server_id)
        if server is None:
            await self._catalog.remove_server(server_id)
        else:
            await self._catalog.replace_server_capabilities(server_id, ())

    def block_server(self, server_id: str) -> None:
        connector = self._connectors.get(server_id)
        admission = getattr(connector, "set_admission", None)
        if callable(admission):
            admission(False)
        owner = connector.connector_id if connector is not None else f"mcp:{server_id}"
        if self._tool_registry is not None:
            self._tool_registry.revoke_owner(owner)
        if self._hands_router is not None:
            self._hands_router.replace_owner_routes(owner, {})

    def _remove_remote_tools(self, server: McpServerDefinition) -> None:
        self.block_server(server.server_id)


McpCatalogReconciler = CapabilityCatalogReconciler


def _catalog_snapshot_digest(
    server: McpServerDefinition,
    snapshot: CapabilitySnapshot,
    descriptors: tuple[CapabilityDescriptor, ...],
) -> str:
    payload = {
        "server_id": server.server_id,
        "config_revision": server.config_revision,
        "source_revision": snapshot.source_revision,
        "capabilities": [
            descriptor.model_dump(mode="json")
            for descriptor in sorted(
                descriptors,
                key=lambda item: (
                    item.kind.value,
                    item.canonical_name,
                    item.version,
                    item.capability_id,
                ),
            )
        ],
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    return f"sha256:{hashlib.sha256(encoded).hexdigest()}"


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
                risk_level=tool.risk_level or ("low" if tool.read_only else "high"),
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
        classification="internal",
        permission=permission,
        risk_level=risk_level,
        status=CapabilityStatus.ACTIVE,
        source_revision=digest,
        updated_at=datetime.now(UTC),
        metadata={
            "source": source,
            "source_type": "mcp",
            "config_revision": int(server.config_revision or 0),
            "server_title": server.title,
            "endpoint": server.endpoint,
            "search_aliases": list(_server_search_aliases(server)),
        },
    )


def _server_search_aliases(server: McpServerDefinition) -> tuple[str, ...]:
    configured = server.metadata.get("search_aliases", ())
    aliases = (
        tuple(str(item) for item in configured if str(item).strip())
        if isinstance(configured, (list, tuple))
        else ()
    )
    return tuple(dict.fromkeys((server.server_id, server.title, *aliases)))


def _tool_capability(
    descriptor: CapabilityDescriptor,
    owner: str,
) -> ToolCapability:
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
        permission=ToolPermission(
            descriptor.permission or ToolPermission.WRITE_WITH_APPROVAL
        ),
        risk_level=RiskLevel(descriptor.risk_level or RiskLevel.HIGH),
        runtime_location="remote-mcp",
        invocation_ref=CapabilityInvocationRef.from_descriptor(descriptor),
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
    # JSON Schema properties/items add structural levels beyond the business
    # object's depth. Validate iteratively before serializing untrusted data.
    _validate_depth(value, depth=0)
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    if len(encoded) > MAX_DESCRIPTOR_BYTES:
        raise CapabilityDescriptorSizeError("remote MCP descriptor exceeds size limit")
    return f"sha256:{hashlib.sha256(encoded).hexdigest()}"


def _validate_depth(value: Any, *, depth: int) -> None:
    pending = [(value, depth)]
    while pending:
        current, current_depth = pending.pop()
        if current_depth > MAX_DESCRIPTOR_DEPTH:
            raise CapabilityDescriptorDepthError(
                "remote MCP descriptor exceeds recursion limit"
            )
        if isinstance(current, dict):
            for key, item in current.items():
                if not isinstance(key, str) or len(key) > 256:
                    raise ValueError("remote MCP descriptor key is invalid")
                pending.append((item, current_depth + 1))
        elif isinstance(current, list):
            pending.extend((item, current_depth + 1) for item in current)
        elif current is not None and not isinstance(current, (str, int, float, bool)):
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
