from __future__ import annotations

import base64
import re
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from auraclaw.action.ports import CapabilityCatalogStore, CapabilityConnector
from auraclaw.action.skill_packages import SkillPackage, SkillPackageRegistry
from auraclaw.contracts.capabilities import McpServerDefinition
from auraclaw.contracts.hands import HandsResourceDescriptor, HandsTrustedContext

_SKILL_URI = re.compile(r"^skill://([^/]+)/([^/]+)/([^/]+)/(.+)$")
_URI_SUFFIX_TO_PATH = {"manifest": "manifest.json"}


@dataclass(frozen=True)
class SkillReconcileResult:
    server_id: str
    published_count: int
    error: str | None = None


class SkillPackageReconciler:
    """Discover signed Skill packages from remote MCP Resources and publish locally."""

    def __init__(
        self,
        *,
        store: CapabilityCatalogStore,
        connectors: dict[str, CapabilityConnector],
        registry: SkillPackageRegistry,
    ) -> None:
        self._store = store
        self._connectors = connectors
        self._registry = registry

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
        return sum(result.published_count for result in results)

    async def reconcile_server(
        self,
        server: McpServerDefinition,
    ) -> SkillReconcileResult:
        connector = self._connectors.get(server.server_id)
        if connector is None or not server.enabled:
            return SkillReconcileResult(
                server_id=server.server_id,
                published_count=0,
                error="transport_unavailable",
            )
        trusted = _reconcile_context(server)
        try:
            snapshot = await connector.snapshot(trusted)
            grouped = _group_skill_resource_uris(snapshot.resources)
            tenant_id = server.tenant_id or "platform"
            published = 0
            for (_publisher, _name, _version), uris in grouped.items():
                if not any(uri.endswith("/manifest") for uri in uris):
                    continue
                package = await _download_skill_package(
                    connector,
                    trusted,
                    uris,
                )
                await self._registry.publish(tenant_id, package)
                published += 1
            return SkillReconcileResult(
                server_id=server.server_id,
                published_count=published,
            )
        except Exception as exc:
            return SkillReconcileResult(
                server_id=server.server_id,
                published_count=0,
                error=type(exc).__name__,
            )


def _group_skill_resource_uris(
    resources: tuple[HandsResourceDescriptor, ...],
) -> dict[tuple[str, str, str], tuple[str, ...]]:
    grouped: dict[tuple[str, str, str], list[str]] = {}
    for resource in resources:
        locator = resource.uri or resource.uri_template or ""
        match = _SKILL_URI.fullmatch(locator)
        if match is None:
            continue
        publisher, name, version, _suffix = match.groups()
        key = (publisher, name, version)
        grouped.setdefault(key, []).append(locator)
    return {key: tuple(sorted(uris)) for key, uris in grouped.items()}


async def _download_skill_package(
    connector: CapabilityConnector,
    trusted: HandsTrustedContext,
    uris: tuple[str, ...],
) -> SkillPackage:
    files: dict[str, bytes] = {}
    for uri in uris:
        match = _SKILL_URI.fullmatch(uri)
        if match is None:
            continue
        suffix = match.group(4)
        path = _URI_SUFFIX_TO_PATH.get(suffix, suffix)
        contents = await connector.read_resource(trusted, uri)
        if not contents:
            raise ValueError(f"Skill resource returned no content: {uri}")
        files[path] = _resource_bytes(contents[0])
    if "manifest.json" not in files:
        raise ValueError("Skill package is missing manifest.json")
    return SkillPackage.from_files(files)


def _resource_bytes(content: object) -> bytes:
    text = getattr(content, "text", None)
    blob = getattr(content, "blob", None)
    if isinstance(text, str):
        return text.encode()
    if isinstance(blob, str):
        return base64.b64decode(blob)
    raise ValueError("Skill resource content is invalid")


def _reconcile_context(server: McpServerDefinition) -> HandsTrustedContext:
    now = datetime.now(UTC)
    return HandsTrustedContext(
        tenant_id=server.tenant_id or "platform",
        root_session_id=f"skills:{server.server_id}",
        session_id=f"skills:{server.server_id}",
        run_id=f"skills:{server.server_id}:{int(now.timestamp())}",
        runtime_id="action-hands-skill-reconciler",
        lease_id=f"skills:{server.server_id}",
        fencing_token=1,
        deadline=now + timedelta(minutes=5),
    )
