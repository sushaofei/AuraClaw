from __future__ import annotations

import base64
import hashlib
import re
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from auraclaw.action.ports import CapabilityCatalogStore, CapabilityConnector
from auraclaw.action.skill_lifecycle import (
    SkillLifecycleStore,
    SkillSourceLease,
    SkillSourcePackageIdentity,
    SkillSourceSnapshotCommit,
)
from auraclaw.action.skill_packages import SkillPackage, skill_package_digest
from auraclaw.action.skill_publication import SkillPublicationService
from auraclaw.action.skill_rebuild import SkillStateRebuilder
from auraclaw.contracts.capabilities import McpServerDefinition
from auraclaw.contracts.errors import VersionConflictError
from auraclaw.contracts.hands import (
    CapabilitySnapshot,
    HandsResourceDescriptor,
    HandsTrustedContext,
)
from auraclaw.contracts.skills import (
    PublishSkillCommand,
    SkillSourceDesiredState,
    SkillSourceKind,
    SkillSourceRecord,
    SkillSourceSyncState,
)

_SKILL_URI = re.compile(r"^skill://([^/]+)/([^/]+)/([^/]+)/(.+)$")
_URI_SUFFIX_TO_PATH = {"manifest": "manifest.json"}
_LEASE_TTL = timedelta(minutes=5)
_MISSING_SNAPSHOT_THRESHOLD = 2


@dataclass(frozen=True)
class SkillPackageReconcileFailure:
    publisher: str
    name: str
    version: str
    error_code: str


@dataclass(frozen=True)
class SkillReconcileResult:
    server_id: str
    published_count: int
    error: str | None = None
    package_failures: tuple[SkillPackageReconcileFailure, ...] = ()


class SkillPackageReconciler:
    """Discover signed Skill packages from remote MCP Resources and publish locally."""

    def __init__(
        self,
        *,
        store: CapabilityCatalogStore,
        connectors: dict[str, CapabilityConnector],
        lifecycle: SkillLifecycleStore,
        publication: SkillPublicationService,
        rebuilder: SkillStateRebuilder,
        owner: str | None = None,
        snapshot_provider: Callable[[str], CapabilitySnapshot | None] | None = None,
    ) -> None:
        self._store = store
        self._connectors = connectors
        self._lifecycle = lifecycle
        self._publication = publication
        self._rebuilder = rebuilder
        self._owner = owner or f"skill-reconciler-{uuid.uuid4().hex}"
        self._snapshot_provider = snapshot_provider

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

    async def reconcile_source(
        self, tenant_id: str, source_id: str
    ) -> SkillReconcileResult:
        source = await self._lifecycle.get_source(tenant_id, source_id)
        if source is None or source.kind is not SkillSourceKind.MCP:
            return SkillReconcileResult(
                server_id="", published_count=0, error="source_not_found"
            )
        server_id = source.config_metadata.get("server_id")
        if not isinstance(server_id, str) or not server_id:
            return SkillReconcileResult(
                server_id="", published_count=0, error="source_config_invalid"
            )
        server = await self._store.get_server(server_id)
        if server is None or (server.tenant_id or "platform") != tenant_id:
            return SkillReconcileResult(
                server_id=server_id, published_count=0, error="server_not_found"
            )
        return await self.reconcile_server(server)

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
        tenant_id = server.tenant_id or "platform"
        lease: SkillSourceLease | None = None
        source: SkillSourceRecord | None = None
        try:
            source = await self._ensure_source(server, tenant_id)
            if source.desired_state is not SkillSourceDesiredState.ENABLED:
                return SkillReconcileResult(
                    server_id=server.server_id,
                    published_count=0,
                    error="source_not_enabled",
                )
            lease = await self._lifecycle.claim_source_lease(
                tenant_id=tenant_id,
                source_id=source.source_id,
                owner=self._owner,
                ttl=_LEASE_TTL,
            )
            if lease is None:
                return SkillReconcileResult(
                    server_id=server.server_id,
                    published_count=0,
                    error="lease_contended",
                )
            snapshot = (
                self._snapshot_provider(server.server_id)
                if self._snapshot_provider is not None
                else None
            )
            if snapshot is None:
                snapshot = await connector.snapshot(trusted)
            lease = await self._renew(lease)
            grouped = _group_skill_resource_uris(snapshot.resources)
            published = 0
            observed: list[SkillSourcePackageIdentity] = []
            package_failures: list[SkillPackageReconcileFailure] = []
            for (publisher, name, version), uris in grouped.items():
                if not any(uri.endswith("/manifest") for uri in uris):
                    continue
                try:
                    package = await _download_skill_package(
                        connector,
                        trusted,
                        uris,
                    )
                    lease = await self._renew(lease)
                    digest = skill_package_digest(package)
                    await self._publication.publish(
                        PublishSkillCommand(
                            tenant_id=tenant_id,
                            actor_id="action-hands-skill-reconciler",
                            source_id=source.source_id,
                            command_id=f"mcp-skill:{server.server_id}:{digest}",
                            correlation_id=f"skill-reconcile:{server.server_id}",
                            causation_id=f"mcp-snapshot:{server.server_id}",
                        ),
                        package,
                        source_lease=lease,
                    )
                except Exception as exc:
                    package_failures.append(
                        SkillPackageReconcileFailure(
                            publisher=publisher,
                            name=name,
                            version=version,
                            error_code=type(exc).__name__,
                        )
                    )
                    continue
                published += 1
                observed.append(SkillSourcePackageIdentity(publisher, name, version))
            lease = await self._renew(lease)
            if package_failures:
                lease = await self._record_failure(
                    source=source,
                    lease=lease,
                    safe_error_code="package_failure",
                ) or lease
                await self._rebuilder.rebuild_tenant(tenant_id)
                return SkillReconcileResult(
                    server_id=server.server_id,
                    published_count=published,
                    error="package_failure",
                    package_failures=tuple(package_failures),
                )
            now = datetime.now(UTC)
            await self._lifecycle.commit_source_snapshot(
                SkillSourceSnapshotCommit(
                    state=SkillSourceSyncState(
                        source_id=source.source_id,
                        tenant_id=tenant_id,
                        generation=lease.fencing_token,
                        cursor=snapshot.source_revision,
                        complete_snapshot=True,
                        last_success_at=now,
                        last_attempt_at=now,
                    ),
                    lease=lease,
                    observed=tuple(sorted(observed)),
                    missing_snapshot_threshold=_MISSING_SNAPSHOT_THRESHOLD,
                    actor_id="action-hands-skill-reconciler",
                    command_prefix=f"skill-source-retire:{source.source_id}",
                    correlation_id=f"skill-reconcile:{server.server_id}",
                    causation_id=f"mcp-snapshot:{server.server_id}",
                    occurred_at=now,
                )
            )
            await self._rebuilder.rebuild_tenant(tenant_id)
            return SkillReconcileResult(
                server_id=server.server_id,
                published_count=published,
            )
        except Exception as exc:
            if lease is not None and source is not None:
                lease = await self._record_failure(
                    source=source,
                    lease=lease,
                    safe_error_code=type(exc).__name__,
                ) or lease
            if source is not None:
                try:
                    await self._rebuilder.rebuild_tenant(tenant_id)
                except Exception:
                    pass
            return SkillReconcileResult(
                server_id=server.server_id,
                published_count=0,
                error=type(exc).__name__,
            )
        finally:
            if lease is not None:
                await self._lifecycle.release_source_lease(lease)

    async def _renew(self, lease: SkillSourceLease) -> SkillSourceLease:
        renewed = await self._lifecycle.renew_source_lease(lease, ttl=_LEASE_TTL)
        if renewed is None:
            raise VersionConflictError("Skill Source lease is stale")
        return renewed

    async def _record_failure(
        self,
        *,
        source: SkillSourceRecord,
        lease: SkillSourceLease,
        safe_error_code: str,
    ) -> SkillSourceLease | None:
        try:
            renewed = await self._lifecycle.renew_source_lease(
                lease, ttl=_LEASE_TTL
            )
            if renewed is None:
                return None
            previous = await self._lifecycle.get_sync_state(
                source.tenant_id, source.source_id
            )
            await self._lifecycle.put_sync_state_fenced(
                SkillSourceSyncState(
                    source_id=source.source_id,
                    tenant_id=source.tenant_id,
                    generation=renewed.fencing_token,
                    cursor=None if previous is None else previous.cursor,
                    complete_snapshot=False,
                    last_success_at=(
                        None if previous is None else previous.last_success_at
                    ),
                    last_attempt_at=datetime.now(UTC),
                    consecutive_failures=(
                        1 if previous is None else previous.consecutive_failures + 1
                    ),
                    safe_error_code=safe_error_code[:128],
                ),
                lease=renewed,
            )
            return renewed
        except Exception:
            return None

    async def _ensure_source(
        self,
        server: McpServerDefinition,
        tenant_id: str,
    ) -> SkillSourceRecord:
        source_id = _source_id(server.server_id)
        existing = await self._lifecycle.get_source(tenant_id, source_id)
        if existing is not None:
            return existing
        allowlist_value = server.metadata.get("skill_publisher_allowlist", ())
        if not isinstance(allowlist_value, (list, tuple)) or not all(
            isinstance(value, str) for value in allowlist_value
        ):
            raise ValueError("MCP Skill publisher allowlist is invalid")
        allowlist = tuple(dict.fromkeys(allowlist_value))
        if not allowlist:
            raise ValueError("MCP Skill publisher allowlist is required")
        now = datetime.now(UTC)
        source = SkillSourceRecord(
            source_id=source_id,
            tenant_id=tenant_id,
            kind=SkillSourceKind.MCP,
            desired_state=SkillSourceDesiredState.ENABLED,
            publisher_allowlist=allowlist,
            config_metadata={"server_id": server.server_id},
            created_by="action-hands-skill-reconciler",
            updated_by="action-hands-skill-reconciler",
            created_at=now,
            updated_at=now,
        )
        try:
            return await self._lifecycle.put_source(source, expected_revision=0)
        except VersionConflictError:
            concurrent = await self._lifecycle.get_source(tenant_id, source_id)
            if concurrent is None:
                raise
            return concurrent


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


def _source_id(server_id: str) -> str:
    digest = hashlib.sha256(server_id.encode()).hexdigest()[:32]
    return f"sks_mcp_{digest}"
