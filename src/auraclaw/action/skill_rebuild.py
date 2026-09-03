from __future__ import annotations

import asyncio
import hashlib
import time
from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Protocol

from auraclaw.action.capability_catalog import CapabilityCatalog
from auraclaw.action.ports import ArtifactContentReader
from auraclaw.action.skill_content_cache import SkillPackageContentCache
from auraclaw.action.skill_lifecycle import SkillLifecycleStore
from auraclaw.action.skill_packages import (
    SkillPackage,
    SkillPackageRegistry,
    skill_capability_descriptor,
    skill_package_digest,
    version_satisfies,
)
from auraclaw.action.skill_publishers import SkillPublisherTrustService
from auraclaw.contracts.capabilities import (
    CapabilityStatus,
    McpServerDefinition,
)
from auraclaw.contracts.observability import MetricPoint
from auraclaw.contracts.skills import (
    PublishedSkill,
    SkillInstallationRecord,
    SkillInstallationStatus,
    SkillPackageRetentionStatus,
    SkillPublicationStatus,
    SkillRevocationAction,
)

_SKILL_MEDIA_TYPE = "application/vnd.auraclaw.skill-package+json"


class MetricWriter(Protocol):
    async def write_metric(self, metric: MetricPoint) -> None: ...


@dataclass(frozen=True)
class SkillRebuildResult:
    tenant_count: int
    publication_count: int
    failure_count: int
    safe_failure_codes: tuple[str, ...] = ()


class SkillStateRebuilder:
    def __init__(
        self,
        *,
        lifecycle: SkillLifecycleStore,
        artifacts: ArtifactContentReader,
        registry: SkillPackageRegistry,
        catalog: CapabilityCatalog,
        publisher_trust: SkillPublisherTrustService | None = None,
        content_cache: SkillPackageContentCache | None = None,
        metric_writer: MetricWriter | None = None,
    ) -> None:
        self._lifecycle = lifecycle
        self._artifacts = artifacts
        self._registry = registry
        self._catalog = catalog
        self._publisher_trust = publisher_trust
        self._content_cache = content_cache or SkillPackageContentCache(
            artifacts, metric_writer=metric_writer
        )
        self._metric_writer = metric_writer
        self._tenant_locks: defaultdict[str, asyncio.Lock] = defaultdict(asyncio.Lock)
        self._rebuild_tasks: dict[
            str, asyncio.Task[tuple[int, tuple[str, ...]]]
        ] = {}
        self._requested_generations: dict[str, int] = {}
        self._rebuild_state_lock = asyncio.Lock()
        self._snapshot_digests: dict[str, str] = {}

    def snapshot_digest(self, tenant_id: str) -> str | None:
        return self._snapshot_digests.get(tenant_id)

    async def rebuild_all(self) -> SkillRebuildResult:
        tenants = await self._lifecycle.list_tenants()
        publication_count = 0
        failures: list[str] = []
        for tenant_id in tenants:
            count, tenant_failures = await self.rebuild_tenant(tenant_id)
            publication_count += count
            failures.extend(tenant_failures)
        return SkillRebuildResult(
            tenant_count=len(tenants),
            publication_count=publication_count,
            failure_count=len(failures),
            safe_failure_codes=tuple(failures),
        )

    async def rebuild_tenant(self, tenant_id: str) -> tuple[int, tuple[str, ...]]:
        async with self._rebuild_state_lock:
            self._requested_generations[tenant_id] = (
                self._requested_generations.get(tenant_id, 0) + 1
            )
            task = self._rebuild_tasks.get(tenant_id)
            if task is None:
                task = asyncio.create_task(self._run_tenant_rebuild(tenant_id))
                self._rebuild_tasks[tenant_id] = task
        return await asyncio.shield(task)

    async def _run_tenant_rebuild(
        self, tenant_id: str
    ) -> tuple[int, tuple[str, ...]]:
        try:
            while True:
                async with self._rebuild_state_lock:
                    generation = self._requested_generations[tenant_id]
                started = time.monotonic()
                async with self._tenant_locks[tenant_id]:
                    result = await self._rebuild_tenant_locked(tenant_id)
                await self._emit(
                    "skill.rebuild.duration.seconds",
                    time.monotonic() - started,
                    tenant_id,
                )
                async with self._rebuild_state_lock:
                    if self._requested_generations.get(tenant_id) == generation:
                        current = asyncio.current_task()
                        if self._rebuild_tasks.get(tenant_id) is current:
                            self._rebuild_tasks.pop(tenant_id, None)
                            self._requested_generations.pop(tenant_id, None)
                        return result
        finally:
            async with self._rebuild_state_lock:
                current = asyncio.current_task()
                if self._rebuild_tasks.get(tenant_id) is current:
                    self._rebuild_tasks.pop(tenant_id, None)
                    self._requested_generations.pop(tenant_id, None)

    async def _rebuild_tenant_locked(
        self, tenant_id: str
    ) -> tuple[int, tuple[str, ...]]:
        installation_records = await self._lifecycle.list_installations(tenant_id)
        installations = {
            (item.publisher, item.name): item
            for item in installation_records
            if item.status is SkillInstallationStatus.ACTIVE
        }
        entries: list[tuple[SkillPackage, PublishedSkill]] = []
        discoverable: set[tuple[str, str, str]] = set()
        failures: list[str] = []
        records = await self._lifecycle.list_publications(tenant_id)
        snapshot_parts: list[tuple[str, ...]] = [
            (
                "installation",
                item.publisher,
                item.name,
                item.status.value,
                str(item.revision),
                item.pinned_package_digest or "",
            )
            for item in sorted(
                installation_records, key=lambda value: (value.publisher, value.name)
            )
        ]
        scanned = 0
        reused = 0
        for record in records:
            if record.status not in {
                SkillPublicationStatus.ACTIVE,
                SkillPublicationStatus.RESTORING,
                SkillPublicationStatus.RETIRED,
            } and not (
                record.status is SkillPublicationStatus.REVOKED
                and record.revocation_action is SkillRevocationAction.CONTINUE
            ):
                continue
            scanned += 1
            package_record = await self._lifecycle.get_package(
                tenant_id, record.publisher, record.name, record.version
            )
            if (
                package_record is None
                or package_record.retention_status is not SkillPackageRetentionStatus.RETAINED
            ):
                failures.append("package_record_unavailable")
                continue
            snapshot_parts.append(
                (
                    "publication",
                    record.publisher,
                    record.name,
                    record.version,
                    record.status.value,
                    str(record.revision),
                    record.package_digest,
                    package_record.retention_status.value,
                    str(package_record.retention_revision),
                )
            )
            if package_record.artifact_ref.media_type != _SKILL_MEDIA_TYPE:
                failures.append("artifact_media_type_invalid")
                continue
            try:
                package = self._registry.cached_package(
                    tenant_id,
                    record.publisher,
                    record.name,
                    record.version,
                    record.package_digest,
                )
                if package is None:
                    package = await self._content_cache.load(
                        tenant_id=tenant_id,
                        package_digest=record.package_digest,
                        artifact_ref=package_record.artifact_ref,
                        actor_id="action-hands-skill-rebuilder",
                        correlation_id=f"skill-rebuild:{tenant_id}",
                    )
                else:
                    reused += 1
                if package.manifest.signature.startswith("ed25519:"):
                    if self._publisher_trust is None:
                        raise ValueError("publisher registry unavailable")
                    package = self._registry.validate_content(package)
                    key_id = await self._publisher_trust.verify_for_restore(tenant_id, package)
                    if key_id != package_record.signature_key_id:
                        raise ValueError("signature key mismatch")
                else:
                    package = self._registry.validate(package)
                if package.manifest != package_record.manifest:
                    raise ValueError("manifest mismatch")
                if skill_package_digest(package) != record.package_digest:
                    raise ValueError("package digest mismatch")
                entries.append(
                    (
                        package,
                        PublishedSkill(
                            tenant_id=tenant_id,
                            manifest=package_record.manifest,
                            package_digest=package_record.package_digest,
                            artifact_ref=package_record.artifact_ref,
                            status=record.status,
                            revocation_action=record.revocation_action,
                        ),
                    )
                )
                installation = installations.get((record.publisher, record.name))
                if installation is not None and _installation_allows(
                    installation, record.version, record.package_digest
                ):
                    discoverable.add((record.publisher, record.name, record.version))
            except Exception as exc:
                failures.append(f"package_restore_{type(exc).__name__}")
        self._registry.replace_tenant(
            tenant_id,
            tuple(entries),
            discoverable=frozenset(discoverable),
            signatures_verified=True,
        )
        server = _skill_server(tenant_id)
        await self._catalog.register_server(server)
        await self._catalog.replace_server_capabilities(
            server.server_id,
            tuple(
                skill_capability_descriptor(
                    publication,
                    server_id=server.server_id,
                ).model_copy(update={"updated_at": datetime.now(UTC)})
                for _package, publication in entries
                if (
                    publication.manifest.publisher,
                    publication.manifest.name,
                    publication.manifest.version,
                )
                in discoverable
            ),
        )
        await self._content_cache.prune_tenant(
            tenant_id,
            retained_digests=frozenset(
                publication.package_digest for _package, publication in entries
            ),
        )
        await self._emit("skill.rebuild.packages.scanned", float(scanned), tenant_id)
        await self._emit("skill.rebuild.packages.reused", float(reused), tenant_id)
        encoded_snapshot = repr(sorted(snapshot_parts)).encode()
        self._snapshot_digests[tenant_id] = (
            f"sha256:{hashlib.sha256(encoded_snapshot).hexdigest()}"
        )
        return len(entries), tuple(failures)

    async def _emit(
        self, name: str, value: float, tenant_id: str | None = None
    ) -> None:
        if self._metric_writer is None:
            return
        try:
            await asyncio.wait_for(
                self._metric_writer.write_metric(
                    MetricPoint(
                        name=name,
                        value=value,
                        observed_at=datetime.now(UTC),
                        tenant_id=tenant_id,
                    )
                ),
                timeout=0.1,
            )
        except Exception:
            return


def _installation_allows(
    installation: SkillInstallationRecord,
    version: str,
    package_digest: str,
) -> bool:
    if (
        installation.pinned_package_digest is not None
        and installation.pinned_package_digest != package_digest
    ):
        return False
    return version_satisfies(version, installation.version_constraint)


def _skill_server(tenant_id: str) -> McpServerDefinition:
    suffix = hashlib.sha256(tenant_id.encode()).hexdigest()[:24]
    return McpServerDefinition(
        server_id=f"skill-registry-{suffix}",
        tenant_id=tenant_id,
        title="AuraClaw Skill Registry",
        endpoint="https://skill-registry.auraclaw.invalid/mcp",
        credential_ref="internal://action-hands/skill-registry",
        status=CapabilityStatus.ACTIVE,
        enabled=True,
        metadata={"managed_source": "skill-lifecycle"},
    )
