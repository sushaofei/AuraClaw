from __future__ import annotations

import asyncio
from collections import defaultdict
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Protocol

from auraclaw.action.ports import SkillArtifactLifecycle, SkillArtifactOrphan
from auraclaw.action.skill_lifecycle import SkillLifecycleStore, SkillOutboxRecord
from auraclaw.action.skill_lifecycle_events import SkillTenantRebuilder
from auraclaw.contracts.observability import MetricPoint
from auraclaw.contracts.tools import ArtifactRef


class MetricWriter(Protocol):
    async def write_metric(self, metric: MetricPoint) -> None: ...


@dataclass(frozen=True)
class SkillReliabilityResult:
    outbox_completed: int = 0
    outbox_failed: int = 0
    orphans_deleted: int = 0
    references_repaired: int = 0
    orphan_failed: int = 0


class SkillPublicationReliabilityWorker:
    def __init__(
        self,
        *,
        lifecycle: SkillLifecycleStore,
        artifacts: SkillArtifactLifecycle,
        rebuilder: SkillTenantRebuilder,
        owner: str,
        max_concurrent: int = 8,
        claim_ttl: timedelta = timedelta(seconds=30),
        metric_writer: MetricWriter | None = None,
    ) -> None:
        if max_concurrent < 1 or claim_ttl <= timedelta(0):
            raise ValueError("Skill reliability capacity and claim TTL must be positive")
        self._lifecycle = lifecycle
        self._artifacts = artifacts
        self._rebuilder = rebuilder
        self._owner = owner
        self._max_concurrent = max_concurrent
        self._claim_ttl = claim_ttl
        self._metric_writer = metric_writer

    async def run_once(self, *, limit: int = 100) -> SkillReliabilityResult:
        completed = 0
        outbox_failed = 0
        deleted = 0
        repaired = 0
        orphan_failed = 0
        try:
            outbox = await self._lifecycle.claim_outbox(
                owner=self._owner,
                limit=min(limit, self._max_concurrent),
                claim_ttl=self._claim_ttl,
            )
        except Exception:
            return SkillReliabilityResult(outbox_failed=1)
        await self._emit("skill.reliability.queue.claimed", float(len(outbox)))
        await self._emit("skill.reliability.in_flight", float(len(outbox)))
        by_tenant: dict[str, list[SkillOutboxRecord]] = defaultdict(list)
        for record in outbox:
            by_tenant[record.tenant_id].append(record)
        tenant_results = await asyncio.gather(
            *(self._process_tenant(records) for records in by_tenant.values()),
            return_exceptions=True,
        )
        for result in tenant_results:
            if isinstance(result, BaseException):
                outbox_failed += 1
            else:
                tenant_completed, tenant_failed = result
                completed += tenant_completed
                outbox_failed += tenant_failed

        try:
            orphans = await self._artifacts.claim_orphans(
                owner=self._owner, limit=min(limit, self._max_concurrent)
            )
        except Exception:
            return SkillReliabilityResult(
                outbox_completed=completed,
                outbox_failed=outbox_failed,
                orphans_deleted=deleted,
                references_repaired=repaired,
                orphan_failed=orphan_failed + 1,
            )
        orphan_results = await asyncio.gather(
            *(self._process_orphan(orphan) for orphan in orphans),
            return_exceptions=True,
        )
        for orphan_result in orphan_results:
            if isinstance(orphan_result, BaseException):
                orphan_failed += 1
            elif orphan_result == "deleted":
                deleted += 1
            else:
                repaired += 1
        summary = SkillReliabilityResult(
            outbox_completed=completed,
            outbox_failed=outbox_failed,
            orphans_deleted=deleted,
            references_repaired=repaired,
            orphan_failed=orphan_failed,
        )
        await self._emit("skill.reliability.in_flight", 0.0)
        return summary

    async def _process_tenant(
        self, records: list[SkillOutboxRecord]
    ) -> tuple[int, int]:
        completed = 0
        failed = 0
        prepared: list[SkillOutboxRecord] = []
        monitors: dict[str, asyncio.Task[None]] = {}
        lease_lost: dict[str, asyncio.Event] = {}
        parent = asyncio.current_task()
        assert parent is not None
        try:
            for record in records:
                lost = asyncio.Event()
                lease_lost[record.outbox_id] = lost
                monitors[record.outbox_id] = asyncio.create_task(
                    self._heartbeat_outbox(record.outbox_id, parent, lost)
                )
                try:
                    if not await self._lifecycle.renew_outbox(
                        outbox_id=record.outbox_id,
                        owner=self._owner,
                        claim_ttl=self._claim_ttl,
                    ):
                        await self._emit(
                            "skill.reliability.duplicate_prevented",
                            1.0,
                            tenant_id=record.tenant_id,
                        )
                        raise RuntimeError("skill_outbox_lease_lost")
                    artifact_payload = record.payload.get("artifact_ref")
                    if not isinstance(artifact_payload, dict):
                        raise ValueError("Skill outbox Artifact Ref is invalid")
                    artifact_ref = ArtifactRef(
                        artifact_id=str(artifact_payload["artifact_id"]),
                        version=int(artifact_payload["version"]),
                        content_hash=str(artifact_payload["content_hash"]),
                        media_type=str(artifact_payload["media_type"]),
                        size=int(artifact_payload["size"]),
                    )
                    if not await self._lifecycle.has_artifact_reference(
                        record.tenant_id, artifact_ref.artifact_id, artifact_ref.version
                    ):
                        prepared.append(record)
                        continue
                    package_digest = str(record.payload["package_digest"])
                    correlation_id = f"skill-outbox:{record.outbox_id}"
                    await self._artifacts.claim_publication(
                        tenant_id=record.tenant_id,
                        artifact_ref=artifact_ref,
                        command_id=record.command_id,
                        correlation_id=correlation_id,
                    )
                    await self._artifacts.bind_publication(
                        tenant_id=record.tenant_id,
                        artifact_ref=artifact_ref,
                        command_id=record.command_id,
                        package_digest=package_digest,
                        correlation_id=correlation_id,
                    )
                    prepared.append(record)
                except Exception as exc:
                    await self._lifecycle.fail_outbox(
                        outbox_id=record.outbox_id,
                        owner=self._owner,
                        safe_error_code=type(exc).__name__,
                    )
                    monitor = monitors.pop(record.outbox_id)
                    monitor.cancel()
                    await asyncio.gather(monitor, return_exceptions=True)
                    lease_lost.pop(record.outbox_id, None)
                    failed += 1
            if prepared:
                await self._rebuilder.rebuild_tenant(prepared[0].tenant_id)
            for record in prepared:
                was_lost = lease_lost[record.outbox_id].is_set()
                committed = await self._lifecycle.complete_outbox(
                    outbox_id=record.outbox_id, owner=self._owner
                )
                if was_lost or not committed:
                    failed += 1
                else:
                    completed += 1
            return completed, failed
        finally:
            for monitor in monitors.values():
                monitor.cancel()
            await asyncio.gather(*monitors.values(), return_exceptions=True)

    async def _heartbeat_outbox(
        self,
        outbox_id: str,
        parent: asyncio.Task[object],
        lease_lost: asyncio.Event,
    ) -> None:
        interval = max(0.01, self._claim_ttl.total_seconds() / 3)
        try:
            while True:
                await asyncio.sleep(interval)
                if not await self._lifecycle.renew_outbox(
                    outbox_id=outbox_id,
                    owner=self._owner,
                    claim_ttl=self._claim_ttl,
                ):
                    await self._emit("skill.reliability.renew_failure", 1.0)
                    lease_lost.set()
                    parent.cancel()
                    return
        except asyncio.CancelledError:
            raise
        except Exception:
            await self._emit("skill.reliability.renew_failure", 1.0)
            lease_lost.set()
            parent.cancel()

    async def _emit(
        self, name: str, value: float, *, tenant_id: str | None = None
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

    async def _process_orphan(self, orphan: SkillArtifactOrphan) -> str:
        referenced = await self._lifecycle.has_artifact_reference(
            orphan.tenant_id,
            orphan.artifact_ref.artifact_id,
            orphan.artifact_ref.version,
        )
        return await self._artifacts.resolve_orphan(
            tenant_id=orphan.tenant_id,
            orphan=orphan,
            referenced=referenced,
            package_digest=(
                f"sha256:{orphan.artifact_ref.content_hash}" if referenced else None
            ),
            correlation_id=f"skill-orphan:{orphan.artifact_ref.artifact_id}",
        )
