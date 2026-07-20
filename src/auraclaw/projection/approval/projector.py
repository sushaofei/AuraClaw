from __future__ import annotations

import asyncio
from collections.abc import Sequence
from dataclasses import replace
from datetime import datetime

from auraclaw.contracts.events import CanonicalEvent
from auraclaw.contracts.tools import ApprovalRecord, ApprovalStatus, RiskLevel
from auraclaw.projection.ports import ProjectionWriter

APPROVAL_EVENTS = {
    "approval.requested",
    "human.response.recorded",
    "approval.approved",
    "approval.rejected",
    "approval.expired",
    "approval.cancelled",
}


class InMemoryApprovalProjection:
    """Disposable approval view derived only from Canonical Session Events."""

    def __init__(self) -> None:
        self._records: dict[tuple[str, str], ApprovalRecord] = {}
        self._event_ids: set[str] = set()
        self._lock = asyncio.Lock()

    async def project(self, events: Sequence[CanonicalEvent]) -> None:
        async with self._lock:
            for event in events:
                if event.event_id in self._event_ids:
                    continue
                if event.type == "approval.requested":
                    payload = event.payload
                    record = ApprovalRecord(
                        approval_id=str(payload["approval_id"]),
                        tenant_id=event.tenant_id,
                        session_id=event.session_id,
                        run_id=str(payload.get("run_id") or event.run_id or ""),
                        action_digest=str(payload["action_digest"]),
                        tool_name=str(payload["tool_name"]),
                        redacted_arguments=dict(payload.get("redacted_arguments", {})),
                        risk=RiskLevel(str(payload["risk"])),
                        reason=str(payload.get("reason", "")),
                        expected_effect=str(payload.get("expected_effect", "")),
                        allowed_decisions=tuple(payload.get("allowed_decisions", ())),
                        assigned_approvers=tuple(payload.get("assigned_approvers", ())),
                        policy_version=str(payload["policy_version"]),
                        expires_at=datetime.fromisoformat(str(payload["expires_at"])),
                        status=ApprovalStatus(str(payload.get("status", "waiting"))),
                    )
                    self._records[(event.tenant_id, record.approval_id)] = record
                elif event.type.startswith("approval.") and event.type != "approval.requested":
                    approval_id = str(event.payload["approval_id"])
                    key = (event.tenant_id, approval_id)
                    current = self._records.get(key)
                    if current is not None:
                        status = ApprovalStatus(event.type.split(".", 1)[1])
                        self._records[key] = replace(
                            current,
                            status=status,
                            decision=event.payload.get("decision"),
                            feedback=event.payload.get("feedback"),
                        )
                self._event_ids.add(event.event_id)

    async def get(self, tenant_id: str, approval_id: str) -> ApprovalRecord | None:
        return self._records.get((tenant_id, approval_id))

    async def find_approved(
        self, tenant_id: str, session_id: str, digest: str, policy_version: str
    ) -> ApprovalRecord | None:
        for (record_tenant, _), record in self._records.items():
            if (
                record_tenant == tenant_id
                and record.session_id == session_id
                and record.action_digest == digest
                and record.policy_version == policy_version
                and record.status is ApprovalStatus.APPROVED
            ):
                return record
        return None


class CompositeProjection:
    def __init__(self, *projectors: ProjectionWriter) -> None:
        self._projectors = projectors

    async def project(self, events: Sequence[CanonicalEvent]) -> None:
        for projector in self._projectors:
            await projector.project(events)
