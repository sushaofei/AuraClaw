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
    "approval.vote.recorded",
    "approval.delegated",
    "approval.escalated",
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
                        required_approvals=max(
                            1, int(payload.get("required_approvals", 1))
                        ),
                        votes=tuple(payload.get("votes", ())),
                        escalation_at=(
                            datetime.fromisoformat(str(payload["escalation_at"]))
                            if payload.get("escalation_at")
                            else None
                        ),
                        escalation_level=int(payload.get("escalation_level", 0)),
                        status=ApprovalStatus(str(payload.get("status", "waiting"))),
                    )
                    self._records[(event.tenant_id, record.approval_id)] = record
                elif event.type == "approval.vote.recorded":
                    approval_id = str(event.payload["approval_id"])
                    key = (event.tenant_id, approval_id)
                    current = self._records.get(key)
                    if current is not None:
                        self._records[key] = replace(
                            current,
                            votes=(
                                *current.votes,
                                {
                                    "actor_id": str(event.payload["actor_id"]),
                                    "decision": str(event.payload["decision"]),
                                    "feedback": event.payload.get("feedback"),
                                    "recorded_at": event.occurred_at.isoformat(),
                                },
                            ),
                        )
                elif event.type == "approval.delegated":
                    approval_id = str(event.payload["approval_id"])
                    key = (event.tenant_id, approval_id)
                    current = self._records.get(key)
                    if current is not None:
                        before = str(event.payload["from_approver"])
                        after = str(event.payload["to_approver"])
                        self._records[key] = replace(
                            current,
                            assigned_approvers=tuple(
                                dict.fromkeys(
                                    after if item == before else item
                                    for item in current.assigned_approvers
                                )
                            ),
                        )
                elif event.type == "approval.escalated":
                    approval_id = str(event.payload["approval_id"])
                    key = (event.tenant_id, approval_id)
                    current = self._records.get(key)
                    if current is not None:
                        additions = tuple(str(item) for item in event.payload["approvers"])
                        self._records[key] = replace(
                            current,
                            assigned_approvers=tuple(
                                dict.fromkeys((*current.assigned_approvers, *additions))
                            ),
                            escalation_level=int(event.payload["escalation_level"]),
                            escalation_at=(
                                datetime.fromisoformat(
                                    str(event.payload["next_escalation_at"])
                                )
                                if event.payload.get("next_escalation_at")
                                else None
                            ),
                        )
                elif event.type.startswith("approval.") and event.type != "approval.requested":
                    approval_id = str(event.payload["approval_id"])
                    key = (event.tenant_id, approval_id)
                    current = self._records.get(key)
                    if current is not None:
                        status = ApprovalStatus(event.type.split(".", 1)[1])
                        votes = current.votes
                        if event.payload.get("actor_id") is not None:
                            actor_id = str(event.payload["actor_id"])
                            if not any(
                                str(vote.get("actor_id")) == actor_id for vote in votes
                            ):
                                votes = (
                                    *votes,
                                    {
                                        "actor_id": actor_id,
                                        "decision": str(
                                            event.payload.get("decision", status.value)
                                        ),
                                        "feedback": event.payload.get("feedback"),
                                        "recorded_at": event.occurred_at.isoformat(),
                                    },
                                )
                        self._records[key] = replace(
                            current,
                            status=status,
                            votes=votes,
                            decision=event.payload.get("decision"),
                            feedback=event.payload.get("feedback"),
                        )
                self._event_ids.add(event.event_id)

    async def get(self, tenant_id: str, approval_id: str) -> ApprovalRecord | None:
        return self._records.get((tenant_id, approval_id))

    async def list_due(self, now: datetime, *, limit: int = 100) -> list[ApprovalRecord]:
        return sorted(
            (
                record
                for record in self._records.values()
                if record.status in {ApprovalStatus.REQUESTED, ApprovalStatus.WAITING}
                and (
                    record.expires_at <= now
                    or (
                        record.escalation_at is not None
                        and record.escalation_at <= now
                    )
                )
            ),
            key=lambda record: (
                min(record.expires_at, record.escalation_at or record.expires_at),
                record.tenant_id,
                record.approval_id,
            ),
        )[:limit]

    async def find_approved(
        self,
        tenant_id: str,
        session_id: str,
        digest: str,
        policy_version: str,
        run_id: str | None = None,
    ) -> ApprovalRecord | None:
        for (record_tenant, _), record in self._records.items():
            if (
                record_tenant == tenant_id
                and record.session_id == session_id
                and (run_id is None or record.run_id == run_id)
                and record.action_digest == digest
                and record.policy_version == policy_version
                and record.status is ApprovalStatus.APPROVED
            ):
                return record
        return None

    async def rebuild(
        self, events: Sequence[CanonicalEvent], tenant_id: str | None = None
    ) -> int:
        async with self._lock:
            if tenant_id is None:
                self._records.clear()
                self._event_ids.clear()
            else:
                self._records = {
                    key: record
                    for key, record in self._records.items()
                    if key[0] != tenant_id
                }
                self._event_ids.difference_update(
                    event.event_id for event in events if event.tenant_id == tenant_id
                )
        selected = [
            event
            for event in events
            if tenant_id is None or event.tenant_id == tenant_id
        ]
        await self.project(selected)
        return len(selected)


class CompositeProjection:
    def __init__(self, *projectors: ProjectionWriter) -> None:
        self._projectors = projectors

    async def project(self, events: Sequence[CanonicalEvent]) -> None:
        for projector in self._projectors:
            await projector.project(events)
