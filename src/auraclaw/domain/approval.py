from __future__ import annotations

import hashlib
import json
from collections.abc import Sequence
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import uuid4

from auraclaw.contracts.errors import ApprovalValidationError
from auraclaw.contracts.events import CanonicalEvent
from auraclaw.contracts.tools import ApprovalRecord, ApprovalStatus, RiskLevel


def _aware_expires_at(value: object) -> datetime:
    expires_at = datetime.fromisoformat(str(value))
    if expires_at.tzinfo is None:
        return expires_at.replace(tzinfo=UTC)
    return expires_at


def action_digest(tool_name: str, tool_version: str, arguments: dict[str, Any]) -> str:
    normalized = json.dumps(
        {"arguments": arguments, "tool_name": tool_name, "tool_version": tool_version},
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode()
    return hashlib.sha256(normalized).hexdigest()


def approval_request_digest(
    *,
    tenant_id: str,
    approval_id: str,
    session_id: str,
    run_id: str,
    action_digest: str,
    policy_version: str,
    expires_at: datetime,
) -> str:
    """Return the immutable identity of one approval request generation."""
    normalized_expiry = expires_at.astimezone(UTC).isoformat(timespec="microseconds")
    normalized = json.dumps(
        {
            "action_digest": action_digest,
            "approval_id": approval_id,
            "expires_at": normalized_expiry,
            "policy_version": policy_version,
            "run_id": run_id,
            "session_id": session_id,
            "tenant_id": tenant_id,
        },
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode()
    return hashlib.sha256(normalized).hexdigest()


class ApprovalAggregate:
    @staticmethod
    def from_events(
        events: Sequence[CanonicalEvent],
        *,
        tenant_id: str,
        session_id: str,
        approval_id: str,
    ) -> ApprovalRecord | None:
        """Rebuild an ApprovalRecord from Canonical Session Events."""
        record: ApprovalRecord | None = None
        for event in events:
            if event.tenant_id != tenant_id or event.session_id != session_id:
                continue
            if str(event.payload.get("approval_id", "")) != approval_id:
                continue
            if event.type == "approval.requested":
                payload = event.payload
                record = ApprovalRecord(
                    approval_id=approval_id,
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
                    expires_at=_aware_expires_at(payload["expires_at"]),
                    status=ApprovalStatus(str(payload.get("status", "waiting"))),
                )
                continue
            if record is None or not event.type.startswith("approval."):
                continue
            status = ApprovalStatus(event.type.split(".", 1)[1])
            record = replace(
                record,
                status=status,
                decision=event.payload.get("decision"),
                feedback=event.payload.get("feedback"),
            )
        return record

    @staticmethod
    def request(
        *,
        tenant_id: str,
        session_id: str,
        run_id: str,
        digest: str,
        tool_name: str,
        redacted_arguments: dict[str, Any],
        risk: RiskLevel,
        reason: str,
        expected_effect: str,
        policy_version: str,
        assigned_approvers: tuple[str, ...] = (),
        ttl: timedelta = timedelta(hours=1),
        now: datetime | None = None,
    ) -> ApprovalRecord:
        requested_at = now or datetime.now(UTC)
        return ApprovalRecord(
            approval_id=f"apr_{uuid4().hex}",
            tenant_id=tenant_id,
            session_id=session_id,
            run_id=run_id,
            action_digest=digest,
            tool_name=tool_name,
            redacted_arguments=redacted_arguments,
            risk=risk,
            reason=reason,
            expected_effect=expected_effect,
            allowed_decisions=("approved", "rejected"),
            assigned_approvers=assigned_approvers,
            policy_version=policy_version,
            expires_at=requested_at + ttl,
        )

    @staticmethod
    def respond(
        record: ApprovalRecord,
        *,
        actor_id: str,
        decision: str,
        feedback: str | None,
        now: datetime | None = None,
    ) -> ApprovalRecord:
        current_time = now or datetime.now(UTC)
        if record.status not in {ApprovalStatus.REQUESTED, ApprovalStatus.WAITING}:
            raise ApprovalValidationError(f"approval is already {record.status.value}")
        if current_time >= record.expires_at:
            raise ApprovalValidationError("approval has expired")
        if record.assigned_approvers and actor_id not in record.assigned_approvers:
            raise ApprovalValidationError("actor is not an assigned approver")
        if decision not in record.allowed_decisions:
            raise ApprovalValidationError(f"unsupported approval decision: {decision}")
        return replace(
            record,
            status=ApprovalStatus(decision),
            decision=decision,
            feedback=feedback,
        )

    @staticmethod
    def validate(
        record: ApprovalRecord,
        *,
        tenant_id: str,
        session_id: str,
        digest: str,
        policy_version: str,
        now: datetime | None = None,
    ) -> None:
        current_time = now or datetime.now(UTC)
        if record.tenant_id != tenant_id or record.session_id != session_id:
            raise ApprovalValidationError("approval belongs to a different tenant or Session")
        if record.action_digest != digest:
            raise ApprovalValidationError("approval action digest does not match")
        if record.policy_version != policy_version:
            raise ApprovalValidationError("approval policy version does not match")
        if record.status is not ApprovalStatus.APPROVED:
            raise ApprovalValidationError(f"approval is {record.status.value}, not approved")
        if current_time >= record.expires_at:
            raise ApprovalValidationError("approval has expired")
