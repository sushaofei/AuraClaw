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
from auraclaw.contracts.tools import ApprovalRecord, ApprovalStatus, RiskLevel, ToolInvocation


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


def invocation_action_digest(invocation: ToolInvocation) -> str:
    """Bind execution replay and approval to the trusted business identity.

    Headless legacy calls without a user/department retain their old digest.
    A user-scoped old result cannot match the new identity-bound digest; it is
    rejected, never cleared or replayed with a different invocation key.
    """
    digest = action_digest(invocation.tool_name, invocation.tool_version, invocation.arguments)
    if (invocation.user_id is None and invocation.dept_id is None
            and invocation.capability_ref is None):
        return digest
    payload = {
        "version": 2,
        "action_digest": digest,
        "tenant_id": invocation.tenant_id,
        "root_session_id": invocation.root_session_id,
        "session_id": invocation.session_id,
        "user_id": invocation.user_id,
        "dept_id": invocation.dept_id,
        "actor_role": invocation.actor_role,
    }
    if invocation.capability_ref is not None:
        payload["capability_ref"] = invocation.capability_ref.model_dump(mode="json")
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


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
                    required_approvals=max(1, int(payload.get("required_approvals", 1))),
                    votes=tuple(payload.get("votes", ())),
                    escalation_at=(
                        _aware_expires_at(payload["escalation_at"])
                        if payload.get("escalation_at")
                        else None
                    ),
                    escalation_level=int(payload.get("escalation_level", 0)),
                    status=ApprovalStatus(str(payload.get("status", "waiting"))),
                )
                continue
            if record is None or not event.type.startswith("approval."):
                continue
            if event.type == "approval.vote.recorded":
                vote = {
                    "actor_id": str(event.payload["actor_id"]),
                    "decision": str(event.payload["decision"]),
                    "feedback": event.payload.get("feedback"),
                    "recorded_at": str(event.occurred_at.isoformat()),
                }
                record = replace(record, votes=(*record.votes, vote))
                continue
            if event.type == "approval.delegated":
                previous = str(event.payload["from_approver"])
                replacement = str(event.payload["to_approver"])
                approvers = tuple(
                    replacement if value == previous else value
                    for value in record.assigned_approvers
                )
                record = replace(record, assigned_approvers=tuple(dict.fromkeys(approvers)))
                continue
            if event.type == "approval.escalated":
                additions = tuple(str(value) for value in event.payload["approvers"])
                record = replace(
                    record,
                    assigned_approvers=tuple(
                        dict.fromkeys((*record.assigned_approvers, *additions))
                    ),
                    escalation_level=int(event.payload["escalation_level"]),
                    escalation_at=(
                        _aware_expires_at(event.payload["next_escalation_at"])
                        if event.payload.get("next_escalation_at")
                        else None
                    ),
                )
                continue
            status = ApprovalStatus(event.type.split(".", 1)[1])
            votes = record.votes
            if event.payload.get("actor_id") is not None:
                actor_id = str(event.payload["actor_id"])
                if not any(str(vote.get("actor_id")) == actor_id for vote in votes):
                    votes = (
                        *votes,
                        {
                            "actor_id": actor_id,
                            "decision": str(event.payload.get("decision", status.value)),
                            "feedback": event.payload.get("feedback"),
                            "recorded_at": event.occurred_at.isoformat(),
                        },
                    )
            record = replace(
                record,
                status=status,
                votes=votes,
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
        required_approvals: int = 1,
        escalation_after: timedelta | None = None,
        ttl: timedelta = timedelta(hours=1),
        now: datetime | None = None,
    ) -> ApprovalRecord:
        requested_at = now or datetime.now(UTC)
        if required_approvals < 1:
            raise ApprovalValidationError("approval quorum must be positive")
        if assigned_approvers and required_approvals > len(set(assigned_approvers)):
            raise ApprovalValidationError("approval quorum exceeds assigned approvers")
        if ttl <= timedelta(0):
            raise ApprovalValidationError("approval TTL must be positive")
        if escalation_after is not None and (
            escalation_after <= timedelta(0) or escalation_after >= ttl
        ):
            raise ApprovalValidationError("approval escalation must occur within its TTL")
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
            required_approvals=required_approvals,
            escalation_at=(
                requested_at + escalation_after if escalation_after is not None else None
            ),
        )

    @staticmethod
    def vote(
        record: ApprovalRecord,
        *,
        actor_id: str,
        decision: str,
        feedback: str | None,
        now: datetime | None = None,
    ) -> tuple[ApprovalRecord, bool]:
        current_time = now or datetime.now(UTC)
        if record.status not in {ApprovalStatus.REQUESTED, ApprovalStatus.WAITING}:
            raise ApprovalValidationError(f"approval is already {record.status.value}")
        if current_time >= record.expires_at:
            raise ApprovalValidationError("approval has expired")
        if record.assigned_approvers and actor_id not in record.assigned_approvers:
            raise ApprovalValidationError("actor is not an assigned approver")
        if decision not in record.allowed_decisions:
            raise ApprovalValidationError(f"unsupported approval decision: {decision}")
        if any(str(vote.get("actor_id")) == actor_id for vote in record.votes):
            raise ApprovalValidationError("actor has already voted on this approval")
        vote = {
            "actor_id": actor_id,
            "decision": decision,
            "feedback": feedback,
            "recorded_at": current_time.isoformat(),
        }
        votes = (*record.votes, vote)
        rejected = decision == ApprovalStatus.REJECTED.value
        approved_count = sum(
            1 for item in votes if item.get("decision") == ApprovalStatus.APPROVED.value
        )
        terminal = rejected or approved_count >= record.required_approvals
        status = (
            ApprovalStatus.REJECTED
            if rejected
            else ApprovalStatus.APPROVED
            if terminal
            else ApprovalStatus.WAITING
        )
        return (
            replace(
                record,
                votes=votes,
                status=status,
                decision=status.value if terminal else None,
                feedback=feedback if terminal else None,
            ),
            terminal,
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
        decided, terminal = ApprovalAggregate.vote(
            record,
            actor_id=actor_id,
            decision=decision,
            feedback=feedback,
            now=now,
        )
        if not terminal:
            raise ApprovalValidationError("approval quorum has not been reached")
        return decided

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
