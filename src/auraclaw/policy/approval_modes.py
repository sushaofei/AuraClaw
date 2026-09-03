"""Resolve human-approval requirements using immutable Canonical Run configuration."""

from __future__ import annotations

import asyncio
import hashlib
import json
from datetime import UTC, datetime, timedelta
from typing import Any, Protocol

from pydantic import BaseModel, ConfigDict, Field

from auraclaw.contracts.approval_mode import ApprovalMode
from auraclaw.contracts.commands import CommandContext
from auraclaw.contracts.errors import AuthorizationError, VersionConflictError
from auraclaw.contracts.events import Actor, CanonicalEvent, NewEvent
from auraclaw.contracts.internal import PolicyEvaluateRequest
from auraclaw.contracts.state import TERMINAL_RUN_STATUSES, Visibility
from auraclaw.contracts.tools import PolicyDecision
from auraclaw.domain.session import SessionAggregate
from auraclaw.session.ports import EventStore


class ReviewResult(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    approved: bool = Field(strict=True)
    reason: str = Field(min_length=1, max_length=1000)


class AutoApprovalReviewer(Protocol):
    async def review(
        self,
        request: PolicyEvaluateRequest,
        *,
        review_id: str,
        user_intent: str,
        action: dict[str, Any],
    ) -> ReviewResult: ...


class ApprovalModeResolver:
    """Never infer authority from model arguments, a UI projection or an SSE connection.

    Canonical completion acts as a first-writer-wins decision. All competing reviewers
    use the same Model Gateway call ID; late results cannot replace a human escalation.
    """

    def __init__(
        self,
        events: EventStore,
        reviewer: AutoApprovalReviewer | None = None,
        *,
        review_timeout: float = 20.0,
    ) -> None:
        self._events = events
        self._reviewer = reviewer
        self._timeout = review_timeout

    async def resolve(
        self,
        request: PolicyEvaluateRequest,
        decision: PolicyDecision,
        policy_version: str,
    ) -> tuple[PolicyDecision, dict[str, Any]]:
        if decision is not PolicyDecision.REQUIRE_APPROVAL:
            return decision, {}
        if not request.session_id or not request.run_id:
            # Non-Session operations have no user-selected mode.
            return decision, {"decision_source": "request_approval"}
        events = await self._events.load(request.context.tenant_id, request.session_id)
        session = self._session(events, request)
        mode = session.approval.effective_approval_mode
        evidence = {
            **session.approval.public_dict(),
            "session_id": request.session_id,
            "run_id": request.run_id,
            "policy_version": policy_version,
        }
        if mode in {None, ApprovalMode.REQUEST_APPROVAL}:
            return decision, {**evidence, "decision_source": "request_approval"}
        assert mode is not None
        action = {
            "action": request.action,
            "resource": request.resource,
            "input_digest": request.input_digest,
            "subject": request.subject,
            "attributes": request.attributes,
        }
        digest = hashlib.sha256(
            json.dumps(
                {"tenant_id": request.context.tenant_id, "action": action, **evidence},
                sort_keys=True,
                separators=(",", ":"),
                default=str,
            ).encode()
        ).hexdigest()
        evidence.update(
            action_digest=digest,
            decision_source=mode.value,
            action_kind=request.attributes.get("action_kind", "operation"),
            action_label=request.resource,
        )
        if mode is ApprovalMode.FULL_ACCESS:
            saved = await self._record(
                request,
                digest,
                "policy.mode.resolved",
                {
                    **evidence,
                    "approved": True,
                    "reason": "无需逐次人工审批",
                },
            )
            return PolicyDecision.ALLOW, saved
        for event in events:
            if event.type == "policy.review.completed" and event.payload.get("review_id") == digest:
                return self._outcome(event.payload)
        started = await self._record(
            request,
            digest,
            "policy.review.requested",
            {
                **evidence,
                "review_id": digest,
                "expires_at": (datetime.now(UTC) + timedelta(seconds=self._timeout)).isoformat(),
            },
        )
        remaining = (
            datetime.fromisoformat(str(started["expires_at"])) - datetime.now(UTC)
        ).total_seconds()
        result = ReviewResult(approved=False, reason="自动审核不可用，需要人工确认")
        review_count = sum(
            event.type == "policy.review.requested" and event.run_id == request.run_id
            for event in events
        )
        if self._reviewer is not None and remaining > 0 and review_count < 32:
            try:
                async with asyncio.timeout(min(remaining, self._timeout)):
                    # The user intent is read from Canonical Events, never a tool-supplied goal.
                    messages = [session.goal] + [
                        str(event.payload["message"])
                        for event in events
                        if event.type == "user.message.appended" and "message" in event.payload
                    ]
                    result = await self._reviewer.review(
                        request,
                        review_id=digest,
                        user_intent="\n".join(messages)[-24000:],
                        action=action,
                    )
            except Exception:
                # Do not leak provider errors or sensitive input into user-visible evidence.
                result = ReviewResult(approved=False, reason="自动审核未能确认安全，需要人工确认")
        saved = await self._record(
            request,
            digest,
            "policy.review.completed",
            {
                **evidence,
                "review_id": digest,
                **result.model_dump(),
                "approval_expires_at": (datetime.now(UTC) + timedelta(minutes=5)).isoformat(),
            },
        )
        return self._outcome(saved)

    @staticmethod
    def _outcome(evidence: dict[str, Any]) -> tuple[PolicyDecision, dict[str, Any]]:
        expires = evidence.get("approval_expires_at")
        if expires and datetime.fromisoformat(str(expires)) <= datetime.now(UTC):
            return PolicyDecision.REQUIRE_APPROVAL, {
                **evidence,
                "approved": False,
                "reason": "自动批准已过期，需要人工确认",
            }
        return (
            PolicyDecision.ALLOW
            if evidence.get("approved") is True
            else PolicyDecision.REQUIRE_APPROVAL
        ), evidence

    @staticmethod
    def _session(events: list[CanonicalEvent], request: PolicyEvaluateRequest) -> SessionAggregate:
        if not events:
            raise AuthorizationError("approval context Session does not exist")
        session = SessionAggregate.from_events(events)
        if session.run_id != request.run_id or session.run_status in TERMINAL_RUN_STATUSES:
            raise AuthorizationError("approval context does not belong to the current Run")
        return session

    async def _record(
        self,
        request: PolicyEvaluateRequest,
        digest: str,
        event_type: str,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        assert request.session_id is not None
        for _ in range(8):
            events = await self._events.load(request.context.tenant_id, request.session_id)
            session = self._session(events, request)
            for event in events:
                if event.type == event_type and event.payload.get("action_digest") == digest:
                    return dict(event.payload)
            if session.approval.approval_mode_revision != payload["approval_mode_revision"]:
                raise AuthorizationError("approval mode changed during evaluation")
            try:
                result = await self._events.append(
                    root_session_id=session.root_session_id,
                    session_id=session.session_id,
                    run_id=request.run_id,
                    context=CommandContext(
                        command_id=f"{event_type}:{digest}",
                        tenant_id=request.context.tenant_id,
                        actor=Actor(type="policy", id="approval-mode-resolver"),
                        correlation_id=request.context.correlation_id,
                        causation_id=request.context.causation_id,
                        expected_version=session.version,
                        operation="approval_mode_decision",
                    ),
                    events=[NewEvent(type=event_type, visibility=Visibility.USER, payload=payload)],
                    command_result=payload,
                )
                return result.command_result
            except VersionConflictError:
                continue
        raise VersionConflictError("Session changed repeatedly during approval evaluation")
