from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

from auraclaw.contracts.internal import (
    ApprovalCommandRequest,
    InternalRequestContext,
    ServiceIdentity,
)
from auraclaw.policy.internal_service import PolicyInternalService


def _command(
    *,
    operation: str,
    request_id: str,
    expires_at: datetime,
    decision: str | None = None,
    action_digest: str = "digest-a",
) -> ApprovalCommandRequest:
    return ApprovalCommandRequest(
        context=InternalRequestContext(
            tenant_id="tenant-a",
            service_identity=(
                ServiceIdentity.TASK_API
                if operation == "record_human_response"
                else ServiceIdentity.ACTION_HANDS
            ),
            request_id=request_id,
            correlation_id="run-a",
            causation_id="approval-a",
        ),
        operation=operation,
        approval_id="approval-a",
        session_id="session-a",
        run_id="run-a",
        action_digest=action_digest,
        policy_version="policy-v1",
        decision=decision,
        actor_id="human-a" if decision is not None else None,
        expires_at=expires_at,
    )


def test_in_memory_approval_state_machine_is_monotonic_and_idempotent() -> None:
    async def scenario() -> None:
        service = PolicyInternalService(version="policy-v1")
        expiry = datetime.now(UTC) + timedelta(minutes=5)
        requested = await service.approval(
            _command(operation="request", request_id="request", expires_at=expiry)
        )
        assert requested.status == "waiting"
        replay = await service.approval(
            _command(operation="request", request_id="replay", expires_at=expiry)
        )
        assert replay.status == "waiting"
        conflict = await service.approval(
            _command(
                operation="request",
                request_id="conflicting-request",
                expires_at=expiry,
                action_digest="digest-b",
            )
        )
        assert conflict.status == "conflict"

        approve, reject = await asyncio.gather(
            service.approval(
                _command(
                    operation="record_human_response",
                    request_id="approve",
                    expires_at=expiry,
                    decision="approve",
                )
            ),
            service.approval(
                _command(
                    operation="record_human_response",
                    request_id="reject",
                    expires_at=expiry,
                    decision="reject",
                )
            ),
        )
        assert sorted((approve.status, reject.status)) == ["approved", "conflict"]
        retry = await service.approval(
            _command(
                operation="record_human_response",
                request_id="approve-retry",
                expires_at=expiry,
                decision="approve",
            )
        )
        assert retry.valid and retry.status == "approved"
        for operation in ("cancel", "expire"):
            blocked = await service.approval(
                _command(operation=operation, request_id=operation, expires_at=expiry)
            )
            assert blocked.status == "conflict"
        validated = await service.approval(
            _command(operation="validate", request_id="validate", expires_at=expiry)
        )
        assert validated.valid and validated.status == "approved"

    asyncio.run(scenario())

def test_in_memory_expired_approval_cannot_be_revived() -> None:
    async def scenario() -> None:
        service = PolicyInternalService(version="policy-v1")
        expiry = datetime.now(UTC) + timedelta(minutes=5)
        await service.approval(
            _command(operation="request", request_id="request", expires_at=expiry)
        )
        stored = service._approvals[("tenant-a", "approval-a")]["request"]
        service._approvals[("tenant-a", "approval-a")]["request"] = stored.model_copy(
            update={"expires_at": datetime.now(UTC) - timedelta(seconds=1)}
        )
        response = await service.approval(
            _command(
                operation="record_human_response",
                request_id="late-approve",
                expires_at=expiry,
                decision="approve",
            )
        )
        assert not response.valid and response.status == "expired"
        replay = await service.approval(
            _command(
                operation="record_human_response",
                request_id="late-approve-retry",
                expires_at=expiry,
                decision="approve",
            )
        )
        assert replay.status == "conflict"

    asyncio.run(scenario())
