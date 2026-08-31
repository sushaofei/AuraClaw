from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from auraclaw.contracts.internal import (
    ApprovalCommandRequest,
    ApprovalValidationResponse,
    PolicyEvaluateRequest,
    PolicyEvaluateResponse,
    PolicyValidateDecisionRequest,
    PolicyValidateDecisionResponse,
)
from auraclaw.domain.approval import approval_request_digest
from auraclaw.infrastructure.persistence.postgres_common import (
    LazyPool,
    json_dumps,
    json_loads,
)


class PostgresPolicyStateStore(LazyPool):
    async def ensure_active_version(self, version: str) -> bool:
        pool = await self.pool()
        await pool.execute(
            """INSERT INTO policy.active_bundle (singleton,policy_version)
            VALUES (true,$1) ON CONFLICT (singleton) DO NOTHING""",
            version,
        )
        active = await pool.fetchval(
            "SELECT policy_version FROM policy.active_bundle WHERE singleton=true"
        )
        return str(active) == version

    async def record_decision(
        self, request: PolicyEvaluateRequest, response: PolicyEvaluateResponse
    ) -> None:
        pool = await self.pool()
        await pool.execute(
            """INSERT INTO policy.decision
            (decision_id,tenant_id,subject,action,resource,input_digest,decision,
             policy_version,constraints,expires_at)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9::jsonb,$10)""",
            response.decision_id,
            request.context.tenant_id,
            request.subject,
            request.action,
            request.resource,
            request.input_digest,
            response.decision,
            response.policy_version,
            json_dumps(response.constraints),
            response.expires_at,
        )

    async def command_approval(
        self, request: ApprovalCommandRequest
    ) -> ApprovalValidationResponse:
        pool = await self.pool()
        tenant_id = request.context.tenant_id
        async with pool.acquire() as connection, connection.transaction():
            database_now = await connection.fetchval("SELECT clock_timestamp()")
            request_digest = (
                approval_request_digest(
                    tenant_id=tenant_id,
                    approval_id=request.approval_id,
                    session_id=request.session_id,
                    run_id=request.run_id,
                    action_digest=request.action_digest,
                    policy_version=request.policy_version,
                    expires_at=request.expires_at,
                )
                if request.expires_at is not None
                else None
            )
            if request.operation == "request":
                if request.expires_at is None or request.expires_at <= database_now:
                    await self._audit_approval_transition(
                        connection,
                        request,
                        request_digest=request_digest,
                        prior_status=None,
                        result_status="expired",
                        outcome="invalid",
                    )
                    return ApprovalValidationResponse(valid=False, status="expired")
                row = await connection.fetchrow(
                    """INSERT INTO policy.approval
                    (tenant_id,approval_id,session_id,run_id,action_digest,policy_version,
                     status,expires_at,request_digest,generation)
                    VALUES ($1,$2,$3,$4,$5,$6,'waiting',$7,$8,1)
                    ON CONFLICT (tenant_id,approval_id) DO NOTHING
                    RETURNING *""",
                    tenant_id,
                    request.approval_id,
                    request.session_id,
                    request.run_id,
                    request.action_digest,
                    request.policy_version,
                    request.expires_at,
                    request_digest,
                )
                if row is not None:
                    await self._audit_approval_transition(
                        connection,
                        request,
                        request_digest=request_digest,
                        prior_status=None,
                        result_status="waiting",
                        outcome="winner",
                    )
                    return ApprovalValidationResponse(valid=False, status="waiting")
                row = await connection.fetchrow(
                    """SELECT * FROM policy.approval
                    WHERE tenant_id=$1 AND approval_id=$2 FOR UPDATE""",
                    tenant_id,
                    request.approval_id,
                )
                assert row is not None
                database_now = await connection.fetchval("SELECT clock_timestamp()")
                same_request = self._same_approval_scope(row, request) and row[
                    "expires_at"
                ] == request.expires_at
                stored_digest = row["request_digest"]
                if same_request and stored_digest is None:
                    await connection.execute(
                        """UPDATE policy.approval SET request_digest=$3
                        WHERE tenant_id=$1 AND approval_id=$2
                        AND request_digest IS NULL""",
                        tenant_id,
                        request.approval_id,
                        request_digest,
                    )
                    stored_digest = request_digest
                if not same_request or stored_digest != request_digest:
                    await self._audit_approval_transition(
                        connection,
                        request,
                        request_digest=request_digest,
                        prior_status=str(row["status"]),
                        result_status="conflict",
                        outcome="conflict",
                        generation=int(row["generation"]),
                    )
                    return ApprovalValidationResponse(valid=False, status="conflict")
                status = str(row["status"])
                await self._audit_approval_transition(
                    connection,
                    request,
                    request_digest=request_digest,
                    prior_status=status,
                    result_status=status,
                    outcome="idempotent",
                    generation=int(row["generation"]),
                )
                return ApprovalValidationResponse(
                    valid=status == "approved" and row["expires_at"] > database_now,
                    status=status,
                )

            row = await connection.fetchrow(
                """SELECT * FROM policy.approval
                WHERE tenant_id=$1 AND approval_id=$2 FOR UPDATE""",
                tenant_id,
                request.approval_id,
            )
            if row is None:
                await self._audit_approval_transition(
                    connection,
                    request,
                    request_digest=request_digest,
                    prior_status=None,
                    result_status="not_found",
                    outcome="not_found",
                )
                return ApprovalValidationResponse(valid=False, status="not_found")
            database_now = await connection.fetchval("SELECT clock_timestamp()")
            generation = int(row["generation"])
            prior_status = str(row["status"])
            stored_digest = str(row["request_digest"] or "")
            if not self._same_approval_scope(row, request):
                await self._audit_approval_transition(
                    connection,
                    request,
                    request_digest=stored_digest or request_digest,
                    prior_status=prior_status,
                    result_status="conflict",
                    outcome="conflict",
                    generation=generation,
                )
                return ApprovalValidationResponse(valid=False, status="conflict")

            if request.operation == "record_human_response":
                if request.decision not in {"approve", "reject"}:
                    result_status, outcome = "conflict", "invalid"
                else:
                    target = "approved" if request.decision == "approve" else "rejected"
                    if prior_status == "waiting" and row["expires_at"] <= database_now:
                        await self._transition_waiting(
                            connection,
                            tenant_id=tenant_id,
                            approval_id=request.approval_id,
                            status="expired",
                            actor_id=None,
                        )
                        result_status, outcome = "expired", "winner"
                    elif prior_status == "waiting":
                        changed = await self._transition_waiting(
                            connection,
                            tenant_id=tenant_id,
                            approval_id=request.approval_id,
                            status=target,
                            decision=request.decision,
                            feedback=request.feedback,
                            actor_id=request.actor_id,
                        )
                        result_status = target if changed else "conflict"
                        outcome = "winner" if changed else "conflict"
                    elif prior_status == target:
                        result_status, outcome = target, "idempotent"
                    else:
                        result_status, outcome = "conflict", "conflict"
                await self._audit_approval_transition(
                    connection,
                    request,
                    request_digest=stored_digest or request_digest,
                    prior_status=prior_status,
                    result_status=result_status,
                    outcome=outcome,
                    generation=generation,
                )
                return ApprovalValidationResponse(
                    valid=result_status == "approved", status=result_status
                )

            if request.operation == "cancel":
                if prior_status == "waiting" and row["expires_at"] <= database_now:
                    await self._transition_waiting(
                        connection,
                        tenant_id=tenant_id,
                        approval_id=request.approval_id,
                        status="expired",
                        actor_id=None,
                    )
                    result_status, outcome = "expired", "winner"
                elif prior_status == "waiting":
                    changed = await self._transition_waiting(
                        connection,
                        tenant_id=tenant_id,
                        approval_id=request.approval_id,
                        status="cancelled",
                        actor_id=request.actor_id,
                    )
                    result_status = "cancelled" if changed else "conflict"
                    outcome = "winner" if changed else "conflict"
                elif prior_status == "cancelled":
                    result_status, outcome = "cancelled", "idempotent"
                else:
                    result_status, outcome = "conflict", "conflict"
                await self._audit_approval_transition(
                    connection,
                    request,
                    request_digest=stored_digest or request_digest,
                    prior_status=prior_status,
                    result_status=result_status,
                    outcome=outcome,
                    generation=generation,
                )
                return ApprovalValidationResponse(valid=False, status=result_status)

            if request.operation == "expire":
                if prior_status == "waiting" and row["expires_at"] <= database_now:
                    changed = await self._transition_waiting(
                        connection,
                        tenant_id=tenant_id,
                        approval_id=request.approval_id,
                        status="expired",
                        actor_id=request.actor_id,
                    )
                    result_status = "expired" if changed else "conflict"
                    outcome = "winner" if changed else "conflict"
                elif prior_status == "expired":
                    result_status, outcome = "expired", "idempotent"
                elif prior_status == "waiting":
                    result_status, outcome = "waiting", "noop"
                else:
                    result_status, outcome = "conflict", "conflict"
                await self._audit_approval_transition(
                    connection,
                    request,
                    request_digest=stored_digest or request_digest,
                    prior_status=prior_status,
                    result_status=result_status,
                    outcome=outcome,
                    generation=generation,
                )
                return ApprovalValidationResponse(valid=False, status=result_status)

            if prior_status == "waiting" and row["expires_at"] <= database_now:
                await self._transition_waiting(
                    connection,
                    tenant_id=tenant_id,
                    approval_id=request.approval_id,
                    status="expired",
                    actor_id=request.actor_id,
                )
                result_status = "expired"
            else:
                result_status = prior_status
            valid = result_status == "approved" and row["expires_at"] > database_now
            await self._audit_approval_transition(
                connection,
                request,
                request_digest=stored_digest or request_digest,
                prior_status=prior_status,
                result_status=result_status,
                outcome="validated" if valid else "invalid",
                generation=generation,
            )
            return ApprovalValidationResponse(valid=valid, status=result_status)

    @staticmethod
    def _same_approval_scope(row: Any, request: ApprovalCommandRequest) -> bool:
        return bool(
            row["session_id"] == request.session_id
            and row["run_id"] == request.run_id
            and row["action_digest"] == request.action_digest
            and row["policy_version"] == request.policy_version
        )

    @staticmethod
    async def _transition_waiting(
        connection: Any,
        *,
        tenant_id: str,
        approval_id: str,
        status: str,
        actor_id: str | None,
        decision: str | None = None,
        feedback: str | None = None,
    ) -> bool:
        row = await connection.fetchrow(
            """UPDATE policy.approval
            SET status=$3,decision=$4,feedback=$5,decided_at=clock_timestamp(),decided_by=$6,
                updated_at=clock_timestamp()
            WHERE tenant_id=$1 AND approval_id=$2 AND status='waiting'
            RETURNING approval_id""",
            tenant_id,
            approval_id,
            status,
            decision,
            feedback,
            actor_id,
        )
        return row is not None

    @staticmethod
    async def _audit_approval_transition(
        connection: Any,
        request: ApprovalCommandRequest,
        *,
        request_digest: str | None,
        prior_status: str | None,
        result_status: str,
        outcome: str,
        generation: int = 1,
    ) -> None:
        await connection.execute(
            """INSERT INTO policy.approval_transition_audit
            (tenant_id,approval_id,generation,operation,actor_id,service_identity,
             decision,request_digest,prior_status,result_status,outcome,request_id,
             correlation_id,causation_id)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)""",
            request.context.tenant_id,
            request.approval_id,
            generation,
            request.operation,
            request.actor_id,
            request.context.service_identity.value,
            request.decision,
            request_digest,
            prior_status,
            result_status,
            outcome,
            request.context.request_id,
            request.context.correlation_id,
            request.context.causation_id,
        )

    async def validate_decision(
        self, request: PolicyValidateDecisionRequest
    ) -> PolicyValidateDecisionResponse:
        pool = await self.pool()
        row = await pool.fetchrow(
            """SELECT * FROM policy.decision WHERE decision_id=$1 AND tenant_id=$2""",
            request.decision_id,
            request.context.tenant_id,
        )
        if row is None:
            return PolicyValidateDecisionResponse(
                valid=False,
                decision="deny",
                policy_version="unknown",
                constraints={},
                expires_at=datetime.now(UTC),
            )
        valid = bool(
            row["action"] == request.action
            and row["resource"] == request.resource
            and row["expires_at"] > datetime.now(UTC)
            and row["decision"] in {"allow", "allow_with_constraints"}
        )
        if not valid:
            return PolicyValidateDecisionResponse(
                valid=False,
                decision="deny",
                policy_version=str(row["policy_version"]),
                constraints={},
                expires_at=row["expires_at"],
            )
        return PolicyValidateDecisionResponse(
            valid=True,
            decision=str(row["decision"]),
            policy_version=str(row["policy_version"]),
            constraints=dict(json_loads(row["constraints"])),
            expires_at=row["expires_at"],
        )
