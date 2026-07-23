from __future__ import annotations

from datetime import UTC, datetime

from auraclaw.contracts.internal import (
    ApprovalCommandRequest,
    ApprovalValidationResponse,
    PolicyEvaluateRequest,
    PolicyEvaluateResponse,
    PolicyValidateDecisionRequest,
    PolicyValidateDecisionResponse,
)
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
        if request.operation == "request":
            if request.expires_at is None or request.expires_at <= datetime.now(UTC):
                return ApprovalValidationResponse(valid=False, status="expired")
            await pool.execute(
                """INSERT INTO policy.approval
                (tenant_id,approval_id,session_id,run_id,action_digest,policy_version,
                 status,expires_at) VALUES ($1,$2,$3,$4,$5,$6,'waiting',$7)
                ON CONFLICT (tenant_id,approval_id) DO NOTHING""",
                tenant_id,
                request.approval_id,
                request.session_id,
                request.run_id,
                request.action_digest,
                request.policy_version,
                request.expires_at,
            )
            return ApprovalValidationResponse(valid=False, status="waiting")
        if request.operation == "record_human_response":
            status = "approved" if request.decision == "approve" else "rejected"
            result = await pool.execute(
                """UPDATE policy.approval SET status=$3,decision=$4,feedback=$5,
                updated_at=now() WHERE tenant_id=$1 AND approval_id=$2
                AND session_id=$6 AND run_id=$7""",
                tenant_id,
                request.approval_id,
                status,
                request.decision,
                request.feedback,
                request.session_id,
                request.run_id,
            )
            return ApprovalValidationResponse(
                valid=result == "UPDATE 1" and status == "approved",
                status=status if result == "UPDATE 1" else "not_found",
            )
        if request.operation in {"cancel", "expire"}:
            status = "cancelled" if request.operation == "cancel" else "expired"
            result = await pool.execute(
                """UPDATE policy.approval SET status=$3,updated_at=now()
                WHERE tenant_id=$1 AND approval_id=$2""",
                tenant_id,
                request.approval_id,
                status,
            )
            return ApprovalValidationResponse(
                valid=False, status=status if result == "UPDATE 1" else "not_found"
            )
        row = await pool.fetchrow(
            """SELECT * FROM policy.approval WHERE tenant_id=$1 AND approval_id=$2""",
            tenant_id,
            request.approval_id,
        )
        valid = bool(
            row is not None
            and row["status"] == "approved"
            and row["expires_at"] > datetime.now(UTC)
            and row["session_id"] == request.session_id
            and row["run_id"] == request.run_id
            and row["action_digest"] == request.action_digest
            and row["policy_version"] == request.policy_version
        )
        return ApprovalValidationResponse(
            valid=valid,
            status=str(row["status"]) if row is not None else "not_found",
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
        valid = bool(
            row is not None
            and row["action"] == request.action
            and row["resource"] == request.resource
            and row["expires_at"] > datetime.now(UTC)
            and row["decision"] in {"allow", "allow_with_constraints"}
        )
        return PolicyValidateDecisionResponse(
            valid=valid,
            decision=str(row["decision"]) if valid else "deny",
            policy_version=str(row["policy_version"]) if row is not None else "unknown",
            constraints=dict(json_loads(row["constraints"])) if valid else {},
            expires_at=row["expires_at"] if row is not None else datetime.now(UTC),
        )
