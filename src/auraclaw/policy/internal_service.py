from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta
from typing import Any, Protocol

from auraclaw.action.policy import PolicyEngine
from auraclaw.contracts.internal import (
    ApprovalCommandRequest,
    ApprovalValidationResponse,
    PolicyEvaluateRequest,
    PolicyEvaluateResponse,
    PolicyValidateDecisionRequest,
    PolicyValidateDecisionResponse,
    ServiceIdentity,
)
from auraclaw.contracts.tools import RiskLevel, ToolCapability, ToolPermission


class PolicyStateStore(Protocol):
    async def record_decision(
        self, request: PolicyEvaluateRequest, response: PolicyEvaluateResponse
    ) -> None: ...

    async def command_approval(
        self, request: ApprovalCommandRequest
    ) -> ApprovalValidationResponse: ...

    async def validate_decision(
        self, request: PolicyValidateDecisionRequest
    ) -> PolicyValidateDecisionResponse: ...


class PolicyInternalService:
    """Authoritative, deterministic policy evaluation boundary."""

    def __init__(
        self,
        *,
        version: str = "s3-v1",
        store: PolicyStateStore | None = None,
    ) -> None:
        self._engine = PolicyEngine(version=version)
        self._store = store
        self._approvals: dict[tuple[str, str], dict[str, Any]] = {}
        self._decisions: dict[str, tuple[PolicyEvaluateRequest, PolicyEvaluateResponse]] = {}

    async def evaluate(self, request: PolicyEvaluateRequest) -> PolicyEvaluateResponse:
        attributes = request.attributes
        capability = ToolCapability(
            name=request.resource,
            version=str(attributes.get("tool_version", "1")),
            description="remote policy input",
            input_schema={},
            output_schema={},
            permission=ToolPermission(str(attributes["permission"])),
            risk_level=RiskLevel(str(attributes.get("risk_level", "low"))),
            runtime_location=str(attributes.get("runtime_location", "hands")),
        )
        decision = self._engine.evaluate(capability)
        response = PolicyEvaluateResponse(
            decision_id=str(uuid.uuid4()),
            decision=decision.value,
            policy_version=self._engine.version,
            expires_at=datetime.now(UTC) + timedelta(minutes=5),
        )
        if self._store is not None:
            await self._store.record_decision(request, response)
        else:
            self._decisions[response.decision_id] = (request, response)
        return response

    async def validate_decision(
        self, request: PolicyValidateDecisionRequest
    ) -> PolicyValidateDecisionResponse:
        if self._store is not None:
            return await self._store.validate_decision(request)
        record = self._decisions.get(request.decision_id)
        if record is None:
            return PolicyValidateDecisionResponse(
                valid=False,
                decision="deny",
                policy_version=self._engine.version,
                expires_at=datetime.now(UTC),
            )
        original, response = record
        valid = (
            original.context.tenant_id == request.context.tenant_id
            and original.action == request.action
            and original.resource == request.resource
            and response.expires_at > datetime.now(UTC)
            and response.decision in {"allow", "allow_with_constraints"}
        )
        return PolicyValidateDecisionResponse(
            valid=valid,
            decision=response.decision if valid else "deny",
            policy_version=response.policy_version,
            constraints=response.constraints if valid else {},
            expires_at=response.expires_at,
        )

    async def approval(
        self, request: ApprovalCommandRequest
    ) -> ApprovalValidationResponse:
        if (
            request.operation == "record_human_response"
            and request.context.service_identity is not ServiceIdentity.TASK_API
        ):
            return ApprovalValidationResponse(valid=False, status="forbidden")
        if self._store is not None:
            return await self._store.command_approval(request)
        key = (request.context.tenant_id, request.approval_id)
        existing = self._approvals.get(key)
        now = datetime.now(UTC)
        if request.operation == "request":
            if request.expires_at is None or request.expires_at <= now:
                return ApprovalValidationResponse(valid=False, status="expired")
            self._approvals[key] = {
                "request": request,
                "status": "waiting",
                "decision": None,
            }
            return ApprovalValidationResponse(valid=False, status="waiting")
        if existing is None:
            return ApprovalValidationResponse(valid=False, status="not_found")
        if request.operation == "record_human_response":
            status = "approved" if request.decision == "approve" else "rejected"
            existing["status"] = status
            existing["decision"] = request.decision
            return ApprovalValidationResponse(valid=status == "approved", status=status)
        if request.operation in {"cancel", "expire"}:
            status = "cancelled" if request.operation == "cancel" else "expired"
            existing["status"] = status
            return ApprovalValidationResponse(valid=False, status=status)
        existing_request = ApprovalCommandRequest.model_validate(existing["request"])
        valid = (
            existing_request.session_id == request.session_id
            and existing_request.run_id == request.run_id
            and existing_request.action_digest == request.action_digest
            and existing_request.policy_version == request.policy_version
            and existing_request.expires_at is not None
            and existing_request.expires_at > now
            and existing["decision"] == "approve"
        )
        return ApprovalValidationResponse(
            valid=valid, status="approved" if valid else "invalid"
        )
