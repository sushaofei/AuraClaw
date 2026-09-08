from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta
from typing import Any, Protocol

from auraclaw.action.policy import PolicyEngine
from auraclaw.contracts.errors import VersionConflictError
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
from auraclaw.domain.approval import approval_request_digest
from auraclaw.policy.approval_modes import ApprovalModeResolver


class PolicyStateStore(Protocol):
    async def ensure_active_version(self, version: str) -> bool: ...

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
        mode_resolver: ApprovalModeResolver | None = None,
    ) -> None:
        self._engine = PolicyEngine(version=version)
        self._version = version
        self._store = store
        self._mode_resolver = mode_resolver
        self._approvals: dict[tuple[str, str], dict[str, Any]] = {}
        self._decisions: dict[str, tuple[PolicyEvaluateRequest, PolicyEvaluateResponse]] = {}

    async def evaluate(self, request: PolicyEvaluateRequest) -> PolicyEvaluateResponse:
        if self._store is not None and not await self._store.ensure_active_version(self._version):
            raise VersionConflictError("configured policy version does not match the active bundle")
        attributes = request.attributes
        capability = ToolCapability(
            name=request.resource,
            version=str(attributes.get("tool_version", "1")),
            description="remote policy input",
            input_schema={},
            output_schema={},
            permission=ToolPermission(str(attributes.get("permission", "read-only"))),
            risk_level=RiskLevel(str(attributes.get("risk_level", "low"))),
            runtime_location=str(attributes.get("runtime_location", "hands")),
        )
        decision = self._engine.evaluate(capability)
        constraints: dict[str, Any] = {}
        if self._mode_resolver is not None and not (
            request.context.service_identity is ServiceIdentity.MODEL_GATEWAY
            and request.attributes.get("purpose") == "approval_review"
        ):
            decision, constraints = await self._mode_resolver.resolve(
                request,
                decision,
                self._engine.version,
            )
        response = PolicyEvaluateResponse(
            decision_id=str(uuid.uuid4()),
            decision=decision.value,
            constraints=constraints,
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

    async def approval(self, request: ApprovalCommandRequest) -> ApprovalValidationResponse:
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
            request_digest = approval_request_digest(
                tenant_id=request.context.tenant_id,
                approval_id=request.approval_id,
                session_id=request.session_id,
                run_id=request.run_id,
                action_digest=request.action_digest,
                policy_version=request.policy_version,
                expires_at=request.expires_at,
            )
            if existing is not None:
                if existing["request_digest"] != request_digest:
                    return ApprovalValidationResponse(valid=False, status="conflict")
                status = str(existing["status"])
                original = ApprovalCommandRequest.model_validate(existing["request"])
                return ApprovalValidationResponse(
                    valid=(
                        status == "approved"
                        and original.expires_at is not None
                        and original.expires_at > now
                    ),
                    status=status,
                )
            self._approvals[key] = {
                "request": request,
                "request_digest": request_digest,
                "status": "waiting",
                "decision": None,
            }
            return ApprovalValidationResponse(valid=False, status="waiting")
        if existing is None:
            return ApprovalValidationResponse(valid=False, status="not_found")
        existing_request = ApprovalCommandRequest.model_validate(existing["request"])
        same_scope = (
            existing_request.session_id == request.session_id
            and existing_request.run_id == request.run_id
            and existing_request.action_digest == request.action_digest
            and existing_request.policy_version == request.policy_version
        )
        if not same_scope:
            return ApprovalValidationResponse(valid=False, status="conflict")
        existing_expiry = existing_request.expires_at
        if existing_expiry is None:
            return ApprovalValidationResponse(valid=False, status="conflict")
        if request.operation == "record_human_response":
            if request.decision not in {"approve", "reject"}:
                return ApprovalValidationResponse(valid=False, status="conflict")
            status = "approved" if request.decision == "approve" else "rejected"
            current_status = str(existing["status"])
            if current_status == "waiting" and existing_expiry <= now:
                existing["status"] = "expired"
                return ApprovalValidationResponse(valid=False, status="expired")
            if current_status == status:
                return ApprovalValidationResponse(valid=status == "approved", status=status)
            if current_status != "waiting":
                return ApprovalValidationResponse(valid=False, status="conflict")
            existing["status"] = status
            existing["decision"] = request.decision
            return ApprovalValidationResponse(valid=status == "approved", status=status)
        if request.operation == "cancel":
            current_status = str(existing["status"])
            if current_status == "waiting" and existing_expiry <= now:
                existing["status"] = "expired"
                return ApprovalValidationResponse(valid=False, status="expired")
            if current_status == "cancelled":
                return ApprovalValidationResponse(valid=False, status="cancelled")
            if current_status != "waiting":
                return ApprovalValidationResponse(valid=False, status="conflict")
            existing["status"] = "cancelled"
            return ApprovalValidationResponse(valid=False, status="cancelled")
        if request.operation == "expire":
            current_status = str(existing["status"])
            if current_status == "expired":
                return ApprovalValidationResponse(valid=False, status="expired")
            if current_status != "waiting":
                return ApprovalValidationResponse(valid=False, status="conflict")
            if existing_expiry > now:
                return ApprovalValidationResponse(valid=False, status="waiting")
            existing["status"] = "expired"
            return ApprovalValidationResponse(valid=False, status="expired")
        if existing["status"] == "waiting" and existing_expiry <= now:
            existing["status"] = "expired"
        valid = (
            existing_expiry > now
            and existing["decision"] == "approve"
            and existing["status"] == "approved"
        )
        return ApprovalValidationResponse(valid=valid, status=str(existing["status"]))
