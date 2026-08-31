from __future__ import annotations

import hashlib
import json
import uuid

import httpx

from auraclaw.action.ports import PolicyEvaluation
from auraclaw.contracts.commands import CommandContext
from auraclaw.contracts.errors import ApprovalValidationError, PolicyDeniedError
from auraclaw.contracts.internal import (
    ApprovalCommandRequest,
    ApprovalValidationResponse,
    InternalRequestContext,
    PolicyEvaluateRequest,
    PolicyEvaluateResponse,
    PolicyValidateDecisionRequest,
    PolicyValidateDecisionResponse,
    ServiceIdentity,
)
from auraclaw.contracts.tools import ApprovalRecord, PolicyDecision, ToolCapability, ToolInvocation
from auraclaw.internal.http import HttpContractClient


class RemotePolicyClient:
    def __init__(
        self,
        base_url: str,
        *,
        bearer_token: str,
        service_identity: ServiceIdentity = ServiceIdentity.ACTION_HANDS,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self.version = "remote"
        self._identity = service_identity
        self._client = httpx.AsyncClient(base_url=base_url, transport=transport)
        self._contract = HttpContractClient(self._client, bearer_token=bearer_token)

    async def aclose(self) -> None:
        await self._client.aclose()

    async def evaluate(
        self,
        capability: ToolCapability,
        invocation: ToolInvocation | None = None,
    ) -> PolicyEvaluation:
        if invocation is None:
            raise ValueError("remote policy evaluation requires invocation context")
        encoded = json.dumps(
            invocation.arguments,
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        ).encode()
        response = await self._contract.call(
            "/internal/v1/policy/evaluate",
            PolicyEvaluateRequest(
                context=InternalRequestContext(
                    tenant_id=invocation.tenant_id,
                    service_identity=self._identity,
                    request_id=str(uuid.uuid4()),
                    correlation_id=invocation.run_id,
                    causation_id=invocation.tool_invocation_id,
                    deadline=invocation.deadline,
                ),
                subject=invocation.actor_id,
                action=invocation.expected_side_effect,
                resource=capability.name,
                input_digest=hashlib.sha256(encoded).hexdigest(),
                attributes={
                    "tool_version": capability.version,
                    "permission": capability.permission.value,
                    "risk_level": capability.risk_level.value,
                    "runtime_location": capability.runtime_location,
                },
            ),
            PolicyEvaluateResponse,
        )
        self.version = response.policy_version
        return PolicyEvaluation(
            decision=PolicyDecision(response.decision),
            decision_id=response.decision_id,
            policy_version=response.policy_version,
        )

    async def evaluate_action(
        self,
        *,
        tenant_id: str,
        subject: str,
        action: str,
        resource: str,
        input_digest: str,
        correlation_id: str,
        attributes: dict[str, object],
    ) -> PolicyEvaluation:
        request_id = str(uuid.uuid4())
        response = await self._contract.call(
            "/internal/v1/policy/evaluate",
            PolicyEvaluateRequest(
                context=InternalRequestContext(
                    tenant_id=tenant_id,
                    service_identity=self._identity,
                    request_id=request_id,
                    correlation_id=correlation_id,
                    causation_id=request_id,
                ),
                subject=subject,
                action=action,
                resource=resource,
                input_digest=input_digest,
                attributes=attributes,
            ),
            PolicyEvaluateResponse,
        )
        return PolicyEvaluation(
            decision=PolicyDecision(response.decision),
            decision_id=response.decision_id,
            policy_version=response.policy_version,
        )

    async def validate_decision(
        self,
        *,
        tenant_id: str,
        decision_id: str,
        action: str,
        resource: str,
    ) -> bool:
        request_id = str(uuid.uuid4())
        response = await self._contract.call(
            "/internal/v1/policy/decisions/validate",
            PolicyValidateDecisionRequest(
                context=InternalRequestContext(
                    tenant_id=tenant_id,
                    service_identity=self._identity,
                    request_id=request_id,
                    correlation_id=decision_id,
                    causation_id=request_id,
                ),
                decision_id=decision_id,
                action=action,
                resource=resource,
            ),
            PolicyValidateDecisionResponse,
        )
        return response.valid

    async def request_approval(self, record: ApprovalRecord) -> None:
        response = await self._approval_command(record, operation="request")
        if response.status == "conflict":
            raise ApprovalValidationError(
                "approval id is already bound to a different request"
            )

    async def validate_approval(
        self,
        *,
        tenant_id: str,
        approval_id: str,
        session_id: str,
        run_id: str,
        action_digest: str,
        policy_version: str,
    ) -> bool:
        request_id = str(uuid.uuid4())
        response = await self._contract.call(
            "/internal/v1/policy/approvals/command",
            ApprovalCommandRequest(
                context=InternalRequestContext(
                    tenant_id=tenant_id,
                    service_identity=self._identity,
                    request_id=request_id,
                    correlation_id=run_id,
                    causation_id=approval_id,
                ),
                operation="validate",
                approval_id=approval_id,
                session_id=session_id,
                run_id=run_id,
                action_digest=action_digest,
                policy_version=policy_version,
            ),
            ApprovalValidationResponse,
        )
        return response.valid

    async def record_human_response(
        self,
        record: ApprovalRecord,
        *,
        decision: str,
        feedback: str | None,
        actor_id: str | None = None,
    ) -> None:
        expected_status = "approved" if decision == "approved" else "rejected"
        response = await self._approval_command(
            record,
            operation="record_human_response",
            decision="approve" if expected_status == "approved" else "reject",
            feedback=feedback,
            actor_id=actor_id,
        )
        if response.status != expected_status:
            raise ApprovalValidationError(
                f"approval decision was not committed: {response.status}"
            )

    async def _approval_command(
        self,
        record: ApprovalRecord,
        *,
        operation: str,
        decision: str | None = None,
        feedback: str | None = None,
        actor_id: str | None = None,
    ) -> ApprovalValidationResponse:
        request_id = str(uuid.uuid4())
        return await self._contract.call(
            "/internal/v1/policy/approvals/command",
            ApprovalCommandRequest(
                context=InternalRequestContext(
                    tenant_id=record.tenant_id,
                    service_identity=self._identity,
                    request_id=request_id,
                    correlation_id=record.run_id,
                    causation_id=record.approval_id,
                ),
                operation=operation,
                approval_id=record.approval_id,
                session_id=record.session_id,
                run_id=record.run_id,
                action_digest=record.action_digest,
                policy_version=record.policy_version,
                decision=decision,
                feedback=feedback,
                actor_id=actor_id,
                expires_at=record.expires_at,
            ),
            ApprovalValidationResponse,
        )


class RemoteTaskAdmissionController:
    def __init__(self, policy: RemotePolicyClient) -> None:
        self._policy = policy

    async def admit(self, *, goal: str, context: CommandContext) -> None:
        digest = hashlib.sha256(goal.encode()).hexdigest()
        evaluation = await self._policy.evaluate_action(
            tenant_id=context.tenant_id,
            subject=context.actor.id,
            action="task.create",
            resource="task",
            input_digest=digest,
            correlation_id=context.correlation_id,
            attributes={
                "permission": "write-autonomous",
                "risk_level": "medium",
            },
        )
        if evaluation.decision not in {
            PolicyDecision.ALLOW,
            PolicyDecision.ALLOW_WITH_CONSTRAINTS,
        }:
            raise PolicyDeniedError("Task admission policy denied request")
