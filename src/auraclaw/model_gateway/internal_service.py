from __future__ import annotations

import hashlib
import json
from typing import Protocol

from auraclaw.action.ports import PolicyEvaluation
from auraclaw.contracts.errors import (
    AuthorizationError,
    BudgetExceededError,
    LeaseConflictError,
    PolicyDeniedError,
    VersionConflictError,
)
from auraclaw.contracts.internal import (
    ModelCancelRequest,
    ModelCancelResponse,
    ModelGenerateRequest,
    ModelGenerateResponse,
    ServiceIdentity,
)
from auraclaw.contracts.tools import PolicyDecision
from auraclaw.model_gateway.ports import ModelStateStore
from auraclaw.runtime.ports import ModelClient, ModelPolicy, ModelRequest


class ModelPolicyEnforcer(Protocol):
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
    ) -> PolicyEvaluation: ...


class ModelGatewayInternalService:
    def __init__(
        self,
        model: ModelClient,
        *,
        policy: ModelPolicyEnforcer | None = None,
        state: ModelStateStore | None = None,
        tenant_token_limit: int = 1_000_000,
    ) -> None:
        self._model = model
        self._policy = policy
        self._state = state
        self._tenant_token_limit = tenant_token_limit

    @staticmethod
    def _require_runtime(identity: ServiceIdentity) -> None:
        if identity is not ServiceIdentity.AGENT_RUNTIME:
            raise AuthorizationError("model calls are restricted to agent-runtime")

    async def generate(self, request: ModelGenerateRequest) -> ModelGenerateResponse:
        self._require_runtime(request.context.service_identity)
        if self._policy is not None:
            encoded = json.dumps(request.messages, sort_keys=True, default=str).encode()
            evaluation = await self._policy.evaluate_action(
                tenant_id=request.context.tenant_id,
                subject=request.context.service_identity.value,
                action="model.generate",
                resource=request.capability,
                input_digest=hashlib.sha256(encoded).hexdigest(),
                correlation_id=request.run_id,
                attributes={
                    "permission": "read-only",
                    "risk_level": "medium",
                    "data_classification": request.data_classification,
                },
            )
            if evaluation.decision not in {
                PolicyDecision.ALLOW,
                PolicyDecision.ALLOW_WITH_CONSTRAINTS,
            }:
                raise PolicyDeniedError("Model policy denied generation")
        request_digest = hashlib.sha256(
            json.dumps(
                {
                    "run_id": request.run_id,
                    "messages": request.messages,
                    "tools": request.tools,
                    "capability": request.capability,
                    "preferred_model": request.preferred_model,
                    "allowed_providers": request.allowed_providers,
                    "data_classification": request.data_classification,
                    "max_output_tokens": request.max_output_tokens,
                },
                sort_keys=True,
                default=str,
            ).encode()
        ).hexdigest()
        if self._state is not None:
            reservation = await self._state.reserve(
                tenant_id=request.context.tenant_id,
                model_call_id=request.model_call_id,
                run_id=request.run_id,
                request_digest=request_digest,
                reserved_tokens=request.max_output_tokens,
                token_limit=self._tenant_token_limit,
            )
            if reservation.status == "completed":
                assert reservation.cached_response is not None
                return reservation.cached_response
            if reservation.status == "conflict":
                raise VersionConflictError("model_call_id was reused with another request")
            if reservation.status == "in_progress":
                raise LeaseConflictError("model call is already in progress")
            if reservation.status == "quota_exceeded":
                raise BudgetExceededError("tenant model token quota is exhausted")
        try:
            response = await self._model.generate(
                ModelRequest(
                    model_call_id=request.model_call_id,
                    tenant_id=request.context.tenant_id,
                    run_id=request.run_id,
                    messages=request.messages,
                    tools=request.tools,
                    policy=ModelPolicy(
                        capability=request.capability,
                        preferred_model=request.preferred_model,
                        allowed_providers=request.allowed_providers,
                        data_classification=request.data_classification,
                    ),
                    max_output_tokens=request.max_output_tokens,
                )
            )
        except Exception as exc:
            if self._state is not None:
                await self._state.fail(
                    tenant_id=request.context.tenant_id,
                    model_call_id=request.model_call_id,
                    error_code=type(exc).__name__,
                )
            raise
        result = ModelGenerateResponse(
            model_call_id=response.model_call_id,
            provider=response.provider,
            model=response.model,
            completed_output=response.completed_output,
            deltas=response.deltas,
            tool_calls=tuple(
                {
                    "tool_invocation_id": call.tool_invocation_id,
                    "name": call.name,
                    "arguments": call.arguments,
                    "version": call.version,
                    "expected_side_effect": call.expected_side_effect,
                    "approval_id": call.approval_id,
                    "credential_ref": call.credential_ref,
                    "idempotency_key": call.idempotency_key,
                }
                for call in response.tool_calls
            ),
            finish_reason=response.finish_reason,
            usage=response.usage,
        )
        if self._state is not None:
            await self._state.complete(
                tenant_id=request.context.tenant_id,
                model_call_id=request.model_call_id,
                response=result,
            )
        return result

    async def cancel(self, request: ModelCancelRequest) -> ModelCancelResponse:
        self._require_runtime(request.context.service_identity)
        return ModelCancelResponse(
            model_call_id=request.model_call_id,
            cancelled=False,
        )
