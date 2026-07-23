from __future__ import annotations

import hashlib
import json
from typing import Protocol

from auraclaw.action.ports import PolicyEvaluation
from auraclaw.contracts.errors import AuthorizationError, PolicyDeniedError
from auraclaw.contracts.internal import (
    ModelCancelRequest,
    ModelCancelResponse,
    ModelGenerateRequest,
    ModelGenerateResponse,
    ServiceIdentity,
)
from auraclaw.contracts.tools import PolicyDecision
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
        self, model: ModelClient, *, policy: ModelPolicyEnforcer | None = None
    ) -> None:
        self._model = model
        self._policy = policy

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
        return ModelGenerateResponse(
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

    async def cancel(self, request: ModelCancelRequest) -> ModelCancelResponse:
        self._require_runtime(request.context.service_identity)
        return ModelCancelResponse(
            model_call_id=request.model_call_id,
            cancelled=False,
        )
