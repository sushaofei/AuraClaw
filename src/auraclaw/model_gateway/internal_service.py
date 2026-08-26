from __future__ import annotations

import asyncio
import hashlib
import json
import logging
from collections.abc import AsyncIterator
from typing import Protocol

from auraclaw.action.ports import PolicyEvaluation
from auraclaw.contracts.errors import (
    AuthorizationError,
    BudgetExceededError,
    LeaseConflictError,
    ModelProviderError,
    PolicyDeniedError,
    VersionConflictError,
)

logger = logging.getLogger(__name__)
from auraclaw.contracts.internal import (
    ModelCancelRequest,
    ModelCancelResponse,
    ModelGenerateRequest,
    ModelGenerateResponse,
    ModelStreamEvent,
    ServiceIdentity,
)
from auraclaw.contracts.tools import PolicyDecision
from auraclaw.model_gateway.ports import ModelCallReservation, ModelStateStore
from auraclaw.runtime.model_stream import iter_model_stream
from auraclaw.runtime.ports import ModelClient, ModelPolicy, ModelRequest, ModelResponse


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
        result: ModelGenerateResponse | None = None
        async for event in self.generate_stream(request):
            if event.type == "completed":
                result = ModelGenerateResponse.model_validate(event.payload)
        if result is None:
            raise ModelProviderError("model stream ended without a completed response")
        return result

    async def generate_stream(
        self, request: ModelGenerateRequest
    ) -> AsyncIterator[ModelStreamEvent]:
        self._require_runtime(request.context.service_identity)
        request_digest = self._request_digest(request)
        reservation = await self._prepare_stream(request, request_digest)
        if reservation is not None:
            if reservation.status == "completed":
                assert reservation.cached_response is not None
                async for event in self._cached_stream(reservation.cached_response):
                    yield event
                return
            if reservation.status == "conflict":
                raise VersionConflictError("model_call_id was reused with another request")
            if reservation.status == "in_progress":
                raise LeaseConflictError("model call is already in progress")
            if reservation.status == "quota_exceeded":
                raise BudgetExceededError("tenant model token quota is exhausted")
        sequence = 0
        response: ModelResponse | None = None
        try:
            async for chunk in iter_model_stream(
                self._model,
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
                ),
            ):
                if chunk.kind == "delta":
                    if not chunk.delta:
                        continue
                    sequence += 1
                    yield ModelStreamEvent(
                        model_call_id=request.model_call_id,
                        sequence=sequence,
                        type="delta",
                        payload={"delta": chunk.delta},
                    )
                elif chunk.kind == "completed":
                    response = chunk.response
        except Exception as exc:
            if self._state is not None:
                await self._state.fail(
                    tenant_id=request.context.tenant_id,
                    model_call_id=request.model_call_id,
                    error_code=type(exc).__name__,
                )
            raise
        if response is None:
            raise ModelProviderError("model stream ended without a completed response")
        result = self._to_generate_response(response)
        # Yield completed before persisting so Runtime receives the terminal event
        # even if the DB write is slow or the SSE connection drops mid-persist.
        sequence += 1
        yield ModelStreamEvent(
            model_call_id=request.model_call_id,
            sequence=sequence,
            type="completed",
            payload=result.model_dump(mode="json"),
        )
        if self._state is not None:
            try:
                await self._state.complete(
                    tenant_id=request.context.tenant_id,
                    model_call_id=request.model_call_id,
                    response=result,
                )
            except Exception:
                logger.exception(
                    "model call completed for client but persistence failed "
                    "tenant=%s model_call=%s",
                    request.context.tenant_id,
                    request.model_call_id,
                )

    async def cancel(self, request: ModelCancelRequest) -> ModelCancelResponse:
        self._require_runtime(request.context.service_identity)
        return ModelCancelResponse(
            model_call_id=request.model_call_id,
            cancelled=False,
        )

    async def _prepare_stream(
        self, request: ModelGenerateRequest, request_digest: str
    ) -> ModelCallReservation | None:
        """Run policy and quota reservation concurrently when both are configured."""

        async def _no_reservation() -> None:
            return None

        policy_result, reservation = await asyncio.gather(
            self._enforce_policy(request),
            (
                self._state.reserve(
                    tenant_id=request.context.tenant_id,
                    model_call_id=request.model_call_id,
                    run_id=request.run_id,
                    request_digest=request_digest,
                    reserved_tokens=request.max_output_tokens,
                    token_limit=self._tenant_token_limit,
                )
                if self._state is not None
                else _no_reservation()
            ),
            return_exceptions=True,
        )
        if isinstance(reservation, BaseException):
            if isinstance(policy_result, BaseException):
                raise policy_result
            raise reservation
        if isinstance(policy_result, BaseException):
            if (
                self._state is not None
                and isinstance(reservation, ModelCallReservation)
                and reservation.status == "reserved"
            ):
                await self._state.fail(
                    tenant_id=request.context.tenant_id,
                    model_call_id=request.model_call_id,
                    error_code=type(policy_result).__name__,
                )
            raise policy_result
        return reservation if isinstance(reservation, ModelCallReservation) else None

    async def _enforce_policy(self, request: ModelGenerateRequest) -> None:
        if self._policy is None:
            return
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

    @staticmethod
    def _request_digest(request: ModelGenerateRequest) -> str:
        return hashlib.sha256(
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

    async def _cached_stream(
        self, cached: ModelGenerateResponse
    ) -> AsyncIterator[ModelStreamEvent]:
        sequence = 0
        for delta in cached.deltas:
            if not delta:
                continue
            sequence += 1
            yield ModelStreamEvent(
                model_call_id=cached.model_call_id,
                sequence=sequence,
                type="delta",
                payload={"delta": delta},
            )
        sequence += 1
        yield ModelStreamEvent(
            model_call_id=cached.model_call_id,
            sequence=sequence,
            type="completed",
            payload=cached.model_dump(mode="json"),
        )

    @staticmethod
    def _to_generate_response(response: ModelResponse) -> ModelGenerateResponse:
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
