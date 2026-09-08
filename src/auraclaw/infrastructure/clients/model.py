from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from contextlib import suppress

import httpx

from auraclaw.contracts.internal import (
    InternalRequestContext,
    ModelCancelRequest,
    ModelCancelResponse,
    ModelGenerateRequest,
    ModelGenerateResponse,
    ModelStreamEvent,
    ServiceIdentity,
)
from auraclaw.internal.http import HttpContractClient
from auraclaw.runtime.ports import ModelRequest, ModelResponse, ModelStreamChunk, ToolCall

logger = logging.getLogger(__name__)


class RemoteModelClient:
    """Runtime model port without Provider credentials or adapters."""

    def __init__(
        self,
        base_url: str,
        *,
        bearer_token: str,
        timeout: float = 120.0,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self._client = httpx.AsyncClient(
            base_url=base_url,
            timeout=timeout,
            transport=transport,
        )
        self._contract = HttpContractClient(self._client, bearer_token=bearer_token)

    async def aclose(self) -> None:
        await self._client.aclose()

    async def prewarm(self) -> None:
        """Warm the Runtime → Model Gateway HTTP connection."""
        with suppress(Exception):
            await self._client.get("/health/live")

    async def generate(self, request: ModelRequest) -> ModelResponse:
        response: ModelResponse | None = None
        async for chunk in self.generate_stream(request):
            if chunk.kind == "completed":
                response = chunk.response
        if response is None:
            raise RuntimeError("model stream ended without a completed response")
        return response

    async def cancel(self, request: ModelRequest) -> ModelCancelResponse:
        return await self._contract.call(
            "/internal/v1/model/cancel",
            ModelCancelRequest(
                context=InternalRequestContext(
                    tenant_id=request.tenant_id,
                    service_identity=ServiceIdentity.AGENT_RUNTIME,
                    request_id=f"cancel-{request.model_call_id}",
                    correlation_id=request.run_id,
                    causation_id=request.model_call_id,
                ),
                model_call_id=request.model_call_id,
                run_id=request.run_id,
            ),
            ModelCancelResponse,
        )

    async def generate_stream(
        self, request: ModelRequest
    ) -> AsyncIterator[ModelStreamChunk]:
        payload = self._payload(request)
        got_completed = False
        async for chunk in self._consume_stream(payload):
            if chunk.kind == "completed":
                got_completed = True
            yield chunk
        if got_completed:
            return
        # Tail event can be lost after gateway has already finished (and cached).
        # Reconnect once; forward only completed to avoid duplicate live deltas.
        logger.warning(
            "model stream ended without completed; reconnecting once model_call=%s",
            request.model_call_id,
        )
        async for chunk in self._consume_stream(payload):
            if chunk.kind == "completed":
                yield chunk
                return

    def _payload(self, request: ModelRequest) -> ModelGenerateRequest:
        return ModelGenerateRequest(
            context=InternalRequestContext(
                tenant_id=request.tenant_id,
                service_identity=ServiceIdentity.AGENT_RUNTIME,
                request_id=request.model_call_id,
                correlation_id=request.run_id,
                causation_id=request.model_call_id,
            ),
            model_call_id=request.model_call_id,
            run_id=request.run_id,
            session_id=request.session_id,
            messages=request.messages,
            tools=request.tools,
            capability=request.policy.capability,
            preferred_model=request.policy.preferred_model,
            allowed_providers=request.policy.allowed_providers,
            data_classification=request.policy.data_classification,
            max_output_tokens=request.max_output_tokens,
            run_max_cost=request.run_max_cost,
            runtime_metrics=request.runtime_metrics,
            prompt_cache_key=request.prompt_cache_key,
        )

    async def _consume_stream(
        self, payload: ModelGenerateRequest
    ) -> AsyncIterator[ModelStreamChunk]:
        async for event in self._contract.stream(
            "/internal/v1/model/stream",
            payload,
            ModelStreamEvent,
        ):
            if event.type == "delta":
                delta = event.payload.get("delta")
                if isinstance(delta, str) and delta:
                    yield ModelStreamChunk(kind="delta", delta=delta)
            elif event.type == "completed":
                response = ModelGenerateResponse.model_validate(event.payload)
                yield ModelStreamChunk(
                    kind="completed",
                    response=self._to_model_response(response),
                )
            elif event.type == "error":
                message = event.payload.get("message") or "model stream reported an error"
                raise RuntimeError(str(message))

    @staticmethod
    def _to_model_response(response: ModelGenerateResponse) -> ModelResponse:
        return ModelResponse(
            model_call_id=response.model_call_id,
            provider=response.provider,
            model=response.model,
            completed_output=response.completed_output,
            deltas=response.deltas,
            tool_calls=tuple(
                ToolCall(
                    tool_invocation_id=str(call["tool_invocation_id"]),
                    name=str(call["name"]),
                    arguments=dict(call.get("arguments", {})),
                    version=str(call.get("version", "1")),
                    expected_side_effect=str(
                        call.get("expected_side_effect", "read")
                    ),
                    approval_id=call.get("approval_id"),
                    credential_ref=call.get("credential_ref"),
                    idempotency_key=call.get("idempotency_key"),
                )
                for call in response.tool_calls
            ),
            finish_reason=response.finish_reason,
            usage=dict(response.usage),
        )
