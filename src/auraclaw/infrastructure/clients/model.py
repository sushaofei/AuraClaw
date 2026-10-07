from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator
from contextlib import suppress

import httpx

from auraclaw.contracts.errors import (
    BudgetExceededError,
    ModelAuthenticationError,
    ModelConnectError,
    ModelProtocolError,
    ModelProviderError,
    ModelRateLimitError,
    ModelReadError,
    ModelTimeoutError,
)
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
        retryable = (ModelConnectError, ModelProtocolError, ModelReadError, ModelTimeoutError)
        last_error: Exception | None = None
        emitted_delta = False
        for attempt in range(3):
            completed = False
            try:
                async for chunk in self._consume_stream(payload):
                    if chunk.kind == "delta":
                        emitted_delta = True
                    if chunk.kind == "completed":
                        completed = True
                    yield chunk
            except retryable as exc:
                last_error = exc
                if emitted_delta or attempt == 2:
                    raise
                logger.warning(
                    "model gateway stream failed; reconnecting model_call=%s attempt=%s error=%s",
                    request.model_call_id,
                    attempt + 1,
                    type(exc).__name__,
                )
                await asyncio.sleep(0.1 * (2**attempt))
                continue
            if completed:
                return
            if emitted_delta:
                raise ModelProviderError(
                    "model stream ended after partial output without a completed response"
                )
            logger.warning(
                "model stream ended without completed; reconnecting model_call=%s attempt=%s",
                request.model_call_id,
                attempt + 1,
            )
            if attempt < 2:
                await asyncio.sleep(0.1 * (2**attempt))
        if last_error is not None:
            raise last_error
        raise ModelProviderError("model stream ended without a completed response")

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
        try:
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
                    error_types = {
                        "model_authentication_failed": ModelAuthenticationError,
                        "model_connect_error": ModelConnectError,
                        "model_protocol_error": ModelProtocolError,
                        "model_provider_error": ModelProviderError,
                        "model_rate_limited": ModelRateLimitError,
                        "model_read_error": ModelReadError,
                        "model_timeout": ModelTimeoutError,
                        "runtime_budget_exceeded": BudgetExceededError,
                    }
                    error_type = error_types.get(str(event.payload.get("code")), ModelProviderError)
                    raise error_type(str(message))
        except httpx.TimeoutException as exc:
            raise ModelTimeoutError("model gateway stream timed out") from exc
        except httpx.ConnectError as exc:
            raise ModelConnectError("model gateway connection failed") from exc
        except httpx.ReadError as exc:
            raise ModelReadError("model gateway stream read failed") from exc
        except httpx.ProtocolError as exc:
            raise ModelProtocolError("model gateway protocol failed") from exc
        except httpx.TransportError as exc:
            raise ModelProviderError("model gateway transport failed") from exc

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
