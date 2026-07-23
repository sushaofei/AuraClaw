from __future__ import annotations

import httpx

from auraclaw.contracts.internal import (
    InternalRequestContext,
    ModelGenerateRequest,
    ModelGenerateResponse,
    ServiceIdentity,
)
from auraclaw.internal.http import HttpContractClient
from auraclaw.runtime.ports import ModelRequest, ModelResponse, ToolCall


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

    async def generate(self, request: ModelRequest) -> ModelResponse:
        response = await self._contract.call(
            "/internal/v1/model/generate",
            ModelGenerateRequest(
                context=InternalRequestContext(
                    tenant_id=request.tenant_id,
                    service_identity=ServiceIdentity.AGENT_RUNTIME,
                    request_id=request.model_call_id,
                    correlation_id=request.run_id,
                    causation_id=request.model_call_id,
                ),
                model_call_id=request.model_call_id,
                run_id=request.run_id,
                messages=request.messages,
                tools=request.tools,
                capability=request.policy.capability,
                preferred_model=request.policy.preferred_model,
                allowed_providers=request.policy.allowed_providers,
                data_classification=request.policy.data_classification,
                max_output_tokens=request.max_output_tokens,
            ),
            ModelGenerateResponse,
        )
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
