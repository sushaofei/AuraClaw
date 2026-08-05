from __future__ import annotations

from collections.abc import AsyncIterator

from auraclaw.contracts.errors import AuthorizationError, BudgetExceededError, ModelProviderError
from auraclaw.runtime.ports import (
    CredentialResolver,
    ModelRequest,
    ModelResponse,
    ModelStreamChunk,
    ProviderAdapter,
)


class ModelGateway:
    """Provider-neutral model boundary and the only component resolving credentials."""

    def __init__(
        self,
        adapters: tuple[ProviderAdapter, ...],
        credentials: CredentialResolver,
        *,
        default_provider: str,
    ) -> None:
        self._adapters = {adapter.name: adapter for adapter in adapters}
        self._credentials = credentials
        self._default_provider = default_provider

    async def generate(self, request: ModelRequest) -> ModelResponse:
        response: ModelResponse | None = None
        async for chunk in self.generate_stream(request):
            if chunk.kind == "completed":
                response = chunk.response
        if response is None:
            raise ModelProviderError("model provider stream ended without a completed response")
        return response

    async def generate_stream(
        self, request: ModelRequest
    ) -> AsyncIterator[ModelStreamChunk]:
        if request.max_output_tokens <= 0:
            raise BudgetExceededError("model output token budget is exhausted")
        allowed = request.policy.allowed_providers
        provider = self._default_provider
        if allowed and provider not in allowed:
            provider = allowed[0]
        adapter = self._adapters.get(provider)
        if adapter is None:
            raise AuthorizationError(f"model provider is not configured: {provider}")
        credential = await self._credentials.resolve(provider, request.tenant_id)
        stream = getattr(adapter, "generate_stream", None)
        if callable(stream):
            async for chunk in stream(request, credential=credential):
                yield chunk
            return
        response = await adapter.generate(request, credential=credential)
        for delta in response.deltas:
            yield ModelStreamChunk(kind="delta", delta=str(delta))
        yield ModelStreamChunk(kind="completed", response=response)


class StaticCredentialResolver:
    """Gateway-owned test/development resolver; credentials are never exposed to Runtime."""

    def __init__(self, credentials: dict[str, str]) -> None:
        self._credentials = dict(credentials)

    async def resolve(self, provider: str, tenant_id: str) -> str:
        del tenant_id
        credential = self._credentials.get(provider)
        if credential is None:
            raise AuthorizationError(f"no credential configured for provider: {provider}")
        return credential
