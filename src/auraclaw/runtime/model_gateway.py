from __future__ import annotations

from auraclaw.contracts.errors import AuthorizationError, BudgetExceededError
from auraclaw.runtime.ports import (
    CredentialResolver,
    ModelRequest,
    ModelResponse,
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
        return await adapter.generate(request, credential=credential)


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
