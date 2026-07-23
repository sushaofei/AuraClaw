from __future__ import annotations

import re
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from typing import Any, Protocol

from auraclaw.contracts.errors import CredentialAccessError
from auraclaw.contracts.tools import CredentialReference

CredentialAdapter = Callable[[dict[str, Any], str], Awaitable[Any] | Any]


class Vault(Protocol):
    async def resolve(self, credential_ref: str) -> str: ...

    async def revoke(self, credential_ref: str) -> None: ...


class CredentialRegistry(Protocol):
    async def get_reference(
        self, tenant_id: str, credential_ref: str
    ) -> CredentialReference | None: ...

    async def save_reference(
        self, tenant_id: str, reference: CredentialReference
    ) -> None: ...

    async def revoke_reference(self, tenant_id: str, credential_ref: str) -> None: ...

    async def record_usage(self, record: dict[str, str]) -> None: ...


class InMemoryVault:
    def __init__(self, secrets: dict[str, str]) -> None:
        self._secrets = dict(secrets)
        self._revoked: set[str] = set()

    async def resolve(self, credential_ref: str) -> str:
        if credential_ref in self._revoked or credential_ref not in self._secrets:
            raise CredentialAccessError("credential is unavailable or revoked")
        return self._secrets[credential_ref]

    async def revoke(self, credential_ref: str) -> None:
        self._revoked.add(credential_ref)


class SecretRedactor:
    _token_patterns = (
        re.compile(r"(?i)(bearer\s+)[A-Za-z0-9._~+/=-]+"),
        re.compile(r"(?i)((?:api[_-]?key|access[_-]?token|secret)\s*[:=]\s*)[^\s,;]+"),
    )

    def __init__(self) -> None:
        self._known_secrets: set[str] = set()

    def register(self, secret: str) -> None:
        if secret:
            self._known_secrets.add(secret)

    def redact(self, value: Any) -> Any:
        if isinstance(value, dict):
            return {str(key): self.redact(item) for key, item in value.items()}
        if isinstance(value, list):
            return [self.redact(item) for item in value]
        if isinstance(value, tuple):
            return [self.redact(item) for item in value]
        if not isinstance(value, str):
            return value
        redacted = value
        for secret in self._known_secrets:
            redacted = redacted.replace(secret, "[REDACTED]")
        for pattern in self._token_patterns:
            redacted = pattern.sub(r"\1[REDACTED]", redacted)
        return redacted


class CredentialProxy:
    """Uses credentials on behalf of Hands without returning them to Runtime or Sandbox."""

    def __init__(
        self,
        vault: Vault,
        *,
        redactor: SecretRedactor | None = None,
        registry: CredentialRegistry | None = None,
    ) -> None:
        self._vault = vault
        self._redactor = redactor or SecretRedactor()
        self._references: dict[tuple[str, str], CredentialReference] = {}
        self._usage_audit: list[dict[str, str]] = []
        self._registry = registry

    def register_reference(self, tenant_id: str, reference: CredentialReference) -> None:
        self._references[(tenant_id, reference.credential_ref)] = reference

    async def save_reference(
        self, tenant_id: str, reference: CredentialReference
    ) -> None:
        self.register_reference(tenant_id, reference)
        if self._registry is not None:
            await self._registry.save_reference(tenant_id, reference)

    async def invoke(
        self,
        *,
        tenant_id: str,
        session_id: str,
        tool_name: str,
        credential_ref: str,
        operation: str,
        request: dict[str, Any],
        adapter: CredentialAdapter | None = None,
        policy_decision_id: str | None = None,
    ) -> Any:
        reference = self._references.get((tenant_id, credential_ref))
        if reference is None and self._registry is not None:
            reference = await self._registry.get_reference(tenant_id, credential_ref)
        if reference is None:
            raise CredentialAccessError("credential reference is not valid for tenant")
        if datetime.now(UTC) >= reference.expires_at:
            raise CredentialAccessError("credential reference has expired")
        if operation not in reference.allowed_operations:
            raise CredentialAccessError("credential operation is outside allowed scope")
        secret = await self._vault.resolve(credential_ref)
        if adapter is None:
            raise CredentialAccessError("credential target adapter is not registered")
        self._redactor.register(secret)
        audit = {
            "tenant_id": tenant_id,
            "session_id": session_id,
            "tool_name": tool_name,
            "credential_ref": credential_ref,
            "operation": operation,
            "policy_decision_id": policy_decision_id or "development",
            "status": "attempting",
            "side_effect_status": "unknown",
        }
        self._usage_audit.append(audit)
        if self._registry is not None:
            await self._registry.record_usage(audit)
        value = adapter(dict(request), secret)
        response = await value if hasattr(value, "__await__") else value
        return self._redactor.redact(response)

    def redact(self, value: Any) -> Any:
        return self._redactor.redact(value)

    def usage_audit(self) -> list[dict[str, str]]:
        return [dict(record) for record in self._usage_audit]

    async def revoke_reference(self, tenant_id: str, credential_ref: str) -> None:
        self._references.pop((tenant_id, credential_ref), None)
        if self._registry is not None:
            await self._registry.revoke_reference(tenant_id, credential_ref)
        await self._vault.revoke(credential_ref)
