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

    async def seed_reference(
        self, tenant_id: str, reference: CredentialReference
    ) -> bool: ...

    async def revoke_reference(self, tenant_id: str, credential_ref: str) -> None: ...

    async def record_usage(self, record: dict[str, str]) -> str: ...

    async def complete_usage(
        self, usage_id: str, *, status: str, side_effect_status: str
    ) -> None: ...


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

    async def seed_reference(
        self, tenant_id: str, reference: CredentialReference
    ) -> bool:
        if self._registry is None:
            self._references.setdefault((tenant_id, reference.credential_ref), reference)
            return True
        active = await self._registry.seed_reference(tenant_id, reference)
        if active:
            self._references[(tenant_id, reference.credential_ref)] = reference
        return active

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
        usage_id: str | None = None,
    ) -> Any:
        if adapter is None:
            raise CredentialAccessError("credential target adapter is not registered")
        secret_required = getattr(adapter, "secret_required", True)
        expected_provider = getattr(adapter, "credential_provider", None)
        expected_scope = getattr(adapter, "credential_scope", None)
        if secret_required:
            reference_tenant = tenant_id
            owner = getattr(adapter, "credential_owner_tenant", None)
            if owner is not None:
                # Only the registered adapter selects the reference owner. Neither
                # caller arguments nor a missing tenant reference can select it.
                shared = getattr(adapter, "credential_is_shared", False) is True
                if owner != tenant_id and not (owner == "platform" and shared):
                    raise CredentialAccessError("credential target is outside tenant scope")
                if credential_ref != getattr(adapter, "credential_ref", None):
                    raise CredentialAccessError("credential reference does not match target")
                reference_tenant = owner
            if self._registry is not None:
                reference = await self._registry.get_reference(reference_tenant, credential_ref)
            else:
                reference = self._references.get((reference_tenant, credential_ref))
            if reference is None:
                raise CredentialAccessError("credential reference is not valid for tenant")
            if datetime.now(UTC) >= reference.expires_at:
                raise CredentialAccessError("credential reference has expired")
            if operation not in reference.allowed_operations:
                raise CredentialAccessError("credential operation is outside allowed scope")
            if (
                expected_provider is not None
                and reference.provider != expected_provider
            ):
                raise CredentialAccessError("credential provider does not match target")
            if expected_scope is not None and reference.account_scope != expected_scope:
                raise CredentialAccessError("credential scope does not match target")
            secret = await self._vault.resolve(credential_ref)
            self._redactor.register(secret)
        else:
            secret = ""
        audit = {
            "usage_id": usage_id or "",
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
            audit["usage_id"] = await self._registry.record_usage(audit)
        try:
            value = adapter(dict(request), secret)
            response = await value if hasattr(value, "__await__") else value
        except Exception:
            audit["status"] = "failed"
            audit["side_effect_status"] = "unknown"
            if self._registry is not None:
                await self._registry.complete_usage(
                    audit["usage_id"], status="failed", side_effect_status="unknown"
                )
            raise
        audit["status"] = "completed"
        audit["side_effect_status"] = "completed"
        if self._registry is not None:
            await self._registry.complete_usage(
                audit["usage_id"], status="completed", side_effect_status="completed"
            )
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
