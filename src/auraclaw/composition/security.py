"""Workload identities, lease keys and persistent fencing adapter selection."""

from __future__ import annotations

import secrets

from auraclaw.config import Settings
from auraclaw.contracts.internal import (
    ServiceIdentity,
)
from auraclaw.infrastructure.persistence.postgres_fencing_ledger import (
    FencingLedgerOwner,
    PostgresFencingTokenLedger,
)
from auraclaw.internal.security import (
    InMemoryFencingTokenLedger,
)


def configured_identities(
    settings: Settings, identities: tuple[ServiceIdentity, ...]
) -> dict[str, ServiceIdentity]:
    configured: dict[str, ServiceIdentity] = {}
    for identity in identities:
        token = settings.workload_token_value(identity.value)
        if token:
            configured[token] = identity
    return configured


def agent_runtime_token(settings: Settings) -> str | None:
    return settings.workload_token_value(ServiceIdentity.AGENT_RUNTIME.value)


def service_bearer_token(settings: Settings, identity: ServiceIdentity) -> str:
    return settings.workload_token_value(identity.value) or secrets.token_urlsafe(32)


def lease_signing_key(settings: Settings) -> bytes:
    if settings.lease_signing_key is not None:
        value = settings.lease_signing_key.get_secret_value().encode()
        if len(value) >= 32:
            return value
    return secrets.token_bytes(32)


def has_workload_tokens(settings: Settings, identities: tuple[ServiceIdentity, ...]) -> bool:
    return all(settings.workload_token_value(identity.value) for identity in identities)


def require_production_security_configuration(
    settings: Settings,
    service_name: str,
    identities: tuple[ServiceIdentity, ...],
    *,
    requires_policy: bool = False,
) -> None:
    if settings.deployment_profile != "production":
        return
    missing = [
        identity.value
        for identity in identities
        if not settings.workload_token_value(identity.value)
    ]
    if requires_policy and not settings.policy_base_url.strip():
        missing.append("policy-base-url")
    if missing:
        raise ValueError(
            f"{service_name} production security configuration is missing: "
            + ", ".join(missing)
        )


def lease_key_configured(settings: Settings) -> bool:
    return (
        settings.lease_signing_key is not None
        and len(settings.lease_signing_key.get_secret_value()) >= 32
    )


def fencing_token_ledger(
    settings: Settings,
    owner: FencingLedgerOwner,
) -> InMemoryFencingTokenLedger | PostgresFencingTokenLedger:
    if settings.sql_storage_enabled:
        return PostgresFencingTokenLedger(
            settings.resolved_database_url,
            owner=owner,
        )
    if settings.deployment_profile == "production":
        raise ValueError(
            f"{owner} production composition requires a persistent fencing token ledger"
        )
    return InMemoryFencingTokenLedger()
