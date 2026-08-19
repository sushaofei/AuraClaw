from __future__ import annotations

from auraclaw.config import Settings
from auraclaw.contracts.identity import IdentityContextVerifier
from auraclaw.infrastructure.identity import (
    DatabaseAssertionReplayGuard,
    DevelopmentHeaderIdentityVerifier,
    SignedAgentContextVerifier,
)


def build_identity_verifier(settings: Settings) -> IdentityContextVerifier:
    if settings.insecure_identity_headers_enabled:
        return DevelopmentHeaderIdentityVerifier()
    token = (
        settings.chaintower_workload_token.get_secret_value()
        if settings.chaintower_workload_token is not None
        else ""
    )
    workload_tokens = {token: "chaintower"} if token else {}
    replay_guard = (
        DatabaseAssertionReplayGuard(settings.resolved_database_url)
        if settings.sql_storage_enabled
        else None
    )
    return SignedAgentContextVerifier(
        workload_tokens=workload_tokens,
        keys=settings.agent_context_signing_keys,
        replay_guard=replay_guard,
        issuer=settings.agent_context_issuer,
        audience=settings.agent_context_audience,
        required_scope=settings.agent_context_required_scope,
        max_ttl_seconds=settings.agent_context_max_ttl_seconds,
        clock_skew_seconds=settings.agent_context_clock_skew_seconds,
    )
