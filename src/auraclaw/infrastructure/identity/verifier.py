from __future__ import annotations

import base64
import hashlib
import hmac
import json
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any

from auraclaw.contracts.identity import (
    AssertionMetadata,
    AuthenticatedCaller,
    IdentityErrorReason,
    IdentityVerificationRequest,
    TrustedUserContext,
    VerifiedIdentityEnvelope,
    identity_error,
)
from auraclaw.infrastructure.identity.replay import (
    AssertionReplayGuard,
    AssertionReplayStore,
)

_REQUIRED_CLAIMS = (
    "iss",
    "aud",
    "tenant_id",
    "user_id",
    "scopes",
    "iat",
    "exp",
    "jti",
    "kid",
)


def _b64url_encode(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).decode().rstrip("=")


def _b64url_decode(value: str) -> bytes:
    padding = "=" * (-len(value) % 4)
    return base64.urlsafe_b64decode(value + padding)


def canonical_assertion_payload(claims: Mapping[str, Any]) -> bytes:
    return json.dumps(dict(claims), sort_keys=True, separators=(",", ":")).encode()


class AgentContextSigner:
    def __init__(self, *, key_id: str, signing_key: bytes) -> None:
        if len(signing_key) < 32:
            raise ValueError("agent context signing key must contain at least 32 bytes")
        self._key_id = key_id
        self._signing_key = bytes(signing_key)

    def sign(self, claims: Mapping[str, Any]) -> str:
        payload = dict(claims)
        payload["kid"] = self._key_id
        canonical = canonical_assertion_payload(payload)
        signature = hmac.new(self._signing_key, canonical, hashlib.sha256).digest()
        return f"{_b64url_encode(canonical)}.{_b64url_encode(signature)}"


class DevelopmentHeaderIdentityVerifier:
    """Development-only adapter. Production composition must not select this type."""

    def __init__(self, *, default_tenant: str = "local", default_user: str = "local-user") -> None:
        self._default_tenant = default_tenant
        self._default_user = default_user

    async def verify(
        self, request: IdentityVerificationRequest
    ) -> VerifiedIdentityEnvelope:
        tenant_id = request.declared_tenant_id or self._default_tenant
        user_id = request.declared_user_id or self._default_user
        if request.bound_session_id is not None and request.declared_tenant_id is not None:
            pass
        return VerifiedIdentityEnvelope(
            caller=AuthenticatedCaller(kind="development", subject="insecure-headers"),
            user=TrustedUserContext(
                tenant_id=tenant_id,
                user_id=user_id,
                dept_id=request.declared_dept_id,
                session_id=request.bound_session_id,
                scopes=("agent.task.invoke",),
            ),
        )


class SignedAgentContextVerifier:
    def __init__(
        self,
        *,
        workload_tokens: Mapping[str, str],
        keys: Mapping[str, bytes],
        replay_guard: AssertionReplayStore | None = None,
        issuer: str = "chaintower",
        audience: str = "auraclaw-task-api",
        required_scope: str = "agent.task.invoke",
        max_ttl_seconds: int = 300,
        clock_skew_seconds: int = 30,
    ) -> None:
        self._workload_tokens = dict(workload_tokens)
        self._keys = {key_id: bytes(value) for key_id, value in keys.items()}
        self._replay = replay_guard or AssertionReplayGuard()
        self._issuer = issuer
        self._audience = audience
        self._required_scope = required_scope
        self._max_ttl_seconds = max_ttl_seconds
        self._clock_skew_seconds = clock_skew_seconds

    async def close(self) -> None:
        await self._replay.close()

    async def verify(
        self, request: IdentityVerificationRequest
    ) -> VerifiedIdentityEnvelope:
        if not self._keys or not self._workload_tokens:
            raise identity_error(
                "identity verifier is unavailable",
                reason=IdentityErrorReason.VERIFIER_UNAVAILABLE,
            )
        token = (request.workload_credential or "").removeprefix("Bearer ").strip()
        if not token:
            raise identity_error(
                "missing chaintower workload credential",
                reason=IdentityErrorReason.MISSING_CREDENTIAL,
            )
        subject = self._workload_tokens.get(token)
        if subject is None:
            raise identity_error(
                "chaintower workload credential is not trusted",
                reason=IdentityErrorReason.WORKLOAD_MISMATCH,
            )
        if not request.assertion:
            raise identity_error(
                "missing signed agent context",
                reason=IdentityErrorReason.MISSING_CREDENTIAL,
            )
        claims = self._parse_and_verify(request.assertion)
        user = TrustedUserContext(
            tenant_id=str(claims["tenant_id"]),
            user_id=str(claims["user_id"]),
            dept_id=_optional_str(claims.get("dept_id")),
            session_id=_optional_str(claims.get("session_id")),
            scopes=_scopes(claims.get("scopes")),
            permission_version=_optional_str(claims.get("permission_version")),
        )
        if self._required_scope not in user.scopes:
            raise identity_error(
                "agent context is missing the required task scope",
                reason=IdentityErrorReason.SCOPE_DENIED,
            )
        self._reject_conflicts(request, user)
        if request.operation != "read" and request.command_id:
            await self._replay.remember_write(
                str(claims["jti"]),
                request.command_id,
                expires_at=int(claims["exp"]) + self._clock_skew_seconds,
            )
        return VerifiedIdentityEnvelope(
            caller=AuthenticatedCaller(kind="chaintower_workload", subject=subject),
            user=user,
            assertion=AssertionMetadata(
                issuer=str(claims["iss"]),
                audience=str(claims["aud"]),
                key_id=str(claims["kid"]),
                jti=str(claims["jti"]),
                issued_at=int(claims["iat"]),
                expires_at=int(claims["exp"]),
            ),
        )

    def _parse_and_verify(self, token: str) -> dict[str, Any]:
        try:
            encoded_payload, encoded_signature = token.split(".", 1)
            canonical = _b64url_decode(encoded_payload)
            signature = _b64url_decode(encoded_signature)
            claims = json.loads(canonical)
        except (ValueError, json.JSONDecodeError) as exc:
            raise identity_error(
                "agent context assertion is malformed",
                reason=IdentityErrorReason.INVALID_SIGNATURE,
            ) from exc
        if not isinstance(claims, dict):
            raise identity_error(
                "agent context assertion is malformed",
                reason=IdentityErrorReason.INVALID_SIGNATURE,
            )
        for field in _REQUIRED_CLAIMS:
            if field not in claims:
                raise identity_error(
                    "agent context assertion is missing a required claim",
                    reason=IdentityErrorReason.INVALID_SIGNATURE,
                    detail=field,
                )
        if str(claims["iss"]) != self._issuer:
            raise identity_error(
                "agent context issuer is not trusted",
                reason=IdentityErrorReason.ISSUER_MISMATCH,
            )
        if str(claims["aud"]) != self._audience:
            raise identity_error(
                "agent context audience mismatch",
                reason=IdentityErrorReason.AUDIENCE_MISMATCH,
            )
        key = self._keys.get(str(claims["kid"]))
        if key is None:
            raise identity_error(
                "agent context key is not trusted",
                reason=IdentityErrorReason.INVALID_SIGNATURE,
            )
        expected = hmac.new(key, canonical, hashlib.sha256).digest()
        if not hmac.compare_digest(signature, expected):
            raise identity_error(
                "agent context signature is invalid",
                reason=IdentityErrorReason.INVALID_SIGNATURE,
            )
        now = int(datetime.now(UTC).timestamp())
        try:
            issued_at = int(claims["iat"])
            expires_at = int(claims["exp"])
        except (TypeError, ValueError) as exc:
            raise identity_error(
                "agent context timestamps are invalid",
                reason=IdentityErrorReason.INVALID_SIGNATURE,
            ) from exc
        if expires_at - issued_at > self._max_ttl_seconds:
            raise identity_error(
                "agent context lifetime exceeds the allowed maximum",
                reason=IdentityErrorReason.EXPIRED,
            )
        if issued_at - self._clock_skew_seconds > now:
            raise identity_error(
                "agent context is not yet valid",
                reason=IdentityErrorReason.EXPIRED,
            )
        if expires_at + self._clock_skew_seconds <= now:
            raise identity_error(
                "agent context assertion expired",
                reason=IdentityErrorReason.EXPIRED,
            )
        return claims

    def _reject_conflicts(
        self, request: IdentityVerificationRequest, user: TrustedUserContext
    ) -> None:
        if (
            request.declared_tenant_id is not None
            and request.declared_tenant_id != user.tenant_id
        ):
            raise identity_error(
                "declared tenant does not match signed agent context",
                reason=IdentityErrorReason.TENANT_SESSION_MISMATCH,
            )
        if (
            request.declared_user_id is not None
            and request.declared_user_id != user.user_id
        ):
            raise identity_error(
                "declared user does not match signed agent context",
                reason=IdentityErrorReason.TENANT_SESSION_MISMATCH,
            )
        if (
            request.declared_dept_id is not None
            and request.declared_dept_id != user.dept_id
        ):
            raise identity_error(
                "declared department does not match signed agent context",
                reason=IdentityErrorReason.TENANT_SESSION_MISMATCH,
            )
        if (
            request.bound_session_id is not None
            and request.bound_session_id != user.session_id
        ):
            raise identity_error(
                "session is not bound to the signed agent context",
                reason=IdentityErrorReason.TENANT_SESSION_MISMATCH,
            )


def _optional_str(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value)
    return text or None


def _scopes(value: Any) -> tuple[str, ...]:
    if value is None:
        return ()
    if isinstance(value, str):
        return tuple(item for item in value.split() if item)
    if isinstance(value, (list, tuple)):
        return tuple(str(item) for item in value if str(item))
    return ()
