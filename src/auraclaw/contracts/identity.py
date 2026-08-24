from __future__ import annotations

import hashlib
from enum import StrEnum
from typing import Protocol

from pydantic import Field, field_serializer

from auraclaw.contracts.errors import AuthorizationError, UnauthenticatedError
from auraclaw.contracts.internal import ContractModel


class IdentityErrorReason(StrEnum):
    MISSING_CREDENTIAL = "missing_credential"
    INVALID_SIGNATURE = "invalid_signature"
    ISSUER_MISMATCH = "issuer_mismatch"
    AUDIENCE_MISMATCH = "audience_mismatch"
    EXPIRED = "expired"
    REPLAYED = "replayed"
    SCOPE_DENIED = "scope_denied"
    TENANT_SESSION_MISMATCH = "tenant_session_mismatch"
    WORKLOAD_MISMATCH = "workload_mismatch"
    VERIFIER_UNAVAILABLE = "verifier_unavailable"


_AUTHENTICATED_REASONS = {
    IdentityErrorReason.SCOPE_DENIED,
    IdentityErrorReason.TENANT_SESSION_MISMATCH,
}


class IdentityAuthenticationError(UnauthenticatedError):
    code = "unauthenticated"

    def __init__(
        self,
        message: str,
        *,
        reason: IdentityErrorReason,
        detail: str | None = None,
    ) -> None:
        super().__init__(message, detail=detail)
        self.reason = reason
        self.code = reason.value


class IdentityAuthorizationError(AuthorizationError):
    code = "identity_forbidden"

    def __init__(
        self,
        message: str,
        *,
        reason: IdentityErrorReason,
        detail: str | None = None,
    ) -> None:
        super().__init__(message, detail=detail)
        self.reason = reason
        self.code = reason.value


def identity_error(
    message: str,
    *,
    reason: IdentityErrorReason,
    detail: str | None = None,
) -> IdentityAuthenticationError | IdentityAuthorizationError:
    if reason in _AUTHENTICATED_REASONS:
        return IdentityAuthorizationError(message, reason=reason, detail=detail)
    return IdentityAuthenticationError(message, reason=reason, detail=detail)


def assertion_jti_digest(jti: str) -> str:
    return hashlib.sha256(jti.encode()).hexdigest()[:12]


class TrustedUserContext(ContractModel):
    tenant_id: str = Field(min_length=1, max_length=128)
    user_id: str = Field(min_length=1, max_length=128)
    dept_id: str | None = Field(default=None, max_length=128)
    session_id: str | None = Field(default=None, max_length=128)
    scopes: tuple[str, ...] = ()
    permission_version: str | None = Field(default=None, max_length=64)


class AuthenticatedCaller(ContractModel):
    kind: str = Field(min_length=1, max_length=64)
    subject: str = Field(min_length=1, max_length=128)


class AssertionMetadata(ContractModel):
    issuer: str = Field(min_length=1, max_length=128)
    audience: str = Field(min_length=1, max_length=128)
    key_id: str = Field(min_length=1, max_length=64)
    jti: str = Field(min_length=1, max_length=128)
    issued_at: int = Field(ge=0)
    expires_at: int = Field(ge=0)


class VerifiedIdentityEnvelope(ContractModel):
    caller: AuthenticatedCaller
    user: TrustedUserContext
    assertion: AssertionMetadata | None = None

    def __repr__(self) -> str:
        session = self.user.session_id or "-"
        kid = self.assertion.key_id if self.assertion is not None else "-"
        jti = self.assertion.jti[:8] if self.assertion is not None else "-"
        return (
            "VerifiedIdentityEnvelope("
            f"tenant={self.user.tenant_id!r}, user={self.user.user_id!r}, "
            f"session={session!r}, kid={kid!r}, jti={jti!r})"
        )


class IdentityVerificationRequest(ContractModel):
    workload_credential: str | None = None
    assertion: str | None = None
    declared_tenant_id: str | None = None
    declared_user_id: str | None = None
    declared_dept_id: str | None = None
    bound_session_id: str | None = None
    command_id: str | None = None
    correlation_id: str | None = None
    operation: str = "read"

    @field_serializer("workload_credential", "assertion")
    def _redact_secrets(self, value: str | None) -> str | None:
        return "[REDACTED]" if value else value

    def __repr__(self) -> str:
        return (
            "IdentityVerificationRequest("
            f"operation={self.operation!r}, command_id={self.command_id!r})"
        )


class IdentityContextVerifier(Protocol):
    async def verify(
        self, request: IdentityVerificationRequest
    ) -> VerifiedIdentityEnvelope: ...
