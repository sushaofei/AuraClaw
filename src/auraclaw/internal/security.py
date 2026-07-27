from __future__ import annotations

import base64
import hashlib
import hmac
import json
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Protocol

from auraclaw.contracts.errors import AuthorizationError, FencingTokenError, LeaseConflictError
from auraclaw.contracts.internal import LeaseAssertion


def _canonical_claims(assertion: LeaseAssertion) -> bytes:
    expires_at = assertion.expires_at
    if expires_at.tzinfo is None:
        expires_at = expires_at.replace(tzinfo=UTC)
    return json.dumps(
        {
            "audience": assertion.audience,
            "expires_at": expires_at.astimezone(UTC).isoformat(),
            "fencing_token": assertion.fencing_token,
            "key_id": assertion.key_id,
            "lease_id": assertion.lease_id,
            "run_id": assertion.run_id,
            "root_session_id": assertion.root_session_id,
            "runtime_id": assertion.runtime_id,
            "session_id": assertion.session_id,
            "tenant_id": assertion.tenant_id,
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode()


class FencingTokenLedger(Protocol):
    async def accept(self, tenant_id: str, session_id: str, fencing_token: int) -> None: ...


class InMemoryFencingTokenLedger:
    """Development ledger; production adapters persist the high-water mark in Session storage."""

    def __init__(self) -> None:
        self._tokens: dict[tuple[str, str], int] = {}

    async def accept(self, tenant_id: str, session_id: str, fencing_token: int) -> None:
        key = (tenant_id, session_id)
        current = self._tokens.get(key, 0)
        if fencing_token < current:
            raise FencingTokenError("lease assertion contains a stale fencing token")
        self._tokens[key] = max(current, fencing_token)


class LeaseAssertionSigner:
    """Rotatable keyed signer used by the development control plane.

    Production can replace this port with workload-identity or asymmetric signing without
    changing the wire contract.
    """

    def __init__(self, *, key_id: str, signing_key: bytes) -> None:
        if len(signing_key) < 32:
            raise ValueError("lease assertion signing key must contain at least 32 bytes")
        self._key_id = key_id
        self._signing_key = bytes(signing_key)

    def sign(self, assertion: LeaseAssertion) -> LeaseAssertion:
        unsigned = assertion.model_copy(update={"key_id": self._key_id, "signature": ""})
        signature = base64.urlsafe_b64encode(
            hmac.new(self._signing_key, _canonical_claims(unsigned), hashlib.sha256).digest()
        ).decode()
        return unsigned.model_copy(update={"signature": signature})


class LeaseAssertionVerifier:
    def __init__(
        self,
        keys: Mapping[str, bytes],
        *,
        ledger: FencingTokenLedger,
        audience: str | tuple[str, ...] = "session",
    ) -> None:
        self._keys = {key_id: bytes(value) for key_id, value in keys.items()}
        self._ledger = ledger
        self._audiences = (audience,) if isinstance(audience, str) else audience

    async def verify(
        self,
        assertion: LeaseAssertion,
        *,
        tenant_id: str,
        session_id: str,
        run_id: str,
        lease_id: str | None = None,
    ) -> None:
        key = self._keys.get(assertion.key_id)
        if key is None:
            raise AuthorizationError("lease assertion key is not trusted")
        if assertion.audience not in self._audiences:
            raise AuthorizationError("lease assertion audience mismatch")
        if (
            assertion.tenant_id != tenant_id
            or assertion.session_id != session_id
            or assertion.run_id != run_id
        ):
            raise AuthorizationError("lease assertion scope mismatch")
        if lease_id is not None and assertion.lease_id != lease_id:
            raise LeaseConflictError("lease assertion id mismatch")
        expires_at = assertion.expires_at
        if expires_at.tzinfo is None:
            expires_at = expires_at.replace(tzinfo=UTC)
        if expires_at <= datetime.now(UTC):
            raise LeaseConflictError("lease assertion expired")
        expected = base64.urlsafe_b64encode(
            hmac.new(key, _canonical_claims(assertion), hashlib.sha256).digest()
        ).decode()
        if not hmac.compare_digest(assertion.signature, expected):
            raise AuthorizationError("lease assertion signature is invalid")
        await self._ledger.accept(tenant_id, session_id, assertion.fencing_token)
