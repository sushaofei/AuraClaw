from __future__ import annotations

import hashlib
import hmac
from collections.abc import MutableMapping
from datetime import UTC, datetime, timedelta
from typing import Any, Protocol

from auraclaw.contracts.identity import IdentityErrorReason, identity_error
from auraclaw.infrastructure.persistence.postgres_common import LazyPool


class AssertionReplayStore(Protocol):
    async def remember_write(
        self, jti: str, command_id: str, *, expires_at: int
    ) -> None: ...

    async def close(self) -> None: ...


class AssertionReplayGuard:
    """Process-local replay guard used only by non-SQL development compositions."""

    def __init__(self, *, ttl: timedelta = timedelta(minutes=10)) -> None:
        self._ttl = ttl
        self._records: MutableMapping[str, tuple[str, datetime]] = {}

    async def remember_write(
        self, jti: str, command_id: str, *, expires_at: int
    ) -> None:
        self._purge()
        existing = self._records.get(jti)
        if existing is not None and existing[0] != command_id:
            raise identity_error(
                "agent context assertion was replayed across commands",
                reason=IdentityErrorReason.REPLAYED,
            )
        assertion_expiry = datetime.fromtimestamp(expires_at, UTC)
        self._records[jti] = (
            command_id,
            max(assertion_expiry, datetime.now(UTC) + self._ttl),
        )

    async def close(self) -> None:
        self._records.clear()

    def _purge(self) -> None:
        now = datetime.now(UTC)
        expired = [key for key, (_, expires_at) in self._records.items() if expires_at <= now]
        for key in expired:
            self._records.pop(key, None)


class DatabaseAssertionReplayGuard(LazyPool):
    """Cross-replica replay guard backed by an atomic SQL uniqueness constraint."""

    async def remember_write(
        self, jti: str, command_id: str, *, expires_at: int
    ) -> None:
        try:
            row = await self._remember_write(jti, command_id, expires_at=expires_at)
        except Exception as exc:
            raise identity_error(
                "agent context replay verifier is unavailable",
                reason=IdentityErrorReason.VERIFIER_UNAVAILABLE,
            ) from exc
        if row is None or str(row["command_id"]) != command_id:
            raise identity_error(
                "agent context assertion was replayed across commands",
                reason=IdentityErrorReason.REPLAYED,
            )

    async def _remember_write(
        self, jti: str, command_id: str, *, expires_at: int
    ) -> Any:
        pool = await self.pool()
        now = datetime.now(UTC)
        expiry = datetime.fromtimestamp(expires_at, UTC)
        jti_hash = hashlib.sha256(jti.encode()).hexdigest()
        table = "security.agent_context_replay"
        await pool.execute(f"DELETE FROM {table} WHERE expires_at <= $1", now)
        await pool.execute(
            f"""INSERT INTO {table} (jti_hash, command_id, expires_at)
            VALUES ($1, $2, $3)
            ON CONFLICT (jti_hash) DO NOTHING""",
            jti_hash,
            command_id,
            expiry,
        )
        return await pool.fetchrow(
            f"SELECT command_id FROM {table} WHERE jti_hash = $1", jti_hash
        )


def digest_jti(jti: str) -> str:
    return hashlib.sha256(jti.encode()).hexdigest()[:12]


def compare_secret(left: str, right: str) -> bool:
    return hmac.compare_digest(left.encode(), right.encode())
