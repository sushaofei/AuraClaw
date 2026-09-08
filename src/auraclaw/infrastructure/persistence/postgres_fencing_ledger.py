from __future__ import annotations

from typing import Literal

from auraclaw.contracts.errors import FencingTokenError
from auraclaw.infrastructure.persistence.postgres_common import LazyPool

FencingLedgerOwner = Literal["session", "control", "hands"]

_TABLE_BY_OWNER: dict[FencingLedgerOwner, str] = {
    "session": "session_core.fencing_token_high_watermark",
    "control": "control.fencing_token_high_watermark",
    "hands": "hands.fencing_token_high_watermark",
}


class PostgresFencingTokenLedger(LazyPool):
    """Persist the highest accepted fencing token in the owning service schema."""

    def __init__(self, database_url: str, *, owner: FencingLedgerOwner) -> None:
        super().__init__(database_url)
        self._table = _TABLE_BY_OWNER[owner]

    async def accept(self, tenant_id: str, session_id: str, fencing_token: int) -> None:
        if fencing_token < 0:
            raise FencingTokenError("lease assertion contains an invalid fencing token")
        pool = await self.pool()
        accepted = await pool.fetchval(
            f"""INSERT INTO {self._table}
                (tenant_id,resource_id,highest_token)
                VALUES ($1,$2,$3)
                ON CONFLICT (tenant_id,resource_id) DO UPDATE
                SET highest_token=EXCLUDED.highest_token,updated_at=now()
                WHERE {self._table}.highest_token <= EXCLUDED.highest_token
                RETURNING highest_token""",
            tenant_id,
            session_id,
            fencing_token,
        )
        if accepted is None:
            raise FencingTokenError("lease assertion contains a stale fencing token")
