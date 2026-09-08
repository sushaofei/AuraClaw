from __future__ import annotations

import asyncio
from pathlib import Path
from uuid import uuid4

import asyncpg
import pytest

from auraclaw.config import get_settings
from auraclaw.contracts.errors import FencingTokenError
from auraclaw.infrastructure.persistence.postgres_common import asyncpg_url
from auraclaw.infrastructure.persistence.postgres_fencing_ledger import (
    FencingLedgerOwner,
    PostgresFencingTokenLedger,
)

SETTINGS = get_settings()
DATABASE_URL = asyncpg_url(SETTINGS.resolved_database_url) if SETTINGS.postgres_enabled else None
ROOT = Path(__file__).resolve().parents[2]
MIGRATION = (ROOT / "migrations/0042_persistent_fencing_tokens.sql").read_text()
TABLE_BY_OWNER: dict[FencingLedgerOwner, str] = {
    "session": "session_core.fencing_token_high_watermark",
    "control": "control.fencing_token_high_watermark",
    "hands": "hands.fencing_token_high_watermark",
}

pytestmark = pytest.mark.skipif(DATABASE_URL is None, reason="PostgreSQL test URL not configured")


@pytest.mark.parametrize("owner", ["session", "control", "hands"])
def test_fencing_high_watermark_survives_replicas_restart_and_concurrency(
    owner: FencingLedgerOwner,
) -> None:
    async def scenario() -> None:
        assert DATABASE_URL is not None
        connection = await asyncpg.connect(DATABASE_URL)
        await connection.execute(MIGRATION)
        suffix = uuid4().hex
        tenant_id = f"tenant-fencing-{suffix}"
        resource_id = f"session-{suffix}"
        first = PostgresFencingTokenLedger(DATABASE_URL, owner=owner)
        second = PostgresFencingTokenLedger(DATABASE_URL, owner=owner)
        restarted: PostgresFencingTokenLedger | None = None
        try:
            await first.accept(tenant_id, resource_id, 2)
            with pytest.raises(FencingTokenError, match="stale"):
                await second.accept(tenant_id, resource_id, 1)
            await second.accept(tenant_id, resource_id, 2)

            await first.close()
            restarted = PostgresFencingTokenLedger(DATABASE_URL, owner=owner)
            with pytest.raises(FencingTokenError, match="stale"):
                await restarted.accept(tenant_id, resource_id, 1)

            await restarted.accept(f"{tenant_id}-other", resource_id, 1)
            await restarted.accept(tenant_id, f"{resource_id}-other", 1)

            concurrent_resource = f"{resource_id}-concurrent"
            await asyncio.gather(
                first.accept(tenant_id, concurrent_resource, 1),
                second.accept(tenant_id, concurrent_resource, 2),
                return_exceptions=True,
            )
            highest = await connection.fetchval(
                f"SELECT highest_token FROM {TABLE_BY_OWNER[owner]} "
                "WHERE tenant_id=$1 AND resource_id=$2",
                tenant_id,
                concurrent_resource,
            )
            assert highest == 2
            with pytest.raises(FencingTokenError, match="stale"):
                await restarted.accept(tenant_id, concurrent_resource, 1)
        finally:
            await first.close()
            await second.close()
            if restarted is not None:
                await restarted.close()
            await connection.execute(
                f"DELETE FROM {TABLE_BY_OWNER[owner]} WHERE tenant_id LIKE $1",
                f"{tenant_id}%",
            )
            await connection.close()

    asyncio.run(scenario())
