from __future__ import annotations

import asyncio
import sys

import asyncpg  # type: ignore[import-untyped]

from auraclaw.config import get_settings
from auraclaw.infrastructure.persistence.postgres_common import asyncpg_url


async def main() -> int:
    settings = get_settings()
    if not settings.kingbase_enabled:
        print("KingBase smoke failed: AURACLAW_STORAGE_BACKEND must be kingbase")
        return 1
    connection = await asyncpg.connect(asyncpg_url(settings.resolved_database_url))
    locked = False
    try:
        version = str(await connection.fetchval("SELECT version()"))
        locked = bool(
            await connection.fetchval(
                "SELECT pg_try_advisory_lock(hashtext('auraclaw-kingbase-smoke'))"
            )
        )
        if not locked:
            raise RuntimeError("advisory lock is unavailable")
        async with connection.transaction():
            await connection.execute(
                """CREATE TEMP TABLE auraclaw_kingbase_smoke (
                id integer PRIMARY KEY,
                payload jsonb NOT NULL,
                observed_at timestamptz NOT NULL DEFAULT now()
                ) ON COMMIT DROP"""
            )
            returned = await connection.fetchval(
                """INSERT INTO auraclaw_kingbase_smoke (id,payload)
                VALUES (1,jsonb_build_object('status','ready'))
                ON CONFLICT (id) DO UPDATE SET payload=EXCLUDED.payload
                RETURNING payload->>'status'"""
            )
            row = await connection.fetchrow(
                """SELECT id,payload,observed_at FROM auraclaw_kingbase_smoke
                FOR UPDATE SKIP LOCKED"""
            )
            if returned != "ready" or row is None or row["id"] != 1:
                raise RuntimeError("jsonb/on conflict/returning/skip locked smoke failed")
        can_create_schema = bool(
            await connection.fetchval(
                "SELECT has_database_privilege(current_user,current_database(),'CREATE')"
            )
        )
        can_create_role = bool(
            await connection.fetchval(
                "SELECT rolcreaterole FROM pg_roles WHERE rolname=current_user"
            )
        )
        engine = "KingbaseES" if "Kingbase" in version else "PostgreSQL-compatible"
        print(
            "KingBase smoke passed: "
            f"engine={engine} jsonb=ok on_conflict=ok returning=ok "
            f"skip_locked=ok timestamptz=ok advisory_lock=ok "
            f"create_schema={str(can_create_schema).lower()} "
            f"create_role={str(can_create_role).lower()}"
        )
        return 0
    finally:
        if locked:
            await connection.execute(
                "SELECT pg_advisory_unlock(hashtext('auraclaw-kingbase-smoke'))"
            )
        await connection.close()


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
