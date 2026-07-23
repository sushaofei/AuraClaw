import asyncio
import shutil
import socket
import subprocess
import tempfile
import time
from pathlib import Path

import asyncpg
import pytest

ROOT = Path(__file__).resolve().parents[2]
MIGRATIONS = ROOT / "migrations"


def _free_port() -> int:
    with socket.socket() as server:
        server.bind(("127.0.0.1", 0))
        return int(server.getsockname()[1])


def test_s3_migration_up_down_roundtrip_in_isolated_postgres() -> None:
    initdb = shutil.which("initdb")
    postgres = shutil.which("postgres")
    if initdb is None or postgres is None:
        pytest.skip("local PostgreSQL binaries are unavailable")

    async def scenario(database_url: str) -> None:
        connection: asyncpg.Connection | None = None
        deadline = time.monotonic() + 15
        while connection is None:
            try:
                connection = await asyncpg.connect(database_url)
            except (OSError, asyncpg.PostgresError):
                if time.monotonic() >= deadline:
                    raise
                await asyncio.sleep(0.1)
        try:
            migrations = [
                next(
                    item
                    for item in sorted(MIGRATIONS.glob(f"{number:04d}_*.sql"))
                    if not item.name.endswith(".down.sql")
                )
                for number in range(1, 9)
            ]
            for migration in migrations:
                await connection.execute(migration.read_text())

            await connection.execute(
                """INSERT INTO security.tool_capability
                (tool_name,version,description,input_schema,output_schema,permission,
                 risk_level,runtime_location,owner)
                VALUES ('legacy-tool','1','legacy','{}','{}','read-only','low',
                        'hands','legacy-owner')"""
            )
            await connection.execute(
                """INSERT INTO security.credential_reference
                (tenant_id,credential_ref,provider,account_scope,allowed_operations,
                 expires_at)
                VALUES ('tenant','legacy-credential','provider','account','["read"]',
                        now() + interval '5 minutes')"""
            )

            await connection.execute(
                (MIGRATIONS / "0009_s3_owner_boundaries.sql").read_text()
            )
            migrated = await connection.fetchrow(
                """SELECT provider,account_scope FROM credential.reference
                WHERE tenant_id='tenant' AND credential_ref='legacy-credential'"""
            )
            assert migrated is not None
            assert dict(migrated) == {
                "provider": "provider",
                "account_scope": "account",
            }
            assert await connection.fetchval(
                "SELECT count(*) FROM hands.tool_capability WHERE tool_name='legacy-tool'"
            ) == 1

            await connection.execute(
                """INSERT INTO hands.tool_capability
                (tool_name,version,description,input_schema,output_schema,permission,
                 risk_level,runtime_location,owner)
                VALUES ('new-tool','1','new','{}','{}','read-only','low','hands',
                        'new-owner')"""
            )
            await connection.execute(
                """INSERT INTO credential.reference
                (tenant_id,credential_ref,resource,provider,account_scope,
                 allowed_operations,expires_at)
                VALUES ('tenant','new-credential','new-provider','new-provider',
                        'new-account','["write"]',now() + interval '5 minutes')"""
            )
            await connection.execute(
                """INSERT INTO hands.invocation
                (tenant_id,tool_invocation_id,idempotency_key,root_session_id,
                 session_id,run_id,tool_name,tool_version,argument_digest,
                 normalized_arguments,status,normalized_result,side_effect_status,
                 fencing_token)
                VALUES ('tenant','new-invocation','new-idempotency','root','session',
                        'run','new-tool','1','digest','{}','completed',
                        '{"status":"success","content":{"ok":true},
                          "summary":"","metadata":{},"error_code":null,
                          "side_effect_status":"completed"}',
                        'completed',1)"""
            )

            await connection.execute(
                (MIGRATIONS / "0009_s3_owner_boundaries.down.sql").read_text()
            )
            assert await connection.fetchval(
                "SELECT count(*) FROM security.tool_capability WHERE tool_name='new-tool'"
            ) == 1
            rolled_back = await connection.fetchrow(
                """SELECT provider,account_scope FROM security.credential_reference
                WHERE tenant_id='tenant' AND credential_ref='new-credential'"""
            )
            assert rolled_back is not None
            assert dict(rolled_back) == {
                "provider": "new-provider",
                "account_scope": "new-account",
            }
            assert await connection.fetchval(
                """SELECT count(*) FROM security.tool_invocation_dedup
                WHERE tenant_id='tenant' AND idempotency_key='new-idempotency'"""
            ) == 1
        finally:
            await connection.close()

    with tempfile.TemporaryDirectory(prefix="auraclaw-pg-") as cluster_dir:
        subprocess.run(
            [initdb, "-D", cluster_dir, "-A", "trust", "-U", "postgres", "--no-locale"],
            check=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        port = _free_port()
        process = subprocess.Popen(
            [
                postgres,
                "-D",
                cluster_dir,
                "-h",
                "127.0.0.1",
                "-p",
                str(port),
                "-c",
                "fsync=off",
                "-c",
                "synchronous_commit=off",
            ],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        try:
            asyncio.run(
                scenario(f"postgresql://postgres@127.0.0.1:{port}/postgres")
            )
        finally:
            process.terminate()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
