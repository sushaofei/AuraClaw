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


def test_s4_migrations_up_down_roundtrip_in_isolated_postgres() -> None:
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
            for number in range(1, 16):
                matches = sorted(MIGRATIONS.glob(f"{number:04d}_*.sql"))
                migration = next(
                    item for item in matches if not item.name.endswith(".down.sql")
                )
                await connection.execute(migration.read_text())

            assert await connection.fetchval(
                "SELECT to_regclass('streaming.runtime_event') IS NOT NULL"
            )
            assert await connection.fetchval(
                "SELECT to_regclass('model_gateway.model_call') IS NOT NULL"
            )
            assert await connection.fetchval(
                "SELECT to_regclass('policy.active_bundle') IS NOT NULL"
            )
            assert await connection.fetchval(
                "SELECT to_regclass('hands.capability_catalog') IS NOT NULL"
            )
            assert await connection.fetchval(
                """SELECT count(*)=2 FROM information_schema.columns
                WHERE table_schema='control' AND table_name='runnable_item'
                  AND column_name IN ('claim_token','claim_expires_at')"""
            )
            assert await connection.fetchval(
                """SELECT count(*)=12 FROM information_schema.columns
                WHERE table_schema='artifact' AND table_name='metadata'
                  AND column_name IN (
                    'upload_mode','multipart_upload_id','multipart_part_size',
                    'multipart_completed_at','scan_started_at','scan_error',
                    'gc_attempt_count','gc_last_error','gc_claim_token',
                    'gc_claim_expires_at','finalize_claim_token',
                    'finalize_claim_expires_at')"""
            )

            for number in range(15, 9, -1):
                down = next(MIGRATIONS.glob(f"{number:04d}_*.down.sql"))
                await connection.execute(down.read_text())

            assert not await connection.fetchval(
                "SELECT to_regnamespace('streaming') IS NOT NULL"
            )
            assert not await connection.fetchval(
                "SELECT to_regnamespace('model_gateway') IS NOT NULL"
            )
            assert not await connection.fetchval(
                "SELECT to_regclass('policy.active_bundle') IS NOT NULL"
            )
            assert not await connection.fetchval(
                "SELECT to_regclass('hands.capability_catalog') IS NOT NULL"
            )
            assert not await connection.fetchval(
                """SELECT EXISTS(SELECT 1 FROM information_schema.columns
                WHERE table_schema='control' AND table_name='runnable_item'
                  AND column_name='claim_token')"""
            )
            assert not await connection.fetchval(
                """SELECT EXISTS(SELECT 1 FROM information_schema.columns
                WHERE table_schema='artifact' AND table_name='metadata'
                  AND column_name='upload_mode')"""
            )
        finally:
            await connection.close()

    with tempfile.TemporaryDirectory(prefix="auraclaw-s4-pg-") as cluster_dir:
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
            asyncio.run(scenario(f"postgresql://postgres@127.0.0.1:{port}/postgres"))
        finally:
            process.terminate()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
