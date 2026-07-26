"""MySQL primary-storage smoke: events, projection, dedup, outbox, control claim."""

from __future__ import annotations

import asyncio
import os
from datetime import timedelta
from urllib.parse import quote
from uuid import uuid4

import pytest

from auraclaw.contracts.commands import CommandContext
from auraclaw.contracts.events import Actor, NewEvent
from auraclaw.contracts.state import Visibility
from auraclaw.control.ports import RunnableItem
from auraclaw.infrastructure.persistence.postgres_control_store import (
    PostgresControlStateStore,
)
from auraclaw.infrastructure.persistence.postgres_event_store import PostgresEventStore
from auraclaw.infrastructure.persistence.sql_dialect import detect_dialect
from auraclaw.infrastructure.projection.postgres_task_store import PostgresTaskProjection


def _mysql_url() -> str | None:
    host = os.environ.get("MYSQL_DB_HOST") or os.environ.get("AURACLAW_MYSQL_SMOKE_HOST")
    if not host:
        return None
    user = os.environ.get("MYSQL_DB_USER") or os.environ.get("DB_USER")
    password = os.environ.get("MYSQL_DB_PWD") or os.environ.get("DB_PWD")
    port = os.environ.get("MYSQL_DB_PORT") or os.environ.get("DB_PORT") or "3306"
    database = os.environ.get("AURACLAW_MYSQL_SMOKE_DB") or "auraclaw_dev"
    if not user or password is None:
        return None
    return (
        f"mysql+aiomysql://{quote(user, safe='')}:{quote(password, safe='')}"
        f"@{host}:{port}/{database}"
    )


MYSQL_URL = _mysql_url()
pytestmark = pytest.mark.skipif(MYSQL_URL is None, reason="MySQL smoke URL not configured")


async def _cleanup_session(store: PostgresEventStore, tenant: str) -> None:
    pool = await store.pool()
    await pool.execute("DELETE FROM session_core.outbox WHERE 1=1")
    await pool.execute(
        "DELETE FROM session_core.canonical_event WHERE tenant_id=$1", tenant
    )
    await pool.execute(
        "DELETE FROM session_core.session_head WHERE tenant_id=$1", tenant
    )
    await pool.execute(
        "DELETE FROM session_core.command_dedup WHERE tenant_id=$1", tenant
    )
    await pool.execute("DELETE FROM projection.task_view WHERE tenant_id=$1", tenant)
    await pool.execute(
        "DELETE FROM projection.processed_event WHERE projector_id='task'"
    )


@pytest.mark.asyncio
async def test_mysql_event_append_load_and_task_projection() -> None:
    assert MYSQL_URL is not None
    assert detect_dialect(MYSQL_URL) == "mysql"
    store = PostgresEventStore(MYSQL_URL)
    projection = PostgresTaskProjection(MYSQL_URL)
    tenant = "mysql-smoke"
    session = "sess_mysql_smoke_1"
    try:
        await _cleanup_session(store, tenant)

        context = CommandContext(
            tenant_id=tenant,
            command_id="cmd_mysql_smoke_1",
            operation="create_task",
            actor=Actor(type="user", id="smoke"),
            correlation_id="corr_smoke",
            causation_id=None,
            expected_version=0,
        )
        result = await store.append(
            root_session_id=session,
            session_id=session,
            run_id=None,
            context=context,
            events=[
                NewEvent(
                    type="session.created",
                    visibility=Visibility.USER,
                    payload={"goal": "mysql smoke", "role": "root"},
                )
            ],
            command_result={"session_id": session},
        )
        assert not result.deduplicated
        assert len(result.events) == 1

        loaded = await store.load(tenant, session)
        assert len(loaded) == 1
        assert loaded[0].type == "session.created"
        assert loaded[0].payload["goal"] == "mysql smoke"

        await projection.project(loaded)
        task = await projection.get_task(tenant, session)
        assert task is not None
        assert task["session_id"] == session
        assert task["goal"] == "mysql smoke"
        assert store.dialect == "mysql"
    finally:
        await store.close()
        await projection.close()


@pytest.mark.asyncio
async def test_mysql_command_dedup_and_outbox_claim() -> None:
    assert MYSQL_URL is not None
    store = PostgresEventStore(MYSQL_URL)
    tenant = "mysql-smoke-dedup"
    session = f"sess_dedup_{uuid4().hex[:8]}"
    command_id = f"cmd_dedup_{uuid4().hex[:8]}"
    try:
        await _cleanup_session(store, tenant)
        context = CommandContext(
            tenant_id=tenant,
            command_id=command_id,
            operation="create_task",
            actor=Actor(type="user", id="smoke"),
            correlation_id="corr_dedup",
            causation_id=None,
            expected_version=0,
        )
        first = await store.append(
            root_session_id=session,
            session_id=session,
            run_id=None,
            context=context,
            events=[
                NewEvent(
                    type="session.created",
                    visibility=Visibility.USER,
                    payload={"goal": "dedup", "role": "root"},
                )
            ],
            command_result={"session_id": session},
        )
        assert not first.deduplicated
        second = await store.append(
            root_session_id=session,
            session_id=session,
            run_id=None,
            context=context,
            events=[
                NewEvent(
                    type="session.created",
                    visibility=Visibility.USER,
                    payload={"goal": "dedup", "role": "root"},
                )
            ],
            command_result={"session_id": session},
        )
        assert second.deduplicated

        claimed = await store.claim_outbox(
            "projection",
            "mysql-smoke-worker",
            limit=10,
            claim_ttl=timedelta(seconds=30),
        )
        assert any(item.event.session_id == session for item in claimed)
        match = next(item for item in claimed if item.event.session_id == session)
        ok = await store.disposition_outbox(
            "projection",
            "mysql-smoke-worker",
            match.outbox_id,
            match.claim_token,
            "ack",
        )
        assert ok
    finally:
        await store.close()


@pytest.mark.asyncio
async def test_mysql_control_enqueue_and_claim() -> None:
    assert MYSQL_URL is not None
    control = PostgresControlStateStore(MYSQL_URL)
    suffix = uuid4().hex[:8]
    tenant = f"mysql-ctl-{suffix}"
    session = f"sess-ctl-{suffix}"
    run_id = f"run-ctl-{suffix}"
    task_id = f"{tenant}:{session}:{run_id}"
    try:
        pool = await control.pool()
        await pool.execute(
            "DELETE FROM control.runnable_item WHERE tenant_id LIKE $1",
            "mysql-ctl-%",
        )
        await pool.execute(
            "DELETE FROM control.runnable_item WHERE tenant_id LIKE $1",
            "dbg%",
        )
        item = RunnableItem(
            task_id=task_id,
            tenant_id=tenant,
            root_session_id=session,
            session_id=session,
            run_id=run_id,
            source_version=1,
        )
        assert await control.enqueue(item)
        assert not await control.enqueue(item)
        claimed = await control.claim("orch-mysql-smoke", limit=1)
        assert len(claimed) == 1
        assert claimed[0].item.task_id == task_id
        assert claimed[0].claimed_by == "orch-mysql-smoke"
        assert claimed[0].claim_token
        empty = await control.claim("orch-other", limit=1)
        assert empty == []
    finally:
        await control.close()


@pytest.mark.asyncio
async def test_mysql_control_lease_and_capacity() -> None:
    assert MYSQL_URL is not None
    control = PostgresControlStateStore(MYSQL_URL)
    suffix = uuid4().hex[:8]
    resource_id = f"session:mysql-lease-{suffix}:sess"
    scope = f"tenant:mysql-cap-{suffix}"
    try:
        pool = await control.pool()
        await pool.execute(
            "DELETE FROM control.runtime_lease WHERE resource_id=$1", resource_id
        )
        await pool.execute(
            "DELETE FROM control.capacity_reservation WHERE scope=$1", scope
        )
        lease = await control.acquire_lease(
            resource_id, "orch-a", ttl=timedelta(seconds=30)
        )
        assert lease is not None
        assert lease.owner == "orch-a"
        assert lease.fencing_token == 1
        blocked = await control.acquire_lease(
            resource_id, "orch-b", ttl=timedelta(seconds=30)
        )
        assert blocked is None
        assert await control.reserve_capacity(scope, 2, limit=3)
        assert not await control.reserve_capacity(scope, 2, limit=3)
        assert await control.reserve_capacity(scope, 1, limit=3)
        await control.release_capacity(scope, 3)
        assert await control.reserve_capacity(scope, 3, limit=3)
    finally:
        await control.close()


if __name__ == "__main__":
    asyncio.run(test_mysql_event_append_load_and_task_projection())
