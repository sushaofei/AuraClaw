from __future__ import annotations

import asyncio

import asyncpg
import pytest

from auraclaw.contracts.errors import VersionConflictError
from auraclaw.infrastructure.observability.stores import InMemoryObservabilityStore
from auraclaw.infrastructure.persistence.postgres_common import LazyPool


def test_retryable_transaction_restarts_whole_callback_and_emits_metric() -> None:
    async def scenario() -> None:
        metrics = InMemoryObservabilityStore()
        store = LazyPool(
            "postgresql://unused",
            transaction_retry_attempts=3,
            transaction_retry_base_delay=0,
            metric_writer=metrics,
        )
        attempts = 0

        async def transaction() -> str:
            nonlocal attempts
            attempts += 1
            if attempts < 3:
                raise asyncpg.DeadlockDetectedError("injected deadlock")
            return "committed"

        assert await store.run_transaction_with_retry("skill.test", transaction) == (
            "committed"
        )
        assert attempts == 3
        points = await metrics.metric_snapshot()
        assert [point.name for point in points] == [
            "postgres.transaction.retry",
            "postgres.transaction.retry",
        ]
        assert all(point.labels["sqlstate"] == "40P01" for point in points)

    asyncio.run(scenario())


def test_retryable_transaction_exhaustion_is_stable_conflict() -> None:
    async def scenario() -> None:
        store = LazyPool(
            "postgresql://unused",
            transaction_retry_attempts=2,
            transaction_retry_base_delay=0,
        )

        async def transaction() -> None:
            raise asyncpg.SerializationError("injected serialization failure")

        with pytest.raises(VersionConflictError) as raised:
            await store.run_transaction_with_retry("skill.test", transaction)
        assert raised.value.retry_after == 1
        assert raised.value.detail == "skill.test"

    asyncio.run(scenario())
