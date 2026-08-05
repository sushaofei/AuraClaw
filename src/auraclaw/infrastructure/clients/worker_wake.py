from __future__ import annotations

import asyncio
import logging
from collections.abc import Iterable, Mapping

import httpx

logger = logging.getLogger(__name__)


class HttpWorkerWakeClient:
    """Fire-and-forget wake of a production worker over the internal network."""

    def __init__(
        self,
        base_url: str,
        *,
        timeout: float = 0.5,
        transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        self._client = httpx.AsyncClient(
            base_url=base_url.rstrip("/"),
            timeout=timeout,
            transport=transport,
        )

    async def aclose(self) -> None:
        await self._client.aclose()

    async def wake(self) -> None:
        try:
            response = await self._client.post("/internal/v1/worker/wake")
            response.raise_for_status()
        except Exception:
            logger.debug("worker wake failed for %s", self._client.base_url, exc_info=True)


class OutboxWakeNotifier:
    """Schedules destination-specific wakes after Session outbox writes."""

    def __init__(self, clients: Mapping[str, HttpWorkerWakeClient]) -> None:
        self._clients = dict(clients)
        self._tasks: set[asyncio.Task[None]] = set()

    async def aclose(self) -> None:
        for task in list(self._tasks):
            task.cancel()
        for task in list(self._tasks):
            try:
                await task
            except asyncio.CancelledError:
                pass
        for client in self._clients.values():
            await client.aclose()

    def schedule(self, destinations: Iterable[str]) -> None:
        for destination in set(destinations):
            client = self._clients.get(destination)
            if client is None:
                continue
            task = asyncio.create_task(
                client.wake(), name=f"outbox-wake-{destination}"
            )
            self._tasks.add(task)
            task.add_done_callback(self._tasks.discard)
