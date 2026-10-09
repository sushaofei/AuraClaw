from __future__ import annotations

import asyncio
import json
import time

import httpx
import pytest

from auraclaw.infrastructure.model.capability_embeddings import (
    OpenAICompatibleCapabilityEmbeddingProvider,
)


def test_embedding_provider_restores_response_order_and_validates_dimension() -> None:
    async def scenario() -> None:
        async def handler(request: httpx.Request) -> httpx.Response:
            assert request.headers["authorization"] == "Bearer managed-key"
            assert json.loads(request.content) == {
                "model": "multilingual-v1",
                "input": ["first", "second"],
            }
            return httpx.Response(
                200,
                json={
                    "data": [
                        {"index": 1, "embedding": [0.0, 1.0, 0.0]},
                        {"index": 0, "embedding": [1.0, 0.0, 0.0]},
                    ]
                },
            )

        client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        provider = OpenAICompatibleCapabilityEmbeddingProvider(
            endpoint="https://embedding.example/v1/embeddings",
            model="multilingual-v1",
            dimensions=3,
            api_key="managed-key",
            client=client,
        )
        assert await provider.embed(("first", "second")) == (
            (1.0, 0.0, 0.0),
            (0.0, 1.0, 0.0),
        )
        await client.aclose()

    asyncio.run(scenario())


def test_embedding_provider_retries_transient_failure_without_leaking_body() -> None:
    async def scenario() -> None:
        calls = 0

        async def handler(request: httpx.Request) -> httpx.Response:
            nonlocal calls
            calls += 1
            if calls == 1:
                return httpx.Response(503, json={"secret": "must-not-surface"})
            return httpx.Response(200, json={"data": [{"index": 0, "embedding": [1.0]}]})

        client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        provider = OpenAICompatibleCapabilityEmbeddingProvider(
            endpoint="https://embedding.example/v1/embeddings",
            model="multilingual-v1",
            dimensions=1,
            client=client,
        )
        assert await provider.embed(("controlled summary",)) == ((1.0,),)
        assert calls == 2
        await client.aclose()

    asyncio.run(scenario())


def test_embedding_provider_applies_one_deadline_to_queue_and_all_retries() -> None:
    async def scenario() -> None:
        async def handler(_request: httpx.Request) -> httpx.Response:
            await asyncio.sleep(0.1)
            return httpx.Response(503)

        client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        provider = OpenAICompatibleCapabilityEmbeddingProvider(
            endpoint="https://embedding.example/v1/embeddings",
            model="multilingual-v1",
            dimensions=1,
            retry_attempts=3,
            client=client,
        )
        started = time.monotonic()
        with pytest.raises(RuntimeError, match="exceeded its deadline"):
            await provider.embed(("controlled summary",), timeout_seconds=0.02)
        assert time.monotonic() - started < 0.08
        await client.aclose()

    asyncio.run(scenario())
