from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator

import httpx

from auraclaw.composition.local_ingress import (
    create_local_ingress_app,
    loopback_connect_host,
    select_upstream_base_url,
)


def test_stream_paths_go_to_streaming_gateway() -> None:
    assert (
        select_upstream_base_url(
            "/v1/streams/ses_1",
            task_api_base_url="http://127.0.0.1:8000",
            streaming_base_url="http://127.0.0.1:8010",
        )
        == "http://127.0.0.1:8010"
    )
    assert (
        select_upstream_base_url(
            "/v1/tasks",
            task_api_base_url="http://127.0.0.1:8000",
            streaming_base_url="http://127.0.0.1:8010",
        )
        == "http://127.0.0.1:8000"
    )
    assert (
        select_upstream_base_url(
            "/health/live",
            task_api_base_url="http://127.0.0.1:8000",
            streaming_base_url="http://127.0.0.1:8010",
        )
        == "http://127.0.0.1:8000"
    )


def test_wildcard_bind_uses_loopback_for_upstream() -> None:
    assert loopback_connect_host("0.0.0.0") == "127.0.0.1"
    assert loopback_connect_host("127.0.0.1") == "127.0.0.1"


def test_ingress_splits_task_and_stream_and_forwards_sse_headers() -> None:
    seen: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen.append(request)
        if request.url.host == "stream.example":
            async def sse_body() -> AsyncIterator[bytes]:
                yield b"id: ses_1:1\nevent: ping\ndata: {}\n\n"

            return httpx.Response(
                200,
                headers={"content-type": "text/event-stream"},
                content=sse_body(),
            )
        return httpx.Response(202, json={"session_id": "ses_1"})

    app = create_local_ingress_app(
        task_api_base_url="http://task.example:8000",
        streaming_base_url="http://stream.example:8010",
        transport=httpx.MockTransport(handler),
    )

    async def scenario() -> None:
        transport = httpx.ASGITransport(app=app)
        try:
            async with httpx.AsyncClient(transport=transport, base_url="http://ingress") as client:
                created = await client.post(
                    "/v1/tasks",
                    json={"goal": "hello"},
                    headers={"X-Tenant-ID": "local", "Idempotency-Key": "create-1"},
                )
                assert created.status_code == 202
                assert created.json()["session_id"] == "ses_1"
                stream = await client.get(
                    "/v1/streams/ses_1",
                    headers={"Last-Event-ID": "ses_1:0", "X-Tenant-ID": "local"},
                )
                assert stream.status_code == 200
                assert "text/event-stream" in stream.headers["content-type"]
                assert stream.headers["x-accel-buffering"] == "no"
                assert b"event: ping" in stream.content
        finally:
            await app.state.http_client.aclose()

    asyncio.run(scenario())
    assert seen[0].url.host == "task.example"
    assert seen[0].url.path == "/v1/tasks"
    assert seen[0].headers["x-tenant-id"] == "local"
    assert seen[1].url.host == "stream.example"
    assert seen[1].url.path == "/v1/streams/ses_1"
    assert seen[1].headers["last-event-id"] == "ses_1:0"
