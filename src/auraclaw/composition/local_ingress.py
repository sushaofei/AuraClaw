from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from urllib.parse import urlsplit

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, StreamingResponse

STREAM_PATH_PREFIX = "/v1/streams/"
_HOP_BY_HOP = {
    "connection",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailers",
    "transfer-encoding",
    "upgrade",
    "proxy-connection",
}


def select_upstream_base_url(
    path: str,
    *,
    task_api_base_url: str,
    streaming_base_url: str,
) -> str:
    """Match production Nginx: only `/v1/streams/` goes to Streaming Gateway."""
    if path.startswith(STREAM_PATH_PREFIX):
        return streaming_base_url.rstrip("/")
    return task_api_base_url.rstrip("/")


def loopback_connect_host(bind_host: str) -> str:
    if bind_host in {"0.0.0.0", "::", "[::]"}:
        return "127.0.0.1"
    return bind_host


def _filtered_request_headers(request: Request, upstream_host: str) -> list[tuple[str, str]]:
    headers: list[tuple[str, str]] = []
    for name, value in request.headers.items():
        lowered = name.lower()
        if lowered in _HOP_BY_HOP or lowered == "host":
            continue
        headers.append((name, value))
    headers.append(("Host", upstream_host))
    return headers


def _filtered_response_headers(response: httpx.Response, *, stream: bool) -> dict[str, str]:
    headers: dict[str, str] = {}
    for name, value in response.headers.items():
        if name.lower() in _HOP_BY_HOP:
            continue
        headers[name] = value
    if stream:
        headers.setdefault("Cache-Control", "no-cache")
        headers.setdefault("X-Accel-Buffering", "no")
    return headers


def create_local_ingress_app(
    *,
    task_api_base_url: str,
    streaming_base_url: str,
    transport: httpx.AsyncBaseTransport | None = None,
) -> FastAPI:
    """Reverse proxy with the same split as `deploy/nginx.conf`."""

    client = httpx.AsyncClient(
        transport=transport,
        timeout=httpx.Timeout(connect=5.0, read=3600.0, write=30.0, pool=5.0),
        follow_redirects=False,
        trust_env=False,
    )

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        app.state.http_client = client
        try:
            yield
        finally:
            await client.aclose()

    app = FastAPI(title="AuraClaw local ingress", lifespan=lifespan)
    app.state.http_client = client

    @app.api_route(
        "/{full_path:path}",
        methods=["GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"],
        response_model=None,
    )
    async def proxy(full_path: str, request: Request) -> StreamingResponse | JSONResponse:
        path = "/" + full_path if full_path else "/"
        base = select_upstream_base_url(
            path,
            task_api_base_url=task_api_base_url,
            streaming_base_url=streaming_base_url,
        )
        target = base + path
        if request.url.query:
            target = f"{target}?{request.url.query}"
        stream = path.startswith(STREAM_PATH_PREFIX)
        upstream_host = urlsplit(base).netloc
        headers = _filtered_request_headers(request, upstream_host)
        body = None if request.method in {"GET", "HEAD", "OPTIONS"} else request.stream()
        client: httpx.AsyncClient = request.app.state.http_client
        try:
            upstream_request = client.build_request(
                request.method,
                target,
                headers=headers,
                content=body,
            )
            upstream = await client.send(upstream_request, stream=True)
        except httpx.RequestError:
            return JSONResponse(
                status_code=502,
                content={
                    "code": "upstream_unavailable",
                    "message": "AuraClaw ingress could not reach upstream",
                },
            )

        response_headers = _filtered_response_headers(upstream, stream=stream)

        async def iterate() -> AsyncIterator[bytes]:
            try:
                try:
                    async for chunk in upstream.aiter_raw():
                        yield chunk
                except httpx.StreamConsumed:
                    data = await upstream.aread()
                    if data:
                        yield data
            finally:
                await upstream.aclose()

        return StreamingResponse(
            iterate(),
            status_code=upstream.status_code,
            headers=response_headers,
        )

    return app
