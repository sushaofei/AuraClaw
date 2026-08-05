"""Helpers for consuming incremental model output without breaking sync clients."""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any

from auraclaw.runtime.ports import ModelRequest, ModelResponse, ModelStreamChunk


async def iter_model_stream(
    client: Any, request: ModelRequest
) -> AsyncIterator[ModelStreamChunk]:
    """Yield deltas as they arrive when the client supports streaming.

    Clients without ``generate_stream`` fall back to ``generate()`` and emit
    buffered deltas before the completed chunk so Harness can share one path.
    """
    stream = getattr(client, "generate_stream", None)
    if callable(stream):
        async for chunk in stream(request):
            yield chunk
        return
    response = await client.generate(request)
    for delta in response.deltas:
        yield ModelStreamChunk(kind="delta", delta=str(delta))
    yield ModelStreamChunk(kind="completed", response=response)


async def collect_model_response(
    client: Any, request: ModelRequest
) -> ModelResponse:
    """Consume a model stream and return the final ModelResponse."""
    response: ModelResponse | None = None
    async for chunk in iter_model_stream(client, request):
        if chunk.kind == "completed":
            response = chunk.response
    if response is None:
        raise RuntimeError("model stream ended without a completed response")
    return response
