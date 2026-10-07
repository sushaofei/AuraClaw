from __future__ import annotations

import asyncio
import math
from collections.abc import Sequence

import httpx


class OpenAICompatibleCapabilityEmbeddingProvider:
    """Bounded OpenAI-compatible embedding adapter for controlled catalog summaries."""

    def __init__(
        self,
        *,
        endpoint: str,
        model: str,
        dimensions: int,
        api_key: str | None = None,
        timeout_seconds: float = 5.0,
        retry_attempts: int = 2,
        max_concurrent: int = 8,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        if not endpoint.startswith(("http://", "https://")):
            raise ValueError("Embedding endpoint must be an absolute HTTP URL")
        if not model.strip() or dimensions < 1 or timeout_seconds <= 0:
            raise ValueError("Embedding model configuration is invalid")
        if retry_attempts < 1 or retry_attempts > 3:
            raise ValueError("Embedding retry attempts must be between 1 and 3")
        if max_concurrent < 1:
            raise ValueError("Embedding concurrency must be positive")
        self._endpoint = endpoint
        self._model = model
        self.dimensions = dimensions
        self.model_version = f"{model}:dim-{dimensions}:l2"
        self._api_key = api_key
        self._timeout_seconds = timeout_seconds
        self._retry_attempts = retry_attempts
        self._semaphore = asyncio.Semaphore(max_concurrent)
        self._owned_client = client is None
        self._client = client or httpx.AsyncClient(
            timeout=timeout_seconds,
            follow_redirects=False,
            trust_env=False,
        )

    async def embed(
        self,
        texts: Sequence[str],
        *,
        timeout_seconds: float | None = None,
    ) -> tuple[tuple[float, ...], ...]:
        if not texts:
            return ()
        if len(texts) > 256:
            raise ValueError("Embedding batch exceeds 256 controlled summaries")
        if any(len(text) > 16_384 for text in texts):
            raise ValueError("Embedding input exceeds the controlled summary limit")
        effective_timeout = self._timeout_seconds if timeout_seconds is None else timeout_seconds
        if effective_timeout <= 0:
            raise ValueError("Embedding timeout must be positive")
        acquired = False
        try:
            async with asyncio.timeout(effective_timeout):
                await self._semaphore.acquire()
                acquired = True
                headers = {"Content-Type": "application/json"}
                if self._api_key:
                    headers["Authorization"] = f"Bearer {self._api_key}"
                failure: Exception | None = None
                for attempt in range(self._retry_attempts):
                    try:
                        response = await self._client.post(
                            self._endpoint,
                            headers=headers,
                            json={"model": self._model, "input": list(texts)},
                            timeout=effective_timeout,
                        )
                        if (
                            response.status_code in {429, 502, 503, 504}
                            and attempt + 1 < self._retry_attempts
                        ):
                            await asyncio.sleep(0.1 * (attempt + 1))
                            continue
                        response.raise_for_status()
                        payload = response.json()
                        data = payload.get("data") if isinstance(payload, dict) else None
                        if not isinstance(data, list) or len(data) != len(texts):
                            raise ValueError("Embedding response count does not match input")
                        ordered: list[tuple[float, ...] | None] = [None] * len(texts)
                        for position, item in enumerate(data):
                            if not isinstance(item, dict) or not isinstance(
                                item.get("embedding"), list
                            ):
                                raise ValueError("Embedding response item is invalid")
                            index = int(item.get("index", position))
                            if index < 0 or index >= len(ordered) or ordered[index] is not None:
                                raise ValueError("Embedding response index is invalid")
                            vector = tuple(float(value) for value in item["embedding"])
                            if len(vector) != self.dimensions or not all(
                                math.isfinite(value) for value in vector
                            ):
                                raise ValueError("Embedding response vector is invalid")
                            ordered[index] = vector
                        if any(item is None for item in ordered):
                            raise ValueError("Embedding response omitted an input")
                        return tuple(item for item in ordered if item is not None)
                    except asyncio.CancelledError:
                        raise
                    except (httpx.HTTPError, ValueError, TypeError) as exc:
                        failure = exc
                        if attempt + 1 < self._retry_attempts and isinstance(
                            exc, (httpx.ConnectError, httpx.ReadError, httpx.TimeoutException)
                        ):
                            await asyncio.sleep(0.1 * (attempt + 1))
                            continue
                        break
                raise RuntimeError("Capability embedding service is unavailable") from failure
        except TimeoutError as exc:
            raise RuntimeError("Capability embedding request exceeded its deadline") from exc
        finally:
            if acquired:
                self._semaphore.release()

    async def close(self) -> None:
        if self._owned_client:
            await self._client.aclose()
