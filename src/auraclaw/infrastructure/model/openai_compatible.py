from __future__ import annotations

import asyncio
import json
import logging
import time
from collections.abc import AsyncIterator
from typing import Any
from urllib.parse import urlparse

import httpx

from auraclaw.contracts.errors import (
    ModelAuthenticationError,
    ModelProviderError,
    ModelRateLimitError,
    ModelTimeoutError,
)
from auraclaw.runtime.ports import (
    ModelRequest,
    ModelResponse,
    ModelStreamChunk,
    ProviderCancellationResult,
    ToolCall,
)

logger = logging.getLogger(__name__)


class OpenAICompatibleProvider:
    """Streaming Chat Completions adapter for provider-neutral OpenAI-compatible APIs."""

    def __init__(
        self,
        *,
        base_url: str,
        model: str,
        name: str = "openai_compatible",
        timeout_seconds: float = 120.0,
        thinking_enabled: bool | None = None,
        prompt_cache_key_enabled: bool = False,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        self.name = name
        self._model = model
        self._endpoint = self._chat_completions_endpoint(base_url)
        self._timeout = timeout_seconds
        self._thinking_enabled = thinking_enabled
        self._prompt_cache_key_enabled = prompt_cache_key_enabled
        self._client = client
        self._owns_client = client is None
        self._active_tasks: dict[str, asyncio.Task[object]] = {}

    async def aclose(self) -> None:
        if self._client is not None and self._owns_client:
            await self._client.aclose()
            self._client = None

    async def prewarm(self, *, credential: str | None = None) -> None:
        """Warm DNS/TCP/TLS (and optionally a 1-token completion) before first user call."""
        client = self._ensure_client()
        origin = self._provider_origin()
        try:
            await client.get(origin, timeout=5.0)
        except httpx.HTTPError:
            # Origin may 404; the connection pool is still warmed.
            pass
        if not credential:
            return
        probe = ModelRequest(
            model_call_id="mdl_prewarm",
            tenant_id="system",
            run_id="prewarm",
            messages=({"role": "user", "content": "ping"},),
            max_output_tokens=1,
        )
        try:
            async for _chunk in self.generate_stream(probe, credential=credential):
                pass
        except Exception as exc:
            logger.warning(
                "model provider probe prewarm failed provider=%s error=%s",
                self.name,
                type(exc).__name__,
            )

    def _ensure_client(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = httpx.AsyncClient(timeout=self._timeout)
            self._owns_client = True
        return self._client

    def _provider_origin(self) -> str:
        parsed = urlparse(self._endpoint)
        if not parsed.scheme or not parsed.netloc:
            return self._endpoint
        return f"{parsed.scheme}://{parsed.netloc}"

    async def generate(self, request: ModelRequest, *, credential: str) -> ModelResponse:
        response: ModelResponse | None = None
        async for chunk in self.generate_stream(request, credential=credential):
            if chunk.kind == "completed":
                response = chunk.response
        if response is None:
            raise ModelProviderError("model provider stream ended without a completed response")
        return response

    async def generate_stream(
        self, request: ModelRequest, *, credential: str
    ) -> AsyncIterator[ModelStreamChunk]:
        task = asyncio.current_task()
        if task is not None:
            self._active_tasks[request.model_call_id] = task
        try:
            async for chunk in self._generate_stream(request, credential=credential):
                yield chunk
        finally:
            if self._active_tasks.get(request.model_call_id) is task:
                self._active_tasks.pop(request.model_call_id, None)

    async def cancel(self, model_call_id: str) -> ProviderCancellationResult:
        task = self._active_tasks.get(model_call_id)
        if task is None or task.done():
            return ProviderCancellationResult(stopped=False)
        task.cancel()
        # Chat Completions does not provide authoritative partial usage when an
        # HTTP stream is interrupted. The caller must reconcile accounting.
        return ProviderCancellationResult(stopped=True, usage_final=False)

    async def _generate_stream(
        self, request: ModelRequest, *, credential: str
    ) -> AsyncIterator[ModelStreamChunk]:
        model = request.policy.preferred_model or self._model
        payload: dict[str, Any] = {
            "model": model,
            "messages": list(request.messages),
            "max_tokens": request.max_output_tokens,
            "stream": True,
            "stream_options": {"include_usage": True},
        }
        if request.tools:
            payload["tools"] = list(request.tools)
        if self._thinking_enabled is not None:
            payload["thinking"] = {
                "type": "enabled" if self._thinking_enabled else "disabled",
            }
        if self._prompt_cache_key_enabled and request.prompt_cache_key is not None:
            payload["prompt_cache_key"] = request.prompt_cache_key

        started = time.perf_counter()
        first_delta_logged = False
        deltas: list[str] = []
        usage: dict[str, int | float] = {}
        finish_reason = "stop"
        tool_fragments: dict[int, dict[str, str]] = {}
        client = self._ensure_client()
        try:
            async with client.stream(
                "POST",
                self._endpoint,
                headers={
                    "Authorization": f"Bearer {credential}",
                    "Content-Type": "application/json",
                },
                json=payload,
            ) as response:
                if response.is_error:
                    await response.aread()
                self._raise_for_status(response)
                async for data in self._stream_data(response):
                    provider_usage = data.get("usage")
                    if isinstance(provider_usage, dict):
                        usage = self._normalize_usage(provider_usage)
                    choices = data.get("choices", [])
                    if not isinstance(choices, list):
                        continue
                    for choice in choices:
                        if not isinstance(choice, dict):
                            continue
                        reason = choice.get("finish_reason")
                        if reason:
                            finish_reason = str(reason)
                        delta = choice.get("delta", {})
                        if not isinstance(delta, dict):
                            continue
                        content = delta.get("content")
                        if content is not None:
                            text = str(content)
                            deltas.append(text)
                            if not first_delta_logged:
                                first_delta_logged = True
                                logger.info(
                                    "provider_ttft_ms=%.2f provider=%s model=%s model_call=%s",
                                    (time.perf_counter() - started) * 1_000,
                                    self.name,
                                    model,
                                    request.model_call_id,
                                )
                            yield ModelStreamChunk(kind="delta", delta=text)
                        self._merge_tool_calls(tool_fragments, delta.get("tool_calls"))
        except httpx.TimeoutException as exc:
            raise ModelTimeoutError("model provider request timed out") from exc
        except httpx.HTTPError as exc:
            raise ModelProviderError("model provider request failed") from exc

        duration_ms = (time.perf_counter() - started) * 1_000
        logger.info(
            "model provider call completed provider=%s model=%s thinking=%s "
            "duration_ms=%.2f usage=%s",
            self.name,
            model,
            self._thinking_enabled,
            duration_ms,
            usage,
        )
        yield ModelStreamChunk(
            kind="completed",
            response=ModelResponse(
                model_call_id=request.model_call_id,
                provider=self.name,
                model=model,
                completed_output="".join(deltas),
                deltas=tuple(deltas),
                tool_calls=self._tool_calls(request, tool_fragments),
                finish_reason=finish_reason,
                usage=usage,
            ),
        )

    @staticmethod
    def _chat_completions_endpoint(base_url: str) -> str:
        endpoint = base_url.rstrip("/")
        if endpoint.endswith("/chat/completions"):
            return endpoint
        return f"{endpoint}/chat/completions"

    @staticmethod
    def _raise_for_status(response: httpx.Response) -> None:
        if response.status_code in {401, 403}:
            raise ModelAuthenticationError("model provider rejected the configured credential")
        if response.status_code == 429:
            raise ModelRateLimitError("model provider rate limit or quota was exhausted")
        if response.is_error:
            detail = OpenAICompatibleProvider._error_detail(response)
            raise ModelProviderError(f"model provider returned HTTP {response.status_code}{detail}")

    @staticmethod
    def _error_detail(response: httpx.Response) -> str:
        try:
            raw = (response.content or b"").decode("utf-8", errors="replace").strip()
        except Exception:
            return ""
        if not raw:
            return ""
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            return f": {raw[:300]}"
        error = payload.get("error") if isinstance(payload, dict) else None
        if isinstance(error, dict):
            message = error.get("message") or error.get("message_zh") or error
            code = error.get("code")
            if code:
                return f": [{code}] {message}"
            return f": {message}"
        if isinstance(error, str):
            return f": {error}"
        return f": {raw[:300]}"

    @staticmethod
    async def _stream_data(response: httpx.Response) -> AsyncIterator[dict[str, Any]]:
        async for line in response.aiter_lines():
            if not line.startswith("data:"):
                continue
            value = line[5:].strip()
            if not value or value == "[DONE]":
                continue
            try:
                decoded = json.loads(value)
            except json.JSONDecodeError as exc:
                raise ModelProviderError("model provider returned invalid stream JSON") from exc
            if isinstance(decoded, dict):
                yield decoded

    @staticmethod
    def _normalize_usage(usage: dict[str, Any]) -> dict[str, int | float]:
        normalized: dict[str, int | float] = {}
        aliases = {
            "prompt_tokens": "input_tokens",
            "input_tokens": "input_tokens",
            "completion_tokens": "output_tokens",
            "output_tokens": "output_tokens",
            "total_tokens": "total_tokens",
        }
        for key, target in aliases.items():
            value = usage.get(key)
            if isinstance(value, (int, float)) and not isinstance(value, bool):
                normalized[target] = value
        prompt_details = usage.get("prompt_tokens_details")
        if not isinstance(prompt_details, dict):
            prompt_details = usage.get("input_tokens_details")
        if isinstance(prompt_details, dict):
            detail_aliases = {
                "cached_tokens": "cached_input_tokens",
                "cache_write_tokens": "cache_write_input_tokens",
            }
            for key, target in detail_aliases.items():
                value = prompt_details.get(key)
                if isinstance(value, (int, float)) and not isinstance(value, bool):
                    normalized[target] = value
        return normalized

    @staticmethod
    def _merge_tool_calls(fragments: dict[int, dict[str, str]], raw_calls: Any) -> None:
        if not isinstance(raw_calls, list):
            return
        for raw in raw_calls:
            if not isinstance(raw, dict):
                continue
            index = int(raw.get("index", 0))
            target = fragments.setdefault(index, {"id": "", "name": "", "arguments": ""})
            if raw.get("id"):
                target["id"] = str(raw["id"])
            function = raw.get("function", {})
            if isinstance(function, dict):
                if function.get("name"):
                    target["name"] += str(function["name"])
                if function.get("arguments"):
                    target["arguments"] += str(function["arguments"])

    @staticmethod
    def _tool_calls(
        request: ModelRequest, fragments: dict[int, dict[str, str]]
    ) -> tuple[ToolCall, ...]:
        calls: list[ToolCall] = []
        for index, fragment in sorted(fragments.items()):
            raw_arguments = fragment["arguments"]
            if not raw_arguments.strip():
                raise ModelProviderError(
                    "model provider omitted tool arguments; expected a JSON object"
                )
            try:
                arguments = json.loads(raw_arguments)
            except json.JSONDecodeError as exc:
                raise ModelProviderError("model provider returned invalid tool arguments") from exc
            if not isinstance(arguments, dict):
                raise ModelProviderError("model provider tool arguments must be an object")
            calls.append(
                ToolCall(
                    tool_invocation_id=fragment["id"] or f"tool_{request.run_id}_{index}",
                    name=fragment["name"],
                    arguments=arguments,
                )
            )
        return tuple(calls)
