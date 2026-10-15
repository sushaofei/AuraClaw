"""Failing model adapter used when no model provider is configured."""

from __future__ import annotations

from typing import Any


class UnavailableModelClient:
    async def generate(self, request: Any) -> Any:
        del request
        raise RuntimeError("model provider is not configured")

    async def aclose(self) -> None:
        return None
