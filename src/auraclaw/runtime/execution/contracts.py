from __future__ import annotations

from collections.abc import Awaitable, Callable
from enum import StrEnum


class InjectionPoint(StrEnum):
    BEFORE_MODEL = "before_model"
    AFTER_MODEL = "after_model"
    BEFORE_TOOL = "before_tool"
    AFTER_TOOL = "after_tool"


class FinishReasonKind(StrEnum):
    STOP = "stop"
    TOOL_CALLS = "tool_calls"
    TRUNCATED = "truncated"
    REFUSED = "refused"
    UNKNOWN = "unknown"


FailureInjector = Callable[[InjectionPoint], Awaitable[None] | None]
