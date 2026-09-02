from __future__ import annotations

from auraclaw.runtime.execution_engine import (
    FailureInjector,
    FinishReasonKind,
    InjectionPoint,
    RuntimeExecutionEngine,
)


class AgentHarness(RuntimeExecutionEngine):
    """Stable Runtime facade; execution behavior lives in RuntimeExecutionEngine."""


__all__ = [
    "AgentHarness",
    "FailureInjector",
    "FinishReasonKind",
    "InjectionPoint",
]
