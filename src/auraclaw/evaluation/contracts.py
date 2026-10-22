from __future__ import annotations

import math
from collections.abc import Awaitable
from dataclasses import dataclass
from typing import Any, Protocol

from auraclaw.evaluation.components.catalog import COMPONENT_BENCHMARKS

UPSTREAMS = {
    "tau-bench": ("sierra-research/tau-bench", "59a200c6d575d595120f1cb70fea53cef0632f6b"),
    "appworld": ("StonyBrookNLP/appworld", "42b5bcf3cd334fee33f0c37c02070a9f5807add5"),
}


@dataclass(frozen=True)
class EvaluationConfig:
    benchmark: str
    split: str
    model: str
    harness_revision: str
    dataset_revision: str
    max_actions: int = 30
    max_output_tokens: int = 32768
    per_turn_output_tokens: int = 4096
    timeout_seconds: float = 600
    seed: int = 10
    user_model: str | None = None
    user_provider: str | None = None
    domain: str | None = None

    def __post_init__(self) -> None:
        if self.benchmark not in UPSTREAMS and self.benchmark not in COMPONENT_BENCHMARKS:
            raise ValueError("Unsupported benchmark")
        component_splits = {
            "longmemeval": {"s_cleaned"},
            "skillsbench": {"registry-v1.1"},
            "mcp-universe": {"mcpuniverse"},
        }
        splits = component_splits.get(self.benchmark) or (
            {"train", "dev", "test"}
            if self.benchmark == "tau-bench"
            else {
                "train",
                "dev",
                "test_normal",
                "test_challenge",
            }
        )
        if self.split not in splits or not all(
            (self.model, self.harness_revision, self.dataset_revision)
        ):
            raise ValueError("Invalid evaluation provenance or split")
        integers = (
            self.max_actions,
            self.max_output_tokens,
            self.per_turn_output_tokens,
            self.seed,
        )
        if any(not isinstance(value, int) or isinstance(value, bool) for value in integers):
            raise ValueError("Evaluation limits and seed must be integers")
        if min(self.max_actions, self.max_output_tokens, self.per_turn_output_tokens) < 1:
            raise ValueError("Evaluation budgets must be positive")
        if not math.isfinite(self.timeout_seconds) or self.timeout_seconds <= 0:
            raise ValueError("Evaluation timeout must be finite and positive")
        if self.benchmark == "tau-bench" and (
            self.domain not in {"retail", "airline"}
            or not self.user_model
            or not self.user_provider
        ):
            raise ValueError("tau-bench requires domain and user simulator model/provider")


class BenchmarkEnvironment(Protocol):
    instructions: str
    observation: str
    tools: tuple[dict[str, Any], ...]
    admitted_tools: tuple[dict[str, Any], ...]
    done: bool

    def invoke(self, name: str, arguments: dict[str, Any]) -> str | Awaitable[str]: ...

    def evaluate(self) -> float | Awaitable[float]: ...

    def close(self) -> None | Awaitable[None]: ...
