"""Frozen upstream sources, kept separate from the legacy two-benchmark runner."""

from __future__ import annotations

from dataclasses import dataclass

LONGMEMEVAL_DATA_REVISION = "98d7416c24c778c2fee6e6f3006e7a073259d48f"
LONGMEMEVAL_DATA_SHA256 = "d6f21ea9d60a0d56f34a05b609c79c88a451d2ae03597821ea3d5a9678c3a442"


@dataclass(frozen=True)
class ComponentBenchmark:
    repository: str
    revision: str
    documentation: str
    conditions: tuple[str, ...]


COMPONENT_BENCHMARKS = {
    "longmemeval": ComponentBenchmark(
        "xiaowu0162/LongMemEval",
        "9e0b455f4ef0e2ab8f2e582289761153549043fc",
        "https://xiaowu0162.github.io/long-mem-eval/",
        ("no-memory", "retrieval-memory", "full-history"),
    ),
    "skillsbench": ComponentBenchmark(
        "benchflow-ai/skillsbench",
        "f860ecf02ef53412a030f8661c83d644b739ff01",
        "https://www.skillsbench.ai/",
        ("without-skill", "with-skill"),
    ),
    "mcp-universe": ComponentBenchmark(
        "SalesforceAIResearch/MCP-Universe",
        "0df6c42717f2a618d34f5b271efd05be462b9e4d",
        "https://mcp-universe.github.io/",
        ("gateway-tools",),
    ),
}
