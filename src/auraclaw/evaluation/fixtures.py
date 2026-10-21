"""Synthetic protocol smoke only; contains no official benchmark tasks or answers."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

from auraclaw.evaluation.environments import AppWorldEnvironment, TauEnvironment, function_tool
from auraclaw.runtime.ports import ModelRequest, ModelResponse, ToolCall


class FixtureModel:
    def __init__(self) -> None:
        self.calls = 0

    async def generate(self, request: ModelRequest) -> ModelResponse:
        self.calls += 1
        if request.tools[0]["function"]["name"] == "appworld_execute":
            call = ToolCall(
                "fixture-1",
                "appworld_execute",
                {
                    "code": "apis.supervisor.complete_task()",
                },
            )
        elif self.calls == 1:
            call = ToolCall("fixture-1", "lookup", {"id": "synthetic"})
        else:
            return ModelResponse(
                request.model_call_id,
                "fixture",
                "scripted",
                "Finished.",
                usage={"output_tokens": 1},
            )
        return ModelResponse(
            request.model_call_id,
            "fixture",
            "scripted",
            "",
            tool_calls=(call,),
            finish_reason="tool_calls",
            usage={"output_tokens": 1},
        )


class FixtureTau:
    wiki = "Synthetic protocol fixture; look up the requested id and respond."
    tools_info = [
        function_tool("lookup", "Look up a synthetic record.", {"id": {"type": "string"}})
    ]

    def reset(self, *, task_index: int) -> Any:
        return SimpleNamespace(observation="Look up synthetic.", info={"hidden": "never expose"})

    def step(self, action: Any) -> Any:
        return SimpleNamespace(
            observation="done" if action.name == "respond" else "found",
            reward=float(action.name == "respond"),
            done=action.name == "respond",
            info={"hidden": "never expose"},
        )


class FixtureWorld:
    def __init__(self) -> None:
        self.task = SimpleNamespace(
            supervisor={"first_name": "Fixture"},
            app_descriptions={"supervisor": "Synthetic app"},
            instruction="Finish the synthetic task.",
        )
        self.completed = False
        self.closed = False

    def execute(self, code: str) -> str:
        self.completed = code == "apis.supervisor.complete_task()"
        return "completed" if self.completed else "not complete"

    def task_completed(self) -> bool:
        return self.completed

    def evaluate(self, **kwargs: Any) -> Any:
        return SimpleNamespace(success=self.completed, num_tests=1)

    def close(self) -> None:
        self.closed = True


def fixture_environment(benchmark: str) -> TauEnvironment | AppWorldEnvironment:
    if benchmark == "tau-bench":
        return TauEnvironment(FixtureTau(), 0, lambda **kwargs: SimpleNamespace(**kwargs))
    return AppWorldEnvironment(FixtureWorld())
