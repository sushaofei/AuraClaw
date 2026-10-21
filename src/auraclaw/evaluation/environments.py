from __future__ import annotations

import copy
import json
from collections.abc import Callable
from typing import Any


def function_tool(name: str, description: str, properties: dict[str, Any]) -> dict[str, Any]:
    return {
        "type": "function",
        "function": {
            "name": name,
            "description": description,
            "parameters": {
                "type": "object",
                "properties": properties,
                "required": list(properties),
                "additionalProperties": False,
            },
        },
    }


class TauEnvironment:
    """Expose observations only, never EnvInfo.task/actions/reward_info to the agent."""

    tools: tuple[dict[str, Any], ...]
    admitted_tools: tuple[dict[str, Any], ...]

    def __init__(self, env: Any, task_index: int, action_factory: Callable[..., Any]) -> None:
        reset = env.reset(task_index=task_index)
        self.instructions = str(env.wiki)
        self.observation = str(reset.observation)
        self.tools = tuple(copy.deepcopy(env.tools_info))
        self.admitted_tools = (
            *self.tools,
            function_tool(
                "respond",
                "Send an assistant message to the simulated user.",
                {"content": {"type": "string"}},
            ),
        )
        if any(tool["function"]["name"] == "respond" for tool in self.tools):
            raise ValueError("Reserved tau-bench respond action collision")
        self.done = False
        self._env = env
        self._action = action_factory
        self._reward = 0.0

    def invoke(self, name: str, arguments: dict[str, Any]) -> str:
        response = self._env.step(self._action(name=name, kwargs=arguments))
        self.done = bool(response.done)
        self._reward = float(response.reward)
        # In particular, do not stringify response.info: it contains hidden solutions.
        return str(response.observation)

    def evaluate(self) -> float:
        return self._reward

    def close(self) -> None:
        pass  # The official tau Env owns no close protocol.


class AppWorldEnvironment:
    """Use only the official execute API; grading is accessible to the runner alone."""

    tools: tuple[dict[str, Any], ...]
    admitted_tools: tuple[dict[str, Any], ...]

    def __init__(self, world: Any) -> None:
        self.instructions = (
            "Solve the task in the AppWorld Python shell using appworld_execute. "
            "Discover APIs with print(apis.api_docs.show_app_descriptions()), "
            "print(apis.api_docs.show_api_descriptions(app_name='APP_NAME')), then "
            "print(apis.api_docs.show_api_doc(app_name='APP_NAME', api_name='API_NAME')). "
            "Use keyword arguments for every API call and inspect its specification first. "
            "Variables persist between executions. Finish using apis.supervisor.complete_task. "
            "Do not access ground truth, evaluator internals, host files or external networks.\n"
            + json.dumps(
                {"supervisor": world.task.supervisor, "apps": world.task.app_descriptions},
                ensure_ascii=False,
            )
        )
        self.observation = str(world.task.instruction)
        self.tools = (
            function_tool(
                "appworld_execute",
                "Run Python code in the isolated official AppWorld shell.",
                {"code": {"type": "string"}},
            ),
        )
        self.admitted_tools = self.tools
        self.done = False
        self._world = world

    def invoke(self, name: str, arguments: dict[str, Any]) -> str:
        if name != "appworld_execute":
            raise ValueError("Unsupported AppWorld tool")
        observation = self._world.execute(arguments["code"])
        self.done = bool(self._world.task_completed())
        return str(observation)

    def evaluate(self) -> float:
        tracker = self._world.evaluate(suppress_errors=True)
        if not isinstance(tracker.num_tests, int) or tracker.num_tests < 1:
            raise ValueError("Official evaluator returned no tests")
        return float(tracker.success)

    def close(self) -> None:
        self._world.close()
