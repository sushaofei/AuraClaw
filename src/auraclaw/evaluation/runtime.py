from __future__ import annotations

import copy
import hashlib
import inspect
import json
import math
from dataclasses import replace
from typing import Any

from auraclaw.contracts.errors import BudgetExceededError, SchemaValidationError
from auraclaw.contracts.json_schema import JsonSchemaValidator
from auraclaw.control.ports import RuntimeAssignment
from auraclaw.evaluation.contracts import BenchmarkEnvironment, EvaluationConfig
from auraclaw.runtime.capabilities.contracts import CAPABILITY_LOAD
from auraclaw.runtime.capability_controller import RuntimeCapabilityController
from auraclaw.runtime.ports import (
    ModelClient,
    ModelRequest,
    ModelResponse,
    SkillResolutionOutcome,
    ToolCall,
)


class ModelAdmissionRejected(RuntimeError):
    """An external model port rejected admission, not an episode action/token budget."""


class ActionBudgetExceeded(RuntimeError):
    pass


class EnvironmentTools:
    """Task-local capability authority and invocation ledger over the official environment."""

    def __init__(self, env: BenchmarkEnvironment, config: EvaluationConfig) -> None:
        self.env = env
        self.config = config
        self.calls = 0
        self._results: dict[str, tuple[str, dict[str, Any]]] = {}
        self.descriptors: dict[str, dict[str, Any]] = {}
        for tool in env.admitted_tools:
            name = str(tool["function"]["name"])
            if name.startswith("auraclaw.") or name in self.descriptors:
                raise ValueError("Benchmark tool name collision")
            self.descriptors[name] = {
                "capability_id": name,
                "canonical_name": name,
                "kind": "tool",
                "version": "1",
                "permission": "write",
                "model_tool": copy.deepcopy(tool),
            }

    async def execute(self, assignment: RuntimeAssignment, call: ToolCall) -> dict[str, Any]:
        del assignment
        if call.name == CAPABILITY_LOAD:
            return {
                "capabilities": [
                    copy.deepcopy(self.descriptors[name])
                    for name in call.arguments["capability_ids"]
                    if name in self.descriptors
                ]
            }
        descriptor = self.descriptors.get(call.name)
        if descriptor is None:
            return {
                "status": "denied",
                "error_code": "capability_not_loaded",
                "side_effect_status": "not_started",
            }
        fingerprint = json.dumps([call.name, call.arguments], sort_keys=True)
        prior = self._results.get(call.tool_invocation_id)
        if prior is not None:
            if prior[0] != fingerprint:
                raise ValueError("Invocation identity reused with different arguments")
            return copy.deepcopy(prior[1])
        if self.calls >= self.config.max_actions:
            raise ActionBudgetExceeded("Benchmark action budget exhausted")
        if self.env.done:
            raise RuntimeError("Cannot execute an action after environment termination")
        try:
            JsonSchemaValidator.validate(
                call.arguments, descriptor["model_tool"]["function"]["parameters"]
            )
        except SchemaValidationError:
            return {
                "status": "error",
                "error_code": "tool_schema_invalid",
                "side_effect_status": "not_started",
            }
        self.calls += 1
        # Deliberately synchronous: official environments can patch process state / clocks.
        # Each trial runs in a dedicated process; no background invocation can outlive close().
        observation = self.env.invoke(call.name, copy.deepcopy(call.arguments))
        if inspect.isawaitable(observation):
            observation = await observation
        result = {
            "status": "success",
            "content": {"observation": observation},
            "side_effect_status": "completed",
        }
        self._results[call.tool_invocation_id] = fingerprint, copy.deepcopy(result)
        return result

    async def list_tools(self, assignment: RuntimeAssignment) -> list[dict[str, Any]]:
        return copy.deepcopy(list(self.env.tools))

    async def list_resources(self, assignment: RuntimeAssignment) -> list[dict[str, Any]]:
        return []

    async def list_resource_templates(self, assignment: RuntimeAssignment) -> list[dict[str, Any]]:
        return []

    async def read_resource(self, assignment: RuntimeAssignment, uri: str) -> list[dict[str, Any]]:
        raise NotImplementedError("Benchmark profile has no resource gateway")

    async def list_prompts(self, assignment: RuntimeAssignment) -> list[dict[str, Any]]:
        return []

    async def get_prompt(
        self, assignment: RuntimeAssignment, name: str, *, arguments: dict[str, str] | None = None
    ) -> dict[str, Any]:
        raise NotImplementedError("Benchmark profile has no prompt gateway")

    async def load_skill_manifest(
        self, assignment: RuntimeAssignment, *, publisher: str, name: str, version: str
    ) -> dict[str, Any]:
        raise NotImplementedError("Benchmark profile has no Skill gateway")

    async def load_skill_part(
        self, assignment: RuntimeAssignment, *, publisher: str, name: str, version: str, path: str
    ) -> list[dict[str, Any]]:
        raise NotImplementedError("Benchmark profile has no Skill gateway")

    async def resolve_skill(
        self,
        assignment: RuntimeAssignment,
        *,
        name: str,
        version: str = "*",
        publisher: str | None = None,
        active_skill_names: tuple[str, ...] = (),
    ) -> SkillResolutionOutcome:
        raise NotImplementedError("Benchmark profile has no Skill gateway")


class FixedToolsController(RuntimeCapabilityController):
    """Benchmark profile: pre-admit fixed tools, retaining actual capability execution checks."""

    def __init__(self, tools: EnvironmentTools) -> None:
        super().__init__(
            tools, max_loaded=max(1, len(tools.descriptors)), max_schema_bytes=2 * 1024 * 1024
        )
        self._fixed = copy.deepcopy(tools.env.tools)

    def model_tools(self, state: dict[str, Any]) -> tuple[dict[str, Any], ...]:
        loaded_names = {item["canonical_name"] for item in state.get("loaded", {}).values()}
        return tuple(
            copy.deepcopy(tool) for tool in self._fixed if tool["function"]["name"] in loaded_names
        )


class BenchmarkModel:
    """Map canonical runtime observations to upstream dialog semantics, without an agent loop."""

    def __init__(self, delegate: ModelClient, tools: EnvironmentTools) -> None:
        self._delegate = delegate
        self._tools = tools
        self.calls = 0
        self.usage: dict[str, int | float] = {}
        self.cost_known = True

    async def generate(self, request: ModelRequest) -> ModelResponse:
        if self._tools.env.done:
            # An environment terminal is not another paid model call or an official success.
            return ModelResponse(
                request.model_call_id,
                "benchmark-control",
                "terminal",
                "",
                usage={"output_tokens": 0},
            )
        if self._tools.calls >= self._tools.config.max_actions:
            raise ActionBudgetExceeded("Benchmark action budget exhausted")
        messages = [{"role": "system", "content": self._tools.env.instructions}]
        responses: set[str] = set()
        for raw in request.messages:
            message = copy.deepcopy(raw)
            if message.get("role") == "assistant":
                calls = message.get("tool_calls", [])
                if calls and calls[0]["function"]["name"] == "respond":
                    responses.add(calls[0]["id"])
                    message = {
                        "role": "assistant",
                        "content": json.loads(calls[0]["function"]["arguments"])["content"],
                    }
            elif message.get("role") == "tool":
                result = json.loads(message["content"])
                content = result.get("content", {})
                observation = content.get("observation")
                if observation is not None:
                    message["content"] = observation
                if message.get("tool_call_id") in responses:
                    message = {"role": "user", "content": message["content"]}
            messages.append(message)
        self.calls += 1
        try:
            response = await self._delegate.generate(replace(request, messages=tuple(messages)))
        except BudgetExceededError as error:
            self.cost_known = False
            raise ModelAdmissionRejected("Model Gateway budget admission rejected") from error
        except BaseException:
            self.cost_known = False
            raise
        tokens = response.usage.get("output_tokens")
        if not isinstance(tokens, int) or isinstance(tokens, bool) or tokens < 0:
            raise RuntimeError("Model output usage is unavailable")
        if any(
            isinstance(value, bool)
            or not isinstance(value, (int, float))
            or not math.isfinite(value)
            or value < 0
            for value in response.usage.values()
        ):
            raise RuntimeError("Invalid model usage")
        self.cost_known = self.cost_known and "cost" in response.usage
        for key, value in response.usage.items():
            self.usage[key] = self.usage.get(key, 0) + value
        if self._tools.config.benchmark == "tau-bench" and not response.tool_calls:
            call = ToolCall(
                "respond_" + hashlib.sha256(request.model_call_id.encode()).hexdigest(),
                "respond",
                {"content": response.completed_output},
            )
            return replace(response, tool_calls=(call,), finish_reason="tool_calls")
        if (
            self._tools.config.benchmark in {"longmemeval", "mcp-universe", "skillsbench"}
            and not response.tool_calls
            and response.finish_reason == "stop"
            and response.completed_output.strip()
        ):
            # These upstreams consume the final assistant response. Represent submission
            # as a canonical local action, without an extra model call or hidden answer.
            skill = self._tools.config.benchmark == "skillsbench"
            call = ToolCall(
                "submit_" + hashlib.sha256(request.model_call_id.encode()).hexdigest(),
                "finish_task" if skill else "submit_answer",
                {} if skill else {"answer": response.completed_output},
            )
            return replace(response, tool_calls=(call,), finish_reason="tool_calls")
        # Match upstream tau's single-action protocol; AppWorld uses the same serialized profile.
        if response.tool_calls:
            return replace(response, tool_calls=response.tool_calls[:1])
        return response
