"""MCP-Universe task bridge. No hidden evaluator data is included in tool descriptors."""

from __future__ import annotations

import copy
import hashlib
from collections.abc import Awaitable, Callable
from typing import Any, Protocol

from auraclaw.evaluation.environments import function_tool


class McpPort(Protocol):
    async def call(self, server: str, tool: str, arguments: dict[str, Any]) -> str: ...

    async def close(self) -> None: ...


class McpUniverseEnvironment:
    def __init__(
        self,
        instruction: str,
        *,
        tools: list[dict[str, Any]],
        gateway: McpPort,
        grade: Callable[[str], Awaitable[float]],
    ) -> None:
        self._gateway, self._grade = gateway, grade
        self._routes: dict[str, tuple[str, str]] = {}
        self._answer: str | None = None
        self.done = False
        self.instructions = (
            "Solve the task using the admitted MCP tools. "
            "Submit your final response using submit_answer."
        )
        self.observation = instruction
        descriptors = []
        for tool in tools:
            server, name = tool["server"], tool["name"]
            identity = (server, name)
            if identity in self._routes.values():
                raise ValueError("Duplicate MCP tool")
            alias = "mcp_" + hashlib.sha256(f"{server}\0{name}".encode()).hexdigest()[:24]
            if alias in self._routes:
                raise ValueError("MCP alias collision")
            self._routes[alias] = identity
            descriptors.append(
                {
                    "type": "function",
                    "function": {
                        "name": alias,
                        "description": f"{server}/{name}: {tool['description']}",
                        "parameters": copy.deepcopy(tool["inputSchema"]),
                    },
                }
            )
        if not descriptors:
            raise ValueError("MCP task has no admitted tools")
        descriptors.append(
            function_tool(
                "submit_answer", "Submit the final response.", {"answer": {"type": "string"}}
            )
        )
        self.tools = tuple(descriptors)
        self.admitted_tools = self.tools

    async def invoke(self, name: str, arguments: dict[str, Any]) -> str:
        if self.done:
            raise RuntimeError("MCP episode is complete")
        if name == "submit_answer":
            self._answer, self.done = str(arguments["answer"]), True
            return "Answer recorded."
        if name not in self._routes:
            raise ValueError("MCP tool was not admitted")
        server, tool = self._routes[name]
        return await self._gateway.call(server, tool, copy.deepcopy(arguments))

    async def evaluate(self) -> float:
        return await self._grade(self._answer or "")

    async def close(self) -> None:
        await self._gateway.close()
