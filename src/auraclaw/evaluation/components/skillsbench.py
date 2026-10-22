"""SkillsBench execution boundary; official sandbox/verifier stay outside the agent."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from pathlib import PurePosixPath
from typing import Any, Protocol

from auraclaw.evaluation.environments import function_tool


class SkillPort(Protocol):
    async def list_skills(self) -> list[dict[str, str]]: ...

    async def read(self, name: str, path: str) -> str: ...


class SandboxPort(Protocol):
    async def execute(self, command: str) -> str: ...

    async def close(self) -> None: ...


class SkillsBenchEnvironment:
    """Two fresh equivalent sandboxes, differing only in admitted Skill access.

    Composition must mount only task inputs/environment and conditionally skills, never
    oracle/verifier. A shell with hidden Skill files still present is not a valid control.
    """

    def __init__(
        self,
        instruction: str,
        *,
        sandbox: SandboxPort,
        skills: SkillPort | None,
        verify: Callable[[], Awaitable[float]],
        oracle_verified: bool,
    ) -> None:
        if not oracle_verified:
            raise ValueError("Official oracle must validate the sandbox before agent execution")
        self._sandbox, self._skills, self._verify = sandbox, skills, verify
        self.done = False
        self.observation = instruction
        self.instructions = (
            "Complete the task in the isolated task workspace using sandbox_execute. "
            "When finished call finish_task. Skill instructions are available only through "
            "the admitted Skill tools. Never access verifier or oracle files."
        )
        tools = [
            function_tool(
                "sandbox_execute",
                "Execute in the isolated task workspace.",
                {"command": {"type": "string"}},
            ),
            function_tool("finish_task", "Finish task execution.", {}),
        ]
        if skills is not None:
            tools.extend(
                [
                    function_tool("skill_list", "List admitted Skill metadata.", {}),
                    function_tool(
                        "skill_read",
                        "Read an admitted Skill part.",
                        {"name": {"type": "string"}, "path": {"type": "string"}},
                    ),
                ]
            )
        self.tools = tuple(tools)
        self.admitted_tools = self.tools

    async def invoke(self, name: str, arguments: dict[str, Any]) -> str:
        if self.done:
            raise RuntimeError("Skill episode is complete")
        if name == "sandbox_execute":
            return await self._sandbox.execute(arguments["command"])
        if name == "finish_task":
            self.done = True
            return "Execution finished."
        if self._skills is not None:
            if name == "skill_list":
                import json

                # Metadata only: runtime discovery must not eagerly load skill bodies.
                return json.dumps(
                    [
                        {
                            key: item[key]
                            for key in ("name", "description", "version")
                            if key in item
                        }
                        for item in await self._skills.list_skills()
                    ]
                )
            if name == "skill_read":
                path = arguments["path"]
                parsed = PurePosixPath(path)
                if not path or parsed.is_absolute() or ".." in parsed.parts or "\\" in path:
                    raise ValueError("Skill part must be a relative path")
                admitted = {item["name"] for item in await self._skills.list_skills()}
                if arguments["name"] not in admitted:
                    raise ValueError("Skill was not admitted")
                return await self._skills.read(arguments["name"], path)
        raise ValueError("Unknown Skill action")

    async def evaluate(self) -> float:
        return await self._verify()

    async def close(self) -> None:
        await self._sandbox.close()
