from __future__ import annotations

import asyncio
import os
import tempfile
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any

from auraclaw.contracts.errors import SandboxViolationError
from auraclaw.contracts.tools import ToolCapability, ToolInvocation

HandsHandler = Callable[[dict[str, Any]], Awaitable[Any] | Any]


class LocalHandsService:
    """Local Hands adapter with no shell, no inherited environment and confined file access."""

    def __init__(
        self,
        *,
        workspace_root: Path,
        allowed_executables: tuple[Path, ...] = (),
        handlers: dict[str, HandsHandler] | None = None,
    ) -> None:
        self._root = workspace_root.resolve()
        self._allowed_executables = {path.resolve() for path in allowed_executables}
        self._handlers = handlers or {}

    async def execute(
        self, invocation: ToolInvocation, capability: ToolCapability
    ) -> Any:
        del capability
        handler = self._handlers.get(invocation.tool_name)
        if handler is None:
            raise SandboxViolationError(f"Hands tool is not installed: {invocation.tool_name}")
        value = handler(dict(invocation.arguments))
        return await value if hasattr(value, "__await__") else value

    async def run_process(
        self,
        executable: Path,
        arguments: tuple[str, ...],
        *,
        timeout_seconds: float,
        stdin: bytes | None = None,
    ) -> dict[str, Any]:
        resolved = await asyncio.to_thread(executable.resolve)
        if resolved not in self._allowed_executables:
            raise SandboxViolationError("executable is not in the Sandbox allowlist")
        with tempfile.TemporaryDirectory(prefix="auraclaw-hands-") as temporary_dir:
            process = await asyncio.create_subprocess_exec(
                str(resolved),
                *arguments,
                cwd=temporary_dir,
                env={"LANG": "C.UTF-8", "PATH": os.defpath},
                stdin=asyncio.subprocess.PIPE if stdin is not None else None,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            try:
                stdout, stderr = await asyncio.wait_for(
                    process.communicate(stdin), timeout=timeout_seconds
                )
            except asyncio.CancelledError:
                process.kill()
                await process.wait()
                raise
            except TimeoutError:
                process.kill()
                await process.wait()
                return {
                    "status": "timeout",
                    "exit_code": None,
                    "stdout": "",
                    "stderr": "process exceeded Sandbox deadline",
                }
        return {
            "status": "success" if process.returncode == 0 else "error",
            "exit_code": process.returncode,
            "stdout": stdout.decode(errors="replace"),
            "stderr": stderr.decode(errors="replace"),
        }

    async def read_file(self, relative_path: str) -> bytes:
        path = self._confined(relative_path)
        return await asyncio.to_thread(path.read_bytes)

    async def write_file(self, relative_path: str, content: bytes) -> dict[str, Any]:
        path = self._confined(relative_path)
        await asyncio.to_thread(path.parent.mkdir, parents=True, exist_ok=True)
        await asyncio.to_thread(path.write_bytes, content)
        return {"path": relative_path, "bytes_written": len(content)}

    async def delete_file(self, relative_path: str) -> dict[str, Any]:
        path = self._confined(relative_path)
        if not path.is_file():
            raise SandboxViolationError("Sandbox deletion target is not a file")
        await asyncio.to_thread(path.unlink)
        return {"path": relative_path, "deleted": True}

    def _confined(self, relative_path: str) -> Path:
        candidate = (self._root / relative_path).resolve()
        if candidate == self._root or self._root not in candidate.parents:
            raise SandboxViolationError("path escapes the configured Sandbox root")
        return candidate
