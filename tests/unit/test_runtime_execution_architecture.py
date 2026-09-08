from __future__ import annotations

import ast
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from auraclaw.contracts.errors import RuntimeCancelledError
from auraclaw.control.ports import RuntimeAssignment
from auraclaw.runtime.execution_guard import RuntimeExecutionGuard
from auraclaw.runtime.execution_state import (
    RuntimePhase,
    legal_successors,
    require_legal_transition,
)
from auraclaw.runtime.progress_store import RuntimeProgressStore

ROOT = Path(__file__).resolve().parents[2]


def _assignment(*, deadline: datetime | None = None) -> RuntimeAssignment:
    return RuntimeAssignment(
        tenant_id="tenant-runtime-state",
        root_session_id="ses_root",
        session_id="ses_runtime",
        run_id="run_runtime",
        runtime_id="runtime-1",
        lease_id="lease-1",
        fencing_token=7,
        role="root",
        resource_profile={},
        deadline=deadline,
    )


def test_runtime_phase_graph_exposes_legal_and_terminal_transitions() -> None:
    assert legal_successors(RuntimePhase.MODEL_PENDING) == {
        RuntimePhase.MODEL_COMPLETED
    }
    assert RuntimePhase.TOOL_PENDING in legal_successors(RuntimePhase.MODEL_RECORDED)
    assert legal_successors(RuntimePhase.COMPLETED) == frozenset()
    require_legal_transition(RuntimePhase.TOOL_PENDING, RuntimePhase.TOOL_COMPLETED)
    with pytest.raises(ValueError, match="illegal Runtime transition"):
        require_legal_transition(RuntimePhase.MODEL_PENDING, RuntimePhase.COMPLETED)


@pytest.mark.asyncio
async def test_progress_store_rejects_unknown_checkpoint_phase() -> None:
    class Control:
        async def save_checkpoint(self, checkpoint: object) -> None:
            raise AssertionError(f"unexpected checkpoint write: {checkpoint}")

    store = RuntimeProgressStore(Control())  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="not a valid RuntimePhase"):
        await store.save_checkpoint(
            _assignment(), RuntimePhase("legacy.unknown"), {}
        )


@pytest.mark.asyncio
async def test_execution_guard_checks_fencing_before_business_cancellation() -> None:
    calls: list[str] = []

    class Control:
        async def assert_fencing(self, resource_id: str, fencing_token: int) -> None:
            assert resource_id == "session:tenant-runtime-state:ses_runtime"
            assert fencing_token == 7
            calls.append("fencing")

        async def is_cancelled(self, tenant_id: str, session_id: str, run_id: str) -> bool:
            del tenant_id, session_id, run_id
            calls.append("cancel")
            return True

    guard = RuntimeExecutionGuard(Control())  # type: ignore[arg-type]
    with pytest.raises(RuntimeCancelledError, match="run cancelled"):
        await guard.check(_assignment(deadline=datetime.now(UTC) + timedelta(minutes=1)))
    assert calls == ["fencing", "cancel"]


def test_agent_harness_is_a_thin_stable_facade() -> None:
    path = ROOT / "src" / "auraclaw" / "runtime" / "harness.py"
    tree = ast.parse(path.read_text())
    harness = next(
        node
        for node in tree.body
        if isinstance(node, ast.ClassDef) and node.name == "AgentHarness"
    )
    methods = [
        node
        for node in harness.body
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    ]
    assert methods == []
