from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any

import pytest

from auraclaw.contracts.errors import RuntimeCancelledError
from auraclaw.contracts.skills import SkillActivation, SkillBinding
from auraclaw.contracts.tools import ArtifactRef
from auraclaw.control.ports import RuntimeAssignment, RuntimeCheckpoint
from auraclaw.projection.task.projector import InMemoryTaskProjection
from auraclaw.runtime.skill_runner import (
    SkillRunner,
    SkillRunnerInjectionPoint,
    SkillStepResult,
)


class _SimulatedProcessDeath(BaseException):
    pass


class _Control:
    def __init__(self) -> None:
        self.checkpoint: RuntimeCheckpoint | None = None
        self.cancelled = False

    async def assert_fencing(self, resource_id: str, fencing_token: int) -> None:
        del resource_id
        assert fencing_token == 1

    async def is_cancelled(
        self,
        tenant_id: str,
        session_id: str,
        run_id: str,
    ) -> bool:
        del tenant_id, session_id, run_id
        return self.cancelled

    async def save_checkpoint(self, checkpoint: RuntimeCheckpoint) -> None:
        self.checkpoint = checkpoint

    async def load_checkpoint(
        self,
        tenant_id: str,
        session_id: str,
        run_id: str,
    ) -> RuntimeCheckpoint | None:
        del tenant_id, session_id, run_id
        return self.checkpoint

    async def finish_assignment(self, task_id: str, outcome: str) -> None:
        del task_id, outcome


class _Session:
    def __init__(self) -> None:
        self.events: list[SimpleNamespace] = []
        self.commands: set[str] = set()

    async def load(self, assignment: RuntimeAssignment) -> list[Any]:
        del assignment
        return list(self.events)

    async def append(
        self,
        assignment: RuntimeAssignment,
        events: list[Any],
        *,
        command_id: str,
        operation: str,
        expected_version: int | None = None,
    ) -> list[Any]:
        del operation, expected_version
        if command_id in self.commands:
            return []
        self.commands.add(command_id)
        appended = []
        for event in events:
            stored = SimpleNamespace(
                event_id=f"event-{len(self.events) + 1}",
                tenant_id=assignment.tenant_id,
                root_session_id=assignment.root_session_id,
                session_id=assignment.session_id,
                run_id=assignment.run_id,
                aggregate_version=len(self.events) + 1,
                type=event.type,
                occurred_at=datetime.now(UTC),
                payload=dict(event.payload),
            )
            self.events.append(stored)
            appended.append(stored)
        return appended


class _Resolver:
    def __init__(self, binding: SkillBinding) -> None:
        self.binding = binding
        self.calls = 0

    async def resolve(self, **arguments: object) -> SkillBinding:
        del arguments
        self.calls += 1
        return self.binding


class _Capabilities:
    pass


class _RuntimeEvents:
    def __init__(self) -> None:
        self.events: list[Any] = []

    async def publish(self, event: Any) -> None:
        self.events.append(event)


class _Steps:
    def __init__(self) -> None:
        self.cursors: list[str] = []

    async def execute_step(
        self,
        *,
        assignment: RuntimeAssignment,
        activation: SkillActivation,
        cursor: str,
        capabilities: Any,
    ) -> SkillStepResult:
        del assignment, activation, capabilities
        self.cursors.append(cursor)
        if cursor == "0":
            return SkillStepResult(next_cursor="1")
        return SkillStepResult(
            next_cursor="2",
            completed=True,
            output_summary="release prepared",
            artifact_refs=("art-result",),
        )


def _assignment() -> RuntimeAssignment:
    return RuntimeAssignment(
        tenant_id="tenant-a",
        root_session_id="session-root",
        session_id="session-child",
        run_id="run-1",
        runtime_id="runtime-1",
        lease_id="lease-1",
        fencing_token=1,
        role="worker",
        resource_profile={},
    )


def _binding() -> SkillBinding:
    return SkillBinding(
        skill_name="release.prepare",
        skill_version="1.5.0",
        publisher="platform",
        package_digest=f"sha256:{'1' * 64}",
        artifact_ref=ArtifactRef(
            artifact_id="art-skill",
            version=1,
            content_hash="2" * 64,
            media_type="application/vnd.auraclaw.skill-package+json",
            size=100,
        ),
        policy_version="policy-42",
        max_steps=5,
        timeout_seconds=60,
    )


def test_skill_runner_recovers_from_checkpoint_without_dependency_drift() -> None:
    async def scenario() -> None:
        assignment = _assignment()
        control = _Control()
        session = _Session()
        resolver = _Resolver(_binding())
        runtime_events = _RuntimeEvents()
        steps = _Steps()
        crashed = False

        def fail_after_first_checkpoint(point: SkillRunnerInjectionPoint) -> None:
            nonlocal crashed
            if (
                point == SkillRunnerInjectionPoint.AFTER_STEP_CHECKPOINT
                and not crashed
            ):
                crashed = True
                raise _SimulatedProcessDeath()

        first = SkillRunner(
            control=control,
            session=session,  # type: ignore[arg-type]
            resolver=resolver,
            capabilities=_Capabilities(),  # type: ignore[arg-type]
            runtime_events=runtime_events,
            steps=steps,
            policy_version="policy-42",
            failure_injector=fail_after_first_checkpoint,
        )
        activation = await first.activate(
            assignment,
            activation_key="primary",
            name="release.prepare",
            version=">=1.4,<2",
            inputs={"repository": "AuraClaw"},
        )
        duplicate = await first.activate(
            assignment,
            activation_key="primary",
            name="release.prepare",
            inputs={"repository": "ignored-on-idempotent-retry"},
        )
        assert duplicate == activation
        assert resolver.calls == 1
        with pytest.raises(_SimulatedProcessDeath):
            await first.execute(assignment, activation)

        assert control.checkpoint is not None
        assert control.checkpoint.state["skill_step_cursor"] == "1"
        recovered = SkillRunner(
            control=control,
            session=session,  # type: ignore[arg-type]
            resolver=resolver,
            capabilities=_Capabilities(),  # type: ignore[arg-type]
            runtime_events=runtime_events,
            steps=steps,
            policy_version="new-policy-must-not-rebind",
        )
        result = await recovered.execute(assignment, activation)
        assert result.completed is True
        assert steps.cursors == ["0", "1"]
        assert resolver.calls == 1
        assert [event.type for event in session.events] == [
            "skill.activated",
            "skill.completed",
        ]
        assert session.events[0].payload["policy_version"] == "policy-42"
        assert len(runtime_events.events) == 2

        repeated = await recovered.execute(assignment, activation)
        assert repeated.output_summary == "release prepared"
        assert steps.cursors == ["0", "1"]

        projection = InMemoryTaskProjection()
        await projection.project(session.events)  # type: ignore[arg-type]
        view = await projection.get_task("tenant-a", "session-child")
        assert view is not None
        assert view["skill_activations"][0]["status"] == "completed"
        assert view["skill_activations"][0]["artifact_refs"] == ["art-result"]

    asyncio.run(scenario())


def test_skill_runner_records_cancellation_as_terminal_fact() -> None:
    async def scenario() -> None:
        assignment = _assignment()
        control = _Control()
        session = _Session()
        runner = SkillRunner(
            control=control,
            session=session,  # type: ignore[arg-type]
            resolver=_Resolver(_binding()),
            capabilities=_Capabilities(),  # type: ignore[arg-type]
            runtime_events=_RuntimeEvents(),
            steps=_Steps(),
            policy_version="policy-42",
        )
        activation = await runner.activate(
            assignment,
            activation_key="cancelled",
            name="release.prepare",
            inputs={},
        )
        control.cancelled = True
        with pytest.raises(RuntimeCancelledError):
            await runner.execute(assignment, activation)
        assert [event.type for event in session.events] == [
            "skill.activated",
            "skill.cancelled",
        ]

    asyncio.run(scenario())
