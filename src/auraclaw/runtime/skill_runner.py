from __future__ import annotations

import hashlib
import inspect
import json
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from typing import Any, Protocol
from uuid import uuid4

from auraclaw.contracts.errors import (
    BudgetExceededError,
    RuntimeCancelledError,
)
from auraclaw.contracts.events import NewEvent
from auraclaw.contracts.skills import SkillActivation
from auraclaw.control.ports import RuntimeAssignment, RuntimeCheckpoint
from auraclaw.runtime.clients import assignment_resource_id
from auraclaw.runtime.ports import (
    CapabilityClient,
    RuntimeControlClient,
    RuntimeEvent,
    RuntimeEventPublisher,
    SessionClient,
    SkillBindingResolver,
)


class SkillRunnerInjectionPoint(StrEnum):
    BEFORE_STEP = "before_step"
    AFTER_STEP_CHECKPOINT = "after_step_checkpoint"


@dataclass(frozen=True)
class SkillStepResult:
    next_cursor: str
    completed: bool = False
    output_summary: str = ""
    artifact_refs: tuple[str, ...] = ()


class SkillStepExecutor(Protocol):
    async def execute_step(
        self,
        *,
        assignment: RuntimeAssignment,
        activation: SkillActivation,
        cursor: str,
        capabilities: CapabilityClient,
    ) -> SkillStepResult: ...


SkillFailureInjector = Callable[
    [SkillRunnerInjectionPoint],
    Awaitable[None] | None,
]


class SkillRunner:
    def __init__(
        self,
        *,
        control: RuntimeControlClient,
        session: SessionClient,
        resolver: SkillBindingResolver,
        capabilities: CapabilityClient,
        runtime_events: RuntimeEventPublisher,
        steps: SkillStepExecutor,
        policy_version: str,
        failure_injector: SkillFailureInjector | None = None,
    ) -> None:
        self._control = control
        self._session = session
        self._resolver = resolver
        self._capabilities = capabilities
        self._runtime_events = runtime_events
        self._steps = steps
        self._policy_version = policy_version
        self._failure_injector = failure_injector

    async def activate(
        self,
        assignment: RuntimeAssignment,
        *,
        activation_key: str,
        name: str,
        inputs: dict[str, Any],
        version: str = "*",
        publisher: str | None = None,
    ) -> SkillActivation:
        await self._guard(assignment)
        events = await self._session.load(assignment)
        existing = next(
            (
                event
                for event in events
                if event.type == "skill.activated"
                and event.run_id == assignment.run_id
                and event.payload.get("activation_key") == activation_key
            ),
            None,
        )
        if existing is not None:
            return SkillActivation.model_validate(existing.payload["activation"])
        binding = await self._resolver.resolve(
            tenant_id=assignment.tenant_id,
            name=name,
            version=version,
            publisher=publisher,
            role=assignment.role,
            policy_version=self._policy_version,
            subject=assignment.runtime_id,
            correlation_id=assignment.run_id,
            active_skill_names=tuple(
                str(event.payload.get("skill_name"))
                for event in events
                if event.type == "skill.activated"
                and event.run_id == assignment.run_id
            ),
        )
        activation = SkillActivation(
            skill_activation_id=_activation_id(assignment, activation_key),
            activation_key=activation_key,
            binding=binding,
            input_digest=_digest(inputs),
        )
        await self._append_terminal_safe(
            assignment,
            event_type="skill.activated",
            activation=activation,
            payload={"activation": activation.model_dump(mode="json")},
        )
        await self._save_checkpoint(
            assignment,
            activation,
            phase="skill.active",
            cursor="0",
            steps_completed=0,
        )
        return activation

    async def execute(
        self,
        assignment: RuntimeAssignment,
        activation: SkillActivation,
    ) -> SkillStepResult:
        terminal = await self._terminal_result(assignment, activation)
        if terminal is not None:
            return terminal
        checkpoint = await self._control.load_checkpoint(
            assignment.tenant_id,
            assignment.session_id,
            assignment.run_id,
        )
        cursor = "0"
        steps_completed = 0
        if (
            checkpoint is not None
            and checkpoint.state.get("skill_activation_id")
            == activation.skill_activation_id
        ):
            cursor = str(checkpoint.state.get("skill_step_cursor", "0"))
            steps_completed = int(checkpoint.state.get("skill_steps_completed", 0))
        started_at = await self._activation_started_at(assignment, activation)
        try:
            while steps_completed < min(
                activation.binding.max_steps,
                assignment.budget.max_steps,
            ):
                await self._guard(assignment)
                if datetime.now(UTC) >= started_at + timedelta(
                    seconds=activation.binding.timeout_seconds
                ):
                    raise BudgetExceededError("Skill execution timeout was exceeded")
                await self._inject(SkillRunnerInjectionPoint.BEFORE_STEP)
                result = await self._steps.execute_step(
                    assignment=assignment,
                    activation=activation,
                    cursor=cursor,
                    capabilities=self._capabilities,
                )
                if not result.next_cursor:
                    raise RuntimeError("Skill step did not return a resume cursor")
                if not result.completed and result.next_cursor == cursor:
                    raise RuntimeError("Skill step cursor did not advance")
                steps_completed += 1
                cursor = result.next_cursor
                await self._save_checkpoint(
                    assignment,
                    activation,
                    phase=(
                        "skill.completed" if result.completed else "skill.running"
                    ),
                    cursor=cursor,
                    steps_completed=steps_completed,
                    result=result,
                )
                await self._publish_step(
                    assignment,
                    activation,
                    cursor=cursor,
                    steps_completed=steps_completed,
                )
                await self._inject(SkillRunnerInjectionPoint.AFTER_STEP_CHECKPOINT)
                if result.completed:
                    await self._append_terminal_safe(
                        assignment,
                        event_type="skill.completed",
                        activation=activation,
                        payload={
                            "output_summary": result.output_summary,
                            "artifact_refs": list(result.artifact_refs),
                            "step_cursor": cursor,
                            "steps_completed": steps_completed,
                        },
                    )
                    return result
            raise BudgetExceededError("Skill step budget was exhausted")
        except RuntimeCancelledError:
            await self._append_terminal_safe(
                assignment,
                event_type="skill.cancelled",
                activation=activation,
                payload={"step_cursor": cursor, "steps_completed": steps_completed},
                guard=False,
            )
            raise
        except Exception as exc:
            await self._append_terminal_safe(
                assignment,
                event_type="skill.failed",
                activation=activation,
                payload={
                    "error": type(exc).__name__,
                    "step_cursor": cursor,
                    "steps_completed": steps_completed,
                },
                guard=False,
            )
            raise

    async def _terminal_result(
        self,
        assignment: RuntimeAssignment,
        activation: SkillActivation,
    ) -> SkillStepResult | None:
        events = await self._session.load(assignment)
        terminal = next(
            (
                event
                for event in reversed(events)
                if event.type
                in {"skill.completed", "skill.failed", "skill.cancelled"}
                and event.payload.get("skill_activation_id")
                == activation.skill_activation_id
            ),
            None,
        )
        if terminal is None:
            return None
        if terminal.type != "skill.completed":
            raise RuntimeError(f"Skill activation is terminal: {terminal.type}")
        return SkillStepResult(
            next_cursor=str(terminal.payload.get("step_cursor", "completed")),
            completed=True,
            output_summary=str(terminal.payload.get("output_summary", "")),
            artifact_refs=tuple(terminal.payload.get("artifact_refs", ())),
        )

    async def _append_terminal_safe(
        self,
        assignment: RuntimeAssignment,
        *,
        event_type: str,
        activation: SkillActivation,
        payload: dict[str, Any],
        guard: bool = True,
    ) -> None:
        events = await self._session.load(assignment)
        if any(
            event.type == event_type
            and event.payload.get("skill_activation_id")
            == activation.skill_activation_id
            for event in events
        ):
            return
        if guard:
            await self._guard(assignment)
        event_payload = {
            "skill_activation_id": activation.skill_activation_id,
            "activation_key": activation.activation_key,
            "skill_name": activation.binding.skill_name,
            "skill_version": activation.binding.skill_version,
            "package_digest": activation.binding.package_digest,
            "policy_version": activation.binding.policy_version,
            "policy_decision_id": activation.binding.policy_decision_id,
            **payload,
        }
        await self._session.append(
            assignment,
            [NewEvent(type=event_type, payload=event_payload)],
            command_id=(
                f"runtime:{event_type}:{activation.skill_activation_id}"
            ),
            operation=f"runtime.{event_type}",
        )

    async def _activation_started_at(
        self,
        assignment: RuntimeAssignment,
        activation: SkillActivation,
    ) -> datetime:
        events = await self._session.load(assignment)
        activated = next(
            (
                event
                for event in events
                if event.type == "skill.activated"
                and event.payload.get("skill_activation_id")
                == activation.skill_activation_id
            ),
            None,
        )
        if activated is None:
            raise RuntimeError("Skill activation fact is missing")
        return activated.occurred_at

    async def _save_checkpoint(
        self,
        assignment: RuntimeAssignment,
        activation: SkillActivation,
        *,
        phase: str,
        cursor: str,
        steps_completed: int,
        result: SkillStepResult | None = None,
    ) -> None:
        state: dict[str, Any] = {
            "skill_activation_id": activation.skill_activation_id,
            "skill_binding": activation.binding.model_dump(mode="json"),
            "skill_step_cursor": cursor,
            "skill_steps_completed": steps_completed,
        }
        if result is not None:
            state["skill_step_result"] = {
                "completed": result.completed,
                "output_summary": result.output_summary,
                "artifact_refs": list(result.artifact_refs),
            }
        await self._control.save_checkpoint(
            RuntimeCheckpoint(
                tenant_id=assignment.tenant_id,
                session_id=assignment.session_id,
                run_id=assignment.run_id,
                fencing_token=assignment.fencing_token,
                phase=phase,
                state=state,
                updated_at=datetime.now(UTC),
            )
        )

    async def _guard(self, assignment: RuntimeAssignment) -> None:
        await self._control.assert_fencing(
            assignment_resource_id(assignment),
            assignment.fencing_token,
        )
        if await self._control.is_cancelled(
            assignment.tenant_id,
            assignment.session_id,
            assignment.run_id,
        ):
            raise RuntimeCancelledError(
                f"Runtime run cancelled: {assignment.run_id}"
            )
        if assignment.deadline is not None and datetime.now(UTC) >= assignment.deadline:
            raise RuntimeCancelledError(
                f"Runtime deadline exceeded: {assignment.run_id}"
            )

    async def _publish_step(
        self,
        assignment: RuntimeAssignment,
        activation: SkillActivation,
        *,
        cursor: str,
        steps_completed: int,
    ) -> None:
        try:
            await self._runtime_events.publish(
                RuntimeEvent(
                    event_id=f"rte_{uuid4().hex}",
                    tenant_id=assignment.tenant_id,
                    root_session_id=assignment.root_session_id,
                    session_id=assignment.session_id,
                    run_id=assignment.run_id,
                    sequence=steps_completed,
                    type="skill.step.completed",
                    timestamp=datetime.now(UTC),
                    payload={
                        "skill_activation_id": activation.skill_activation_id,
                        "step_cursor": cursor,
                    },
                )
            )
        except Exception:
            return

    async def _inject(self, point: SkillRunnerInjectionPoint) -> None:
        if self._failure_injector is None:
            return
        result = self._failure_injector(point)
        if inspect.isawaitable(result):
            await result


def _activation_id(
    assignment: RuntimeAssignment,
    activation_key: str,
) -> str:
    value = ":".join(
        (
            assignment.tenant_id,
            assignment.session_id,
            assignment.run_id,
            activation_key,
        )
    )
    return f"ska_{hashlib.sha256(value.encode()).hexdigest()[:32]}"


def _digest(value: dict[str, Any]) -> str:
    payload = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    return f"sha256:{hashlib.sha256(payload).hexdigest()}"
