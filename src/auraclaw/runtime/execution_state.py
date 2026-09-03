from __future__ import annotations

from enum import StrEnum


class RuntimePhase(StrEnum):
    MODEL_PENDING = "model_pending"
    MODEL_COMPLETED = "model_completed"
    MODEL_RECORDED = "model_recorded"
    TOOL_PENDING = "tool_pending"
    TOOL_COMPLETED = "tool_completed"
    APPROVAL_WAITING = "approval_waiting"
    COMPLETED = "completed"
    CAPABILITY_MODEL_PENDING = "capability.model_pending"
    CAPABILITY_MODEL_COMPLETED = "capability.model_completed"
    CAPABILITY_WORKFLOW_RUNNING = "capability.workflow_running"
    CAPABILITY_CALL_COMPLETED = "capability.call_completed"
    CAPABILITY_APPROVAL_WAITING = "capability.approval_waiting"
    CAPABILITY_SKILL_REVOKED_PAUSED = "capability.skill_revoked_paused"
    CAPABILITY_SKILL_REVOKED_CANCELLED = "capability.skill_revoked_cancelled"
    CAPABILITY_COMPLETED = "capability.completed"
    AGENT_WAITING_CHILDREN = "agent.waiting_children"
    AGENT_COMPLETED = "agent.completed"


_LEGAL_TRANSITIONS: dict[RuntimePhase, frozenset[RuntimePhase]] = {
    RuntimePhase.MODEL_PENDING: frozenset({RuntimePhase.MODEL_COMPLETED}),
    RuntimePhase.MODEL_COMPLETED: frozenset({RuntimePhase.MODEL_RECORDED}),
    RuntimePhase.MODEL_RECORDED: frozenset(
        {RuntimePhase.TOOL_PENDING, RuntimePhase.COMPLETED}
    ),
    RuntimePhase.TOOL_PENDING: frozenset({RuntimePhase.TOOL_COMPLETED}),
    RuntimePhase.TOOL_COMPLETED: frozenset(
        {RuntimePhase.MODEL_RECORDED, RuntimePhase.APPROVAL_WAITING}
    ),
    RuntimePhase.APPROVAL_WAITING: frozenset(
        {RuntimePhase.TOOL_PENDING, RuntimePhase.MODEL_RECORDED}
    ),
    RuntimePhase.COMPLETED: frozenset(),
    RuntimePhase.CAPABILITY_MODEL_PENDING: frozenset(
        {RuntimePhase.CAPABILITY_MODEL_COMPLETED}
    ),
    RuntimePhase.CAPABILITY_MODEL_COMPLETED: frozenset(
        {
            RuntimePhase.CAPABILITY_WORKFLOW_RUNNING,
            RuntimePhase.CAPABILITY_CALL_COMPLETED,
            RuntimePhase.CAPABILITY_MODEL_PENDING,
            RuntimePhase.AGENT_WAITING_CHILDREN,
        }
    ),
    RuntimePhase.CAPABILITY_WORKFLOW_RUNNING: frozenset(
        {RuntimePhase.CAPABILITY_WORKFLOW_RUNNING, RuntimePhase.CAPABILITY_CALL_COMPLETED}
    ),
    RuntimePhase.CAPABILITY_CALL_COMPLETED: frozenset(
        {
            RuntimePhase.CAPABILITY_WORKFLOW_RUNNING,
            RuntimePhase.CAPABILITY_MODEL_PENDING,
            RuntimePhase.CAPABILITY_APPROVAL_WAITING,
            RuntimePhase.CAPABILITY_COMPLETED,
            RuntimePhase.AGENT_WAITING_CHILDREN,
            RuntimePhase.AGENT_COMPLETED,
        }
    ),
    RuntimePhase.CAPABILITY_APPROVAL_WAITING: frozenset(
        {RuntimePhase.CAPABILITY_MODEL_COMPLETED}
    ),
    RuntimePhase.CAPABILITY_SKILL_REVOKED_PAUSED: frozenset(
        {RuntimePhase.CAPABILITY_MODEL_PENDING, RuntimePhase.CAPABILITY_SKILL_REVOKED_CANCELLED}
    ),
    RuntimePhase.CAPABILITY_SKILL_REVOKED_CANCELLED: frozenset(),
    RuntimePhase.CAPABILITY_COMPLETED: frozenset(),
    RuntimePhase.AGENT_WAITING_CHILDREN: frozenset(
        {RuntimePhase.CAPABILITY_MODEL_PENDING, RuntimePhase.AGENT_COMPLETED}
    ),
    RuntimePhase.AGENT_COMPLETED: frozenset(),
}


def legal_successors(phase: RuntimePhase) -> frozenset[RuntimePhase]:
    """Return every state the persisted execution may legally advance to next."""

    return _LEGAL_TRANSITIONS[phase]


def require_legal_transition(current: RuntimePhase, target: RuntimePhase) -> None:
    if target not in legal_successors(current):
        raise ValueError(f"illegal Runtime transition: {current} -> {target}")
