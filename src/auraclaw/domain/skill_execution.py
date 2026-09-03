from __future__ import annotations

from collections.abc import Sequence

from auraclaw.contracts.events import CanonicalEvent

SKILL_TERMINAL_EVENTS = frozenset({"skill.completed", "skill.failed", "skill.cancelled"})
RUN_TERMINAL_EVENTS = frozenset({"run.completed", "run.failed", "run.cancelled"})


def pending_skill_invocations(
    events: Sequence[CanonicalEvent], *, run_id: str | None = None
) -> tuple[CanonicalEvent, ...]:
    """A Run terminal fact does not establish a previously issued write's outcome."""
    pending: dict[tuple[str, str | None, str, str, str], CanonicalEvent] = {}
    for event in events:
        if run_id is not None and event.run_id != run_id:
            continue
        activation_id = event.payload.get("skill_activation_id")
        invocation_id = event.payload.get("tool_invocation_id")
        if not isinstance(activation_id, str) or not isinstance(invocation_id, str):
            continue
        key = (
            event.session_id,
            event.run_id,
            activation_id,
            invocation_id,
            str(event.payload.get("invocation_cycle", 0)),
        )
        if event.type == "skill.invocation.requested":
            pending[key] = event
        elif event.type == "skill.invocation.settled":
            pending.pop(key, None)
    return tuple(pending.values())


def has_active_skill_reference(
    events: Sequence[CanonicalEvent],
    publisher: str,
    name: str,
    package_digest: str | None = None,
) -> bool:
    terminal_runs = {(e.session_id, e.run_id) for e in events if e.type in RUN_TERMINAL_EVENTS}
    terminal_skills = {
        (e.session_id, e.run_id, e.payload.get("skill_activation_id"))
        for e in events
        if e.type in SKILL_TERMINAL_EVENTS
    }
    pending = {
        (e.session_id, e.run_id, e.payload.get("skill_activation_id"))
        for e in pending_skill_invocations(events)
    }
    for event in events:
        if event.type != "skill.activated" or event.run_id is None:
            continue
        activation = event.payload.get("activation")
        binding = activation.get("binding") if isinstance(activation, dict) else None
        if not isinstance(binding, dict):
            binding = {}
        identity = (event.session_id, event.run_id, event.payload.get("skill_activation_id"))
        if identity not in pending and (
            identity in terminal_skills or (event.session_id, event.run_id) in terminal_runs
        ):
            continue
        references = (
            binding,
            *(r for r in binding.get("resolved_skills", ()) if isinstance(r, dict)),
        )
        if package_digest is not None and event.payload.get("package_digest") == package_digest:
            return True
        if any(
            (reference.get("package_digest") == package_digest)
            if package_digest is not None
            else (
                reference.get("publisher") == publisher
                and reference.get("skill_name", reference.get("name")) == name
            )
            for reference in references
        ):
            return True
    return False
