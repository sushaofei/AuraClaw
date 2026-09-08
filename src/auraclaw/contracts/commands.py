from dataclasses import dataclass

from auraclaw.contracts.events import Actor


@dataclass(frozen=True)
class CommandContext:
    command_id: str
    tenant_id: str
    actor: Actor
    correlation_id: str
    expected_version: int
    operation: str = "append"
    causation_id: str | None = None
    dept_id: str | None = None
