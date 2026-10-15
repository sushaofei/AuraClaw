from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any

from auraclaw.contracts.errors import (
    RuntimeCostBudgetExceededError,
    RuntimeOutputTokenBudgetExceededError,
    TerminalBudgetExceededError,
)
from auraclaw.control.ports import RuntimeAssignment
from auraclaw.runtime.event_committer import CanonicalEventCommitter
from auraclaw.runtime.ports import ModelResponse, SessionClient
from auraclaw.runtime.repeat_policy import no_progress

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class TurnBudget:
    max_output_tokens: int
    include_truncation_guidance: bool


class RuntimeBudgetService:
    """Reserve canonical budget before dispatch and validate provider usage."""

    def __init__(
        self,
        session: SessionClient,
        events: CanonicalEventCommitter,
        *,
        per_turn_output_tokens: int,
        terminal_output_reserve: int,
    ) -> None:
        self._per_turn_output_tokens = per_turn_output_tokens
        self._terminal_output_reserve = terminal_output_reserve
        self._session = session
        self._events = events

    async def reserve_budget(
        self, assignment: RuntimeAssignment, identity: str, kind: str, tokens: int
    ) -> None:
        facts = await self._session.load(assignment)
        await self._events.append_once(
            assignment,
            facts,
            "runtime.budget.reserved",
            {"reservation_id": identity, "kind": kind, "output_tokens": tokens},
            identity=identity,
        )

    @staticmethod
    async def validate_usage(assignment: RuntimeAssignment, response: ModelResponse) -> None:
        output_tokens = int(response.usage.get("output_tokens", 0))
        if output_tokens > assignment.budget.max_output_tokens:
            raise RuntimeOutputTokenBudgetExceededError(
                "provider output exceeded Runtime token budget"
            )
        cost = float(response.usage.get("cost", 0.0))
        if assignment.budget.max_cost is not None and cost > assignment.budget.max_cost:
            raise RuntimeCostBudgetExceededError("provider output exceeded Runtime cost budget")

    @staticmethod
    def accumulate_usage(
        current: dict[str, int | float],
        update: dict[str, int | float],
    ) -> dict[str, int | float]:
        result = dict(current)
        for key, value in update.items():
            if isinstance(value, (int, float)) and not isinstance(value, bool):
                result[key] = result.get(key, 0) + value
        return result

    @staticmethod
    def validate_cumulative_usage(
        assignment: RuntimeAssignment,
        usage: dict[str, int | float],
    ) -> None:
        if int(usage.get("output_tokens", 0)) > assignment.budget.max_output_tokens:
            raise RuntimeOutputTokenBudgetExceededError(
                "provider cumulative output exceeded Runtime token budget"
            )
        if (
            assignment.budget.max_cost is not None
            and float(usage.get("cost", 0.0)) > assignment.budget.max_cost
        ):
            raise RuntimeCostBudgetExceededError("provider cumulative cost exceeded Runtime budget")

    @staticmethod
    def concluding_reason(
        assignment: RuntimeAssignment, state: dict[str, Any], turn_events: list[Any]
    ) -> str | None:
        if assignment.budget.policy_version == "2":
            reserve_steps = min(2, max(0, assignment.budget.max_steps // 4))
            reserve_tokens = min(1024, assignment.budget.max_output_tokens // 4)
            if no_progress(turn_events, assignment.run_id):
                return "runtime_no_progress_detected"
            elif assignment.budget.max_steps - int(state.get("steps_used", 0)) <= reserve_steps:
                return "runtime_step_budget_exceeded"
            elif (
                assignment.budget.max_output_tokens
                - int(state.get("usage", {}).get("output_tokens", 0))
                <= reserve_tokens
            ):
                return "runtime_output_token_budget_exceeded"

        return None

    def allocate_turn(self, assignment: RuntimeAssignment, state: dict[str, Any]) -> TurnBudget:
        turn_index = int(state.get("turn_index", 0))
        output_tokens_used = int(dict(state.get("usage", {})).get("output_tokens", 0))
        remaining_output_tokens = assignment.budget.max_output_tokens - output_tokens_used
        if remaining_output_tokens < 1:
            raise RuntimeOutputTokenBudgetExceededError(
                "Runtime cumulative output token budget was exhausted"
            )
        terminal_role = assignment.role in {"worker", "repair", "reviewer"}
        recovery_turn = bool(state.get("truncation_recovery_pending"))
        terminal_reserve = min(
            (1024 if assignment.budget.policy_version == "2" else self._terminal_output_reserve),
            max(1, assignment.budget.max_output_tokens // 4),
        )
        available_for_turn = remaining_output_tokens
        if assignment.budget.policy_version == "2":
            if state.get("concluding_reason"):
                available_for_turn = min(remaining_output_tokens, max(1, terminal_reserve))
            else:
                available_for_turn = max(
                    1, remaining_output_tokens - min(1024, assignment.budget.max_output_tokens // 4)
                )
        elif terminal_role and not recovery_turn:
            available_for_turn -= terminal_reserve
            if available_for_turn < 1:
                logger.warning(
                    "agent.terminal_budget_exhausted role=%s turn=%s remaining=%s reserve=%s",
                    assignment.role,
                    turn_index,
                    remaining_output_tokens,
                    terminal_reserve,
                )
                raise TerminalBudgetExceededError(
                    "terminal collaboration reserve leaves no model turn budget"
                )
        elif recovery_turn:
            available_for_turn = min(available_for_turn, terminal_reserve)
        return TurnBudget(
            min(self._per_turn_output_tokens, available_for_turn),
            recovery_turn and assignment.budget.policy_version != "2",
        )
