"""Trusted per-model tariffs and conservative, exact-decimal reservations."""

from __future__ import annotations

import json
from decimal import Decimal
from typing import Any

from auraclaw.contracts.errors import RuntimeCostReservationUnavailableError


def amount(value: Any) -> Decimal:
    result = Decimal(str(value))
    if not result.is_finite() or result < 0:
        raise ValueError("costs must be finite and nonnegative")
    return result


def quote(
    profile: dict[str, Any], messages: Any, tools: Any, output_tokens: int, limit: float
) -> dict[str, Any]:
    required = {
        "provider",
        "model",
        "currency",
        "version",
        "max_input_tokens",
        "input_per_million",
        "output_per_million",
    }
    if not required.issubset(profile) or any(
        not profile[k] for k in ("provider", "model", "currency", "version")
    ):
        raise RuntimeCostReservationUnavailableError("trusted model pricing is not configured")
    input_cap = int(profile["max_input_tokens"])
    if input_cap < 1:
        raise ValueError("pricing input context cap must be positive")
    # Deliberately conservative offline bound; no tokenizer downloads or untrusted rates.
    prompt_bound = len(json.dumps([messages, tools], ensure_ascii=False).encode()) + 4096
    if prompt_bound > input_cap:
        raise RuntimeCostReservationUnavailableError("prompt exceeds priced input context cap")
    charge = (
        amount(profile["input_per_million"]) * input_cap
        + amount(profile["output_per_million"]) * output_tokens
    ) / Decimal(1_000_000)
    return {"reserved": str(charge), "limit": str(amount(limit)), "profile": profile}


def settled_cost(
    reservation: dict[str, Any],
    usage: dict[str, Any],
    *,
    provider: str | None = None,
    model: str | None = None,
) -> Decimal:
    profile = reservation["profile"]
    if provider is not None and (profile["provider"] != provider or profile["model"] != model):
        raise RuntimeCostReservationUnavailableError(
            "provider model does not match reserved tariff"
        )
    if "input_tokens" not in usage or "output_tokens" not in usage:
        raise RuntimeCostReservationUnavailableError("final priced usage is unknown")
    return (
        amount(usage["input_tokens"]) * amount(profile["input_per_million"])
        + amount(usage["output_tokens"]) * amount(profile["output_per_million"])
    ) / Decimal(1_000_000)
