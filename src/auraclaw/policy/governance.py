from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from auraclaw.contracts.internal import PolicyEvaluateRequest
from auraclaw.contracts.tools import PolicyDecision


@dataclass(frozen=True)
class ProductionPolicy:
    """Authoritative product constraints owned by the Policy service."""

    runtime_budget: dict[str, Any]
    model_provider: str | None
    model_name: str | None
    model_data_region: str
    allowed_data_regions: tuple[str, ...]
    artifact_share_max_ttl_seconds: int
    artifact_share_classifications: tuple[str, ...]

    def govern(
        self,
        request: PolicyEvaluateRequest,
        decision: PolicyDecision,
        constraints: dict[str, Any],
    ) -> tuple[PolicyDecision, dict[str, Any]]:
        if decision not in {PolicyDecision.ALLOW, PolicyDecision.ALLOW_WITH_CONSTRAINTS}:
            return decision, constraints
        governed = dict(constraints)
        if request.action == "task.create":
            governed["runtime_budget"] = dict(self.runtime_budget)
        elif request.action == "model.generate":
            required_region = str(
                request.attributes.get("required_data_region") or self.model_data_region
            )
            if required_region not in self.allowed_data_regions:
                return PolicyDecision.DENY, {}
            requested_providers = tuple(
                str(value) for value in request.attributes.get("allowed_providers", ())
            )
            if (
                self.model_provider
                and requested_providers
                and (self.model_provider not in requested_providers)
            ):
                return PolicyDecision.DENY, {}
            requested_model = request.attributes.get("preferred_model")
            if self.model_name and requested_model and requested_model != self.model_name:
                return PolicyDecision.DENY, {}
            governed.update(
                {
                    "data_region": required_region,
                    "max_output_tokens": min(
                        int(request.attributes.get("max_output_tokens", 8192)),
                        int(self.runtime_budget["max_output_tokens"]),
                    ),
                }
            )
            if self.model_provider:
                governed["allowed_providers"] = [self.model_provider]
            if self.model_name:
                governed["preferred_model"] = self.model_name
            max_cost = self.runtime_budget.get("max_cost")
            if max_cost is not None:
                governed["run_max_cost"] = float(max_cost)
        elif request.action == "artifact.share":
            classification = str(request.attributes.get("classification", "internal"))
            if classification not in self.artifact_share_classifications:
                return PolicyDecision.DENY, {}
            audience = str(request.attributes.get("audience", "")).strip()
            if not audience:
                return PolicyDecision.DENY, {}
            requested_ttl = int(request.attributes.get("ttl_seconds", 300))
            governed.update(
                {
                    "audience": audience,
                    "ttl_seconds": min(requested_ttl, self.artifact_share_max_ttl_seconds),
                    "classification": classification,
                }
            )
        if governed != constraints:
            return PolicyDecision.ALLOW_WITH_CONSTRAINTS, governed
        return decision, governed
