from __future__ import annotations

from dataclasses import asdict, dataclass
from enum import StrEnum
from typing import Any


class ErrorCategory(StrEnum):
    REQUEST = "request"
    AUTHENTICATION = "authentication"
    AUTHORIZATION = "authorization"
    CONFLICT = "conflict"
    CAPACITY = "capacity"
    DEPENDENCY = "dependency"
    RUNTIME = "runtime"
    INTERNAL = "internal"


class OperatorAction(StrEnum):
    CORRECT_REQUEST = "correct_request"
    REFRESH_IDENTITY = "refresh_identity"
    RESOLVE_CONFLICT = "resolve_conflict"
    RETRY_WITH_BACKOFF = "retry_with_backoff"
    INSPECT_DEPENDENCY = "inspect_dependency"
    INSPECT_RUNTIME = "inspect_runtime"
    ESCALATE = "escalate"


@dataclass(frozen=True)
class ErrorDisposition:
    category: ErrorCategory
    retryable: bool
    operator_action: OperatorAction


_CODE_DISPOSITIONS: dict[str, ErrorDisposition] = {
    "unauthenticated": ErrorDisposition(
        ErrorCategory.AUTHENTICATION, False, OperatorAction.REFRESH_IDENTITY
    ),
    "authorization_denied": ErrorDisposition(
        ErrorCategory.AUTHORIZATION, False, OperatorAction.CORRECT_REQUEST
    ),
    "policy_denied": ErrorDisposition(
        ErrorCategory.AUTHORIZATION, False, OperatorAction.CORRECT_REQUEST
    ),
    "credential_access_denied": ErrorDisposition(
        ErrorCategory.AUTHORIZATION, False, OperatorAction.CORRECT_REQUEST
    ),
    "artifact_access_denied": ErrorDisposition(
        ErrorCategory.AUTHORIZATION, False, OperatorAction.CORRECT_REQUEST
    ),
    "sandbox_violation": ErrorDisposition(
        ErrorCategory.AUTHORIZATION, False, OperatorAction.CORRECT_REQUEST
    ),
    "not_found": ErrorDisposition(
        ErrorCategory.REQUEST, False, OperatorAction.CORRECT_REQUEST
    ),
    "tool_schema_invalid": ErrorDisposition(
        ErrorCategory.REQUEST, False, OperatorAction.CORRECT_REQUEST
    ),
    "tool_schema_definition_invalid": ErrorDisposition(
        ErrorCategory.REQUEST, False, OperatorAction.CORRECT_REQUEST
    ),
    "version_conflict": ErrorDisposition(
        ErrorCategory.CONFLICT, False, OperatorAction.RESOLVE_CONFLICT
    ),
    "stale_capability_snapshot": ErrorDisposition(
        ErrorCategory.CONFLICT, False, OperatorAction.RESOLVE_CONFLICT
    ),
    "invalid_transition": ErrorDisposition(
        ErrorCategory.CONFLICT, False, OperatorAction.RESOLVE_CONFLICT
    ),
    "lease_conflict": ErrorDisposition(
        ErrorCategory.CONFLICT, True, OperatorAction.RETRY_WITH_BACKOFF
    ),
    "stale_fencing_token": ErrorDisposition(
        ErrorCategory.CONFLICT, False, OperatorAction.RESOLVE_CONFLICT
    ),
    "sync_invoke_busy": ErrorDisposition(
        ErrorCategory.CAPACITY, True, OperatorAction.RETRY_WITH_BACKOFF
    ),
    "resource_gateway_busy": ErrorDisposition(
        ErrorCategory.CAPACITY, True, OperatorAction.RETRY_WITH_BACKOFF
    ),
    "model_rate_limited": ErrorDisposition(
        ErrorCategory.CAPACITY, True, OperatorAction.RETRY_WITH_BACKOFF
    ),
    "service_draining": ErrorDisposition(
        ErrorCategory.CAPACITY, True, OperatorAction.RETRY_WITH_BACKOFF
    ),
    "model_authentication_failed": ErrorDisposition(
        ErrorCategory.DEPENDENCY, False, OperatorAction.INSPECT_DEPENDENCY
    ),
    "model_timeout": ErrorDisposition(
        ErrorCategory.DEPENDENCY, True, OperatorAction.RETRY_WITH_BACKOFF
    ),
    "model_provider_error": ErrorDisposition(
        ErrorCategory.DEPENDENCY, True, OperatorAction.INSPECT_DEPENDENCY
    ),
    "model_connect_error": ErrorDisposition(
        ErrorCategory.DEPENDENCY, True, OperatorAction.INSPECT_DEPENDENCY
    ),
    "model_read_error": ErrorDisposition(
        ErrorCategory.DEPENDENCY, True, OperatorAction.INSPECT_DEPENDENCY
    ),
    "model_protocol_error": ErrorDisposition(
        ErrorCategory.DEPENDENCY, False, OperatorAction.INSPECT_DEPENDENCY
    ),
}


def error_disposition(code: str, status_code: int) -> ErrorDisposition:
    exact = _CODE_DISPOSITIONS.get(code)
    if exact is not None:
        return exact
    if code.startswith("skill_content_"):
        return ErrorDisposition(
            ErrorCategory.AUTHORIZATION, False, OperatorAction.CORRECT_REQUEST
        )
    if code.startswith("runtime_") or code.startswith("agent_terminal_"):
        return ErrorDisposition(
            ErrorCategory.RUNTIME, False, OperatorAction.INSPECT_RUNTIME
        )
    if status_code == 401:
        return ErrorDisposition(
            ErrorCategory.AUTHENTICATION, False, OperatorAction.REFRESH_IDENTITY
        )
    if status_code == 403:
        return ErrorDisposition(
            ErrorCategory.AUTHORIZATION, False, OperatorAction.CORRECT_REQUEST
        )
    if status_code == 409:
        return ErrorDisposition(
            ErrorCategory.CONFLICT, False, OperatorAction.RESOLVE_CONFLICT
        )
    if status_code == 429:
        return ErrorDisposition(
            ErrorCategory.CAPACITY, True, OperatorAction.RETRY_WITH_BACKOFF
        )
    if 400 <= status_code < 500:
        return ErrorDisposition(
            ErrorCategory.REQUEST, False, OperatorAction.CORRECT_REQUEST
        )
    return ErrorDisposition(ErrorCategory.INTERNAL, True, OperatorAction.ESCALATE)


class FailureQueueState(StrEnum):
    PENDING = "pending"
    CLAIMED = "claimed"
    RETRY_WAIT = "retry_wait"
    QUARANTINED = "quarantined"
    DEAD_LETTERED = "dead_lettered"
    RECONCILING = "reconciling"
    COMPLETED = "completed"


@dataclass(frozen=True)
class FailureQueueContract:
    queue: str
    owner: str
    durable_source: str
    failure_state: FailureQueueState | None
    recovery_action: str
    lifecycle_events: tuple[str, ...]

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


FAILURE_QUEUE_CONTRACTS = (
    FailureQueueContract(
        queue="projection",
        owner="projection-worker",
        durable_source="canonical_session_events",
        failure_state=FailureQueueState.QUARANTINED,
        recovery_action="redrive_or_rebuild",
        lifecycle_events=("projection.poisoned", "projection.redriven", "projection.rebuilt"),
    ),
    FailureQueueContract(
        queue="delivery",
        owner="delivery-worker",
        durable_source="delivery_job",
        failure_state=FailureQueueState.DEAD_LETTERED,
        recovery_action="redrive_or_reconcile",
        lifecycle_events=(
            "delivery.retrying",
            "delivery.dead_lettered",
            "delivery.reconciling",
            "delivery.succeeded",
        ),
    ),
    FailureQueueContract(
        queue="skill_lifecycle",
        owner="action-hands",
        durable_source="skill_lifecycle_broadcast_outbox",
        failure_state=FailureQueueState.RETRY_WAIT,
        recovery_action="retry_or_snapshot_reconcile",
        lifecycle_events=("skill.lifecycle.snapshot_changed",),
    ),
    FailureQueueContract(
        queue="runtime_event",
        owner="streaming-gateway",
        durable_source="canonical_session_events",
        failure_state=None,
        recovery_action="reconnect_then_read_result_api",
        lifecycle_events=("runtime.event.expired", "runtime.connection.replayed"),
    ),
)


def operations_contract() -> dict[str, Any]:
    from auraclaw.contracts.errors import AuraClawError

    error_types: list[type[AuraClawError]] = []
    pending = list(AuraClawError.__subclasses__())
    while pending:
        error_type = pending.pop()
        error_types.append(error_type)
        pending.extend(error_type.__subclasses__())
    codes = sorted({str(error_type.code) for error_type in error_types})
    return {
        "schema_version": 1,
        "error_categories": [category.value for category in ErrorCategory],
        "operator_actions": [action.value for action in OperatorAction],
        "failure_queue_states": [state.value for state in FailureQueueState],
        "errors": [
            {
                "code": code,
                **asdict(
                    error_disposition(
                        code,
                        next(
                            error_type.status_code
                            for error_type in error_types
                            if error_type.code == code
                        ),
                    )
                ),
            }
            for code in codes
        ],
        "failure_queues": [contract.as_dict() for contract in FAILURE_QUEUE_CONTRACTS],
    }
