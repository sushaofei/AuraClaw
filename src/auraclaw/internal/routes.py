from __future__ import annotations

from auraclaw.contracts.internal import (
    CancellationRequest,
    CancellationResponse,
    CheckpointResponse,
    LoadCheckpointRequest,
    SaveCheckpointRequest,
    SessionAppendRequest,
    SessionAppendResponse,
    SessionFeedRequest,
    SessionFeedResponse,
    ValidateLeaseRequest,
    ValidateLeaseResponse,
)
from auraclaw.control.internal_service import ControlInternalService
from auraclaw.internal.http import ContractRoute, contract_route
from auraclaw.session.internal_service import SessionInternalService


def session_routes(service: SessionInternalService) -> dict[str, ContractRoute]:
    return {
        "/internal/v1/session/append": contract_route(
            SessionAppendRequest, SessionAppendResponse, service.append
        ),
        "/internal/v1/session/feed": contract_route(
            SessionFeedRequest, SessionFeedResponse, service.feed
        ),
    }


def control_routes(service: ControlInternalService) -> dict[str, ContractRoute]:
    return {
        "/internal/v1/control/checkpoints/save": contract_route(
            SaveCheckpointRequest, CheckpointResponse, service.save_checkpoint
        ),
        "/internal/v1/control/checkpoints/load": contract_route(
            LoadCheckpointRequest, CheckpointResponse, service.load_checkpoint
        ),
        "/internal/v1/control/cancellation/request": contract_route(
            CancellationRequest, CancellationResponse, service.request_cancel
        ),
        "/internal/v1/control/cancellation/status": contract_route(
            CancellationRequest, CancellationResponse, service.is_cancelled
        ),
        "/internal/v1/control/leases/validate": contract_route(
            ValidateLeaseRequest, ValidateLeaseResponse, service.validate_lease
        ),
    }
