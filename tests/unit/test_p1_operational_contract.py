from fastapi.testclient import TestClient

from auraclaw.composition.api import create_app
from auraclaw.composition.providers import get_settings
from auraclaw.contracts.operations import (
    ErrorCategory,
    OperatorAction,
    error_disposition,
    operations_contract,
)


def test_error_dispositions_are_stable_and_actionable() -> None:
    assert error_disposition("version_conflict", 409).category == ErrorCategory.CONFLICT
    assert error_disposition("model_timeout", 504).retryable is True
    assert (
        error_disposition("skill_content_secret", 403).operator_action
        == OperatorAction.CORRECT_REQUEST
    )
    assert error_disposition("new_server_failure", 500).operator_action == OperatorAction.ESCALATE


def test_operational_contract_covers_declared_errors_and_owned_failure_queues() -> None:
    contract = operations_contract()
    codes = {item["code"] for item in contract["errors"]}
    assert {
        "not_found",
        "authorization_denied",
        "runtime_deadline_exceeded",
        "model_provider_error",
    } <= codes
    queues = {item["queue"]: item for item in contract["failure_queues"]}
    assert queues["projection"]["failure_state"] == "quarantined"
    assert queues["delivery"]["failure_state"] == "dead_lettered"
    assert queues["skill_lifecycle"]["owner"] == "action-hands"
    assert queues["runtime_event"]["failure_state"] is None
    assert queues["runtime_event"]["durable_source"] == "canonical_session_events"


def test_public_error_and_operations_contract_share_taxonomy() -> None:
    settings = get_settings()
    settings.storage_backend = "memory"
    settings.allow_insecure_identity_headers = True
    with TestClient(create_app(profile="task-api")) as client:
        missing = client.get(
            "/v1/tasks/missing-session",
            headers={"X-Tenant-ID": "tenant-contract"},
        )
        assert missing.status_code == 404
        payload = missing.json()
        assert payload["code"] == "not_found"
        assert payload["category"] == "request"
        assert payload["retryable"] is False
        assert payload["operator_action"] == "correct_request"
        assert len(payload["trace_id"]) == 32

        contract = client.get(
            "/v1/operations/contract",
            headers={"X-Tenant-ID": "tenant-contract"},
        )
        assert contract.status_code == 200
        assert contract.json()["schema_version"] == 1
        assert contract.json()["tenant_id"] == "tenant-contract"
