from datetime import UTC, datetime

from auraclaw.contracts.events import Actor, CanonicalEvent
from auraclaw.contracts.state import Visibility
from auraclaw.runtime.event_committer import CanonicalEventCommitter


def _event(
    *,
    version: int,
    event_type: str,
    payload: dict,
    run_id: str | None = None,
) -> CanonicalEvent:
    return CanonicalEvent(
        event_id=f"evt_{version}",
        tenant_id="tenant-1",
        root_session_id="ses_1",
        session_id="ses_1",
        run_id=run_id,
        aggregate_version=version,
        type=event_type,
        occurred_at=datetime(2026, 8, 27, 10, 0, version, tzinfo=UTC),
        actor=Actor(type="runtime", id="runtime-1"),
        correlation_id="corr",
        causation_id="cause",
        visibility=Visibility.USER,
        schema_version=1,
        payload=payload,
    )


def test_approval_request_is_pending_until_terminal_decision() -> None:
    existing = [
        _event(
            version=1,
            event_type="approval.requested",
            payload={"approval_id": "apr_1"},
            run_id="run-1",
        )
    ]
    assert CanonicalEventCommitter.approval_request_is_pending(existing, "apr_1") is True

    existing.append(
        _event(
            version=2,
            event_type="approval.approved",
            payload={"approval_id": "apr_1", "decision": "approved"},
            run_id="run-1",
        )
    )
    assert CanonicalEventCommitter.approval_request_is_pending(existing, "apr_1") is False


def test_approval_request_is_pending_allows_same_id_after_rejection() -> None:
    existing = [
        _event(
            version=1,
            event_type="approval.requested",
            payload={"approval_id": "apr_1"},
            run_id="run-1",
        ),
        _event(
            version=2,
            event_type="approval.rejected",
            payload={"approval_id": "apr_1", "decision": "rejected"},
            run_id="run-1",
        ),
        _event(
            version=3,
            event_type="approval.requested",
            payload={"approval_id": "apr_1"},
            run_id="run-2",
        ),
    ]
    assert CanonicalEventCommitter.approval_request_is_pending(existing, "apr_1") is True
