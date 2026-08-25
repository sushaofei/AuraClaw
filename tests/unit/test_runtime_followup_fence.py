from datetime import UTC, datetime, timedelta

from auraclaw.contracts.internal import LeaseAssertion
from auraclaw.control.ports import RuntimeAssignment
from auraclaw.infrastructure.clients.runtime import select_assignment_for_fence


def _assignment(
    run_id: str,
    *,
    expires_at: datetime,
    fencing_token: int = 1,
) -> RuntimeAssignment:
    return RuntimeAssignment(
        tenant_id="local",
        root_session_id="ses_1",
        session_id="ses_1",
        run_id=run_id,
        runtime_id="runtime-1",
        lease_id=f"lea_{run_id}",
        fencing_token=fencing_token,
        role="root",
        resource_profile={},
        lease_assertion=LeaseAssertion(
            key_id="test",
            audience="runtime",
            tenant_id="local",
            session_id="ses_1",
            run_id=run_id,
            runtime_id="runtime-1",
            lease_id=f"lea_{run_id}",
            fencing_token=fencing_token,
            expires_at=expires_at,
            signature="sig",
        ),
    )


def test_select_assignment_for_fence_skips_expired_prior_run() -> None:
    now = datetime(2026, 8, 25, 1, 30, tzinfo=UTC)
    cache = {
        ("local", "ses_1", "run_old"): (
            "task-old",
            _assignment("run_old", expires_at=now - timedelta(minutes=10)),
        ),
        ("local", "ses_1", "run_new"): (
            "task-new",
            _assignment("run_new", expires_at=now + timedelta(minutes=5)),
        ),
    }
    selected = select_assignment_for_fence(
        cache, "session:local:ses_1", 1, now=now
    )
    assert selected is not None
    assert selected.run_id == "run_new"


def test_select_assignment_for_fence_returns_none_when_all_expired() -> None:
    now = datetime(2026, 8, 25, 1, 30, tzinfo=UTC)
    cache = {
        ("local", "ses_1", "run_old"): (
            "task-old",
            _assignment("run_old", expires_at=now - timedelta(seconds=1)),
        ),
    }
    assert (
        select_assignment_for_fence(cache, "session:local:ses_1", 1, now=now) is None
    )
