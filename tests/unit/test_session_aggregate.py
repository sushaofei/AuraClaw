import pytest

from auraclaw.contracts.errors import InvalidTransitionError
from auraclaw.contracts.state import SessionStatus
from auraclaw.domain.session import SessionAggregate


def test_create_session_emits_fact_and_run_request() -> None:
    session = SessionAggregate.empty("ses_1", "tenant_1")

    session.create(goal="build a report", run_id="run_1")

    events = session.release_pending_events()
    assert [event.type for event in events] == ["session.created", "run.requested"]
    assert session.status is SessionStatus.PENDING
    assert session.run_id == "run_1"


def test_terminal_session_cannot_be_cancelled_twice() -> None:
    session = SessionAggregate.empty("ses_1", "tenant_1")
    session.create(goal="build a report", run_id="run_1")
    session.release_pending_events()
    session.cancel("stop")

    with pytest.raises(InvalidTransitionError):
        session.cancel("stop again")
