import pytest

from auraclaw.contracts.errors import InvalidTransitionError
from auraclaw.contracts.state import RunStatus, SessionStatus
from auraclaw.domain.session import SessionAggregate


def test_create_session_emits_fact_and_run_request() -> None:
    session = SessionAggregate.empty("ses_1", "tenant_1")

    session.create(goal="build a report", run_id="run_1")

    events = session.release_pending_events()
    assert [event.type for event in events] == ["session.created", "run.requested"]
    assert session.status is SessionStatus.PENDING
    assert session.run_id == "run_1"


def test_create_session_freezes_department_snapshot() -> None:
    session = SessionAggregate.empty("ses_1", "tenant_1")
    session.create(goal="price insight", run_id="run_1", dept_id="9")

    events = session.release_pending_events()
    assert events[0].payload["dept_id"] == "9"
    assert events[1].payload["dept_id"] == "9"
    assert session.dept_id == "9"
    session.apply("run.completed", {"result_summary": "done"})
    session.request_run("run_2")
    assert session.release_pending_events()[0].payload["dept_id"] == "9"
    session.apply("session.paused", {})
    session.resume("run_3")
    assert session.release_pending_events()[0].payload["dept_id"] == "9"


def test_terminal_session_cannot_be_cancelled_twice() -> None:
    session = SessionAggregate.empty("ses_1", "tenant_1")
    session.create(goal="build a report", run_id="run_1")
    session.release_pending_events()
    session.cancel("stop")

    with pytest.raises(InvalidTransitionError):
        session.cancel("stop again")


def test_root_run_completion_returns_session_to_ready_until_explicit_close() -> None:
    session = SessionAggregate.empty("ses_1", "tenant_1")
    session.create(goal="multi turn chat", run_id="run_1")
    session.release_pending_events()
    session.apply("run.completed", {"result_summary": "first answer"})

    assert session.status is SessionStatus.READY
    assert session.run_status is RunStatus.COMPLETED

    session.append_message(message="follow up")
    session.request_run("run_2")
    assert [event.type for event in session.release_pending_events()] == [
        "user.message.appended",
        "run.requested",
    ]
    assert session.status is SessionStatus.PENDING
    assert session.run_status is RunStatus.PENDING

    session.apply("run.completed", {"result_summary": "second answer"})
    session.close("done")
    assert session.status is SessionStatus.CLOSED
    with pytest.raises(InvalidTransitionError):
        session.append_message(message="too late")


def test_legacy_root_terminal_snapshot_is_restored_as_ready() -> None:
    restored = SessionAggregate.from_snapshot(
        {
            "session_id": "ses_legacy",
            "root_session_id": "ses_legacy",
            "tenant_id": "tenant_1",
            "status": "completed",
            "goal": "legacy chat",
            "run_id": "run_legacy",
            "role": "root",
            "result_summary": "answer",
        },
        version=5,
    )

    assert restored.status is SessionStatus.READY
    assert restored.run_status is RunStatus.COMPLETED
