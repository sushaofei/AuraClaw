from __future__ import annotations

from auraclaw.projection.task.listing import page_task_views


def _view(
    session_id: str,
    *,
    source: str = "chat",
    status: str = "ready",
    projected_at: str,
    role: str = "root",
) -> dict[str, object]:
    return {
        "session_id": session_id,
        "source": source,
        "status": status,
        "projected_at": projected_at,
        "role": role,
        "goal": session_id,
    }


def test_page_task_views_filters_roots_and_pages_newest_first() -> None:
    views = [
        _view("ses_old_chat", projected_at="2026-08-24T01:00:00+00:00"),
        _view("ses_new_chat", projected_at="2026-08-24T03:00:00+00:00"),
        _view(
            "ses_schedule",
            source="schedule",
            projected_at="2026-08-24T04:00:00+00:00",
        ),
        _view(
            "ses_child",
            projected_at="2026-08-24T05:00:00+00:00",
            role="worker",
        ),
    ]

    first = page_task_views(views, kind="chat", status=None, cursor=None, limit=1)
    assert [item["session_id"] for item in first["tasks"]] == ["ses_new_chat"]
    assert first["next_cursor"] is not None

    second = page_task_views(
        views, kind="chat", status=None, cursor=first["next_cursor"], limit=1
    )
    assert [item["session_id"] for item in second["tasks"]] == ["ses_old_chat"]
    assert second["next_cursor"] is None

    scheduled = page_task_views(
        views, kind="scheduled", status=None, cursor=None, limit=10
    )
    assert [item["session_id"] for item in scheduled["tasks"]] == ["ses_schedule"]
