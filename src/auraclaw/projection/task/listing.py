from __future__ import annotations

import base64
from datetime import datetime
from typing import Any


def encode_task_cursor(*, projected_at: str, session_id: str) -> str:
    raw = f"{projected_at}|{session_id}"
    return base64.urlsafe_b64encode(raw.encode()).decode().rstrip("=")


def decode_task_cursor(cursor: str) -> tuple[str, str]:
    padding = "=" * (-len(cursor) % 4)
    raw = base64.urlsafe_b64decode(f"{cursor}{padding}").decode()
    projected_at, session_id = raw.split("|", 1)
    return projected_at, session_id


def source_for_kind(kind: str | None) -> str | None:
    if kind in {None, "", "chat"}:
        return None if kind in {None, ""} else "chat"
    if kind == "scheduled":
        return "schedule"
    raise ValueError(f"unsupported task kind: {kind}")


def page_task_views(
    views: list[dict[str, Any]],
    *,
    kind: str | None,
    status: str | None,
    cursor: str | None,
    limit: int,
) -> dict[str, Any]:
    source = source_for_kind(kind)
    cursor_at: datetime | None = None
    cursor_id: str | None = None
    if cursor:
        projected_at, cursor_id = decode_task_cursor(cursor)
        cursor_at = datetime.fromisoformat(projected_at)

    def sort_key(view: dict[str, Any]) -> tuple[str, str]:
        return (str(view.get("projected_at") or ""), str(view["session_id"]))

    filtered: list[dict[str, Any]] = []
    for view in views:
        if view.get("role", "root") != "root":
            continue
        if source is not None and view.get("source", "chat") != source:
            continue
        if status is not None and view.get("status") != status:
            continue
        if cursor_at is not None and cursor_id is not None:
            item_at = datetime.fromisoformat(str(view.get("projected_at")))
            if (item_at, view["session_id"]) >= (cursor_at, cursor_id):
                continue
        filtered.append(view)
    filtered.sort(key=sort_key, reverse=True)
    page = filtered[: limit + 1]
    next_cursor = None
    if len(page) > limit:
        last = page[limit - 1]
        next_cursor = encode_task_cursor(
            projected_at=str(last["projected_at"]),
            session_id=str(last["session_id"]),
        )
        page = page[:limit]
    return {"tasks": page, "next_cursor": next_cursor}
