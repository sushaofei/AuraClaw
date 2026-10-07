"""Compatibility exports for Activity query folding rules."""

from auraclaw.projection.activity_view import (
    ACTIVITY_EVENT_TYPES,
    activity_node_id,
    build_activity,
    fold_activity_event,
    page_activity,
)

__all__ = [
    "ACTIVITY_EVENT_TYPES",
    "activity_node_id",
    "build_activity",
    "fold_activity_event",
    "page_activity",
]
