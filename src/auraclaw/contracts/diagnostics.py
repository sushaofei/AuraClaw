from __future__ import annotations

import re
from typing import Any


def safe_error_text(value: Any, *, limit: int = 1024) -> str:
    """Keep actionable remote messages without copying credential or debug payloads."""
    text = str(value)[:8192]
    if re.search(r"(?i)traceback|\bselect\b.{0,120}\bfrom\b|\binsert\s+into\b", text):
        return "Remote operation failed; consult the correlated diagnostic record"
    text = re.sub(r"(?i)\bBearer\s+\S+", "Bearer [REDACTED]", text)
    text = re.sub(r"(?i)(?:https?://)\S+", "[URL]", text)
    text = re.sub(
        r"(?i)((?:api[_-]?key|access[_-]?token|refresh[_-]?token|password|secret|authorization)"
        r"[\s\"']*[:=][\s\"']*)[^\s,;}]+",
        r"\1[REDACTED]",
        text,
    )
    return text[:limit]
