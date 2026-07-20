from __future__ import annotations

import json
import re
from collections.abc import Sequence
from typing import Any

_SENSITIVE_KEY = re.compile(
    r"(^|_)(authorization|cookie|credential|password|secret|token|api_key|private_key)($|_)",
    re.IGNORECASE,
)
_BEARER = re.compile(r"(?i)bearer\s+[a-z0-9._~+/=-]+")
_PRIVATE_KEY = re.compile(r"-----BEGIN [A-Z ]*PRIVATE KEY-----")


def redact_sensitive(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            str(key): "[REDACTED]" if _SENSITIVE_KEY.search(str(key)) else redact_sensitive(item)
            for key, item in value.items()
        }
    if isinstance(value, (list, tuple)):
        return [redact_sensitive(item) for item in value]
    if isinstance(value, str):
        return _PRIVATE_KEY.sub("[REDACTED]", _BEARER.sub("Bearer [REDACTED]", value))
    return value


def contains_sensitive(value: Any, *, known_secrets: Sequence[str] = ()) -> bool:
    serialized = json.dumps(value, default=str, sort_keys=True)
    if _PRIVATE_KEY.search(serialized) or _BEARER.search(serialized):
        return True
    if any(secret and secret in serialized for secret in known_secrets):
        return True
    if isinstance(value, dict):
        return any(
            (_SENSITIVE_KEY.search(str(key)) and item != "[REDACTED]")
            or contains_sensitive(item, known_secrets=known_secrets)
            for key, item in value.items()
        )
    if isinstance(value, (list, tuple)):
        return any(contains_sensitive(item, known_secrets=known_secrets) for item in value)
    return False
