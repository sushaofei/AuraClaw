from __future__ import annotations

import hashlib
from typing import Any
from uuid import uuid4

from auraclaw.control.ports import RuntimeAssignment


def authority_request_id(assignment: RuntimeAssignment, operation: str) -> str:
    """A new logical read; transport retries reuse the resulting ToolCall."""
    scope = hashlib.sha256(
        "\0".join((assignment.tenant_id, assignment.session_id, assignment.run_id)).encode()
    ).hexdigest()[:16]
    return f"query_{operation}_{scope}_{uuid4().hex}"


def binding_disposition_result(result: dict[str, Any]) -> dict[str, Any]:
    content = result.get("content")
    payload = dict(content) if isinstance(content, dict) else dict(result)
    # Legacy in-process ports return the disposition directly; HTTP ports carry
    # an explicit ToolResult status. Neither path may infer permission on error.
    if result.get("status", "success") != "success" or payload.get("action") not in {
        "continue", "pause", "cancel"
    }:
        return {
            "action": "pause",
            "reason_code": "binding_status_unavailable",
            "policy_version": "skill-revocation-v1",
        }
    return payload
