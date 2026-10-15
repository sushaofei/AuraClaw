from __future__ import annotations

from typing import Any

from auraclaw.runtime.execution.contracts import FinishReasonKind
from auraclaw.runtime.ports import ModelResponse, ToolCall


def classify_finish_reason(response: ModelResponse) -> FinishReasonKind:
    if response.tool_calls:
        return FinishReasonKind.TOOL_CALLS
    reason = response.finish_reason.strip().lower().replace("-", "_")
    if reason in {"stop", "end_turn", "completed"}:
        return FinishReasonKind.STOP
    if reason in {"tool_calls", "function_call", "function_calls"}:
        return FinishReasonKind.TOOL_CALLS
    if reason in {"length", "max_tokens", "max_output_tokens"}:
        return FinishReasonKind.TRUNCATED
    if reason in {"content_filter", "safety", "refusal", "refused"}:
        return FinishReasonKind.REFUSED
    return FinishReasonKind.UNKNOWN


def tool_call_to_dict(call: ToolCall) -> dict[str, Any]:
    return {
        "tool_invocation_id": call.tool_invocation_id,
        "name": call.name,
        "arguments": call.arguments,
        "version": call.version,
        "expected_side_effect": call.expected_side_effect,
        "approval_id": call.approval_id,
        "credential_ref": call.credential_ref,
        "idempotency_key": call.idempotency_key,
    }


def response_to_dict(response: ModelResponse) -> dict[str, Any]:
    return {
        "model_call_id": response.model_call_id,
        "provider": response.provider,
        "model": response.model,
        "completed_output": response.completed_output,
        "deltas": list(response.deltas),
        "tool_calls": [
            {
                "tool_invocation_id": call.tool_invocation_id,
                "name": call.name,
                "arguments": call.arguments,
                "version": call.version,
                "expected_side_effect": call.expected_side_effect,
                "approval_id": call.approval_id,
                "credential_ref": call.credential_ref,
                "idempotency_key": call.idempotency_key,
            }
            for call in response.tool_calls
        ],
        "finish_reason": response.finish_reason,
        "usage": response.usage,
    }


def response_from_dict(data: dict[str, Any]) -> ModelResponse:
    return ModelResponse(
        model_call_id=str(data["model_call_id"]),
        provider=str(data["provider"]),
        model=str(data["model"]),
        completed_output=str(data["completed_output"]),
        deltas=tuple(str(delta) for delta in data.get("deltas", [])),
        tool_calls=tuple(
            ToolCall(
                tool_invocation_id=str(call["tool_invocation_id"]),
                name=str(call["name"]),
                arguments=dict(call.get("arguments", {})),
                version=str(call.get("version", "1")),
                expected_side_effect=str(call.get("expected_side_effect", "read")),
                approval_id=call.get("approval_id"),
                credential_ref=call.get("credential_ref"),
                idempotency_key=call.get("idempotency_key"),
            )
            for call in data.get("tool_calls", [])
        ),
        finish_reason=str(data.get("finish_reason", "stop")),
        usage=dict(data.get("usage", {})),
    )
