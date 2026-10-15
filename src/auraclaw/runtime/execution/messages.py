from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from typing import Any

from auraclaw.runtime.ports import ModelRequest, ToolCall


def model_input_evidence(request: ModelRequest, *, turn_index: int) -> dict[str, Any]:
    roles: dict[str, int] = {}
    user_prompt_preview = ""
    for message in request.messages:
        role = str(message.get("role", "unknown"))
        roles[role] = roles.get(role, 0) + 1
        if role == "user" and isinstance(message.get("content"), str):
            user_prompt_preview = " ".join(str(message["content"]).split())[:800]
    tool_names: list[str] = []
    for tool in request.tools:
        function = tool.get("function")
        if isinstance(function, dict) and function.get("name"):
            tool_names.append(str(function["name"]))
    encoded = json.dumps(
        {"messages": request.messages, "tools": request.tools},
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        default=str,
    ).encode()
    tool_encoded = json.dumps(
        request.tools,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        default=str,
    ).encode()
    return {
        "model_call_id": request.model_call_id,
        "turn_index": turn_index,
        "message_count": len(request.messages),
        "role_counts": roles,
        "user_prompt_preview": user_prompt_preview,
        "input_digest": f"sha256:{hashlib.sha256(encoded).hexdigest()}",
        "tool_schema_digest": f"sha256:{hashlib.sha256(tool_encoded).hexdigest()}",
        "tool_names": tool_names,
        "preferred_model": request.policy.preferred_model,
        "allowed_providers": list(request.policy.allowed_providers),
        "data_classification": request.policy.data_classification,
        "max_output_tokens": request.max_output_tokens,
        "trusted_instruction_count": roles.get("system", 0),
    }


def tool_activity_metadata(capability_state: dict[str, Any], call: ToolCall) -> dict[str, Any]:
    for capability_id, item in dict(capability_state.get("loaded", {})).items():
        if not isinstance(item, dict):
            continue
        if item.get("kind") != "tool":
            continue
        model_name = item.get("model_tool", {}).get("function", {}).get("name")
        if call.name not in {model_name, item.get("canonical_name")}:
            continue
        server_id = str(item.get("server_id", ""))
        return {
            "source": "mcp" if server_id else "catalog",
            **(
                {
                    "canonical_name": item.get("canonical_name"),
                    "invocation_ref": item["invocation_ref"],
                }
                if isinstance(item.get("invocation_ref"), dict)
                else {}
            ),
            "capability_id": str(item.get("capability_id") or capability_id),
            "kind": "tool",
            "server_id": server_id or None,
            "version": str(item.get("version") or call.version),
        }
    return {
        "source": "auraclaw" if call.name.startswith("auraclaw.") else "tool",
        "version": call.version,
    }


def build_messages(events: list[Any]) -> tuple[dict[str, Any], ...]:
    messages: list[dict[str, Any]] = []
    for event in events:
        if event.type == "session.created":
            messages.append({"role": "user", "content": event.payload.get("goal", "")})
        elif event.type == "child.created":
            child_context = {
                "goal": event.payload.get("goal", ""),
                "output_contract": event.payload.get("output_contract", {}),
                "input_refs": event.payload.get("input_refs", []),
                "tool_permissions": event.payload.get("tool_permissions", []),
            }
            messages.append(
                {
                    "role": "user",
                    "content": (
                        "Authoritative Child assignment:\n"
                        + json.dumps(
                            child_context,
                            ensure_ascii=False,
                            sort_keys=True,
                            separators=(",", ":"),
                        )
                    ),
                }
            )
        elif event.type == "user.message.appended":
            messages.append({"role": "user", "content": event.payload.get("message", "")})
        elif event.type == "model.output.completed":
            messages.append({"role": "assistant", "content": event.payload.get("output", "")})
    return tuple(messages)


def build_capability_messages(
    events: list[Any],
    *,
    current_run_id: str | None = None,
) -> tuple[dict[str, Any], ...]:
    del current_run_id
    messages: list[dict[str, Any]] = []
    for event in events:
        if event.type == "session.created":
            messages.append({"role": "user", "content": event.payload.get("goal", "")})
        elif event.type == "child.created":
            child_context = {
                "goal": event.payload.get("goal", ""),
                "output_contract": event.payload.get("output_contract", {}),
                "input_refs": event.payload.get("input_refs", []),
                "tool_permissions": event.payload.get("tool_permissions", []),
            }
            messages.append(
                {
                    "role": "user",
                    "content": (
                        "Authoritative Child assignment:\n"
                        + json.dumps(
                            child_context,
                            ensure_ascii=False,
                            sort_keys=True,
                            separators=(",", ":"),
                        )
                    ),
                }
            )
        elif event.type == "user.message.appended":
            messages.append(
                {
                    "role": "user",
                    "content": event.payload.get("message", ""),
                }
            )
        elif event.type == "model.turn.completed":
            if str(event.payload.get("purpose", "")).startswith("router_semantic_plan_"):
                continue
            calls = []
            for raw_call in event.payload.get("tool_calls", ()):
                if not isinstance(raw_call, dict):
                    continue
                calls.append(
                    {
                        "id": str(raw_call["tool_invocation_id"]),
                        "type": "function",
                        "function": {
                            "name": str(raw_call["name"]),
                            "arguments": json.dumps(
                                raw_call.get("arguments", {}),
                                sort_keys=True,
                                separators=(",", ":"),
                            ),
                        },
                    }
                )
            message: dict[str, Any] = {
                "role": "assistant",
                "content": str(event.payload.get("output", "")),
            }
            if calls:
                message["tool_calls"] = calls
            messages.append(message)
        elif event.type == "tool.call.completed":
            messages.append(
                {
                    "role": "tool",
                    "tool_call_id": str(event.payload.get("tool_invocation_id", "")),
                    "content": json.dumps(
                        event.payload.get("result", {}),
                        sort_keys=True,
                        separators=(",", ":"),
                        default=str,
                    ),
                }
            )
        elif event.type == "model.output.completed":
            messages.append(
                {
                    "role": "assistant",
                    "content": event.payload.get("output", ""),
                }
            )
    return tuple(messages)


def scope_model_call_id_to_request(request: ModelRequest) -> ModelRequest:
    """Keep retries idempotent without colliding after the logical turn changes.

    A coordinator can resume the same turn and fencing epoch after a child or
    capability changes the canonical prompt.  The Model Gateway correctly
    rejects reusing an idempotency key for that different request, so bind the
    durable call id to the request content as well as the logical turn.
    """
    digest = hashlib.sha256(
        json.dumps(
            {
                "run_id": request.run_id,
                "session_id": request.session_id,
                "messages": request.messages,
                "tools": request.tools,
                "capability": request.policy.capability,
                "preferred_model": request.policy.preferred_model,
                "allowed_providers": request.policy.allowed_providers,
                "data_classification": request.policy.data_classification,
                "max_output_tokens": request.max_output_tokens,
                "run_max_cost": request.run_max_cost,
            },
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        ).encode()
    ).hexdigest()[:16]
    return replace(request, model_call_id=f"{request.model_call_id}_req_{digest}")


def truncation_recovery_messages() -> tuple[dict[str, Any], ...]:
    return (
        {
            "role": "system",
            "content": (
                "The previous model output was truncated before the required "
                "terminal collaboration tool call. Do not repeat or continue the "
                "large body. Persist any large payload through an authorized "
                "Artifact or Resource write capability, then call the required "
                "publish_result, publish_review, or join tool with a short summary "
                "and valid references. If no governed write path exists, publish "
                "an explicit limitation or failure instead of inventing a "
                "reference."
            ),
        },
    )
