from __future__ import annotations

import json
import re
from typing import Any

from auraclaw.runtime.ports import ModelRequest, ModelResponse, ToolCall

_SKILL_NAME = "procurement.price-insight.generate"
_DATA_QUALITY_TOOL = "procurement.price_insight.data_quality"
_SNAPSHOT_TOOL = "procurement.price_insight.snapshot"


class DevelopmentPriceInsightModel:
    """Deterministic local model used to exercise the real governed Agent Loop."""

    async def generate(self, request: ModelRequest) -> ModelResponse:
        called = _called_tools(request.messages)
        available = _available_tools(request.tools)
        arguments: dict[str, Any]
        name: str

        if "auraclaw.capabilities.search" not in called:
            name = "auraclaw.capabilities.search"
            arguments = {
                "query": "采购 价格洞察 行业均价",
                "kinds": ["skill"],
                "limit": 5,
            }
        elif "auraclaw.capabilities.load" not in called:
            skill_id = _price_skill_id(request.messages)
            if skill_id is None:
                name = "auraclaw.capabilities.search"
                arguments = {
                    "query": "procurement.price-insight.generate",
                    "kinds": ["skill"],
                    "limit": 5,
                }
            else:
                name = "auraclaw.capabilities.load"
                arguments = {"capability_ids": [skill_id]}
        elif "auraclaw.skills.activate" not in called:
            skill_id = _price_skill_id(request.messages)
            if skill_id is None:
                raise RuntimeError(
                    "Price Insight Skill disappeared after capability load"
                )
            name = "auraclaw.skills.activate"
            arguments = {
                "capability_id": skill_id,
                "inputs": {},
            }
        elif _DATA_QUALITY_TOOL in available and _DATA_QUALITY_TOOL not in called:
            name = _DATA_QUALITY_TOOL
            arguments = {"filter": _filter_from_goal(request.messages)}
        elif _SNAPSHOT_TOOL in available and _SNAPSHOT_TOOL not in called:
            name = _SNAPSHOT_TOOL
            arguments = {"filter": _filter_from_goal(request.messages)}
        else:
            snapshot = _latest_tool_content(request.messages, _SNAPSHOT_TOOL)
            kpis = snapshot.get("kpis", [])
            source_revision = snapshot.get("filter", {}).get(
                "source_revision", "unknown"
            )
            output = (
                f"价格洞察已完成：数据源 {source_revision}，"
                f"数据质量 {snapshot.get('data_quality', {}).get('status', 'unknown')}，"
                f"已生成 {len(kpis) if isinstance(kpis, list) else 0} 项关键指标。"
            )
            return ModelResponse(
                model_call_id=request.model_call_id,
                provider="development-scripted",
                model="price-insight-loop-v1",
                completed_output=output,
                deltas=(output,),
                usage={"output_tokens": 32},
            )

        return ModelResponse(
            model_call_id=request.model_call_id,
            provider="development-scripted",
            model="price-insight-loop-v1",
            completed_output="",
            tool_calls=(
                ToolCall(
                    tool_invocation_id=f"{request.model_call_id}_{name.replace('.', '_')}",
                    name=name,
                    arguments=arguments,
                ),
            ),
            finish_reason="tool_calls",
            usage={"output_tokens": 8},
        )


def _called_tools(messages: tuple[dict[str, Any], ...]) -> set[str]:
    called: set[str] = set()
    for message in messages:
        if message.get("role") != "assistant":
            continue
        for item in message.get("tool_calls", ()):
            if not isinstance(item, dict):
                continue
            function = item.get("function")
            if isinstance(function, dict) and function.get("name"):
                called.add(str(function["name"]))
    return called


def _available_tools(tools: tuple[dict[str, Any], ...]) -> set[str]:
    available: set[str] = set()
    for item in tools:
        function = item.get("function")
        if isinstance(function, dict) and function.get("name"):
            available.add(str(function["name"]))
    return available


def _tool_payloads(messages: tuple[dict[str, Any], ...]) -> list[dict[str, Any]]:
    payloads: list[dict[str, Any]] = []
    for message in messages:
        if message.get("role") != "tool":
            continue
        try:
            payload = json.loads(str(message.get("content", "{}")))
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict):
            payloads.append(payload)
    return payloads


def _price_skill_id(messages: tuple[dict[str, Any], ...]) -> str | None:
    for payload in _tool_payloads(messages):
        content = payload.get("content")
        selected = content if isinstance(content, dict) else payload
        for item in selected.get("capabilities", ()):
            if (
                isinstance(item, dict)
                and item.get("kind") == "skill"
                and item.get("canonical_name") == _SKILL_NAME
            ):
                return str(item["capability_id"])
    return None


def _latest_tool_content(
    messages: tuple[dict[str, Any], ...],
    tool_name: str,
) -> dict[str, Any]:
    call_ids: set[str] = set()
    for message in messages:
        if message.get("role") != "assistant":
            continue
        for item in message.get("tool_calls", ()):
            if not isinstance(item, dict):
                continue
            function = item.get("function")
            if isinstance(function, dict) and function.get("name") == tool_name:
                call_ids.add(str(item.get("id", "")))
    for message in reversed(messages):
        if message.get("role") != "tool":
            continue
        if str(message.get("tool_call_id", "")) not in call_ids:
            continue
        try:
            payload = json.loads(str(message.get("content", "{}")))
        except json.JSONDecodeError:
            return {}
        if not isinstance(payload, dict):
            return {}
        content = payload.get("content")
        return content if isinstance(content, dict) else payload
    return {}


def _filter_from_goal(messages: tuple[dict[str, Any], ...]) -> dict[str, Any]:
    goal = next(
        (
            str(message.get("content", ""))
            for message in messages
            if message.get("role") == "user"
        ),
        "",
    )
    periods = re.search(r"(\d{4}-\d{2})\s*至\s*(\d{4}-\d{2})", goal)
    threshold = re.search(r"偏离阈值为\s*([0-9]+(?:\.[0-9]+)?)%", goal)
    anchor = (
        "history"
        if "主要对标 历史价格" in goal
        else "region"
        if "主要对标 跨区域价格" in goal
        else "market"
    )
    return {
        "period_from": periods.group(1) if periods else "2026-01",
        "period_to": periods.group(2) if periods else "2026-02",
        "anchor": anchor,
        "deviation_threshold_pct": float(threshold.group(1)) if threshold else 8,
    }
