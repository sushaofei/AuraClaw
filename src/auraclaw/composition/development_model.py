from __future__ import annotations

import json
import re
from typing import Any

from auraclaw.runtime.ports import ModelRequest, ModelResponse, ToolCall

_SKILL_NAME = "procurement.price-insight.generate"
_SCOPE_PROFILE_TOOL = "procurement.price.dataset.profile"
_QUALITY_CHECK_TOOL = "procurement.price.dataset.quality.check"
_METRIC_KEYS = (
    "history_dev_pct",
    "region_gap_max",
    "market_dev_pct",
    "impact_amount",
    "impact_neg_amount",
    "impact_share_pct",
    "impact_neg_share_pct",
    "deviation_cnt",
)
_METRIC_TOOLS = {
    "history_dev_pct": "procurement.price.metric.history-deviation.compute",
    "region_gap_max": "procurement.price.metric.region-max-gap.compute",
    "market_dev_pct": "procurement.price.metric.market-deviation.compute",
    "impact_amount": "procurement.price.metric.positive-impact-amount.compute",
    "impact_neg_amount": "procurement.price.metric.negative-impact-amount.compute",
    "impact_share_pct": "procurement.price.metric.positive-impact-share.compute",
    "impact_neg_share_pct": "procurement.price.metric.negative-impact-share.compute",
    "deviation_cnt": "procurement.price.metric.market-deviation-count.compute",
}


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
        elif _SCOPE_PROFILE_TOOL in available and _SCOPE_PROFILE_TOOL not in called:
            name = _SCOPE_PROFILE_TOOL
            arguments = {"filter": _filter_from_goal(request.messages)}
        elif _QUALITY_CHECK_TOOL in available and _QUALITY_CHECK_TOOL not in called:
            name = _QUALITY_CHECK_TOOL
            arguments = {"filter": _filter_from_goal(request.messages)}
        elif (
            _QUALITY_CHECK_TOOL in called
            and _latest_tool_content(request.messages, _QUALITY_CHECK_TOOL).get("status")
            == "blocked"
        ):
            quality = _latest_tool_content(request.messages, _QUALITY_CHECK_TOOL)
            output = (
                "价格洞察已被数据质量门禁阻断；请先修复："
                f"{json.dumps(quality.get('findings', []), ensure_ascii=False)}"
            )
            return ModelResponse(
                model_call_id=request.model_call_id,
                provider="development-scripted",
                model="price-insight-loop-v3",
                completed_output=output,
                deltas=(output,),
                usage={"output_tokens": 32},
            )
        elif (
            _QUALITY_CHECK_TOOL in called
            and len(_price_source_revisions(request.messages)) > 1
        ):
            revisions = sorted(_price_source_revisions(request.messages))
            output = (
                "价格洞察检测到数据版本漂移，已停止拼接跨版本指标："
                f"{', '.join(revisions)}"
            )
            return ModelResponse(
                model_call_id=request.model_call_id,
                provider="development-scripted",
                model="price-insight-loop-v3",
                completed_output=output,
                deltas=(output,),
                usage={"output_tokens": 32},
            )
        elif any(tool in available for tool in _METRIC_TOOLS.values()):
            computed = _called_metric_keys(request.messages)
            remaining = [key for key in _METRIC_KEYS if key not in computed]
            if remaining:
                name = _METRIC_TOOLS[remaining[0]]
                arguments = {"filter": _filter_from_goal(request.messages)}
            else:
                profile = _latest_tool_content(request.messages, _SCOPE_PROFILE_TOOL)
                source_revision = profile.get("source_revision", "unknown")
                quality_status = _latest_tool_content(
                    request.messages,
                    _QUALITY_CHECK_TOOL,
                ).get("status", "unknown")
                output = (
                    f"价格洞察原子 SOP 已完成：数据源 {source_revision}，"
                    f"数据质量 {quality_status}，"
                    f"已逐项生成 {len(computed)} 项关键指标。"
                )
                return ModelResponse(
                    model_call_id=request.model_call_id,
                    provider="development-scripted",
                    model="price-insight-loop-v3",
                    completed_output=output,
                    deltas=(output,),
                    usage={"output_tokens": 32},
                )
        else:
            raise RuntimeError(
                "Price Insight atomic tools are unavailable after Skill activation"
            )

        return ModelResponse(
            model_call_id=request.model_call_id,
            provider="development-scripted",
            model="price-insight-loop-v2",
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


def _called_metric_keys(messages: tuple[dict[str, Any], ...]) -> set[str]:
    keys: set[str] = set()
    for message in messages:
        if message.get("role") != "assistant":
            continue
        for item in message.get("tool_calls", ()):
            if not isinstance(item, dict):
                continue
            function = item.get("function")
            if not isinstance(function, dict):
                continue
            name = str(function.get("name", ""))
            metric_key = next(
                (key for key, tool in _METRIC_TOOLS.items() if tool == name),
                None,
            )
            if metric_key is not None:
                keys.add(metric_key)
    return keys


def _price_source_revisions(
    messages: tuple[dict[str, Any], ...],
) -> set[str]:
    governed_tools = {
        _SCOPE_PROFILE_TOOL,
        _QUALITY_CHECK_TOOL,
        *_METRIC_TOOLS.values(),
    }
    call_ids: set[str] = set()
    for message in messages:
        if message.get("role") != "assistant":
            continue
        for item in message.get("tool_calls", ()):
            if not isinstance(item, dict):
                continue
            function = item.get("function")
            if (
                isinstance(function, dict)
                and function.get("name") in governed_tools
            ):
                call_ids.add(str(item.get("id", "")))
    revisions: set[str] = set()
    for message in messages:
        if (
            message.get("role") != "tool"
            or str(message.get("tool_call_id", "")) not in call_ids
        ):
            continue
        try:
            payload = json.loads(str(message.get("content", "{}")))
        except json.JSONDecodeError:
            continue
        if not isinstance(payload, dict):
            continue
        content = payload.get("content")
        selected = content if isinstance(content, dict) else payload
        revision = selected.get("source_revision")
        if isinstance(revision, str) and revision:
            revisions.add(revision)
    return revisions


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
