from __future__ import annotations

import asyncio
import json

from auraclaw.composition.development_capabilities import (
    build_development_capability_client,
)
from auraclaw.composition.development_model import DevelopmentPriceInsightModel
from auraclaw.config import Settings
from auraclaw.control.ports import RuntimeAssignment, RuntimeBudget
from auraclaw.runtime.ports import ModelRequest, ToolCall


def _assignment() -> RuntimeAssignment:
    return RuntimeAssignment(
        tenant_id="development",
        root_session_id="root-local-debug",
        session_id="session-local-debug",
        run_id="run-local-debug",
        runtime_id="runtime-local-debug",
        lease_id="lease-local-debug",
        fencing_token=1,
        role="root",
        resource_profile={},
        budget=RuntimeBudget(),
    )


def _assistant_call(call_id: str, name: str) -> dict[str, object]:
    return {
        "role": "assistant",
        "content": "",
        "tool_calls": [
            {
                "id": call_id,
                "type": "function",
                "function": {"name": name, "arguments": "{}"},
            }
        ],
    }


def _tool_result(call_id: str, content: dict[str, object]) -> dict[str, object]:
    return {
        "role": "tool",
        "tool_call_id": call_id,
        "content": json.dumps({"status": "success", "content": content}),
    }


def test_development_capability_client_publishes_price_insight_skill() -> None:
    async def scenario() -> None:
        client = build_development_capability_client(
            Settings(
                price_insight_source="fixture",
                price_insight_target_tenant_id="development",
            )
        )
        assert client is not None
        result = await client.execute(
            _assignment(),
            ToolCall(
                tool_invocation_id="search-local-price",
                name="auraclaw.capabilities.search",
                arguments={
                    "query": "采购 价格洞察 行业均价",
                    "kinds": ["skill"],
                },
            ),
        )
        capabilities = result["content"]["capabilities"]
        assert len(capabilities) == 1
        assert (
            capabilities[0]["canonical_name"]
            == "procurement.price-insight.generate"
        )

    asyncio.run(scenario())


def test_development_model_drives_filter_aware_price_loop() -> None:
    async def scenario() -> None:
        model = DevelopmentPriceInsightModel()
        messages: tuple[dict[str, object], ...] = (
            {
                "role": "user",
                "content": (
                    "分析周期为 2026-03 至 2026-04，主要对标 跨区域价格，"
                    "偏离阈值为 9.5%。"
                ),
            },
            _assistant_call("search", "auraclaw.capabilities.search"),
            _tool_result(
                "search",
                {
                    "capabilities": [
                        {
                            "capability_id": "cap-price-skill",
                            "kind": "skill",
                            "canonical_name": "procurement.price-insight.generate",
                        }
                    ]
                },
            ),
            _assistant_call("load", "auraclaw.capabilities.load"),
            _tool_result("load", {"capabilities": []}),
            _assistant_call("activate", "auraclaw.skills.activate"),
            _tool_result("activate", {"status": "activated"}),
            _assistant_call(
                "quality",
                "procurement.price_insight.data_quality",
            ),
            _tool_result("quality", {"status": "pass"}),
        )
        request = ModelRequest(
            model_call_id="model-local-debug",
            tenant_id="development",
            run_id="run-local-debug",
            messages=messages,
            tools=(
                {
                    "type": "function",
                    "function": {
                        "name": "procurement.price_insight.snapshot",
                        "parameters": {"type": "object"},
                    },
                },
                {
                    "type": "function",
                    "function": {
                        "name": "procurement.price_insight.data_quality",
                        "parameters": {"type": "object"},
                    },
                },
            ),
        )
        response = await model.generate(request)

        assert response.tool_calls[0].name == "procurement.price_insight.snapshot"
        assert response.tool_calls[0].arguments["filter"] == {
            "period_from": "2026-03",
            "period_to": "2026-04",
            "anchor": "region",
            "deviation_threshold_pct": 9.5,
        }

    asyncio.run(scenario())
