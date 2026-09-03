from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from typing import Any, cast

import pytest

from auraclaw.action.mcp_connection_manager import (
    McpConnectionManager,
    _contains_expected,
)
from auraclaw.action.mcp_registry import McpServerRegistryService
from auraclaw.action.ports import CapabilityConnector
from auraclaw.contracts.capabilities import (
    CapabilityDescriptor,
    CapabilityKind,
    CapabilityStatus,
)
from auraclaw.contracts.errors import AuthorizationError
from auraclaw.contracts.hands import HandsResourceContent, HandsToolResult


def _tool(*, permission: str) -> CapabilityDescriptor:
    return CapabilityDescriptor(
        capability_id="cap-market-quote",
        kind=CapabilityKind.TOOL,
        server_id="market-mcp",
        canonical_name="market.quote.get",
        version="1",
        content_digest="sha256:quote",
        title="Market quote",
        tenant_id="tenant-a",
        permission=permission,
        risk_level="low",
        status=CapabilityStatus.ACTIVE,
        updated_at=datetime.now(UTC),
        metadata={
            "source": {
                "inputSchema": {
                    "type": "object",
                    "properties": {"symbol": {"type": "string"}},
                    "required": ["symbol"],
                },
                "outputSchema": {
                    "type": "object",
                    "properties": {"price": {"type": "number"}},
                    "required": ["price"],
                },
            }
        },
    )


class _Catalog:
    def __init__(self, capability: CapabilityDescriptor) -> None:
        self.capability = capability

    async def get(self, **_: object) -> CapabilityDescriptor:
        return self.capability


class _Connector:
    connector_id = "mcp:market-mcp"

    def __init__(self) -> None:
        self.arguments: dict[str, Any] | None = None
        self.resource_uri: str | None = None

    async def read_resource(
        self, _trusted: object, uri: str
    ) -> tuple[HandsResourceContent, ...]:
        self.resource_uri = uri
        return ()

    async def call_tool(self, _trusted: object, **kwargs: Any) -> HandsToolResult:
        self.arguments = dict(kwargs["arguments"])
        return HandsToolResult(
            status="success",
            content={"symbol": self.arguments["symbol"], "price": 123.45},
        )


def _manager(
    capability: CapabilityDescriptor, connector: _Connector
) -> McpConnectionManager:
    return McpConnectionManager(
        registry=cast(McpServerRegistryService, object()),
        connectors={"market-mcp": cast(CapabilityConnector, connector)},
        factory=cast(Any, lambda _: connector),
        catalog=cast(Any, _Catalog(capability)),
    )


def test_read_only_capability_test_invokes_input_and_validates_output() -> None:
    connector = _Connector()

    result = asyncio.run(
        _manager(_tool(permission="read-only"), connector).test_capability(
            tenant_id="tenant-a",
            actor_id="admin-1",
            dept_id="dept-a",
            server_id="market-mcp",
            capability_id="cap-market-quote",
            input_payload={"symbol": "AAPL"},
            expected_output={"price": 123.45},
        )
    )

    assert connector.arguments == {"symbol": "AAPL"}
    assert result["status"] == "passed"
    assert result["schema_valid"] is True
    assert result["expectation_matched"] is True
    assert result["output"] == {"symbol": "AAPL", "price": 123.45}


def test_write_capability_test_is_rejected_by_backend() -> None:
    connector = _Connector()

    with pytest.raises(AuthorizationError):
        asyncio.run(
            _manager(
                _tool(permission="write-with-approval"), connector
            ).test_capability(
                tenant_id="tenant-a",
                actor_id="admin-1",
                dept_id=None,
                server_id="market-mcp",
                capability_id="cap-market-quote",
                input_payload={"symbol": "AAPL"},
            )
        )

    assert connector.arguments is None


@pytest.mark.parametrize("template", [False, True])
def test_resource_probe_cannot_read_outside_selected_capability(template: bool) -> None:
    connector = _Connector()
    capability = _tool(permission="read-only").model_copy(update={
        "kind": CapabilityKind.RESOURCE_TEMPLATE if template else CapabilityKind.RESOURCE,
        "metadata": {"source": (
            {"uri_template": "repo://public/{name}"} if template
            else {"uri": "repo://public/readme"}
        )},
    })
    with pytest.raises(AuthorizationError):
        asyncio.run(_manager(capability, connector).test_capability(
            tenant_id="tenant-a", actor_id="admin-1", dept_id=None,
            server_id="market-mcp", capability_id=capability.capability_id,
            input_payload={"uri": "repo://private/secret"},
        ))
    assert connector.resource_uri is None


@pytest.mark.parametrize("template", [False, True])
def test_resource_probe_accepts_selected_capability_uri(template: bool) -> None:
    connector = _Connector()
    capability = _tool(permission="read-only").model_copy(update={
        "kind": CapabilityKind.RESOURCE_TEMPLATE if template else CapabilityKind.RESOURCE,
        "metadata": {"source": (
            {"uri_template": "repo://public/{name}"} if template
            else {"uri": "repo://public/readme"}
        )},
    })
    result = asyncio.run(_manager(capability, connector).test_capability(
        tenant_id="tenant-a", actor_id="admin-1", dept_id=None,
        server_id="market-mcp", capability_id=capability.capability_id,
        input_payload={"uri": "repo://public/readme"} if template else {},
    ))
    assert result["status"] == "passed"
    assert connector.resource_uri == "repo://public/readme"


def test_expected_json_boolean_is_not_a_number() -> None:
    assert not _contains_expected({"accepted": 1}, {"accepted": True})
    assert not _contains_expected({"count": False}, {"count": 0})
    assert _contains_expected({"price": 1.0}, {"price": 1})
