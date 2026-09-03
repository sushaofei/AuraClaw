from __future__ import annotations

import asyncio
import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

import httpx

from auraclaw.action.capability_catalog import (
    CapabilityCatalog,
    CapabilityLoadExecutor,
    InMemoryCapabilityCatalogStore,
    RoutedHandsExecutor,
    capability_load_tool,
)
from auraclaw.action.catalog_reconciler import CapabilityCatalogReconciler
from auraclaw.action.hands import HandsGateway
from auraclaw.action.hands_http import StaticHandsAuthenticator, create_hands_http_app
from auraclaw.action.policy import PolicyEngine
from auraclaw.action.ports import PolicyEvaluation
from auraclaw.action.tool_gateway import ToolGateway, ToolRegistry
from auraclaw.contracts.capabilities import CapabilityStatus, McpAuthStrategy, McpNetworkMode
from auraclaw.contracts.hands import HandsTrustedContext
from auraclaw.contracts.mcp_registry import McpDesiredState, McpObservedState, McpServerConfig
from auraclaw.contracts.tools import PolicyDecision
from auraclaw.control.ports import RuntimeAssignment
from auraclaw.infrastructure.artifacts.store import ArtifactStore, InMemoryObjectStorage
from auraclaw.infrastructure.connectors.mcp.connector import ManagedMcpConnector
from auraclaw.infrastructure.credentials.mcp_egress import ManagedMcpEgressAdapter
from auraclaw.projection.approval.projector import InMemoryApprovalProjection
from auraclaw.runtime.capability_controller import RuntimeCapabilityController
from auraclaw.runtime.hands_adapter import HandsRuntimeAdapter
from auraclaw.runtime.hands_client import HttpHandsClient
from auraclaw.runtime.ports import ToolCall


class Handler(BaseHTTPRequestHandler):
    marker: str
    requests: list[dict[str, Any]]

    def log_message(self, format: str, *args: object) -> None:
        pass

    def do_POST(self) -> None:
        request = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        self.requests.append(request)
        method = request["method"]
        if method == "initialize":
            result = {
                "protocolVersion": "2025-11-25",
                "capabilities": {"tools": {}},
                "serverInfo": {"name": self.marker, "version": "1"},
            }
        elif method == "tools/list":
            result = {
                "tools": [
                    {
                        "name": "lookup",
                        "description": "Read a value",
                        "inputSchema": {
                            "type": "object",
                            "properties": {"value": {"type": "integer"}},
                            "required": ["value"],
                        },
                        "annotations": {"readOnlyHint": True},
                    }
                ]
            }
        elif method == "tools/call":
            assert request["params"]["name"] == "lookup"
            result = {
                "structuredContent": {
                    "server": self.marker,
                    "value": request["params"]["arguments"]["value"],
                }
            }
        else:
            result = {}
        body = json.dumps({"jsonrpc": "2.0", "id": request.get("id"), "result": result}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


class Credentials:
    def __init__(self, adapter: ManagedMcpEgressAdapter) -> None:
        self.adapter = adapter

    async def invoke(self, **kwargs: Any) -> Any:
        return await self.adapter(kwargs["request"], None)

    def redact(self, value: Any) -> Any:
        return value


class Allow:
    async def evaluate_action(self, **kwargs: Any) -> PolicyEvaluation:
        return PolicyEvaluation(
            decision=PolicyDecision.ALLOW, decision_id="test", policy_version="v1"
        )


class NoFallback:
    async def execute(self, *args: Any) -> Any:
        raise AssertionError("No fallback routing allowed")


def test_runtime_hands_targets_two_real_http_servers_with_same_tool_name() -> None:
    servers = []
    for marker in ("one", "two"):
        handler = type(f"Handler_{marker}", (Handler,), {"marker": marker, "requests": []})
        server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        servers.append((marker, server))

    async def scenario() -> None:
        store = InMemoryCapabilityCatalogStore()
        catalog = CapabilityCatalog(store)
        load = capability_load_tool()
        registry = ToolRegistry((load,))
        router = RoutedHandsExecutor(NoFallback(), {load.name: CapabilityLoadExecutor(catalog)})
        connectors, adapters, definitions = {}, [], []
        for marker, server in servers:
            definition = McpServerConfig(
                server_id=marker,
                tenant_id="tenant-a",
                title=marker,
                endpoint=f"http://127.0.0.1:{server.server_port}/mcp",
                protocol_revision="2025-11-25",
                auth_strategy=McpAuthStrategy.NONE,
                network_mode=McpNetworkMode.LOOPBACK,
            ).materialize(
                revision=1,
                desired_state=McpDesiredState.ENABLED,
                observed_state=McpObservedState.ACTIVE,
            )
            definitions.append(definition)
            adapter = ManagedMcpEgressAdapter(definition)
            adapters.append(adapter)
            connectors[marker] = ManagedMcpConnector(
                definition, credentials=Credentials(adapter), policy=Allow()
            )
        try:
            reconciler = CapabilityCatalogReconciler(
                catalog=catalog,
                store=store,
                connectors=connectors,
                tool_registry=registry,
                hands_router=router,
            )
            ids = []
            for definition in definitions:
                await catalog.register_server(definition)
                assert (
                    await reconciler.reconcile_server(definition)
                ).status == CapabilityStatus.ACTIVE
                ids.extend(
                    item.capability_id
                    for item in await store.list_server_capabilities(
                        "tenant-a", definition.server_id
                    )
                )
            gateway = HandsGateway(
                registry=registry,
                gateway=ToolGateway(
                    registry=registry,
                    policy=PolicyEngine(),
                    hands=router,
                    approvals=InMemoryApprovalProjection(),
                    artifacts=ArtifactStore(
                        InMemoryObjectStorage(), signing_key=b"target-http-test-key"
                    ),
                ),
            )
            trusted = HandsTrustedContext(
                tenant_id="tenant-a",
                root_session_id="root",
                session_id="session",
                run_id="run",
                runtime_id="runtime",
                lease_id="lease",
                fencing_token=1,
                user_id="user-a",
                dept_id="dept-a",
            )
            app = create_hands_http_app(
                gateway, authenticator=StaticHandsAuthenticator({"token": trusted})
            )
            assignment = RuntimeAssignment(
                tenant_id="tenant-a",
                root_session_id="root",
                session_id="session",
                run_id="run",
                runtime_id="runtime",
                lease_id="lease",
                fencing_token=1,
                role="worker",
                resource_profile={},
                user_id="user-a",
                dept_id="dept-a",
            )
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app), base_url="http://hands"
            ) as raw:
                client = HandsRuntimeAdapter(
                    HttpHandsClient(raw, bearer_tokens={"runtime": "token"})
                )
                loaded = await client.execute(
                    assignment,
                    ToolCall(
                        tool_invocation_id="load", name=load.name, arguments={"capability_ids": ids}
                    ),
                )
                state = {
                    "loaded": {
                        item["capability_id"]: item for item in loaded["content"]["capabilities"]
                    }
                }
                controller = RuntimeCapabilityController(client)
                for index, item in enumerate(state["loaded"].values()):
                    result = await controller.execute(
                        assignment,
                        ToolCall(
                            tool_invocation_id=f"call-{index}",
                            arguments={"value": 42},
                            name=item["model_tool"]["function"]["name"],
                        ),
                        state,
                    )
                    assert result.result["content"] == {"server": item["server_id"], "value": 42}
                ambiguous = await controller.execute(
                    assignment,
                    ToolCall(
                        tool_invocation_id="ambiguous", name="lookup", arguments={"value": 42}
                    ),
                    state,
                )
                assert ambiguous.result["error_code"] == "ambiguous_capability"
            for _, server in servers:
                calls = [
                    req
                    for req in server.RequestHandlerClass.requests
                    if req["method"] == "tools/call"
                ]
                assert len(calls) == 1 and calls[0]["params"]["arguments"] == {"value": 42}
        finally:
            for adapter in adapters:
                await adapter.aclose()

    try:
        asyncio.run(scenario())
    finally:
        for _, server in servers:
            server.shutdown()
            server.server_close()
