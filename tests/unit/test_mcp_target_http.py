from __future__ import annotations

import asyncio
import json
import os
import threading
from datetime import timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from uuid import uuid4

import httpx
import pytest

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
from auraclaw.infrastructure.model.openai_compatible import OpenAICompatibleProvider
from auraclaw.projection.approval.projector import InMemoryApprovalProjection
from auraclaw.runtime.capability_controller import RuntimeCapabilityController
from auraclaw.runtime.hands_adapter import HandsRuntimeAdapter
from auraclaw.runtime.hands_client import HttpHandsClient
from auraclaw.runtime.ports import ModelRequest, ToolCall

TEST_DATABASE_URL = (
    os.environ.get("AURACLAW_DATABASE_URL")
    if os.environ.get("AURACLAW_STORAGE_BACKEND") == "postgres"
    else None
)


class Handler(BaseHTTPRequestHandler):
    marker: str
    requests: list[dict[str, Any]]

    def log_message(self, format: str, *args: object) -> None:
        pass

    def do_POST(self) -> None:
        request = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
        self.requests.append(request)
        method = request["method"]
        if method == "notifications/initialized":
            assert "id" not in request
            self.send_response(202)
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
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
                        "inputSchema": getattr(
                            self,
                            "input_schema",
                            {
                                "type": "object",
                                "properties": {"value": {"type": "integer"}},
                                "required": ["value"],
                            },
                        ),
                        "annotations": {"readOnlyHint": True},
                    }
                ]
            }
        elif method == "tools/call":
            assert request["params"]["name"] == "lookup"
            result = {
                "structuredContent": {
                    "server": self.marker,
                    "value": request["params"]["arguments"][
                        "input" if getattr(self, "nested", False) else "value"
                    ],
                    **({"status": "error"} if getattr(self, "nested", False) else {}),
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


@pytest.mark.parametrize("cold", [False, True])
def test_runtime_hands_targets_two_real_http_servers_with_same_tool_name(cold: bool) -> None:
    _exercise_two_servers(cold=cold)


def test_postgres_cold_replica_targets_real_http_servers() -> None:
    if TEST_DATABASE_URL is None:
        pytest.skip("explicit PostgreSQL test URL not configured")
    _exercise_two_servers(cold=True, database_url=TEST_DATABASE_URL)


def test_provider_streamed_nested_arguments_match_actual_mcp_schema_and_wire() -> None:
    _exercise_two_servers(cold=True, nested=True)


def _exercise_two_servers(
    *, cold: bool, database_url: str | None = None, nested: bool = False
) -> None:
    arguments = (
        {"input": {"name": "中文", "items": [3, True, None], "limit": 10}}
        if nested
        else {"value": 42}
    )
    nested_schema = {
        "type": "object",
        "$defs": {"item": {"type": ["integer", "boolean", "null"]}},
        "properties": {
            "input": {
                "type": "object",
                "properties": {
                    "name": {"type": "string"},
                    "items": {"type": "array", "items": {"$ref": "#/$defs/item"}},
                    "limit": {
                        "anyOf": [{"type": "integer"}, {"type": "null"}],
                        "description": "Result limit",
                        "default": 20,
                    },
                },
                "required": ["limit"],
                "additionalProperties": False,
            }
        },
        "required": ["input"],
    }

    servers = []
    suffix = uuid4().hex[:10] if database_url else ""
    for marker in ("one" + suffix, "two" + suffix):
        handler = type(f"Handler_{marker}", (Handler,), {"marker": marker, "requests": []})
        if nested:
            handler.input_schema = nested_schema
            handler.nested = True
        server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        servers.append((marker, server))

    async def scenario() -> None:
        from auraclaw.infrastructure.persistence.postgres_capability_catalog import (
            PostgresCapabilityCatalogStore,
        )

        store = (
            InMemoryCapabilityCatalogStore()
            if database_url is None
            else PostgresCapabilityCatalogStore(database_url)
        )
        follower_store = (
            store if database_url is None else PostgresCapabilityCatalogStore(database_url)
        )
        leases = []
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
            if cold:
                before = sum(len(server.RequestHandlerClass.requests) for _, server in servers)
                catalog = CapabilityCatalog(follower_store)
                registry = ToolRegistry((load,))
                router = RoutedHandsExecutor(
                    NoFallback(), {load.name: CapabilityLoadExecutor(catalog)}
                )
                cold_connectors = {
                    definition.server_id: ManagedMcpConnector(
                        definition, credentials=Credentials(adapter), policy=Allow()
                    )
                    for definition, adapter in zip(definitions, adapters, strict=True)
                }
                follower = CapabilityCatalogReconciler(
                    catalog=catalog,
                    store=follower_store,
                    connectors=cold_connectors,
                    tool_registry=registry,
                    hands_router=router,
                )
                for definition in definitions:
                    lease = await store.claim_catalog_reconcile(
                        server_id=definition.server_id, owner="leader", ttl=timedelta(minutes=1)
                    )
                    assert lease is not None
                    leases.append(lease)
                    result = await follower.reconcile_server(definition)
                    assert result.status is CapabilityStatus.ACTIVE, result.error
                assert (
                    sum(len(server.RequestHandlerClass.requests) for _, server in servers) == before
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
                    call = ToolCall(
                        tool_invocation_id=f"call-{index}",
                        arguments=arguments,
                        name=item["model_tool"]["function"]["name"],
                    )
                    if nested:

                        def model_handler(request, call=call):
                            body = json.loads(request.content)
                            exposed = next(
                                tool
                                for tool in body["tools"]
                                if tool["function"]["name"] == call.name
                            )
                            assert exposed["function"]["parameters"] == nested_schema
                            encoded = json.dumps(arguments, ensure_ascii=False)
                            chunks = []
                            for offset in range(0, len(encoded), 3):
                                chunks.append(
                                    "data: "
                                    + json.dumps(
                                        {
                                            "choices": [
                                                {
                                                    "delta": {
                                                        "tool_calls": [
                                                            {
                                                                "index": 0,
                                                                **(
                                                                    {"id": call.tool_invocation_id}
                                                                    if offset == 0
                                                                    else {}
                                                                ),
                                                                "function": {
                                                                    **(
                                                                        {"name": call.name}
                                                                        if offset == 0
                                                                        else {}
                                                                    ),
                                                                    "arguments": encoded[
                                                                        offset : offset + 3
                                                                    ],
                                                                },
                                                            }
                                                        ]
                                                    }
                                                }
                                            ]
                                        },
                                        ensure_ascii=False,
                                    )
                                    + "\n\n"
                                )
                            return httpx.Response(
                                200,
                                text="".join(chunks) + "data: [DONE]\n\n",
                                headers={"content-type": "text/event-stream"},
                            )

                        async with httpx.AsyncClient(
                            transport=httpx.MockTransport(model_handler)
                        ) as model_http:
                            provider = OpenAICompatibleProvider(
                                base_url="https://model.test/v1", model="fixture", client=model_http
                            )
                            response = await provider.generate(
                                ModelRequest(
                                    model_call_id=f"model-{index}",
                                    tenant_id="tenant-a",
                                    run_id="run",
                                    messages=(
                                        {"role": "user", "content": "查询中文库存，limit 10"},
                                    ),
                                    tools=controller.model_tools(state),
                                ),
                                credential="fixture",
                            )
                            call = response.tool_calls[0]
                            assert call.arguments == arguments
                    result = await controller.execute(assignment, call, state)
                    expected = {
                        "server": item["server_id"],
                        "value": arguments["input"] if nested else 42,
                        **({"status": "error"} if nested else {}),
                    }
                    assert result.result["status"] == "success"
                    assert result.result["content"] == expected
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
                assert len(calls) == 1 and calls[0]["params"]["arguments"] == arguments
        finally:
            for lease in leases:
                await store.release_catalog_reconcile(lease)
            if database_url is not None:
                for definition in definitions:
                    await store.remove_server(definition.server_id)
                await store.close()
                await follower_store.close()
            for adapter in adapters:
                await adapter.aclose()

    try:
        asyncio.run(scenario())
    finally:
        for _, server in servers:
            server.shutdown()
            server.server_close()
