from __future__ import annotations

import asyncio
import json
import socket
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest

from auraclaw.contracts.capabilities import CapabilityStatus, McpAuthStrategy, McpNetworkMode
from auraclaw.contracts.errors import CredentialAccessError
from auraclaw.contracts.mcp_registry import McpDesiredState, McpObservedState, McpServerConfig
from auraclaw.infrastructure.connectors.mcp.wire import (
    MCP_CLIENT_CAPABILITIES_META_KEY,
    MCP_PROTOCOL_VERSION,
    MCP_PROTOCOL_VERSION_META_KEY,
)
from auraclaw.infrastructure.credentials.mcp_egress import (
    HttpxPinnedMcpSender,
    ManagedMcpEgressAdapter,
    SystemMcpDnsResolver,
)


def _free_port() -> int:
    with socket.socket() as server:
        server.bind(("127.0.0.1", 0))
        return int(server.getsockname()[1])


class _LocalMcpHandler(BaseHTTPRequestHandler):
    calls: list[str] = []

    def log_message(self, format: str, *args: object) -> None:
        del format, args

    def do_POST(self) -> None:
        length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(length)
        payload = json.loads(raw.decode())
        method = str(payload.get("method", ""))
        type(self).calls.append(method)
        if method == "initialize":
            result = {
                "protocolVersion": MCP_PROTOCOL_VERSION,
                "capabilities": {},
                "serverInfo": {"name": "local-smoke", "version": "1"},
            }
        elif method == "tools/list":
            result = {
                "tools": [
                    {
                        "name": "demo.ping",
                        "description": "ping",
                        "inputSchema": {"type": "object"},
                    }
                ]
            }
        elif method == "tools/call":
            result = {"content": [{"type": "text", "text": "pong"}]}
        else:
            result = {}
        body = json.dumps(
            {"jsonrpc": "2.0", "id": payload.get("id"), "result": result},
            separators=(",", ":"),
        ).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


@pytest.mark.parametrize("host", ("127.0.0.1",))
def test_loopback_auth_none_reaches_real_local_mcp(host: str) -> None:
    port = _free_port()
    _LocalMcpHandler.calls = []
    server = ThreadingHTTPServer(("127.0.0.1", port), _LocalMcpHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:

        async def scenario() -> None:
            endpoint = f"http://{host}:{port}/mcp"
            definition = McpServerConfig(
                server_id="local-smoke-mcp",
                tenant_id="development",
                title="Local Smoke MCP",
                endpoint=endpoint,
                network_mode=McpNetworkMode.LOOPBACK,
                auth_strategy=McpAuthStrategy.NONE,
            ).materialize(
                revision=1,
                desired_state=McpDesiredState.ENABLED,
                observed_state=McpObservedState.ACTIVE,
            ).model_copy(
                update={
                    "enabled": True,
                    "status": CapabilityStatus.ACTIVE,
                }
            )
            adapter = ManagedMcpEgressAdapter(
                definition,
                resolver=SystemMcpDnsResolver(),
                sender=HttpxPinnedMcpSender(timeout_seconds=5.0),
            )
            try:
                listed = await adapter(
                    {
                        "server_id": "local-smoke-mcp",
                        "config_revision": 1,
                        "jsonrpc": "2.0",
                        "id": 1,
                        "method": "tools/list",
                        "params": {
                            "_meta": {
                                MCP_PROTOCOL_VERSION_META_KEY: MCP_PROTOCOL_VERSION,
                                MCP_CLIENT_CAPABILITIES_META_KEY: {},
                            }
                        },
                    },
                    "",
                )
                assert listed["result"]["tools"][0]["name"] == "demo.ping"
                called = await adapter(
                    {
                        "server_id": "local-smoke-mcp",
                        "config_revision": 1,
                        "jsonrpc": "2.0",
                        "id": 2,
                        "method": "tools/call",
                        "params": {
                            "name": "demo.ping",
                            "arguments": {},
                            "_meta": {
                                MCP_PROTOCOL_VERSION_META_KEY: MCP_PROTOCOL_VERSION,
                                MCP_CLIENT_CAPABILITIES_META_KEY: {},
                            },
                        },
                    },
                    "",
                )
                assert called["result"]["content"][0]["text"] == "pong"
            finally:
                await adapter.aclose()

        asyncio.run(scenario())
        assert "tools/list" in _LocalMcpHandler.calls
        assert "tools/call" in _LocalMcpHandler.calls
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=2)


def test_unreachable_loopback_mcp_is_credential_access_error() -> None:
    port = _free_port()

    async def scenario() -> None:
        endpoint = f"http://127.0.0.1:{port}/mcp"
        definition = McpServerConfig(
            server_id="down-smoke-mcp",
            tenant_id="development",
            title="Down Smoke MCP",
            endpoint=endpoint,
            network_mode=McpNetworkMode.LOOPBACK,
            auth_strategy=McpAuthStrategy.NONE,
        ).materialize(
            revision=1,
            desired_state=McpDesiredState.ENABLED,
            observed_state=McpObservedState.ACTIVE,
        ).model_copy(
            update={
                "enabled": True,
                "status": CapabilityStatus.ACTIVE,
            }
        )
        adapter = ManagedMcpEgressAdapter(
            definition,
            resolver=SystemMcpDnsResolver(),
            sender=HttpxPinnedMcpSender(timeout_seconds=2.0),
        )
        try:
            with pytest.raises(CredentialAccessError, match="unreachable"):
                await adapter(
                    {
                        "server_id": "down-smoke-mcp",
                        "config_revision": 1,
                        "jsonrpc": "2.0",
                        "id": 1,
                        "method": "tools/list",
                        "params": {
                            "_meta": {
                                MCP_PROTOCOL_VERSION_META_KEY: MCP_PROTOCOL_VERSION,
                                MCP_CLIENT_CAPABILITIES_META_KEY: {},
                            }
                        },
                    },
                    "",
                )
        finally:
            await adapter.aclose()

    asyncio.run(scenario())


def test_localhost_and_ipv6_loopback_configs_are_accepted() -> None:
    for endpoint in (
        "http://localhost:48080/mcp",
        "http://127.0.0.1:48080/mcp",
        "http://[::1]:48080/mcp",
    ):
        config = McpServerConfig(
            server_id="local-v6",
            title="Local Loopback",
            endpoint=endpoint,
            network_mode=McpNetworkMode.LOOPBACK,
            auth_strategy=McpAuthStrategy.NONE,
        )
        assert config.network_mode is McpNetworkMode.LOOPBACK
