from __future__ import annotations

import asyncio
import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

import pytest

from auraclaw.contracts.capabilities import McpServerDefinition
from auraclaw.contracts.errors import CredentialAccessError, McpTransportError
from auraclaw.infrastructure.credentials.mcp_egress import (
    ManagedMcpEgressAdapter,
    McpEgressResponse,
    _decode_mcp_response,
)


def test_sse_multiline_data_and_empty_priming_event() -> None:
    result = _decode_mcp_response(
        McpEgressResponse(
            200,
            {"content-type": "text/event-stream"},
            b'id: 1\ndata:\n\ndata: {"jsonrpc":"2.0",\r\n'
            b'data: "id":1,"result":{"ok":true}}\r\n\r\n',
        ),
        1024,
    )
    assert result["result"] == {"ok": True}


@pytest.mark.parametrize("stateful", [True, False])
def test_standard_http_session_lifecycle_and_identity_isolation(stateful: bool) -> None:
    events: list[tuple[str, str | None, str | None]] = []
    sessions: dict[str, bool] = {}
    expire = False
    delete_fail = False

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, format: str, *args: object) -> None:
            pass

        def reply(self, status: int, data: Any = None, session: str | None = None) -> None:
            content = b"" if data is None else json.dumps(data).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(content)))
            if session:
                self.send_header("Mcp-Session-Id", session)
            self.end_headers()
            self.wfile.write(content)

        def do_POST(self) -> None:
            nonlocal expire
            body = json.loads(self.rfile.read(int(self.headers["Content-Length"])))
            method = body["method"]
            session = self.headers.get("Mcp-Session-Id")
            dept = self.headers.get("X-CT-Dept-ID")
            events.append((method, session, dept))
            assert self.headers["Authorization"] == "Bearer fixture-workload"
            assert self.headers["MCP-Protocol-Version"] == "2025-11-25"
            if method == "initialize":
                assert session is None
                session = f"session-{len(sessions)}" if stateful else None
                sessions[session or "stateless"] = False
                self.reply(
                    200,
                    {
                        "jsonrpc": "2.0",
                        "id": body["id"],
                        "result": {
                            "protocolVersion": "2025-11-25",
                            "capabilities": {"tools": {}},
                            "serverInfo": {"name": "strict", "version": "1"},
                        },
                    },
                    session,
                )
                return
            if stateful and session not in sessions:
                self.reply(404)
                return
            if method == "notifications/initialized":
                assert "id" not in body
                sessions[session or "stateless"] = True
                self.reply(202)
                return
            assert sessions[session or "stateless"]
            if expire:
                expire = False
                self.reply(404)
                return
            self.reply(200, {"jsonrpc": "2.0", "id": body["id"], "result": {"ok": True}})

        def do_DELETE(self) -> None:
            events.append(
                ("DELETE", self.headers.get("Mcp-Session-Id"), self.headers.get("X-CT-Dept-ID"))
            )
            self.reply(503 if delete_fail else 405)

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

    async def scenario() -> None:
        nonlocal expire, delete_fail
        definition = McpServerDefinition(
            server_id="strict",
            tenant_id="tenant",
            title="Strict MCP",
            endpoint=f"http://127.0.0.1:{server.server_port}/mcp",
            enabled=True,
            network_mode="loopback",
            auth_strategy="workload_trusted_context",
            credential_ref="vault/fixture",
            protocol_revision="2025-11-25",
            config_revision=1,
        )
        adapter = ManagedMcpEgressAdapter(definition)

        async def call(dept: str = "dept-a", *, revision: int = 1) -> dict[str, Any]:
            return await adapter(
                {
                    "id": "write",
                    "server_id": "strict",
                    "config_revision": revision,
                    "method": "tools/call",
                    "params": {"name": "write", "arguments": {"value": 7}},
                    "_auraclaw_identity": {
                        "tenant_id": "tenant",
                        "user_id": "user",
                        "dept_id": dept,
                        "session_id": "chat",
                    },
                },
                "fixture-workload",
            )

        try:
            results = await asyncio.gather(*(call() for _ in range(5)))
            assert all(r["result"]["ok"] for r in results)
            assert [e[0] for e in events[:2]] == ["initialize", "notifications/initialized"]
            assert sum(e[0] == "initialize" for e in events) == 1
            await call("dept-b")
            assert sum(e[0] == "initialize" for e in events) == 2
            if stateful:
                dept_a = {e[1] for e in events if e[0] == "tools/call" and e[2] == "dept-a"}
                dept_b = {e[1] for e in events if e[0] == "tools/call" and e[2] == "dept-b"}
                assert dept_a.isdisjoint(dept_b)
                expire = True
                before = sum(e[0] == "tools/call" for e in events)
                with pytest.raises(McpTransportError) as error:
                    await call()
                assert error.value.code == "mcp_session_expired"
                assert error.value.side_effect_status == "unknown"
                assert sum(e[0] == "tools/call" for e in events) == before + 1
                assert events[-1][0] == "notifications/initialized"
                await call()
            before = len(events)
            with pytest.raises(CredentialAccessError, match="revision"):
                await call(revision=2)
            assert len(events) == before
            if stateful:
                delete_fail = True
                with pytest.raises(McpTransportError, match="termination"):
                    await adapter.aclose()
                with pytest.raises(CredentialAccessError, match="revoked"):
                    await call()
                delete_fail = False
            await adapter.aclose()
            assert not adapter._sessions
        finally:
            delete_fail = False
            await adapter.aclose()

    try:
        asyncio.run(scenario())
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


def test_revocation_during_initialize_drains_late_session_without_business_dispatch() -> None:
    async def scenario() -> None:
        started, release = asyncio.Event(), asyncio.Event()
        methods: list[str] = []

        class Resolver:
            async def resolve(self, host: str, port: int) -> tuple[str, ...]:
                return ("127.0.0.1",)

        class Sender:
            async def send(self, **request: Any) -> McpEgressResponse:
                if request["method"] == "DELETE":
                    methods.append("DELETE")
                    return McpEgressResponse(204, {}, b"")
                body = json.loads(request["content"])
                methods.append(body["method"])
                assert body["method"] == "initialize"
                started.set()
                await release.wait()
                return McpEgressResponse(
                    200,
                    {"mcp-session-id": "late"},
                    json.dumps(
                        {
                            "jsonrpc": "2.0",
                            "id": body["id"],
                            "result": {
                                "protocolVersion": "2025-06-18",
                                "capabilities": {"tools": {}},
                            },
                        }
                    ).encode(),
                )

        adapter = ManagedMcpEgressAdapter(
            McpServerDefinition(
                server_id="strict",
                tenant_id="t",
                title="t",
                endpoint="http://localhost/mcp",
                enabled=True,
                network_mode="loopback",
                auth_strategy="none",
                protocol_revision="2025-06-18",
            ),
            resolver=Resolver(),
            sender=Sender(),
        )
        task = asyncio.create_task(
            adapter(
                {
                    "id": 1,
                    "server_id": "strict",
                    "method": "tools/call",
                    "params": {"name": "write"},
                },
                "",
            )
        )
        await started.wait()
        adapter.set_admission(False)
        close = asyncio.create_task(adapter.aclose())
        release.set()
        with pytest.raises(CredentialAccessError, match="revoked"):
            await task
        await close
        assert methods == ["initialize", "DELETE"]
        assert not adapter._sessions

    asyncio.run(scenario())


def test_cancelled_write_is_signalled_once_without_replay() -> None:
    async def scenario() -> None:
        started = asyncio.Event()
        methods: list[str] = []

        class Resolver:
            async def resolve(self, host: str, port: int) -> tuple[str, ...]:
                return ("127.0.0.1",)

        class Sender:
            async def send(self, **request: Any) -> McpEgressResponse:
                body = json.loads(request["content"])
                method = body["method"]
                methods.append(method)
                if method.startswith("notifications/"):
                    assert "id" not in body
                    if method == "notifications/cancelled":
                        assert body["params"]["requestId"] == "write-1"
                    return McpEgressResponse(202, {}, b"")
                if method == "tools/call":
                    started.set()
                    await asyncio.Event().wait()
                return McpEgressResponse(
                    200,
                    {},
                    json.dumps(
                        {
                            "jsonrpc": "2.0",
                            "id": body["id"],
                            "result": {
                                "protocolVersion": "2025-06-18",
                                "capabilities": {"tools": {}},
                            },
                        }
                    ).encode(),
                )

        adapter = ManagedMcpEgressAdapter(
            McpServerDefinition(
                server_id="strict",
                tenant_id="t",
                title="t",
                endpoint="http://localhost/mcp",
                enabled=True,
                network_mode="loopback",
                auth_strategy="none",
                protocol_revision="2025-06-18",
            ),
            resolver=Resolver(),
            sender=Sender(),
        )
        task = asyncio.create_task(
            adapter(
                {
                    "id": "write-1",
                    "server_id": "strict",
                    "method": "tools/call",
                    "params": {"name": "write"},
                },
                "",
            )
        )
        await started.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert methods == [
            "initialize",
            "notifications/initialized",
            "tools/call",
            "notifications/cancelled",
        ]
        await adapter.aclose()

    asyncio.run(scenario())
