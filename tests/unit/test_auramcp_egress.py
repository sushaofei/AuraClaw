from __future__ import annotations

import asyncio
import os
import socket
import subprocess
import sys
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import httpx
import pytest

from auraclaw.action.ports import PolicyEvaluation
from auraclaw.contracts.capabilities import (
    CapabilityTrustLevel,
    McpAuthStrategy,
    McpNetworkMode,
)
from auraclaw.contracts.errors import CredentialAccessError
from auraclaw.contracts.hands import HandsTrustedContext
from auraclaw.contracts.mcp_registry import (
    McpDesiredState,
    McpObservedState,
    McpServerConfig,
)
from auraclaw.contracts.tools import CredentialReference, PolicyDecision
from auraclaw.infrastructure.connectors.mcp.connector import ManagedMcpConnector
from auraclaw.infrastructure.credentials.mcp_egress import (
    HttpxPinnedMcpSender,
    ManagedMcpEgressAdapter,
    SystemMcpDnsResolver,
)
from auraclaw.infrastructure.credentials.proxy import CredentialProxy, InMemoryVault

_REPO_ROOT = Path(__file__).resolve().parents[2]
_WORKLOAD = "auramcp-hands-smoke-token"


def _auramcp_src() -> Path | None:
    configured = os.environ.get("AURAMCP_SRC")
    if configured:
        path = Path(configured)
        return path if (path / "auramcp").is_dir() else None
    sibling = _REPO_ROOT.parent / "AuraMCP" / "src"
    return sibling if (sibling / "auramcp").is_dir() else None


def _free_port() -> int:
    with socket.socket() as server:
        server.bind(("127.0.0.1", 0))
        return int(server.getsockname()[1])


def _trusted() -> HandsTrustedContext:
    return HandsTrustedContext(
        tenant_id="tenant-a",
        root_session_id="root-1",
        session_id="ses-1",
        run_id="run-1",
        runtime_id="runtime-1",
        lease_id="lease-1",
        fencing_token=1,
        user_id="user-a",
        dept_id="9",
    )


class _AllowPolicy:
    async def evaluate_action(self, **arguments: object) -> PolicyEvaluation:
        del arguments
        return PolicyEvaluation(
            decision=PolicyDecision.ALLOW,
            decision_id="policy-auramcp",
            policy_version="auramcp-v1",
        )


class _AdapterCredentials:
    def __init__(self, proxy: CredentialProxy, adapter: ManagedMcpEgressAdapter) -> None:
        self._proxy = proxy
        self._adapter = adapter

    async def invoke(self, **arguments: Any) -> Any:
        return await self._proxy.invoke(adapter=self._adapter, **arguments)

    def redact(self, value: Any) -> Any:
        return self._proxy.redact(value)


def _config(endpoint: str) -> McpServerConfig:
    return McpServerConfig(
        server_id="auramcp",
        tenant_id="tenant-a",
        title="AuraMCP extensions",
        endpoint=endpoint,
        protocol_revision="2026-07-28",
        network_mode=McpNetworkMode.LOOPBACK,
        auth_strategy=McpAuthStrategy.WORKLOAD_TRUSTED_CONTEXT,
        credential_ref="vault/auramcp#workload",
        trust_level=CapabilityTrustLevel.PLATFORM,
        allowed_tool_prefixes=("auramcp.",),
        allowed_resource_schemes=("auramcp",),
        allowed_prompt_prefixes=("auramcp.",),
    )


def test_auramcp_hot_config_is_workload_trusted_context() -> None:
    config = _config("http://127.0.0.1:8020/mcp")
    assert config.auth_strategy is McpAuthStrategy.WORKLOAD_TRUSTED_CONTEXT
    assert config.trust_level is CapabilityTrustLevel.PLATFORM
    assert config.network_mode is McpNetworkMode.LOOPBACK
    assert config.allowed_tool_prefixes == ("auramcp.",)
    assert config.allowed_resource_schemes == ("auramcp",)
    server = config.materialize(
        revision=1,
        desired_state=McpDesiredState.ENABLED,
        observed_state=McpObservedState.ACTIVE,
    )
    assert server.enabled is True
    assert server.allowed_private_hosts == ("127.0.0.1",)
    assert server.config_revision == 1


def test_auraclaw_hands_reaches_live_auramcp() -> None:
    src = _auramcp_src()
    if src is None:
        pytest.skip("AuraMCP source tree is not available")
    port = _free_port()
    env = os.environ.copy()
    pythonpath = str(src)
    existing = env.get("PYTHONPATH")
    if existing:
        pythonpath = pythonpath + os.pathsep + existing
    env["PYTHONPATH"] = pythonpath
    env.update(
        {
            "AURAMCP_DEPLOYMENT_PROFILE": "development",
            "AURAMCP_PLATFORM_STORE": "memory",
            "AURAMCP_ALLOW_INSECURE_IDENTITY": "false",
            "AURAMCP_HANDS_WORKLOAD_TOKEN": _WORKLOAD,
            "AURAMCP_ADMIN_TOKEN": "",
            "AURAMCP_HOST": "127.0.0.1",
            "AURAMCP_PORT": str(port),
            "AURAMCP_DB_HOST": "",
        }
    )
    process = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "auramcp",
            "run",
            "--host",
            "127.0.0.1",
            "--port",
            str(port),
        ],
        cwd=str(src.parent),
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        _wait_healthy(port, process)
        asyncio.run(_exercise(port))
    finally:
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)


def _wait_healthy(port: int, process: subprocess.Popen[bytes]) -> None:
    deadline = time.time() + 15
    url = f"http://127.0.0.1:{port}/internal/v1/health"
    last_error = "AuraMCP did not become healthy"
    while time.time() < deadline:
        if process.poll() is not None:
            stderr = (process.stderr.read() if process.stderr else b"").decode()
            stdout = (process.stdout.read() if process.stdout else b"").decode()
            raise RuntimeError(
                f"AuraMCP exited {process.returncode}\nstdout:\n{stdout}\nstderr:\n{stderr}"
            )
        try:
            response = httpx.get(url, timeout=0.5)
            if response.status_code == 200 and response.json().get("status") == "ok":
                return
            last_error = f"health HTTP {response.status_code}: {response.text}"
        except (httpx.HTTPError, ValueError) as exc:
            last_error = str(exc)
        time.sleep(0.1)
    raise RuntimeError(last_error)


async def _exercise(port: int) -> None:
    endpoint = f"http://127.0.0.1:{port}/mcp"
    server = _config(endpoint).materialize(
        revision=1,
        desired_state=McpDesiredState.ENABLED,
        observed_state=McpObservedState.ACTIVE,
    )
    adapter = ManagedMcpEgressAdapter(
        server,
        resolver=SystemMcpDnsResolver(),
        sender=HttpxPinnedMcpSender(timeout_seconds=5.0),
    )
    proxy = CredentialProxy(InMemoryVault({server.credential_ref: _WORKLOAD}))
    assert server.credential_ref is not None
    proxy.register_reference(
        "tenant-a",
        CredentialReference(
            credential_ref=server.credential_ref,
            provider=server.server_id,
            account_scope=adapter.credential_scope,
            allowed_operations=("mcp.invoke",),
            expires_at=datetime.now(UTC) + timedelta(hours=1),
        ),
    )
    connector = ManagedMcpConnector(
        server,
        credentials=_AdapterCredentials(proxy, adapter),
        policy=_AllowPolicy(),
    )
    try:
        trusted = _trusted()
        snapshot = await connector.snapshot(trusted)
        names = {item.name for item in snapshot.tools}
        assert "auramcp.health.ping" in names
        assert "auramcp.example.echo" in names
        assert "auramcp.ops.catalog_snapshot" in names
        assert all(name.startswith("auramcp.") for name in names)
        uris = {item.uri for item in snapshot.resources}
        assert "auramcp://builtin/about" in uris
        ping = await connector.call_tool(
            trusted,
            name="auramcp.health.ping",
            arguments={},
            invocation_id="inv-ping",
        )
        assert ping.status == "success"
        assert ping.content == {"ok": True, "service": "auramcp"}
        echo = await connector.call_tool(
            trusted,
            name="auramcp.example.echo",
            arguments={"message": "hello-auramcp"},
            invocation_id="inv-echo",
        )
        assert echo.status == "success"
        assert echo.content == {"echo": "hello-auramcp", "tenant": "tenant-a"}
        about = await connector.read_resource(trusted, "auramcp://builtin/about")
        assert about[0].text is not None and about[0].text.startswith("AuraMCP")
        prompt = await connector.get_prompt(
            trusted,
            "auramcp.ops.diagnose",
            arguments={"symptom": "timeout"},
        )
        assert "timeout" in prompt.messages[0].content["text"]
        with pytest.raises(CredentialAccessError, match="HTTP 401"):
            await adapter(
                {
                    "server_id": "auramcp",
                    "config_revision": 1,
                    "jsonrpc": "2.0",
                    "id": "denied",
                    "method": "tools/list",
                    "params": {
                        "_meta": {
                            "io.modelcontextprotocol/protocolVersion": "2026-07-28",
                            "io.modelcontextprotocol/clientCapabilities": {},
                        }
                    },
                    "_auraclaw_identity": {
                        "tenant_id": "tenant-a",
                        "user_id": "user-a",
                        "session_id": "ses-1",
                    },
                },
                "wrong-workload",
            )
    finally:
        await adapter.aclose()
