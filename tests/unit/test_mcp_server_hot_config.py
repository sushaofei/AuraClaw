from __future__ import annotations

import asyncio

import pytest
from pydantic import ValidationError

from auraclaw.action.mcp_connection_manager import McpConnectionManager
from auraclaw.action.mcp_registry import (
    InMemoryMcpServerRegistryStore,
    McpServerRegistryService,
)
from auraclaw.action.ports import CapabilityConnector
from auraclaw.contracts.capabilities import McpAuthStrategy, McpNetworkMode
from auraclaw.contracts.errors import (
    AuthorizationError,
    CredentialAccessError,
    VersionConflictError,
)
from auraclaw.contracts.hands import (
    CapabilitySnapshot,
    HandsPromptResult,
    HandsResourceContent,
    HandsToolResult,
    HandsTrustedContext,
)
from auraclaw.contracts.mcp_registry import (
    McpDesiredState,
    McpServerConfig,
    McpServerLifecycleCommand,
    McpServerWriteCommand,
)
from auraclaw.infrastructure.connectors.mcp.wire import (
    MCP_CLIENT_CAPABILITIES_META_KEY,
    MCP_PROTOCOL_VERSION,
    MCP_PROTOCOL_VERSION_META_KEY,
)
from auraclaw.infrastructure.credentials.mcp_egress import (
    ManagedMcpEgressAdapter,
    McpEgressResponse,
    _approved_addresses,
)


def _config(**overrides: object) -> McpServerConfig:
    payload: dict[str, object] = {
        "server_id": "local-order-mcp",
        "tenant_id": "tenant-a",
        "title": "Local Order MCP",
        "endpoint": "http://127.0.0.1:48080/mcp",
        "network_mode": "loopback",
        "auth_strategy": "none",
        "allowed_tool_prefixes": ("order.",),
        "trust_level": "tenant_verified",
    }
    payload.update(overrides)
    return McpServerConfig.model_validate(payload)


def _write(config: McpServerConfig, **overrides: object) -> McpServerWriteCommand:
    payload: dict[str, object] = {
        "command_id": "cmd-1",
        "tenant_id": "tenant-a",
        "actor_id": "admin-1",
        "correlation_id": "corr-1",
        "causation_id": "cause-1",
        "expected_revision": 0,
        "config": config,
    }
    payload.update(overrides)
    return McpServerWriteCommand.model_validate(payload)


def _life(**overrides: object) -> McpServerLifecycleCommand:
    payload: dict[str, object] = {
        "command_id": "cmd-enable",
        "tenant_id": "tenant-a",
        "actor_id": "admin-1",
        "correlation_id": "corr-1",
        "causation_id": "cause-1",
        "expected_revision": 1,
    }
    payload.update(overrides)
    return McpServerLifecycleCommand.model_validate(payload)


class _FakeConnector:
    connector_id = "mcp:local-order-mcp"
    snapshots = 0

    async def snapshot(self, trusted: HandsTrustedContext) -> CapabilitySnapshot:
        del trusted
        type(self).snapshots += 1
        return CapabilitySnapshot(connector_id=self.connector_id)

    async def read_resource(
        self, trusted: HandsTrustedContext, uri: str
    ) -> tuple[HandsResourceContent, ...]:
        del trusted, uri
        return ()

    async def get_prompt(
        self,
        trusted: HandsTrustedContext,
        name: str,
        *,
        arguments: dict[str, str] | None = None,
    ) -> HandsPromptResult:
        del trusted, name, arguments
        return HandsPromptResult(description="", messages=())

    async def call_tool(
        self,
        trusted: HandsTrustedContext,
        *,
        name: str,
        arguments: dict[str, object],
        invocation_id: str,
    ) -> HandsToolResult:
        del trusted, name, arguments, invocation_id
        return HandsToolResult(status="completed", content={})

    async def aclose(self) -> None:
        return None


def test_loopback_none_config_is_accepted() -> None:
    config = _config()
    definition = config.materialize(
        revision=1,
        desired_state=McpDesiredState.ENABLED,
        observed_state=__import__(
            "auraclaw.contracts.mcp_registry", fromlist=["McpObservedState"]
        ).McpObservedState.ACTIVE,
    )
    assert definition.network_mode is McpNetworkMode.LOOPBACK
    assert definition.resolved_auth_strategy is McpAuthStrategy.NONE
    assert definition.credential_ref == "mcp:none:local-order-mcp"
    assert definition.allowed_private_hosts == ("127.0.0.1",)


def test_public_none_and_http_are_rejected() -> None:
    with pytest.raises(ValidationError):
        _config(network_mode="public", endpoint="https://mcp.example.com/mcp")
    with pytest.raises(ValidationError):
        _config(
            network_mode="public",
            endpoint="http://mcp.example.com/mcp",
            auth_strategy="workload_trusted_context",
            credential_ref="vault/x#s",
        )
    with pytest.raises(ValidationError):
        _config(endpoint="http://user:pass@127.0.0.1/mcp")


def test_registry_create_is_idempotent_and_conflicts_on_revision() -> None:
    async def scenario() -> None:
        service = McpServerRegistryService(InMemoryMcpServerRegistryStore())
        first = await service.create(_write(_config()))
        again = await service.create(_write(_config()))
        assert first.operation_id == again.operation_id
        with pytest.raises(VersionConflictError):
            await service.create(_write(_config(), command_id="cmd-2"))
        updated = await service.update(
            _write(_config(title="Renamed"), command_id="cmd-3", expected_revision=1)
        )
        assert updated.result["latest_revision"] == 2
        with pytest.raises(VersionConflictError):
            await service.update(
                _write(_config(), command_id="cmd-4", expected_revision=1)
            )
        with pytest.raises(AuthorizationError):
            await service.get_server(
                tenant_id="tenant-b",
                server_id="local-order-mcp",
                actor_id="other",
            )

    asyncio.run(scenario())


def test_failed_enable_keeps_previous_active_revision() -> None:
    async def scenario() -> None:
        store = InMemoryMcpServerRegistryStore()
        service = McpServerRegistryService(store)
        await service.create(_write(_config()))

        class Boom:
            async def test(self, entry: object) -> None:
                del entry
                raise RuntimeError("dial failed")

            async def apply(self, entry: object) -> None:
                del entry

            async def revoke(self, server_id: str) -> None:
                del server_id

        class Ok:
            async def test(self, entry: object) -> None:
                del entry

            async def apply(self, entry: object) -> None:
                del entry

            async def revoke(self, server_id: str) -> None:
                del server_id

        service.bind_runtime(Boom())
        failed = await service.enable("local-order-mcp", _life())
        assert failed.status.value == "failed"
        record = await service.get_server(
            tenant_id="tenant-a",
            server_id="local-order-mcp",
            actor_id="admin-1",
        )
        assert record.active_revision is None
        assert record.desired_state is McpDesiredState.DISABLED

        service.bind_runtime(Ok())
        enabled = await service.enable(
            "local-order-mcp", _life(command_id="cmd-enable-2")
        )
        assert enabled.status.value == "succeeded"
        await service.update(
            _write(_config(title="v2"), command_id="cmd-5", expected_revision=1)
        )
        service.bind_runtime(Boom())
        failed_promote = await service.enable(
            "local-order-mcp",
            _life(command_id="cmd-enable-4", expected_revision=2, target_revision=2),
        )
        assert failed_promote.status.value == "failed"
        record = await service.get_server(
            tenant_id="tenant-a",
            server_id="local-order-mcp",
            actor_id="admin-1",
        )
        assert record.active_revision == 1

    asyncio.run(scenario())


def test_connection_manager_hot_swaps_and_restore() -> None:
    async def scenario() -> None:
        _FakeConnector.snapshots = 0
        store = InMemoryMcpServerRegistryStore()
        service = McpServerRegistryService(store)
        connectors: dict[str, CapabilityConnector] = {}
        manager = McpConnectionManager(
            registry=service,
            connectors=connectors,
            factory=lambda _server: _FakeConnector(),
            drain_seconds=0,
        )
        service.bind_runtime(manager)
        await service.create(_write(_config()))
        await service.enable("local-order-mcp", _life())
        assert "local-order-mcp" in connectors
        await service.disable(
            "local-order-mcp", _life(command_id="cmd-disable", expected_revision=1)
        )
        assert "local-order-mcp" not in connectors
        await service.enable(
            "local-order-mcp", _life(command_id="cmd-reenable", expected_revision=1)
        )
        connectors.clear()
        restored = await manager.restore()
        assert restored == 1
        assert "local-order-mcp" in connectors

    asyncio.run(scenario())


def test_network_mode_address_policy() -> None:
    loopback = _approved_addresses(
        ("127.0.0.1",),
        hostname="localhost",
        network_mode=McpNetworkMode.LOOPBACK,
        allowed_private_hosts=("localhost",),
        scheme="http",
    )
    assert loopback == ("127.0.0.1",)
    with pytest.raises(CredentialAccessError, match="loopback"):
        _approved_addresses(
            ("10.0.0.8",),
            hostname="localhost",
            network_mode=McpNetworkMode.LOOPBACK,
            allowed_private_hosts=("localhost",),
            scheme="http",
        )
    with pytest.raises(CredentialAccessError, match="allowed_cidrs"):
        _approved_addresses(
            ("10.0.0.8",),
            hostname="inventory.internal",
            network_mode=McpNetworkMode.PRIVATE,
            allowed_private_hosts=("inventory.internal",),
            scheme="http",
        )
    private = _approved_addresses(
        ("10.0.0.8",),
        hostname="inventory.internal",
        network_mode=McpNetworkMode.PRIVATE,
        allowed_private_hosts=("inventory.internal",),
        allowed_cidrs=("10.0.0.0/8",),
        scheme="http",
    )
    assert private == ("10.0.0.8",)
    with pytest.raises(CredentialAccessError, match="outside allowed_cidrs"):
        _approved_addresses(
            ("10.0.0.8",),
            hostname="inventory.internal",
            network_mode=McpNetworkMode.PRIVATE,
            allowed_private_hosts=("inventory.internal",),
            allowed_cidrs=("192.168.0.0/16",),
            scheme="http",
        )
    with pytest.raises(CredentialAccessError, match="forbidden"):
        _approved_addresses(
            ("169.254.1.1",),
            hostname="mcp.example.com",
            network_mode=McpNetworkMode.PUBLIC,
            allowed_private_hosts=(),
            scheme="https",
        )


def test_auth_none_adapter_skips_authorization_header() -> None:
    async def scenario() -> None:
        class Sender:
            def __init__(self) -> None:
                self.headers: dict[str, str] = {}

            async def send(self, **request: object) -> McpEgressResponse:
                self.headers = dict(request["headers"])  # type: ignore[arg-type]
                return McpEgressResponse(
                    status_code=200,
                    headers={"content-type": "application/json"},
                    content=b'{"jsonrpc":"2.0","id":1,"result":{"ok":true}}',
                )

        class Resolver:
            async def resolve(self, host: str, port: int) -> tuple[str, ...]:
                del host, port
                return ("127.0.0.1",)

        sender = Sender()
        definition = _config().materialize(
            revision=1,
            desired_state=McpDesiredState.ENABLED,
            observed_state=__import__(
                "auraclaw.contracts.mcp_registry", fromlist=["McpObservedState"]
            ).McpObservedState.ACTIVE,
        )
        adapter = ManagedMcpEgressAdapter(
            definition.model_copy(update={"enabled": True, "status": "active"}),
            resolver=Resolver(),
            sender=sender,
        )
        assert adapter.secret_required is False
        params = {
            "_meta": {
                MCP_PROTOCOL_VERSION_META_KEY: MCP_PROTOCOL_VERSION,
                MCP_CLIENT_CAPABILITIES_META_KEY: {},
            }
        }
        await adapter(
            {
                "server_id": "local-order-mcp",
                "config_revision": 1,
                "jsonrpc": "2.0",
                "id": 1,
                "method": "initialize",
                "params": params,
            },
            "",
        )
        assert "Authorization" not in sender.headers
        with pytest.raises(CredentialAccessError, match="revision"):
            await adapter(
                {
                    "server_id": "local-order-mcp",
                    "config_revision": 2,
                    "jsonrpc": "2.0",
                    "id": 2,
                    "method": "initialize",
                    "params": params,
                },
                "",
            )

    asyncio.run(scenario())


def test_private_auth_none_requires_platform_policy() -> None:
    async def scenario() -> None:
        service = McpServerRegistryService(
            InMemoryMcpServerRegistryStore(),
            allow_private_auth_none=False,
        )
        config = _config(
            endpoint="http://inventory.internal/mcp",
            network_mode="private",
            allowed_cidrs=("10.0.0.0/8",),
        )
        with pytest.raises(AuthorizationError, match="auth_strategy none"):
            await service.create(_write(config))

    asyncio.run(scenario())
