from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime

import httpx
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
    NotFoundError,
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
    McpActiveSnapshotEntry,
    McpDesiredState,
    McpObservedState,
    McpServerConfig,
    McpServerLifecycleCommand,
    McpServerRuntimeRecord,
    McpServerWriteCommand,
)
from auraclaw.infrastructure.clients.mcp_egress import RemoteMcpEgressClient
from auraclaw.infrastructure.clients.mcp_registry import RemoteMcpRegistryClient
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


class _BoomConnector(_FakeConnector):
    async def snapshot(self, trusted: HandsTrustedContext) -> CapabilitySnapshot:
        del trusted
        raise RuntimeError("probe failed")


class _FakeEgress:
    def __init__(self) -> None:
        self.events: list[tuple[str, str]] = []
        self.loaded: set[str] = set()

    async def apply(self, entry: McpActiveSnapshotEntry) -> None:
        self.events.append(("apply", entry.server_id))
        self.loaded.add(entry.server_id)

    async def revoke(self, server_id: str, *, expected_revision: int | None = None) -> None:
        self.events.append(("revoke", server_id))
        self.loaded.discard(server_id)


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
            await service.update(_write(_config(), command_id="cmd-4", expected_revision=1))
        with pytest.raises(AuthorizationError):
            await service.get_server(
                tenant_id="tenant-b",
                server_id="local-order-mcp",
                actor_id="other",
            )

    asyncio.run(scenario())


def test_registry_lists_retired_and_create_revives() -> None:
    async def scenario() -> None:
        service = McpServerRegistryService(InMemoryMcpServerRegistryStore())
        await service.create(_write(_config()))
        retired = await service.retire("local-order-mcp", _life(command_id="cmd-retire"))
        assert retired.status.value == "succeeded"
        listed = await service.list_servers(tenant_id="tenant-a")
        assert [item.server_id for item in listed] == ["local-order-mcp"]
        assert listed[0].desired_state is McpDesiredState.RETIRED
        fetched = await service.get_server(
            tenant_id="tenant-a",
            server_id="local-order-mcp",
            actor_id="admin-1",
        )
        assert fetched.desired_state is McpDesiredState.RETIRED
        revived = await service.create(_write(_config(title="Back"), command_id="cmd-revive"))
        assert revived.result["desired_state"] == "disabled"
        assert revived.result["latest_revision"] == 2
        listed = await service.list_servers(tenant_id="tenant-a")
        assert listed[0].desired_state is McpDesiredState.DISABLED
        assert listed[0].latest_config is not None
        assert listed[0].latest_config.title == "Back"

    asyncio.run(scenario())


def test_registry_hard_delete_removes_registration_and_allows_fresh_create() -> None:
    async def scenario() -> None:
        service = McpServerRegistryService(InMemoryMcpServerRegistryStore())
        await service.create(_write(_config()))
        await service.retire("local-order-mcp", _life(command_id="cmd-retire"))
        deleted = await service.delete(
            "local-order-mcp",
            _life(command_id="cmd-delete", expected_revision=1),
        )
        assert deleted.status.value == "succeeded"
        assert deleted.result["deleted"] is True
        assert await service.list_servers(tenant_id="tenant-a") == ()
        with pytest.raises(NotFoundError):
            await service.get_server(
                tenant_id="tenant-a",
                server_id="local-order-mcp",
                actor_id="admin-1",
            )
        recreated = await service.create(_write(_config(), command_id="cmd-recreate"))
        assert recreated.result["latest_revision"] == 1
        listed = await service.list_servers(tenant_id="tenant-a")
        assert len(listed) == 1
        assert listed[0].desired_state is McpDesiredState.DISABLED

    asyncio.run(scenario())


def test_registry_delete_is_idempotent() -> None:
    async def scenario() -> None:
        service = McpServerRegistryService(InMemoryMcpServerRegistryStore())
        await service.create(_write(_config()))
        command = _life(command_id="cmd-delete-once", expected_revision=1)
        first = await service.delete("local-order-mcp", command)
        second = await service.delete("local-order-mcp", command)
        assert first.operation_id == second.operation_id

    asyncio.run(scenario())


def test_failed_enable_persists_intent_for_reconciliation() -> None:
    async def scenario() -> None:
        store = InMemoryMcpServerRegistryStore()
        service = McpServerRegistryService(store)
        await service.create(_write(_config()))

        class Boom:
            async def test(self, entry: object) -> None:
                del entry

            async def apply(self, entry: object) -> None:
                del entry
                raise RuntimeError("dial failed")

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
        assert failed.status.value == "reconciling"
        record = await service.get_server(
            tenant_id="tenant-a",
            server_id="local-order-mcp",
            actor_id="admin-1",
        )
        assert record.active_revision == 1
        assert record.desired_state is McpDesiredState.ENABLED

        service.bind_runtime(Ok())
        enabled = await service.enable("local-order-mcp", _life(command_id="cmd-enable-2"))
        assert enabled.status.value == "succeeded"
        await service.update(_write(_config(title="v2"), command_id="cmd-5", expected_revision=1))
        service.bind_runtime(Boom())
        failed_promote = await service.enable(
            "local-order-mcp",
            _life(command_id="cmd-enable-4", expected_revision=2, target_revision=2),
        )
        assert failed_promote.status.value == "reconciling"
        record = await service.get_server(
            tenant_id="tenant-a",
            server_id="local-order-mcp",
            actor_id="admin-1",
        )
        assert record.active_revision == 2

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


def test_connection_manager_restore_skips_unreachable_server() -> None:
    async def scenario() -> None:
        boom = False

        def factory(server: object) -> CapabilityConnector:
            from auraclaw.contracts.capabilities import McpServerDefinition

            assert isinstance(server, McpServerDefinition)
            if boom and server.server_id == "down-mcp":
                raise ConnectionError("MCP server is not running")
            return _FakeConnector()

        store = InMemoryMcpServerRegistryStore()
        service = McpServerRegistryService(store)
        connectors: dict[str, CapabilityConnector] = {}
        manager = McpConnectionManager(
            registry=service,
            connectors=connectors,
            factory=factory,
            drain_seconds=0,
        )
        service.bind_runtime(manager)
        await service.create(_write(_config()))
        await service.create(
            _write(
                _config(server_id="down-mcp", endpoint="http://127.0.0.1:9/mcp"),
                command_id="cmd-down",
            )
        )
        await service.enable("local-order-mcp", _life())
        await service.enable(
            "down-mcp",
            _life(command_id="cmd-enable-down"),
        )
        connectors.clear()
        boom = True
        restored = await manager.restore()
        assert restored == 1
        assert "local-order-mcp" in connectors
        assert "down-mcp" not in connectors
        down = await service.get_server(
            tenant_id="tenant-a",
            server_id="down-mcp",
            actor_id="admin-1",
        )
        assert down.desired_state is McpDesiredState.ENABLED
        assert down.runtime is not None
        assert down.runtime.observed_state is McpObservedState.UNAVAILABLE

    asyncio.run(scenario())


def test_connection_manager_reconcile_continues_after_unreachable_server() -> None:
    async def scenario() -> None:
        boom = False

        def factory(server: object) -> CapabilityConnector:
            from auraclaw.contracts.capabilities import McpServerDefinition

            assert isinstance(server, McpServerDefinition)
            if boom and server.server_id == "down-mcp":
                raise ConnectionError("MCP server is not running")
            return _FakeConnector()

        store = InMemoryMcpServerRegistryStore()
        service = McpServerRegistryService(store)
        connectors: dict[str, CapabilityConnector] = {}
        manager = McpConnectionManager(
            registry=service,
            connectors=connectors,
            factory=factory,
            drain_seconds=0,
        )
        service.bind_runtime(manager)
        await service.create(_write(_config()))
        await service.create(
            _write(
                _config(server_id="down-mcp", endpoint="http://127.0.0.1:9/mcp"),
                command_id="cmd-down",
            )
        )
        await service.enable("local-order-mcp", _life())
        await service.enable(
            "down-mcp",
            _life(command_id="cmd-enable-down"),
        )
        connectors.pop("down-mcp", None)
        manager._generations.pop("down-mcp", None)
        boom = True
        changed = await manager.reconcile_loaded()
        assert changed == 0
        assert "local-order-mcp" in connectors
        down = await service.get_server(
            tenant_id="tenant-a",
            server_id="down-mcp",
            actor_id="admin-1",
        )
        assert down.runtime is not None
        assert down.runtime.observed_state is McpObservedState.UNAVAILABLE

    asyncio.run(scenario())


def test_runtime_health_is_instance_scoped_and_aggregated_without_last_writer_wins() -> None:
    async def scenario() -> None:
        store = InMemoryMcpServerRegistryStore()
        service = McpServerRegistryService(store)
        await service.create(_write(_config()))
        now = datetime.now(UTC)
        await service.record_runtime(
            McpServerRuntimeRecord(
                server_id="local-order-mcp",
                instance_id="hands-a",
                loaded_revision=1,
                observed_state=McpObservedState.ACTIVE,
                updated_at=now,
            )
        )
        await service.record_runtime(
            McpServerRuntimeRecord(
                server_id="local-order-mcp",
                instance_id="hands-b",
                observed_state=McpObservedState.UNAVAILABLE,
                consecutive_failures=3,
                updated_at=now,
            )
        )
        record = await service.get_server(
            tenant_id="tenant-a",
            server_id="local-order-mcp",
            actor_id="admin-1",
        )
        assert {item.instance_id for item in record.runtimes} >= {
            "hands-a",
            "hands-b",
        }
        assert record.runtime is not None
        assert record.runtime.instance_id == "aggregate"
        assert record.runtime.observed_state is McpObservedState.ACTIVE

    asyncio.run(scenario())


def test_connection_manager_test_records_last_test_at() -> None:
    async def scenario() -> None:
        store = InMemoryMcpServerRegistryStore()
        service = McpServerRegistryService(store)
        manager = McpConnectionManager(
            registry=service,
            connectors={},
            factory=lambda _server: _FakeConnector(),
            drain_seconds=0,
        )
        service.bind_runtime(manager)
        await service.create(_write(_config()))
        result = await service.test("local-order-mcp", _life(command_id="cmd-test"))
        assert result.status.value == "succeeded"
        record = await service.get_server(
            tenant_id="tenant-a",
            server_id="local-order-mcp",
            actor_id="admin-1",
        )
        assert record.runtime is not None
        assert record.runtime.last_test_at is not None
        assert record.runtime.observed_state is McpObservedState.PENDING
        assert record.desired_state is McpDesiredState.DISABLED

    asyncio.run(scenario())


def test_connection_manager_test_loads_then_revokes_egress() -> None:
    async def scenario() -> None:
        store = InMemoryMcpServerRegistryStore()
        service = McpServerRegistryService(store)
        egress = _FakeEgress()
        manager = McpConnectionManager(
            registry=service,
            connectors={},
            factory=lambda _server: _FakeConnector(),
            egress=egress,
            drain_seconds=0,
        )
        service.bind_runtime(manager)
        await service.create(_write(_config()))
        result = await service.test("local-order-mcp", _life(command_id="cmd-test-egress"))
        assert result.status.value == "succeeded"
        assert egress.events == [
            ("apply", "local-order-mcp"),
            ("revoke", "local-order-mcp"),
        ]
        assert egress.loaded == set()

    asyncio.run(scenario())


def test_connection_manager_enable_persists_egress() -> None:
    async def scenario() -> None:
        store = InMemoryMcpServerRegistryStore()
        service = McpServerRegistryService(store)
        egress = _FakeEgress()
        connectors: dict[str, CapabilityConnector] = {}
        manager = McpConnectionManager(
            registry=service,
            connectors=connectors,
            factory=lambda _server: _FakeConnector(),
            egress=egress,
            drain_seconds=0,
        )
        service.bind_runtime(manager)
        await service.create(_write(_config()))
        result = await service.enable("local-order-mcp", _life())
        assert result.status.value == "succeeded"
        assert "local-order-mcp" in connectors
        assert egress.loaded == {"local-order-mcp"}
        assert egress.events == [("apply", "local-order-mcp")]

    asyncio.run(scenario())


def test_connection_manager_disable_revokes_egress() -> None:
    async def scenario() -> None:
        store = InMemoryMcpServerRegistryStore()
        service = McpServerRegistryService(store)
        egress = _FakeEgress()
        connectors: dict[str, CapabilityConnector] = {}
        manager = McpConnectionManager(
            registry=service,
            connectors=connectors,
            factory=lambda _server: _FakeConnector(),
            egress=egress,
            drain_seconds=0,
        )
        service.bind_runtime(manager)
        await service.create(_write(_config()))
        await service.enable("local-order-mcp", _life())
        await service.disable(
            "local-order-mcp", _life(command_id="cmd-disable", expected_revision=1)
        )
        assert "local-order-mcp" not in connectors
        assert egress.loaded == set()
        assert egress.events[-1] == ("revoke", "local-order-mcp")

    asyncio.run(scenario())


def test_egress_manager_reconcile_keeps_unlisted_adapter() -> None:
    async def scenario() -> None:
        from auraclaw.infrastructure.credentials.mcp_egress_manager import (
            McpEgressManager,
        )
        from auraclaw.infrastructure.credentials.proxy import (
            CredentialProxy,
            InMemoryVault,
        )

        proxy = CredentialProxy(InMemoryVault({}))
        adapters: dict[str, object] = {}
        manager = McpEgressManager(adapters=adapters, proxy=proxy)
        entry = McpActiveSnapshotEntry(
            server_id="auramcp",
            tenant_id="platform",
            revision=2,
            config=_config(server_id="auramcp", tenant_id="platform"),
            desired_state=McpDesiredState.DISABLED,
            observed_state=McpObservedState.PENDING,
        )
        await manager.apply(entry)
        assert "mcp:auramcp:probe:2" in adapters
        changed = await manager.reconcile(())
        assert changed == 0
        assert "mcp:auramcp:probe:2" in adapters
        await manager.revoke("auramcp")
        assert "mcp:auramcp:probe:2" not in adapters

    asyncio.run(scenario())


def test_connection_manager_test_revokes_egress_after_probe_failure() -> None:
    async def scenario() -> None:
        store = InMemoryMcpServerRegistryStore()
        service = McpServerRegistryService(store)
        egress = _FakeEgress()
        manager = McpConnectionManager(
            registry=service,
            connectors={},
            factory=lambda _server: _BoomConnector(),
            egress=egress,
            drain_seconds=0,
        )
        service.bind_runtime(manager)
        await service.create(_write(_config()))
        result = await service.test("local-order-mcp", _life(command_id="cmd-test-boom"))
        assert result.status.value == "failed"
        assert result.result["error_type"] == "RuntimeError"
        assert egress.events == [
            ("apply", "local-order-mcp"),
            ("revoke", "local-order-mcp"),
        ]
        assert egress.loaded == set()

    asyncio.run(scenario())


def test_remote_mcp_egress_client_forwards_apply() -> None:
    async def scenario() -> None:
        captured: dict[str, object] = {}

        def handler(request: httpx.Request) -> httpx.Response:
            captured["path"] = request.url.path
            captured["body"] = json.loads(request.content)
            return httpx.Response(
                200,
                json={
                    "api_version": "2026-07-22",
                    "server_id": "auramcp",
                    "operation": "apply",
                    "status": "applied",
                },
            )

        client = RemoteMcpEgressClient(
            "http://credential-proxy.test",
            bearer_token="hands-token",
            transport=httpx.MockTransport(handler),
        )
        entry = McpActiveSnapshotEntry(
            server_id="auramcp",
            tenant_id="platform",
            revision=2,
            config=_config(server_id="auramcp", tenant_id="platform"),
            desired_state=McpDesiredState.DISABLED,
            observed_state=McpObservedState.PENDING,
        )
        try:
            await client.apply(entry)
        finally:
            await client.aclose()
        assert captured["path"] == "/internal/v1/credentials/mcp-egress"
        body = captured["body"]
        assert isinstance(body, dict)
        assert body["operation"] == "apply"
        assert body["server_id"] == "auramcp"
        context = body["context"]
        assert isinstance(context, dict)
        assert context["service_identity"] == "action-hands"

    asyncio.run(scenario())


def test_remote_mcp_registry_client_forwards_test_to_hands() -> None:
    async def scenario() -> None:
        captured: dict[str, object] = {}

        def handler(request: httpx.Request) -> httpx.Response:
            captured["path"] = request.url.path
            captured["body"] = json.loads(request.content)
            return httpx.Response(
                200,
                json={
                    "api_version": "2026-07-22",
                    "operation_id": "op-1",
                    "status": "succeeded",
                    "server_id": "auramcp",
                    "target_revision": 1,
                    "result": {"desired_state": "disabled"},
                    "safe_error_code": None,
                },
            )

        client = RemoteMcpRegistryClient(
            "http://hands.test",
            bearer_token="task-token",
            transport=httpx.MockTransport(handler),
        )
        try:
            record = await client.test("auramcp", _life(command_id="cmd-test"))
        finally:
            await client.aclose()
        assert record.status.value == "succeeded"
        assert record.server_id == "auramcp"
        assert captured["path"] == "/internal/v1/mcp-registry/command"
        body = captured["body"]
        assert isinstance(body, dict)
        assert body["operation"] == "test"
        assert body["server_id"] == "auramcp"
        context = body["context"]
        assert isinstance(context, dict)
        assert context["service_identity"] == "task-api"

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


def test_historical_mcp_config_load_discards_retired_trust_without_mutation() -> None:
    from copy import deepcopy

    from auraclaw.infrastructure.persistence.postgres_mcp_registry import _stored_config

    legacy = _config().model_dump(mode="json")
    legacy["trust_level"] = "external_untrusted"
    legacy["metadata"] = {
        "tool_policy_overrides": {"order.list": {"permission": "write-with-approval"}},
        "tool_name_aliases": {"order.old": "order.list"},
    }
    original = deepcopy(legacy)
    loaded = _stored_config(legacy)
    assert legacy == original
    assert "trust_level" not in loaded.model_dump()
    assert loaded.metadata == {"tool_name_aliases": {"order.old": "order.list"}}
    assert "trust_level" not in McpServerConfig.model_json_schema()["properties"]


@pytest.mark.parametrize("prefixes", [["old."], [], [""], None])
def test_retired_tool_prefixes_are_read_only_history(prefixes: list[str] | None) -> None:
    from copy import deepcopy

    from auraclaw.infrastructure.persistence.postgres_mcp_registry import _stored_config

    legacy = _config().model_dump(mode="json")
    if prefixes is not None:
        legacy["allowed_tool_prefixes"] = prefixes
    original = deepcopy(legacy)
    loaded = _stored_config(legacy)
    assert legacy == original
    assert "allowed_tool_prefixes" not in loaded.model_dump()
    assert "allowed_tool_prefixes" not in McpServerConfig.model_json_schema()["properties"]
    assert loaded.config_digest() == _config().config_digest()
    if prefixes is not None:
        with pytest.raises(ValidationError, match="allowed_tool_prefixes"):
            McpServerConfig.model_validate(legacy)


def test_failed_revoke_retains_pending_work_and_retries_next_reconciliation() -> None:
    async def scenario() -> None:
        store = InMemoryMcpServerRegistryStore()
        service = McpServerRegistryService(store)
        await service.create(_write(_config()))

        class FailOnce(_FakeEgress):
            failures = 1

            async def revoke(self, server_id, *, expected_revision=None):
                if self.failures:
                    self.failures -= 1
                    raise RuntimeError("injected egress failure")
                await super().revoke(server_id, expected_revision=expected_revision)

        egress = FailOnce()
        # Initial probe cleanup is permitted; inject failure only on disable.
        egress.failures = 0
        connectors = {}
        manager = McpConnectionManager(
            registry=service,
            connectors=connectors,
            factory=lambda server: _FakeConnector(),
            egress=egress,
            drain_seconds=0,
        )
        service.bind_runtime(manager)
        assert (await service.enable("local-order-mcp", _life())).status.value == "succeeded"
        egress.failures = 1
        operation = await service.disable("local-order-mcp", _life(command_id="disable-retry"))
        assert operation.status.value == "reconciling"
        assert not connectors
        assert "local-order-mcp" in manager._pending_revokes
        assert await manager.reconcile_loaded() == 1
        assert not manager._pending_revokes and not manager._generations
        assert "local-order-mcp" not in egress.loaded

    asyncio.run(scenario())


def test_egress_revision_fence_and_authoritative_removal() -> None:
    from auraclaw.infrastructure.credentials.mcp_egress_manager import McpEgressManager
    from auraclaw.infrastructure.credentials.proxy import CredentialProxy, InMemoryVault

    async def scenario() -> None:
        entry = McpActiveSnapshotEntry(
            server_id="auramcp",
            tenant_id="platform",
            revision=1,
            config=_config(server_id="auramcp", tenant_id="platform"),
            desired_state=McpDesiredState.ENABLED,
            observed_state=McpObservedState.ACTIVE,
        )
        desired = (entry,)

        async def authority():
            return desired

        adapters = {}
        manager = McpEgressManager(
            adapters=adapters,
            proxy=CredentialProxy(InMemoryVault({})),
            snapshot_provider=authority,
            drain_seconds=0,
        )
        await manager.apply(entry)
        desired = (entry.model_copy(update={"revision": 2}),)
        await manager.apply(desired[0])
        await manager.revoke("auramcp", expected_revision=1)
        assert manager.loaded_revision("auramcp") == 2
        await manager.revoke("auramcp", expected_revision=2)
        assert "mcp:auramcp" in adapters  # Still enabled by authority.
        desired = ()
        assert await manager.reconcile(desired) == 1
        assert "mcp:auramcp" not in adapters
        with pytest.raises(Exception, match="stale MCP"):
            await manager.apply(entry)

    asyncio.run(scenario())


def test_failed_delete_keeps_retired_intent_and_finishes_after_service_restart() -> None:
    async def scenario() -> None:
        store = InMemoryMcpServerRegistryStore()
        service = McpServerRegistryService(store)
        await service.create(_write(_config()))

        class Fails:
            async def revoke(self, server_id):
                raise RuntimeError("injected cleanup failure")

        service.bind_runtime(Fails())
        pending = await service.delete("local-order-mcp", _life(command_id="delete-pending"))
        assert pending.status.value == "reconciling"
        record = await store.get_server("local-order-mcp")
        assert record.desired_state is McpDesiredState.RETIRED
        assert await service.active_snapshot() == ()

        class RecoveredRuntime:
            revoked = []

            async def revoke(self, server_id):
                self.revoked.append(server_id)

        recovered_runtime = RecoveredRuntime()
        recovered = McpServerRegistryService(store)
        recovered.bind_runtime(recovered_runtime)
        assert await recovered.reconcile_pending_deletes() == 1
        assert await store.get_server("local-order-mcp") is None
        assert recovered_runtime.revoked == ["local-order-mcp"]
        operation = await store.get_operation(pending.operation_id)
        assert operation.status.value == "succeeded"

    asyncio.run(scenario())


def test_probe_revision_does_not_replace_active_and_expires() -> None:
    from auraclaw.infrastructure.credentials.mcp_egress_manager import McpEgressManager
    from auraclaw.infrastructure.credentials.proxy import CredentialProxy, InMemoryVault

    async def scenario() -> None:
        entry = McpActiveSnapshotEntry(
            server_id="auramcp",
            tenant_id="platform",
            revision=1,
            config=_config(server_id="auramcp", tenant_id="platform"),
            desired_state=McpDesiredState.ENABLED,
            observed_state=McpObservedState.ACTIVE,
        )
        adapters = {}
        manager = McpEgressManager(
            adapters=adapters,
            proxy=CredentialProxy(InMemoryVault({})),
            drain_seconds=0,
            probe_ttl_seconds=0,
        )
        await manager.apply(entry)
        active = adapters["mcp:auramcp"]
        await manager.apply(
            entry.model_copy(update={"revision": 2, "desired_state": McpDesiredState.DISABLED})
        )
        probe = adapters["mcp:auramcp:probe:2"]
        assert adapters["mcp:auramcp"] is active
        assert manager.loaded_revision("auramcp") == 1
        with pytest.raises(CredentialAccessError, match="probe cannot"):
            await probe({"method": "tools/call"}, "")
        await manager.reconcile((entry,))
        assert "mcp:auramcp:probe:2" not in adapters
        assert adapters["mcp:auramcp"] is active
        await manager.revoke("auramcp")

    asyncio.run(scenario())


def test_egress_close_failure_is_retried_without_blocking_other_servers() -> None:
    from auraclaw.infrastructure.credentials.mcp_egress_manager import McpEgressManager
    from auraclaw.infrastructure.credentials.proxy import CredentialProxy, InMemoryVault

    async def scenario() -> None:
        adapters = {}
        manager = McpEgressManager(
            adapters=adapters, proxy=CredentialProxy(InMemoryVault({})), drain_seconds=0
        )
        for server_id in ("one", "two"):
            await manager.apply(
                McpActiveSnapshotEntry(
                    server_id=server_id,
                    tenant_id="platform",
                    revision=1,
                    config=_config(server_id=server_id, tenant_id="platform"),
                    desired_state=McpDesiredState.ENABLED,
                    observed_state=McpObservedState.ACTIVE,
                )
            )
        captured = adapters["mcp:one"]
        attempts = 0

        async def flaky_close():
            nonlocal attempts
            attempts += 1
            if attempts == 1:
                raise RuntimeError("injected close failure")

        captured.aclose = flaky_close
        await manager.reconcile(())
        assert adapters == {}
        with pytest.raises(CredentialAccessError, match="revoked"):
            await captured({}, "")
        assert manager.loaded_revision("one") is None
        await manager.reconcile(())
        assert attempts == 2
        assert manager._closing == {}

    asyncio.run(scenario())


def test_late_delete_cannot_remove_recreated_configuration() -> None:
    async def scenario() -> None:
        store = InMemoryMcpServerRegistryStore()
        service = McpServerRegistryService(store)
        await service.create(_write(_config()))

        class RacingRuntime:
            async def revoke(self, server_id):
                await service.create(_write(_config(), command_id="recreate"))

        service.bind_runtime(RacingRuntime())
        result = await service.delete("local-order-mcp", _life(command_id="old-delete"))
        assert result.status.value == "reconciling"
        record = await store.get_server("local-order-mcp")
        assert record.latest_revision == 2
        assert record.desired_state is McpDesiredState.DISABLED
        assert await service.reconcile_pending_deletes() == 0

    asyncio.run(scenario())
