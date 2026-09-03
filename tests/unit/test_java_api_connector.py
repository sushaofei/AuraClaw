from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from auraclaw.action.capability_catalog import (
    CapabilityCatalog,
    InMemoryCapabilityCatalogStore,
    RoutedHandsExecutor,
)
from auraclaw.action.catalog_reconciler import CapabilityCatalogReconciler
from auraclaw.action.hands import HandsGateway
from auraclaw.action.policy import PolicyEngine
from auraclaw.action.ports import PolicyEvaluation
from auraclaw.action.tool_gateway import ToolGateway, ToolRegistry
from auraclaw.contracts.capabilities import (
    CapabilityStatus,
    JavaApiArgumentBinding,
    JavaApiOperationDefinition,
    JavaApiServerDefinition,
)
from auraclaw.contracts.errors import CredentialAccessError, PolicyDeniedError
from auraclaw.contracts.hands import HandsToolCall, HandsTrustedContext
from auraclaw.contracts.tools import (
    PolicyDecision,
)
from auraclaw.control.ports import RuntimeAssignment
from auraclaw.infrastructure.artifacts.store import ArtifactStore, InMemoryObjectStorage
from auraclaw.infrastructure.connectors.http.connector import (
    ManagedJavaApiConnector,
    catalog_server_definition,
)
from auraclaw.infrastructure.connectors.http.egress import ManagedJavaApiEgressAdapter
from auraclaw.internal.hands import InProcessHandsClient


class _AllowPolicy:
    async def evaluate_action(self, **arguments: object) -> PolicyEvaluation:
        del arguments
        return PolicyEvaluation(
            decision=PolicyDecision.ALLOW,
            decision_id="policy-java-api",
            policy_version="java-api-v1",
        )


class _DenyRemotePolicy(_AllowPolicy):
    async def evaluate_action(self, **arguments: object) -> PolicyEvaluation:
        del arguments
        return PolicyEvaluation(
            decision=PolicyDecision.DENY,
            decision_id="policy-java-api-deny",
            policy_version="java-api-v1",
        )


class _InventoryBackend:
    def __init__(self) -> None:
        self.reserves = 0
        self.seen_keys: set[str] = set()

    async def invoke(self, **arguments: object) -> dict[str, object]:
        request = arguments["request"]
        assert isinstance(request, dict)
        if request["operation_id"] == "get-sku":
            return {"sku": request["path"].rsplit("/", 1)[-1], "qty": 4}
        if request["operation_id"] == "reserve-sku":
            key = str(request["idempotency_key"])
            if key not in self.seen_keys:
                self.reserves += 1
                self.seen_keys.add(key)
            return {"reserved": True, "sku": request["path"].split("/")[2]}
        raise AssertionError(request)

    def redact(self, value: object) -> object:
        return value


class _UnexpectedHands:
    async def execute(self, invocation: object, capability: object) -> object:
        raise AssertionError(f"unexpected local Tool: {invocation}, {capability}")


def _server() -> JavaApiServerDefinition:
    return JavaApiServerDefinition(
        server_id="inventory-api",
        tenant_id="tenant-a",
        title="Inventory Java API",
        base_url="https://inventory.example/api",
        credential_ref="vault/inventory#token",
        operations=(
            JavaApiOperationDefinition(
                operation_id="get-sku",
                tool_name="inventory.sku.get",
                version="1.0.0",
                description="Read inventory quantity",
                method="GET",
                path_template="/inventory/{sku}",
                argument_bindings=(
                    JavaApiArgumentBinding(name="sku", location="path", required=True),
                ),
                input_schema={
                    "type": "object",
                    "properties": {"sku": {"type": "string"}},
                    "required": ["sku"],
                    "additionalProperties": False,
                },
                output_schema={"type": "object"},
                read_only=True,
                idempotent=True,
                permission="read-only",
                risk_level="low",
            ),
            JavaApiOperationDefinition(
                operation_id="reserve-sku",
                tool_name="inventory.sku.reserve",
                version="1.0.0",
                description="Reserve inventory",
                method="POST",
                path_template="/inventory/{sku}/reserve",
                argument_bindings=(
                    JavaApiArgumentBinding(name="sku", location="path", required=True),
                    JavaApiArgumentBinding(name="qty", location="body", required=True),
                ),
                input_schema={
                    "type": "object",
                    "properties": {
                        "sku": {"type": "string"},
                        "qty": {"type": "integer"},
                    },
                    "required": ["sku", "qty"],
                    "additionalProperties": False,
                },
                output_schema={"type": "object"},
                idempotent=True,
                permission="write-with-approval",
                risk_level="high",
            ),
        ),
        status=CapabilityStatus.ACTIVE,
        enabled=True,
    )


def _trusted() -> HandsTrustedContext:
    return HandsTrustedContext(
        tenant_id="tenant-a",
        root_session_id="session-root",
        session_id="session-child",
        run_id="run-1",
        runtime_id="runtime-1",
        lease_id="lease-1",
        fencing_token=1,
        deadline=datetime.now(UTC) + timedelta(minutes=1),
    )


def _assignment() -> RuntimeAssignment:
    return RuntimeAssignment(
        tenant_id="tenant-a",
        root_session_id="session-root",
        session_id="session-child",
        run_id="run-1",
        runtime_id="runtime-1",
        lease_id="lease-1",
        fencing_token=1,
        role="worker",
        resource_profile={},
        deadline=datetime.now(UTC) + timedelta(minutes=1),
    )


def test_java_api_connector_rejects_transport_overrides_and_cross_tenant() -> None:
    async def scenario() -> None:
        backend = _InventoryBackend()
        connector = ManagedJavaApiConnector(
            _server(), credentials=backend, policy=_AllowPolicy()
        )
        snapshot = await connector.snapshot(_trusted())
        assert [item.name for item in snapshot.tools] == [
            "inventory.sku.get",
            "inventory.sku.reserve",
        ]
        result = await connector.call_tool(
            _trusted(),
            name="inventory.sku.get",
            arguments={"sku": "SKU-1"},
            invocation_id="get-1",
        )
        assert result.content == {"sku": "SKU-1", "qty": 4}
        with pytest.raises(ValueError, match="undeclared"):
            await connector.call_tool(
                _trusted(),
                name="inventory.sku.get",
                arguments={"sku": "SKU-1", "url": "https://attacker.example"},
                invocation_id="get-bad",
            )
        with pytest.raises(ValueError, match="unsafe"):
            await connector.call_tool(
                _trusted(),
                name="inventory.sku.get",
                arguments={"sku": "../admin"},
                invocation_id="get-path",
            )
        other = _trusted().model_copy(update={"tenant_id": "tenant-b"})
        with pytest.raises(PolicyDeniedError):
            await connector.call_tool(
                other,
                name="inventory.sku.get",
                arguments={"sku": "SKU-1"},
                invocation_id="get-cross",
            )
        denied = ManagedJavaApiConnector(
            _server(), credentials=backend, policy=_DenyRemotePolicy()
        )
        with pytest.raises(PolicyDeniedError):
            await denied.call_tool(
                _trusted(),
                name="inventory.sku.get",
                arguments={"sku": "SKU-1"},
                invocation_id="get-policy",
            )

    asyncio.run(scenario())


def test_java_api_tools_share_gateway_approval_and_idempotency() -> None:
    async def scenario() -> None:
        backend = _InventoryBackend()
        server = _server()
        connector = ManagedJavaApiConnector(
            server, credentials=backend, policy=_AllowPolicy()
        )
        store = InMemoryCapabilityCatalogStore()
        catalog = CapabilityCatalog(store)
        catalog_server = catalog_server_definition(server)
        await catalog.register_server(catalog_server)
        registry = ToolRegistry()
        router = RoutedHandsExecutor(_UnexpectedHands(), {})
        reconciler = CapabilityCatalogReconciler(
            catalog=catalog,
            store=store,
            connectors={server.server_id: connector},
            tool_registry=registry,
            hands_router=router,
        )
        result = await reconciler.reconcile_server(catalog_server)
        assert result.status == CapabilityStatus.ACTIVE
        assert result.capability_count == 2

        class _NoApprovals:
            async def get(self, tenant_id: str, approval_id: str) -> None:
                del tenant_id, approval_id
                return None

            async def find_approved(
                self,
                tenant_id: str,
                session_id: str,
                digest: str,
                policy_version: str,
            ) -> None:
                del tenant_id, session_id, digest, policy_version
                return None

        gateway = ToolGateway(
            registry=registry,
            policy=PolicyEngine(),
            approvals=_NoApprovals(),
            hands=router,
            artifacts=ArtifactStore(
                InMemoryObjectStorage(), signing_key=b"java-api-artifact-key"
            ),
        )
        client = InProcessHandsClient(
            HandsGateway(registry=registry, gateway=gateway)
        )
        assignment = _assignment()
        read = await connector.call_tool(
            _trusted(),
            name="inventory.sku.get",
            arguments={"sku": "SKU-1"},
            invocation_id="read-1",
        )
        assert read.status == "success"
        assert read.content == {"sku": "SKU-1", "qty": 4}
        write = await client.call_tool(
            assignment,
            HandsToolCall(
                tool_invocation_id="write-1",
                name="inventory.sku.reserve",
                version="1.0.0",
                arguments={"sku": "SKU-1", "qty": 1},
                expected_side_effect="write",
            ),
        )
        assert write.status == "denied"
        assert write.error_code == "approval_required"
        assert backend.reserves == 0
        reserved = await connector.call_tool(
            _trusted(),
            name="inventory.sku.reserve",
            arguments={"sku": "SKU-1", "qty": 1},
            invocation_id="write-ok-1",
        )
        assert reserved.status == "success"
        repeated = await connector.call_tool(
            _trusted(),
            name="inventory.sku.reserve",
            arguments={"sku": "SKU-1", "qty": 1},
            invocation_id="write-ok-1",
        )
        assert repeated == reserved
        assert backend.reserves == 1

    asyncio.run(scenario())


class _PrivateResolver:
    async def resolve(self, host: str, port: int) -> tuple[str, ...]:
        del port
        if host == "inventory.internal":
            return ("10.0.0.8",)
        if host == "inventory.example":
            return ("1.2.3.4",)
        return ("127.0.0.1",)


class _RecordingSender:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []
        self.redirect = False

    async def send(self, **arguments: object) -> Any:
        self.calls.append(arguments)
        from auraclaw.infrastructure.credentials.mcp_egress import McpEgressResponse

        if self.redirect:
            return McpEgressResponse(
                status_code=302,
                headers={"location": "https://attacker.example"},
                content=b"",
            )
        return McpEgressResponse(
            status_code=200,
            headers={"content-type": "application/json"},
            content=b'{"ok":true,"authorization":"Bearer super-secret"}',
        )


def test_java_api_egress_pins_dns_blocks_private_and_redacts_secrets() -> None:
    async def scenario() -> None:
        sender = _RecordingSender()
        adapter = ManagedJavaApiEgressAdapter(
            _server(),
            resolver=_PrivateResolver(),
            sender=sender,
        )
        redacted = await adapter(
            {
                "server_id": "inventory-api",
                "operation_id": "get-sku",
                "path": "/inventory/SKU-1",
                "query": {},
                "body": {},
                "idempotency_key": "get-1",
            },
            "super-secret",
        )
        assert redacted == {"ok": True, "authorization": "Bearer [REDACTED]"}
        assert sender.calls[0]["approved_ip"] == "1.2.3.4"
        assert "super-secret" not in str(redacted)

        with pytest.raises(CredentialAccessError, match="unsupported fields"):
            await adapter(
                {
                    "server_id": "inventory-api",
                    "operation_id": "get-sku",
                    "path": "/inventory/SKU-1",
                    "headers": {"Authorization": "Bearer stolen"},
                },
                "super-secret",
            )
        private_server = _server().model_copy(
            update={"base_url": "https://inventory.internal/api"}
        )
        private_adapter = ManagedJavaApiEgressAdapter(
            private_server,
            resolver=_PrivateResolver(),
            sender=sender,
        )
        with pytest.raises(CredentialAccessError, match="non-public"):
            await private_adapter(
                {
                    "server_id": "inventory-api",
                    "operation_id": "get-sku",
                    "path": "/inventory/SKU-1",
                    "query": {},
                    "body": {},
                },
                "super-secret",
            )
        allowlisted = ManagedJavaApiEgressAdapter(
            private_server.model_copy(
                update={"allowed_private_hosts": ("inventory.internal",)}
            ),
            resolver=_PrivateResolver(),
            sender=sender,
        )
        allowed = await allowlisted(
            {
                "server_id": "inventory-api",
                "operation_id": "get-sku",
                "path": "/inventory/SKU-1",
                "query": {},
                "body": {},
            },
            "super-secret",
        )
        assert allowed["ok"] is True
        sender.redirect = True
        with pytest.raises(CredentialAccessError, match="HTTP 302"):
            await adapter(
                {
                    "server_id": "inventory-api",
                    "operation_id": "get-sku",
                    "path": "/inventory/SKU-1",
                    "query": {},
                    "body": {},
                },
                "super-secret",
            )

    asyncio.run(scenario())
