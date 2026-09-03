"""Real process/HTTP regression; requires an explicitly selected disposable PostgreSQL DB."""

from __future__ import annotations

import asyncio
import multiprocessing
import os
import socket
import threading
import time
from datetime import timedelta
from http.server import ThreadingHTTPServer
from uuid import uuid4

import httpx
import pytest
import uvicorn
from tests.unit.test_mcp_target_http import Allow, Handler, NoFallback

from auraclaw.action.capability_catalog import (
    CapabilityCatalog,
    CapabilityLoadExecutor,
    CapabilitySearchExecutor,
    RoutedHandsExecutor,
    capability_load_tool,
    capability_search_tool,
)
from auraclaw.action.catalog_reconciler import CapabilityCatalogReconciler
from auraclaw.action.hands import HandsGateway
from auraclaw.action.hands_http import StaticHandsAuthenticator, create_hands_http_app
from auraclaw.action.mcp_connection_manager import McpConnectionManager
from auraclaw.action.mcp_registry import McpServerRegistryService
from auraclaw.action.policy import PolicyEngine
from auraclaw.action.tool_gateway import ToolGateway, ToolRegistry
from auraclaw.config import get_settings
from auraclaw.contracts.hands import HandsTrustedContext
from auraclaw.contracts.mcp_registry import (
    McpServerConfig,
    McpServerLifecycleCommand,
    McpServerWriteCommand,
)
from auraclaw.control.ports import RuntimeAssignment
from auraclaw.infrastructure.artifacts.store import ArtifactStore, InMemoryObjectStorage
from auraclaw.infrastructure.connectors.mcp.connector import ManagedMcpConnector
from auraclaw.infrastructure.credentials.mcp_egress_manager import McpEgressManager
from auraclaw.infrastructure.credentials.proxy import CredentialProxy, InMemoryVault
from auraclaw.infrastructure.persistence.postgres_capability_catalog import (
    PostgresCapabilityCatalogStore,
)
from auraclaw.infrastructure.persistence.postgres_mcp_registry import PostgresMcpServerRegistryStore
from auraclaw.projection.approval.projector import InMemoryApprovalProjection
from auraclaw.runtime.capability_controller import RuntimeCapabilityController
from auraclaw.runtime.hands_adapter import HandsRuntimeAdapter
from auraclaw.runtime.hands_client import HttpHandsClient
from auraclaw.runtime.ports import ToolCall

SETTINGS = get_settings()
DATABASE_URL = SETTINGS.resolved_database_url if SETTINGS.postgres_enabled else None
pytestmark = pytest.mark.skipif(DATABASE_URL is None, reason="Explicit PostgreSQL required")


class Replica:
    def __init__(self, database_url, tenant, instance):
        self.store = PostgresCapabilityCatalogStore(database_url)
        self.registry_store = PostgresMcpServerRegistryStore(database_url)
        self.service = McpServerRegistryService(self.registry_store)
        self.catalog = CapabilityCatalog(self.store)
        self.adapters = {}
        self.connectors = {}
        proxy = CredentialProxy(InMemoryVault({}))
        self.egress = McpEgressManager(
            adapters=self.adapters,
            proxy=proxy,
            drain_seconds=0,
            snapshot_provider=self.service.active_snapshot,
        )
        adapters = self.adapters

        class Credentials:
            async def invoke(self, **kwargs):
                return await proxy.invoke(**kwargs, adapter=adapters.get(kwargs["tool_name"]))

            def redact(self, value):
                return proxy.redact(value)

        search, load = capability_search_tool(), capability_load_tool()
        self.registry = ToolRegistry((search, load))
        router = RoutedHandsExecutor(
            NoFallback(),
            {
                search.name: CapabilitySearchExecutor(self.catalog),
                load.name: CapabilityLoadExecutor(self.catalog),
            },
        )
        self.reconciler = CapabilityCatalogReconciler(
            catalog=self.catalog,
            store=self.store,
            connectors=self.connectors,
            tool_registry=self.registry,
            hands_router=router,
        )
        self.manager = McpConnectionManager(
            registry=self.service,
            connectors=self.connectors,
            catalog=self.catalog,
            reconciler=self.reconciler,
            egress=self.egress,
            drain_seconds=0,
            instance_id=instance,
            factory=lambda definition: ManagedMcpConnector(
                definition,
                credentials=Credentials(),
                policy=Allow(),
            ),
        )
        trusted = HandsTrustedContext(
            tenant_id=tenant,
            root_session_id="root",
            session_id="session",
            run_id="run",
            runtime_id="runtime",
            lease_id="lease",
            fencing_token=1,
            user_id="user",
            dept_id="dept",
        )
        gateway = HandsGateway(
            registry=self.registry,
            gateway=ToolGateway(
                registry=self.registry,
                hands=router,
                policy=PolicyEngine(),
                approvals=InMemoryApprovalProjection(),
                artifacts=ArtifactStore(
                    InMemoryObjectStorage(), signing_key=b"mcp-replica-process-test"
                ),
            ),
        )
        self.app = create_hands_http_app(
            gateway,
            authenticator=StaticHandsAuthenticator({"test": trusted}),
        )

    async def close(self):
        for connector in self.connectors.values():
            await connector.aclose()
        for adapter in self.adapters.values():
            await adapter.aclose()
        await self.store.close()
        await self.registry_store.close()


def replica_process(database_url, tenant, instance, pipe):
    async def run():
        replica = Replica(database_url, tenant, instance)
        listener = socket.socket()
        listener.bind(("127.0.0.1", 0))
        server = uvicorn.Server(uvicorn.Config(replica.app, log_level="error", lifespan="off"))
        start = time.monotonic()
        await replica.manager.restore()
        serving = asyncio.create_task(server.serve(sockets=[listener]))
        while not server.started:
            if serving.done():
                await serving
            await asyncio.sleep(0.01)

        async def reconcile():
            while True:
                await replica.manager.reconcile_loaded()
                await replica.egress.reconcile(await replica.service.active_snapshot())
                await asyncio.sleep(0.1)

        ticking = asyncio.create_task(reconcile())
        pipe.send(
            {
                "port": listener.getsockname()[1],
                "pid": os.getpid(),
                "restore_seconds": time.monotonic() - start,
            }
        )
        try:
            while True:
                command = await asyncio.to_thread(pipe.recv)
                if command == "stop":
                    break
                if ticking.done():
                    await ticking
                pipe.send(
                    {
                        "connectors": list(replica.connectors),
                        "adapters": list(replica.adapters),
                        "generations": {
                            key: snapshot.extra.get("_auraclaw_catalog_generation")
                            for key, snapshot in replica.reconciler._snapshots.items()
                        },
                    }
                )
        finally:
            ticking.cancel()
            await asyncio.gather(ticking, return_exceptions=True)
            server.should_exit = True
            await serving
            await replica.close()
            listener.close()
            pipe.close()

    asyncio.run(run())


def test_cold_processes_restart_and_revoke_without_notifications():
    async def scenario():
        assert DATABASE_URL is not None
        tenant = "replica-" + uuid4().hex[:12]
        leader = Replica(DATABASE_URL, tenant, tenant + "-leader")
        servers, ids, processes, leases = [], [], [], []
        context = multiprocessing.get_context("spawn")

        async def receive(pipe):
            assert await asyncio.to_thread(pipe.poll, 15), "Hands process did not answer"
            return pipe.recv()

        async def start(index):
            parent, child = context.Pipe()
            process = context.Process(
                target=replica_process,
                args=(DATABASE_URL, tenant, f"{tenant}-{index}", child),
            )
            process.start()
            child.close()
            processes.append((process, parent))
            info = await receive(parent)
            assert info["restore_seconds"] < 5
            return process, parent, info

        def lifecycle(identifier):
            return McpServerLifecycleCommand(
                tenant_id=tenant,
                actor_id="admin",
                command_id=identifier,
                correlation_id=identifier,
                causation_id=identifier,
                expected_revision=1,
            )

        assignment = RuntimeAssignment(
            tenant_id=tenant,
            root_session_id="root",
            session_id="session",
            run_id="run",
            runtime_id="runtime",
            lease_id="lease",
            fencing_token=1,
            role="worker",
            resource_profile={},
            user_id="user",
            dept_id="dept",
        )

        async def load_and_call(info, expected_servers):
            async with httpx.AsyncClient(base_url=f"http://127.0.0.1:{info['port']}") as raw:
                client = HandsRuntimeAdapter(
                    HttpHandsClient(raw, bearer_tokens={"runtime": "test"})
                )
                searched = await client.execute(
                    assignment,
                    ToolCall(
                        tool_invocation_id=uuid4().hex,
                        name=capability_search_tool().name,
                        arguments={"query": "lookup", "kinds": ["tool"]},
                    ),
                )
                assert searched["status"] == "success", searched
                found = searched["content"]["capabilities"]
                assert {item["server_id"] for item in found} == set(expected_servers)
                loaded = await client.execute(
                    assignment,
                    ToolCall(
                        tool_invocation_id=uuid4().hex,
                        name=capability_load_tool().name,
                        arguments={"capability_ids": [item["capability_id"] for item in found]},
                    ),
                )
                state = {
                    "loaded": {
                        item["capability_id"]: item for item in loaded["content"]["capabilities"]
                    }
                }
                controller = RuntimeCapabilityController(client)
                for item in state["loaded"].values():
                    result = await controller.execute(
                        assignment,
                        ToolCall(
                            tool_invocation_id=uuid4().hex,
                            name=item["model_tool"]["function"]["name"],
                            arguments={"value": 42},
                        ),
                        state,
                    )
                    assert result.result["status"] == "success", result
                    assert result.result["content"] == {"server": item["server_id"], "value": 42}
                return state

        try:
            for index in range(2):
                identifier = f"{tenant}-{index}"
                ids.append(identifier)
                handler = type("ProcessHandler", (Handler,), {"marker": identifier, "requests": []})
                server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
                threading.Thread(target=server.serve_forever, daemon=True).start()
                servers.append(server)
                config = McpServerConfig(
                    server_id=identifier,
                    tenant_id=tenant,
                    title=identifier,
                    endpoint=f"http://127.0.0.1:{server.server_port}/mcp",
                    protocol_revision="2025-11-25",
                    auth_strategy="none",
                    network_mode="loopback",
                )
                await leader.service.create(
                    McpServerWriteCommand(
                        tenant_id=tenant,
                        actor_id="admin",
                        command_id=identifier,
                        correlation_id=identifier,
                        causation_id=identifier,
                        expected_revision=0,
                        config=config,
                    )
                )
                await leader.service.enable(identifier, lifecycle(identifier + "-enable"))
            assert await leader.manager.restore() == 2
            for identifier in ids:
                lease = await leader.store.claim_catalog_reconcile(
                    server_id=identifier,
                    owner="held-leader",
                    ttl=timedelta(minutes=2),
                )
                assert lease is not None
                leases.append(lease)
            lists_before = sum(
                sum(r["method"] == "tools/list" for r in s.RequestHandlerClass.requests)
                for s in servers
            )
            one, two = await asyncio.gather(start(1), start(2))
            assert one[2]["pid"] != two[2]["pid"] != os.getpid()
            for _, pipe, _ in (one, two):
                pipe.send("status")
                status = await receive(pipe)
                assert set(status["connectors"]) == set(ids)
                assert set(status["generations"]) == set(ids)
            old_state, _ = await asyncio.gather(
                load_and_call(one[2], ids), load_and_call(two[2], ids)
            )
            one[0].terminate()
            await asyncio.to_thread(one[0].join, 5)
            restarted = await start(3)
            await load_and_call(restarted[2], ids)
            assert (
                sum(
                    sum(r["method"] == "tools/list" for r in s.RequestHandlerClass.requests)
                    for s in servers
                )
                == lists_before
            )  # no rediscovery despite restart/lease contention
            before_revoke = len(
                [r for r in servers[0].RequestHandlerClass.requests if r["method"] == "tools/call"]
            )
            started = time.monotonic()
            await leader.service.disable(ids[0], lifecycle(ids[0] + "-disable"))
            # Another owner removes the shared catalog before these replicas revoke locally.
            await leader.store.remove_server(ids[0])
            for _, pipe, _ in (two, restarted):
                while True:
                    pipe.send("status")
                    status = await receive(pipe)
                    if (
                        ids[0] not in status["connectors"]
                        and f"mcp:{ids[0]}" not in status["adapters"]
                    ):
                        break
                    assert time.monotonic() - started < 5, status
                    await asyncio.sleep(0.02)
                assert ids[0] not in status["generations"]
            assert time.monotonic() - started < 5
            for _, _, info in (two, restarted):
                await load_and_call(info, [ids[1]])
                async with httpx.AsyncClient(base_url=f"http://127.0.0.1:{info['port']}") as raw:
                    client = HandsRuntimeAdapter(
                        HttpHandsClient(raw, bearer_tokens={"runtime": "test"})
                    )
                    item = next(
                        item for item in old_state["loaded"].values() if item["server_id"] == ids[0]
                    )
                    result = await RuntimeCapabilityController(client).execute(
                        assignment,
                        ToolCall(
                            tool_invocation_id=uuid4().hex,
                            name=item["model_tool"]["function"]["name"],
                            arguments={"value": 42},
                        ),
                        old_state,
                    )
                    assert result.result["status"] != "success"
            assert (
                len(
                    [
                        r
                        for r in servers[0].RequestHandlerClass.requests
                        if r["method"] == "tools/call"
                    ]
                )
                == before_revoke
            )
        finally:
            for process, pipe in processes:
                if process.is_alive():
                    pipe.send("stop")
                    await asyncio.to_thread(process.join, 5)
                    if process.is_alive():
                        process.terminate()
                        await asyncio.to_thread(process.join, 5)
                pipe.close()
            for lease in leases:
                await leader.store.release_catalog_reconcile(lease)
            for identifier in ids:
                await leader.store.remove_server(identifier)
                await leader.registry_store.delete_server(identifier)
            await leader.close()
            for server in servers:
                server.shutdown()
                server.server_close()

    asyncio.run(scenario())
