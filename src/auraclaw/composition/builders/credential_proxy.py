from __future__ import annotations

from fastapi import FastAPI

from auraclaw.composition.services import (
    ServiceSpec,
    _base_service_app,
    _configured_identities,
    _hands_mcp_snapshot,
    _require_production_security_configuration,
    _seed_managed_connector_credentials,
)
from auraclaw.config import Settings
from auraclaw.contracts.internal import ServiceIdentity
from auraclaw.contracts.mcp_registry import McpActiveSnapshotEntry
from auraclaw.credential_proxy.internal_service import (
    CredentialProxyInternalService,
    CredentialTargetAdapter,
)
from auraclaw.infrastructure.clients.policy import RemotePolicyClient
from auraclaw.infrastructure.connectors.http.egress import ManagedJavaApiEgressAdapter
from auraclaw.infrastructure.credentials.mcp_egress_manager import McpEgressManager
from auraclaw.infrastructure.credentials.proxy import CredentialProxy, InMemoryVault
from auraclaw.infrastructure.credentials.vault import HashiCorpVault
from auraclaw.infrastructure.credentials.webhook import ManagedWebhookCredentialAdapter
from auraclaw.infrastructure.persistence.postgres_credential_registry import (
    PostgresCredentialRegistry,
)
from auraclaw.internal.http import create_contract_app
from auraclaw.internal.routes import credential_routes


def build_credential_proxy_app(spec: ServiceSpec, settings: Settings) -> FastAPI:
    _require_production_security_configuration(
        settings,
        spec.name,
        (
            ServiceIdentity.TASK_API,
            ServiceIdentity.ACTION_HANDS,
            ServiceIdentity.DELIVERY_WORKER,
            ServiceIdentity.CREDENTIAL_PROXY,
        ),
        requires_policy=True,
    )
    if settings.deployment_profile == "production":
        if settings.debug_vault_secrets:
            raise ValueError("credential-proxy production forbids debug Vault secrets")
        if not settings.credential_vault_addr or settings.credential_vault_token is None:
            raise ValueError("credential-proxy production requires an external Vault")
    registry = (
        PostgresCredentialRegistry(settings.resolved_database_url)
        if settings.sql_storage_enabled
        else None
    )
    vault: InMemoryVault | HashiCorpVault
    if settings.credential_vault_addr and settings.credential_vault_token is not None:
        vault = HashiCorpVault(
            settings.credential_vault_addr,
            token=settings.credential_vault_token.get_secret_value(),
            mount=settings.credential_vault_mount,
        )
    else:
        vault = InMemoryVault(settings.debug_vault_secrets)
    policy: RemotePolicyClient | None = None
    token = settings.workload_token_value(ServiceIdentity.CREDENTIAL_PROXY.value)
    if token:
        policy = RemotePolicyClient(
            settings.policy_base_url,
            bearer_token=token,
            service_identity=ServiceIdentity.CREDENTIAL_PROXY,
        )
    java_api_adapters = {
        f"java-api:{server.server_id}": ManagedJavaApiEgressAdapter(server)
        for server in settings.java_api_servers
    }
    closeables: tuple[object, ...] = (
        *((registry,) if registry is not None else ()),
        *((vault,) if isinstance(vault, HashiCorpVault) else ()),
        *((policy,) if policy is not None else ()),
        *java_api_adapters.values(),
    )
    app = _base_service_app(
        spec,
        settings,
        closeables=closeables,
        readiness_probe=(vault.readiness if isinstance(vault, HashiCorpVault) else None),
    )
    proxy = CredentialProxy(vault, registry=registry)
    adapters: dict[str, CredentialTargetAdapter] = {
        "webhook": ManagedWebhookCredentialAdapter(
            allowed_hosts=settings.allowed_credential_egress_hosts
        ),
        **java_api_adapters,
    }
    async def mcp_snapshot_authority() -> tuple[McpActiveSnapshotEntry, ...]:
        snapshot = await _hands_mcp_snapshot(settings)
        if snapshot is None:
            raise RuntimeError("MCP snapshot authority is unavailable")
        return snapshot

    mcp_egress = McpEgressManager(
        adapters=adapters, proxy=proxy, snapshot_provider=mcp_snapshot_authority
    )
    service = CredentialProxyInternalService(
        proxy,
        adapters=adapters,
        policy=policy,
        mcp_egress=mcp_egress,
    )

    async def restore_mcp_egress() -> None:
        snapshot = await _hands_mcp_snapshot(settings)
        if snapshot is None:
            return
        await mcp_egress.restore(snapshot)

    async def initialize() -> None:
        await _seed_managed_connector_credentials(proxy, settings)
        await restore_mcp_egress()

    async def reconcile_mcp_egress() -> int:
        snapshot = await _hands_mcp_snapshot(settings)
        if snapshot is None:
            return 0
        return await mcp_egress.reconcile(snapshot)

    app.state.initialize = initialize
    app.state.tick = reconcile_mcp_egress
    app.state.worker = True
    app.state.worker_interval = settings.mcp_revision_reconcile_interval_seconds
    contract_app = create_contract_app(
        "credential-proxy",
        credential_routes(service),
        workload_identities=_configured_identities(
            settings,
            (
                ServiceIdentity.TASK_API,
                ServiceIdentity.ACTION_HANDS,
                ServiceIdentity.DELIVERY_WORKER,
            ),
        ),
    )
    app.mount("/", contract_app)
    return app
