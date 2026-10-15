"""Credential bootstrap and restoration from the authoritative Hands snapshot."""

from __future__ import annotations

import logging
import secrets
from datetime import UTC, datetime, timedelta

import httpx

from auraclaw.config import Settings
from auraclaw.contracts.internal import (
    InternalRequestContext,
    McpRegistrySnapshotRequest,
    McpRegistrySnapshotResponse,
    ServiceIdentity,
)
from auraclaw.contracts.mcp_registry import McpActiveSnapshotEntry
from auraclaw.contracts.tools import CredentialReference
from auraclaw.infrastructure.credentials.proxy import CredentialProxy
from auraclaw.internal.http import HttpContractClient

logger = logging.getLogger(__name__)


async def hands_mcp_snapshot(
    settings: Settings,
) -> tuple[McpActiveSnapshotEntry, ...] | None:
    token = settings.workload_token_value(ServiceIdentity.CREDENTIAL_PROXY.value)
    if not token:
        return None
    client = httpx.AsyncClient(base_url=settings.hands_url)
    contract = HttpContractClient(client, bearer_token=token)
    try:
        response = await contract.call(
            "/internal/v1/mcp-registry/snapshot",
            McpRegistrySnapshotRequest(
                context=InternalRequestContext(
                    tenant_id="platform",
                    service_identity=ServiceIdentity.CREDENTIAL_PROXY,
                    request_id=secrets.token_hex(12),
                    correlation_id="mcp-egress-restore",
                    causation_id="mcp-egress-restore",
                )
            ),
            McpRegistrySnapshotResponse,
        )
    except Exception:
        logger.warning("MCP registry snapshot is unavailable; retrying on the next tick")
        return None
    finally:
        await client.aclose()
    return tuple(McpActiveSnapshotEntry.model_validate(item) for item in response.servers)


async def seed_managed_connector_credentials(
    proxy: CredentialProxy,
    settings: Settings,
) -> int:
    expires_at = datetime.now(UTC) + timedelta(days=365)
    debug_tenants = (
        ("local", "development", "1") if settings.deployment_profile == "development" else ()
    )
    seeded = 0
    for java_server in settings.java_api_servers:
        if java_server.credential_ref is None:
            continue
        reference = CredentialReference(
            credential_ref=java_server.credential_ref,
            provider=java_server.server_id,
            account_scope=java_server.base_url,
            allowed_operations=("http.invoke",),
            expires_at=expires_at,
        )
        tenants = {java_server.tenant_id or "platform", *debug_tenants}
        for tenant_id in tenants:
            seeded += int(await proxy.seed_reference(tenant_id, reference))
    return seeded
