from __future__ import annotations

from collections.abc import MutableMapping
from datetime import UTC, datetime, timedelta
from typing import Any

from auraclaw.contracts.capabilities import McpAuthStrategy, McpServerDefinition
from auraclaw.contracts.mcp_registry import (
    McpActiveSnapshotEntry,
    McpDesiredState,
    none_credential_ref,
)
from auraclaw.contracts.tools import CredentialReference
from auraclaw.infrastructure.credentials.mcp_egress import ManagedMcpEgressAdapter
from auraclaw.infrastructure.credentials.proxy import CredentialProxy


class McpEgressManager:
    """Loads Credential Proxy adapters from the Hands registry snapshot."""

    def __init__(
        self,
        *,
        adapters: MutableMapping[str, Any],
        proxy: CredentialProxy,
        drain_seconds: float = 5.0,
    ) -> None:
        self._adapters = adapters
        self._proxy = proxy
        self._drain_seconds = drain_seconds
        self._generations: dict[str, int] = {}

    async def restore(self, snapshot: tuple[McpActiveSnapshotEntry, ...]) -> int:
        for entry in snapshot:
            await self.apply(entry)
        return len(snapshot)

    async def apply(self, entry: McpActiveSnapshotEntry) -> None:
        definition = entry.config.materialize(
            revision=entry.revision,
            desired_state=McpDesiredState.ENABLED,
            observed_state=entry.observed_state,
        ).model_copy(update={"enabled": True})
        adapter = ManagedMcpEgressAdapter(definition)
        self._adapters[f"mcp:{entry.server_id}"] = adapter
        self._generations[entry.server_id] = entry.revision
        await self._ensure_reference(definition)

    async def revoke(self, server_id: str) -> None:
        self._adapters.pop(f"mcp:{server_id}", None)
        self._generations.pop(server_id, None)

    async def reconcile(self, snapshot: tuple[McpActiveSnapshotEntry, ...]) -> int:
        desired = {entry.server_id: entry for entry in snapshot}
        changed = 0
        for server_id in list(self._generations):
            if server_id not in desired:
                await self.revoke(server_id)
                changed += 1
        for server_id, entry in desired.items():
            if self._generations.get(server_id) != entry.revision:
                await self.apply(entry)
                changed += 1
        return changed

    def loaded_revision(self, server_id: str) -> int | None:
        return self._generations.get(server_id)

    async def _ensure_reference(self, definition: McpServerDefinition) -> None:
        if definition.resolved_auth_strategy is McpAuthStrategy.NONE:
            credential_ref = none_credential_ref(definition.server_id)
        elif definition.credential_ref is None:
            return
        else:
            credential_ref = definition.credential_ref
        reference = CredentialReference(
            credential_ref=credential_ref,
            provider=definition.server_id,
            account_scope=(
                definition.oauth.resource
                if definition.oauth is not None
                else definition.endpoint
            ),
            allowed_operations=("mcp.invoke",),
            expires_at=datetime.now(UTC) + timedelta(days=365),
        )
        tenants = {definition.tenant_id or "platform", "development"}
        for tenant_id in tenants:
            save = getattr(self._proxy, "save_reference", None)
            if save is not None:
                await save(tenant_id, reference)
            else:
                self._proxy.register_reference(tenant_id, reference)
