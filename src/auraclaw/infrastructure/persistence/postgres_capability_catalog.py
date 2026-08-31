from __future__ import annotations

from datetime import datetime

from auraclaw.action.ports import CatalogSyncHealth
from auraclaw.contracts.capabilities import (
    CapabilityDescriptor,
    CapabilityKind,
    CapabilityStatus,
    CapabilityTrustLevel,
    McpOAuthConfiguration,
    McpServerDefinition,
)
from auraclaw.infrastructure.persistence.postgres_common import (
    LazyPool,
    json_dumps,
    json_loads,
)


class PostgresCapabilityCatalogStore(LazyPool):
    async def upsert_server(self, server: McpServerDefinition) -> None:
        pool = await self.pool()
        await pool.execute(
            """INSERT INTO hands.downstream_mcp_server
            (server_id,tenant_id,title,endpoint,protocol_revision,credential_ref,
             trust_level,allowed_tool_prefixes,allowed_resource_schemes,
             allowed_prompt_prefixes,status,enabled,metadata,updated_at)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8::jsonb,$9::jsonb,$10::jsonb,$11,$12,
                    $13::jsonb,now())
            ON CONFLICT (server_id) DO UPDATE SET
              tenant_id=EXCLUDED.tenant_id,title=EXCLUDED.title,
              endpoint=EXCLUDED.endpoint,protocol_revision=EXCLUDED.protocol_revision,
              credential_ref=EXCLUDED.credential_ref,trust_level=EXCLUDED.trust_level,
              allowed_tool_prefixes=EXCLUDED.allowed_tool_prefixes,
              allowed_resource_schemes=EXCLUDED.allowed_resource_schemes,
              allowed_prompt_prefixes=EXCLUDED.allowed_prompt_prefixes,
              status=EXCLUDED.status,enabled=EXCLUDED.enabled,
              metadata=EXCLUDED.metadata,updated_at=now()""",
            server.server_id,
            server.tenant_id,
            server.title,
            server.endpoint,
            server.protocol_revision,
            server.credential_ref,
            server.trust_level.value,
            json_dumps(server.allowed_tool_prefixes),
            json_dumps(server.allowed_resource_schemes),
            json_dumps(server.allowed_prompt_prefixes),
            server.status.value,
            server.enabled,
            json_dumps(
                {
                    **server.metadata,
                    **(
                        {"_auraclaw_oauth": server.oauth.model_dump(mode="json")}
                        if server.oauth is not None
                        else {}
                    ),
                    **(
                        {
                            "_auraclaw_allowed_private_hosts": list(
                                server.allowed_private_hosts
                            )
                        }
                        if server.allowed_private_hosts
                        else {}
                    ),
                }
            ),
        )

    async def get_server(self, server_id: str) -> McpServerDefinition | None:
        pool = await self.pool()
        row = await pool.fetchrow(
            "SELECT * FROM hands.downstream_mcp_server WHERE server_id=$1",
            server_id,
        )
        return _server(row) if row is not None else None

    async def list_servers(self, tenant_id: str) -> tuple[McpServerDefinition, ...]:
        pool = await self.pool()
        rows = await pool.fetch(
            """SELECT * FROM hands.downstream_mcp_server
            WHERE tenant_id IS NULL OR tenant_id=$1 ORDER BY server_id""",
            tenant_id,
        )
        return tuple(_server(row) for row in rows)

    async def replace_capabilities(
        self,
        server_id: str,
        capabilities: tuple[CapabilityDescriptor, ...],
    ) -> None:
        pool = await self.pool()
        async with pool.acquire() as connection, connection.transaction():
            current_generation = await connection.fetchval(
                """SELECT active_catalog_generation FROM hands.downstream_mcp_server
                WHERE server_id=$1 FOR UPDATE""",
                server_id,
            )
            registered = await connection.fetchval(
                "SELECT true FROM hands.downstream_mcp_server WHERE server_id=$1",
                server_id,
            )
            if registered is None:
                raise ValueError(f"MCP server is not registered: {server_id}")
            generation = int(current_generation or 0) + 1
            capability_ids = [capability.capability_id for capability in capabilities]
            if capability_ids:
                conflict = await connection.fetchval(
                    """SELECT capability_id FROM hands.capability_catalog
                    WHERE capability_id=ANY($1::text[]) AND server_id<>$2 LIMIT 1""",
                    capability_ids,
                    server_id,
                )
                if conflict is not None:
                    raise ValueError(
                        f"Capability id belongs to another MCP server: {conflict}"
                    )
            await connection.execute(
                "DELETE FROM hands.capability_catalog WHERE server_id=$1",
                server_id,
            )
            if capabilities:
                await connection.executemany(
                    """INSERT INTO hands.capability_catalog
                    (capability_id,kind,server_id,canonical_name,version,content_digest,
                     title,description,tags,tenant_id,trust_level,classification,
                     permission,risk_level,required_scopes,status,source_revision,
                     capability_metadata,updated_at,catalog_generation)
                    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9::jsonb,$10,$11,$12,$13,$14,
                            $15::jsonb,$16,$17,$18::jsonb,$19,$20)""",
                    [
                        (
                            capability.capability_id,
                            capability.kind.value,
                            capability.server_id,
                            capability.canonical_name,
                            capability.version,
                            capability.content_digest,
                            capability.title,
                            capability.description,
                            json_dumps(capability.tags),
                            capability.tenant_id,
                            capability.trust_level.value,
                            capability.classification,
                            capability.permission,
                            capability.risk_level,
                            json_dumps(capability.required_scopes),
                            capability.status.value,
                            capability.source_revision,
                            json_dumps(capability.metadata),
                            capability.updated_at,
                            generation,
                        )
                        for capability in capabilities
                    ],
                )
            await connection.execute(
                """UPDATE hands.downstream_mcp_server
                SET active_catalog_generation=$2,
                    last_good_catalog_at=now()
                WHERE server_id=$1""",
                server_id,
                generation,
            )

    async def get_active_generation(self, server_id: str) -> int | None:
        pool = await self.pool()
        value = await pool.fetchval(
            """SELECT active_catalog_generation
            FROM hands.downstream_mcp_server WHERE server_id=$1""",
            server_id,
        )
        return None if value is None else int(value)

    async def record_catalog_sync(
        self,
        server_id: str,
        *,
        succeeded: bool,
        attempted_at: datetime,
        safe_error_code: str | None,
        quarantine_after_failures: int,
    ) -> CatalogSyncHealth:
        if quarantine_after_failures < 1:
            raise ValueError("quarantine_after_failures must be positive")
        pool = await self.pool()
        row = await pool.fetchrow(
            """UPDATE hands.downstream_mcp_server SET
               consecutive_sync_failures=CASE WHEN $2 THEN 0
                 ELSE consecutive_sync_failures+1 END,
               last_catalog_sync_at=$3,
               last_catalog_sync_error=CASE WHEN $2 THEN NULL ELSE $4 END,
               catalog_quarantined_at=CASE
                 WHEN $2 THEN NULL
                 WHEN consecutive_sync_failures+1 >= $5
                   THEN COALESCE(catalog_quarantined_at,$3)
                 ELSE catalog_quarantined_at END,
               status=CASE
                 WHEN $2 THEN 'active'
                 WHEN consecutive_sync_failures+1 >= $5 THEN 'quarantined'
                 ELSE status END,
               updated_at=now()
            WHERE server_id=$1
            RETURNING consecutive_sync_failures,catalog_quarantined_at IS NOT NULL
                      AS quarantined""",
            server_id,
            succeeded,
            attempted_at,
            safe_error_code,
            quarantine_after_failures,
        )
        if row is None:
            raise ValueError(f"MCP server is not registered: {server_id}")
        return CatalogSyncHealth(
            consecutive_failures=int(row["consecutive_sync_failures"]),
            quarantined=bool(row["quarantined"]),
        )

    async def remove_server(self, server_id: str) -> None:
        pool = await self.pool()
        async with pool.acquire() as connection, connection.transaction():
            await connection.execute(
                "DELETE FROM hands.capability_catalog WHERE server_id=$1",
                server_id,
            )
            await connection.execute(
                "DELETE FROM hands.downstream_mcp_server WHERE server_id=$1",
                server_id,
            )

    async def list_capabilities(
        self, tenant_id: str
    ) -> tuple[CapabilityDescriptor, ...]:
        pool = await self.pool()
        rows = await pool.fetch(
            """SELECT c.* FROM hands.capability_catalog AS c
            JOIN hands.downstream_mcp_server AS s ON s.server_id=c.server_id
            WHERE (c.tenant_id IS NULL OR c.tenant_id=$1)
              AND s.enabled AND s.status IN ('active','degraded')
            ORDER BY c.canonical_name,c.version""",
            tenant_id,
        )
        return tuple(_capability(row) for row in rows)

    async def list_server_capabilities(
        self, tenant_id: str, server_id: str
    ) -> tuple[CapabilityDescriptor, ...]:
        pool = await self.pool()
        rows = await pool.fetch(
            """SELECT * FROM hands.capability_catalog
            WHERE server_id=$1 AND (tenant_id IS NULL OR tenant_id=$2)
            ORDER BY canonical_name, version""",
            server_id,
            tenant_id,
        )
        return tuple(_capability(row) for row in rows)

    async def get_capability(
        self, tenant_id: str, capability_id: str
    ) -> CapabilityDescriptor | None:
        pool = await self.pool()
        row = await pool.fetchrow(
            """SELECT c.* FROM hands.capability_catalog AS c
            JOIN hands.downstream_mcp_server AS s ON s.server_id=c.server_id
            WHERE c.capability_id=$1
              AND (c.tenant_id IS NULL OR c.tenant_id=$2)
              AND s.enabled AND s.status IN ('active','degraded')""",
            capability_id,
            tenant_id,
        )
        return _capability(row) if row is not None else None


def _server(row: object) -> McpServerDefinition:
    metadata = dict(json_loads(row["metadata"]))  # type: ignore[index]
    if "consecutive_sync_failures" in row:  # type: ignore[operator]
        metadata.update(
            {
                "consecutive_sync_failures": int(
                    row["consecutive_sync_failures"]  # type: ignore[index]
                ),
                "last_sync_at": (
                    row["last_catalog_sync_at"].isoformat()  # type: ignore[index]
                    if row["last_catalog_sync_at"] is not None  # type: ignore[index]
                    else None
                ),
                "last_sync_error": row["last_catalog_sync_error"],  # type: ignore[index]
                "catalog_quarantined_at": (
                    row["catalog_quarantined_at"].isoformat()  # type: ignore[index]
                    if row["catalog_quarantined_at"] is not None  # type: ignore[index]
                    else None
                ),
            }
        )
    oauth_payload = metadata.pop("_auraclaw_oauth", None)
    allowed_private_hosts = metadata.pop("_auraclaw_allowed_private_hosts", ())
    return McpServerDefinition(
        server_id=str(row["server_id"]),  # type: ignore[index]
        tenant_id=row["tenant_id"],  # type: ignore[index]
        title=str(row["title"]),  # type: ignore[index]
        endpoint=str(row["endpoint"]),  # type: ignore[index]
        protocol_revision=str(row["protocol_revision"]),  # type: ignore[index]
        credential_ref=row["credential_ref"],  # type: ignore[index]
        oauth=(
            McpOAuthConfiguration.model_validate(oauth_payload)
            if oauth_payload is not None
            else None
        ),
        trust_level=CapabilityTrustLevel(str(row["trust_level"])),  # type: ignore[index]
        allowed_tool_prefixes=tuple(
            json_loads(row["allowed_tool_prefixes"])  # type: ignore[index]
        ),
        allowed_resource_schemes=tuple(
            json_loads(row["allowed_resource_schemes"])  # type: ignore[index]
        ),
        allowed_prompt_prefixes=tuple(
            json_loads(row["allowed_prompt_prefixes"])  # type: ignore[index]
        ),
        allowed_private_hosts=tuple(allowed_private_hosts or ()),
        status=CapabilityStatus(str(row["status"])),  # type: ignore[index]
        enabled=bool(row["enabled"]),  # type: ignore[index]
        metadata=metadata,
    )


def _capability(row: object) -> CapabilityDescriptor:
    metadata = dict(json_loads(row["capability_metadata"]))  # type: ignore[index]
    generation = row.get("catalog_generation") if hasattr(row, "get") else None
    if generation is not None:
        metadata["catalog_generation"] = int(generation)
    return CapabilityDescriptor(
        capability_id=str(row["capability_id"]),  # type: ignore[index]
        kind=CapabilityKind(str(row["kind"])),  # type: ignore[index]
        server_id=str(row["server_id"]),  # type: ignore[index]
        canonical_name=str(row["canonical_name"]),  # type: ignore[index]
        version=str(row["version"]),  # type: ignore[index]
        content_digest=str(row["content_digest"]),  # type: ignore[index]
        title=str(row["title"]),  # type: ignore[index]
        description=str(row["description"]),  # type: ignore[index]
        tags=tuple(json_loads(row["tags"])),  # type: ignore[index]
        tenant_id=row["tenant_id"],  # type: ignore[index]
        trust_level=CapabilityTrustLevel(str(row["trust_level"])),  # type: ignore[index]
        classification=str(row["classification"]),  # type: ignore[index]
        permission=row["permission"],  # type: ignore[index]
        risk_level=row["risk_level"],  # type: ignore[index]
        required_scopes=tuple(
            json_loads(row["required_scopes"])  # type: ignore[index]
        ),
        status=CapabilityStatus(str(row["status"])),  # type: ignore[index]
        source_revision=row["source_revision"],  # type: ignore[index]
        updated_at=row["updated_at"],  # type: ignore[index]
        metadata=metadata,
    )
