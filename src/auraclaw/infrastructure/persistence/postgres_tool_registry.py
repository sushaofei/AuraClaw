from __future__ import annotations

from auraclaw.action.tool_gateway import ToolRegistry
from auraclaw.contracts.tools import RiskLevel, ToolCapability, ToolPermission
from auraclaw.infrastructure.persistence.postgres_common import LazyPool, json_loads


class PostgresToolRegistryStore(LazyPool):
    async def load_into(self, registry: ToolRegistry) -> int:
        pool = await self.pool()
        rows = await pool.fetch(
            """SELECT * FROM hands.tool_capability
            WHERE enabled ORDER BY tool_name,version"""
        )
        for row in rows:
            registry.register(
                ToolCapability(
                    name=str(row["tool_name"]),
                    version=str(row["version"]),
                    description=str(row["description"]),
                    input_schema=dict(json_loads(row["input_schema"])),
                    output_schema=dict(json_loads(row["output_schema"])),
                    permission=ToolPermission(str(row["permission"])),
                    risk_level=RiskLevel(str(row["risk_level"])),
                    runtime_location=str(row["runtime_location"]),
                    owner=str(row["owner"]),
                    allowed_credential_operations=tuple(
                        json_loads(row["allowed_credential_operations"])
                    ),
                )
            )
        return len(rows)
