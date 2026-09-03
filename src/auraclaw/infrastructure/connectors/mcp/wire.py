from __future__ import annotations

from datetime import datetime
from typing import Any, Literal, Protocol

from pydantic import Field

from auraclaw.contracts.internal import ContractModel, LeaseAssertion

MCP_PROTOCOL_VERSION = "2026-07-28"
MCP_LEGACY_PROTOCOL_VERSION = "2025-11-25"
MCP_JAVA_PROTOCOL_VERSION = "2025-06-18"
MCP_INITIALIZE_PROTOCOL_VERSIONS = (
    MCP_LEGACY_PROTOCOL_VERSION,
    MCP_JAVA_PROTOCOL_VERSION,
)
MCP_SUPPORTED_PROTOCOL_VERSIONS = (
    MCP_PROTOCOL_VERSION,
    *MCP_INITIALIZE_PROTOCOL_VERSIONS,
)
MCP_JSONRPC_VERSION = "2.0"
MCP_PROTOCOL_VERSION_META_KEY = "io.modelcontextprotocol/protocolVersion"
MCP_CLIENT_INFO_META_KEY = "io.modelcontextprotocol/clientInfo"
MCP_CLIENT_CAPABILITIES_META_KEY = "io.modelcontextprotocol/clientCapabilities"
MCP_SERVER_INFO_META_KEY = "io.modelcontextprotocol/serverInfo"
MCP_AURACLAW_INVOCATION_ID_META_KEY = "io.auraclaw/invocationId"
MCP_AURACLAW_TENANT_ID_META_KEY = "io.auraclaw/tenantId"
MCP_AURACLAW_USER_ID_META_KEY = "io.auraclaw/userId"
MCP_AURACLAW_DEPT_ID_META_KEY = "io.auraclaw/deptId"


class McpTrustedContext(ContractModel):
    tenant_id: str
    root_session_id: str
    session_id: str
    run_id: str
    runtime_id: str
    lease_id: str
    fencing_token: int = Field(ge=1)
    deadline: datetime | None = None
    lease_assertion: LeaseAssertion | None = None
    user_id: str | None = None
    dept_id: str | None = None


class McpJsonRpcRequest(ContractModel):
    jsonrpc: Literal["2.0"] = "2.0"
    id: str | int | None = None
    method: str
    params: dict[str, Any] = Field(default_factory=dict)


class McpJsonRpcError(ContractModel):
    code: int
    message: str
    data: Any = None


class McpJsonRpcResponse(ContractModel):
    jsonrpc: Literal["2.0"] = "2.0"
    id: str | int | None = None
    result: dict[str, Any] | None = None
    error: McpJsonRpcError | None = None


class McpTransport(Protocol):
    async def send(
        self,
        request: McpJsonRpcRequest,
        *,
        trusted_context: McpTrustedContext,
    ) -> McpJsonRpcResponse: ...
