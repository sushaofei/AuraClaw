from __future__ import annotations

from datetime import datetime
from typing import Any, Literal, Protocol

from pydantic import Field

from auraclaw.contracts.internal import ContractModel

MCP_PROTOCOL_VERSION = "2025-11-25"
MCP_JSONRPC_VERSION = "2.0"


class McpTrustedContext(ContractModel):
    tenant_id: str
    root_session_id: str
    session_id: str
    run_id: str
    runtime_id: str
    lease_id: str
    fencing_token: int = Field(ge=1)
    deadline: datetime | None = None


class McpJsonRpcRequest(ContractModel):
    jsonrpc: Literal["2.0"] = "2.0"
    id: str | int | None = None
    method: str
    params: dict[str, Any] = Field(default_factory=dict)


class McpJsonRpcError(ContractModel):
    code: int
    message: str
    data: dict[str, Any] = Field(default_factory=dict)


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
