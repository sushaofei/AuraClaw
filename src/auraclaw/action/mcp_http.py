from __future__ import annotations

from collections.abc import Mapping
from typing import Protocol

from fastapi import FastAPI, Header, HTTPException

from auraclaw.action.mcp import HandsMcpServer
from auraclaw.contracts.mcp import McpJsonRpcRequest, McpJsonRpcResponse, McpTrustedContext


class WorkloadAuthenticator(Protocol):
    async def authenticate(self, authorization: str | None) -> McpTrustedContext: ...


class StaticWorkloadAuthenticator:
    """Development/test authenticator; production uses verified workload capabilities."""

    def __init__(self, contexts: Mapping[str, McpTrustedContext]) -> None:
        self._contexts = dict(contexts)

    async def authenticate(self, authorization: str | None) -> McpTrustedContext:
        if authorization is None or not authorization.startswith("Bearer "):
            raise HTTPException(status_code=401, detail="missing workload bearer token")
        context = self._contexts.get(authorization.removeprefix("Bearer "))
        if context is None:
            raise HTTPException(status_code=403, detail="invalid workload bearer token")
        return context


def create_hands_mcp_app(
    server: HandsMcpServer,
    *,
    authenticator: WorkloadAuthenticator,
) -> FastAPI:
    app = FastAPI(title="AuraClaw Action Hands MCP Server", version="2025-11-25")

    @app.post("/mcp", response_model=McpJsonRpcResponse)
    async def mcp_endpoint(
        request: McpJsonRpcRequest,
        authorization: str | None = Header(default=None),
    ) -> McpJsonRpcResponse:
        trusted = await authenticator.authenticate(authorization)
        return await server.handle(request, trusted_context=trusted)

    return app
