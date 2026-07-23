from __future__ import annotations

from collections.abc import Mapping
from typing import Protocol

from fastapi import FastAPI, Header, HTTPException

from auraclaw.action.mcp import HandsMcpServer
from auraclaw.contracts.internal import LeaseAssertion
from auraclaw.contracts.mcp import McpJsonRpcRequest, McpJsonRpcResponse, McpTrustedContext
from auraclaw.internal.security import LeaseAssertionVerifier


class WorkloadAuthenticator(Protocol):
    async def authenticate(
        self, authorization: str | None, lease_assertion: str | None
    ) -> McpTrustedContext: ...


class StaticWorkloadAuthenticator:
    """Development/test authenticator; production uses verified workload capabilities."""

    def __init__(self, contexts: Mapping[str, McpTrustedContext]) -> None:
        self._contexts = dict(contexts)

    async def authenticate(
        self, authorization: str | None, lease_assertion: str | None
    ) -> McpTrustedContext:
        del lease_assertion
        if authorization is None or not authorization.startswith("Bearer "):
            raise HTTPException(status_code=401, detail="missing workload bearer token")
        context = self._contexts.get(authorization.removeprefix("Bearer "))
        if context is None:
            raise HTTPException(status_code=403, detail="invalid workload bearer token")
        return context


class SignedLeaseWorkloadAuthenticator:
    """Production authenticator deriving trusted scope from a signed lease capability."""

    def __init__(
        self,
        runtimes: Mapping[str, str],
        *,
        verifier: LeaseAssertionVerifier,
    ) -> None:
        self._runtimes = dict(runtimes)
        self._verifier = verifier

    async def authenticate(
        self, authorization: str | None, lease_assertion: str | None
    ) -> McpTrustedContext:
        if authorization is None or not authorization.startswith("Bearer "):
            raise HTTPException(status_code=401, detail="missing workload bearer token")
        runtime_id = self._runtimes.get(authorization.removeprefix("Bearer "))
        if runtime_id is None:
            raise HTTPException(status_code=403, detail="invalid workload bearer token")
        if lease_assertion is None:
            raise HTTPException(status_code=401, detail="missing lease assertion")
        try:
            assertion = LeaseAssertion.model_validate_json(lease_assertion)
            await self._verifier.verify(
                assertion,
                tenant_id=assertion.tenant_id,
                session_id=assertion.session_id,
                run_id=assertion.run_id,
            )
        except Exception as exc:
            raise HTTPException(status_code=403, detail="invalid lease assertion") from exc
        return McpTrustedContext(
            tenant_id=assertion.tenant_id,
            root_session_id=assertion.root_session_id or assertion.session_id,
            session_id=assertion.session_id,
            run_id=assertion.run_id,
            runtime_id=runtime_id,
            lease_id=assertion.lease_id,
            fencing_token=assertion.fencing_token,
            deadline=assertion.expires_at,
            lease_assertion=assertion,
        )


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
        lease_assertion: str | None = Header(
            default=None, alias="X-AuraClaw-Lease-Assertion"
        ),
    ) -> McpJsonRpcResponse:
        trusted = await authenticator.authenticate(authorization, lease_assertion)
        return await server.handle(request, trusted_context=trusted)

    return app
