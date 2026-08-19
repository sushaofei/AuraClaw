from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Protocol

from fastapi import FastAPI, Header, HTTPException
from fastapi.responses import JSONResponse

from auraclaw.action.mcp import HandsMcpServer
from auraclaw.contracts.internal import LeaseAssertion
from auraclaw.contracts.mcp import (
    MCP_LEGACY_PROTOCOL_VERSION,
    MCP_PROTOCOL_VERSION,
    MCP_PROTOCOL_VERSION_META_KEY,
    MCP_SUPPORTED_PROTOCOL_VERSIONS,
    McpJsonRpcError,
    McpJsonRpcRequest,
    McpJsonRpcResponse,
    McpTrustedContext,
)
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
        expected_runtime_id = self._runtimes.get(
            authorization.removeprefix("Bearer ")
        )
        if expected_runtime_id is None:
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
        runtime_id = assertion.runtime_id
        if expected_runtime_id == "*":
            if runtime_id is None:
                raise HTTPException(
                    status_code=403, detail="lease assertion has no runtime identity"
                )
        elif runtime_id is None:
            runtime_id = expected_runtime_id
        elif runtime_id != expected_runtime_id:
            raise HTTPException(status_code=403, detail="runtime identity mismatch")
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
    app = FastAPI(title="AuraClaw Action Hands MCP Server", version=MCP_PROTOCOL_VERSION)

    @app.post("/mcp", response_model=None)
    async def mcp_endpoint(
        request: McpJsonRpcRequest,
        authorization: str | None = Header(default=None),
        protocol_version: str | None = Header(
            default=None, alias="MCP-Protocol-Version"
        ),
        mcp_method: str | None = Header(default=None, alias="Mcp-Method"),
        mcp_name: str | None = Header(default=None, alias="Mcp-Name"),
        lease_assertion: str | None = Header(
            default=None, alias="X-AuraClaw-Lease-Assertion"
        ),
    ) -> Any:
        trusted = await authenticator.authenticate(authorization, lease_assertion)
        header_error = _validate_modern_headers(
            request,
            protocol_version=protocol_version,
            mcp_method=mcp_method,
            mcp_name=mcp_name,
        )
        if header_error is not None:
            return JSONResponse(
                status_code=400,
                content=header_error.model_dump(mode="json"),
            )
        return await server.handle(request, trusted_context=trusted)

    return app


def _validate_modern_headers(
    request: McpJsonRpcRequest,
    *,
    protocol_version: str | None,
    mcp_method: str | None,
    mcp_name: str | None,
) -> McpJsonRpcResponse | None:
    if request.method == "initialize":
        return None
    if protocol_version == MCP_LEGACY_PROTOCOL_VERSION:
        return None
    raw_meta = request.params.get("_meta")
    meta = raw_meta if isinstance(raw_meta, dict) else {}
    body_version = meta.get(MCP_PROTOCOL_VERSION_META_KEY)
    if protocol_version != MCP_PROTOCOL_VERSION or body_version != protocol_version:
        return _http_protocol_error(
            request,
            -32022,
            "Unsupported MCP protocol version",
            {
                "requested": protocol_version or body_version or "",
                "supported": list(MCP_SUPPORTED_PROTOCOL_VERSIONS),
            },
        )
    expected_name = _request_target_name(request)
    if mcp_method != request.method or mcp_name != expected_name:
        return _http_protocol_error(
            request,
            -32020,
            "MCP routing headers do not match the request body",
        )
    return None


def _request_target_name(request: McpJsonRpcRequest) -> str | None:
    key = {
        "tools/call": "name",
        "prompts/get": "name",
        "resources/read": "uri",
    }.get(request.method)
    value = request.params.get(key) if key is not None else None
    return str(value) if value is not None else None


def _http_protocol_error(
    request: McpJsonRpcRequest,
    code: int,
    message: str,
    data: dict[str, Any] | None = None,
) -> McpJsonRpcResponse:
    return McpJsonRpcResponse(
        id=request.id,
        error=McpJsonRpcError(code=code, message=message, data=data or {}),
    )
