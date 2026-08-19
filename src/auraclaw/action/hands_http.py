from __future__ import annotations

from collections.abc import Mapping
from typing import Any, Protocol

from fastapi import FastAPI, Header, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import ValidationError

from auraclaw.action.hands import HandsGateway
from auraclaw.contracts.errors import AuraClawError
from auraclaw.contracts.hands import (
    HANDS_CONTRACT_VERSION,
    HANDS_INVOCATIONS_CANCEL,
    HANDS_MAX_REQUEST_BYTES,
    HANDS_PROMPTS_GET,
    HANDS_PROMPTS_LIST,
    HANDS_RESOURCE_TEMPLATES_LIST,
    HANDS_RESOURCES_LIST,
    HANDS_RESOURCES_READ,
    HANDS_TOOLS_CALL,
    HANDS_TOOLS_LIST,
    HandsCancelRequest,
    HandsCancelResponse,
    HandsGetPromptRequest,
    HandsListRequest,
    HandsPromptResult,
    HandsReadResourceRequest,
    HandsReadResourceResponse,
    HandsToolCall,
    HandsTrustedContext,
    hands_error_from_exception,
)
from auraclaw.contracts.internal import (
    INTERNAL_API_VERSION,
    InternalError,
    InternalErrorCode,
    LeaseAssertion,
)
from auraclaw.internal.http import _error_code
from auraclaw.internal.security import LeaseAssertionVerifier


class HandsWorkloadAuthenticator(Protocol):
    async def authenticate(
        self, authorization: str | None, lease_assertion: str | None
    ) -> HandsTrustedContext: ...


class StaticHandsAuthenticator:
    """Development/test authenticator; production uses verified lease assertions."""

    def __init__(self, contexts: Mapping[str, HandsTrustedContext]) -> None:
        self._contexts = dict(contexts)

    async def authenticate(
        self, authorization: str | None, lease_assertion: str | None
    ) -> HandsTrustedContext:
        del lease_assertion
        if authorization is None or not authorization.startswith("Bearer "):
            raise HTTPException(status_code=401, detail="missing workload bearer token")
        context = self._contexts.get(authorization.removeprefix("Bearer "))
        if context is None:
            raise HTTPException(status_code=403, detail="invalid workload bearer token")
        return context


class SignedLeaseHandsAuthenticator:
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
    ) -> HandsTrustedContext:
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
        return HandsTrustedContext(
            tenant_id=assertion.tenant_id,
            root_session_id=assertion.root_session_id or assertion.session_id,
            session_id=assertion.session_id,
            run_id=assertion.run_id,
            runtime_id=runtime_id,
            lease_id=assertion.lease_id,
            fencing_token=assertion.fencing_token,
            deadline=assertion.expires_at,
            lease_assertion=assertion,
            user_id=assertion.user_id,
        )


def create_hands_http_app(
    gateway: HandsGateway,
    *,
    authenticator: HandsWorkloadAuthenticator,
    max_request_bytes: int = HANDS_MAX_REQUEST_BYTES,
) -> FastAPI:
    app = FastAPI(title="AuraClaw Action Hands", version=HANDS_CONTRACT_VERSION)

    @app.middleware("http")
    async def enforce_size_and_version(
        raw_request: Request, call_next: Any
    ) -> Any:
        length = raw_request.headers.get("content-length")
        if length is not None:
            try:
                if int(length) > max_request_bytes:
                    return _error_response(
                        413,
                        InternalErrorCode.INVALID_REQUEST,
                        "Hands request exceeds the configured size limit",
                    )
            except ValueError:
                return _error_response(
                    400,
                    InternalErrorCode.INVALID_REQUEST,
                    "invalid Content-Length",
                )
        supplied = raw_request.headers.get("X-AuraClaw-Contract-Version")
        if raw_request.url.path.startswith("/internal/v1/hands/") and (
            supplied not in {None, INTERNAL_API_VERSION}
        ):
            return _error_response(
                426,
                InternalErrorCode.INVALID_REQUEST,
                "unsupported internal contract version",
                detail=f"expected {INTERNAL_API_VERSION}",
            )
        return await call_next(raw_request)

    async def _trusted(
        authorization: str | None,
        lease_assertion: str | None,
    ) -> HandsTrustedContext:
        return await authenticator.authenticate(authorization, lease_assertion)

    @app.post(HANDS_TOOLS_LIST)
    async def list_tools(
        payload: dict[str, Any],
        authorization: str | None = Header(default=None),
        lease_assertion: str | None = Header(
            default=None, alias="X-AuraClaw-Lease-Assertion"
        ),
    ) -> Any:
        request = HandsListRequest.model_validate(payload)
        trusted = await _trusted(authorization, lease_assertion)
        page = await gateway.list_tools(trusted, cursor=request.cursor)
        return page.model_dump(mode="json")

    @app.post(HANDS_TOOLS_CALL)
    async def call_tool(
        payload: dict[str, Any],
        authorization: str | None = Header(default=None),
        lease_assertion: str | None = Header(
            default=None, alias="X-AuraClaw-Lease-Assertion"
        ),
    ) -> Any:
        call = HandsToolCall.model_validate(payload)
        trusted = await _trusted(authorization, lease_assertion)
        result = await gateway.call_tool(trusted, call)
        return result.model_dump(mode="json")

    @app.post(HANDS_RESOURCES_LIST)
    async def list_resources(
        payload: dict[str, Any],
        authorization: str | None = Header(default=None),
        lease_assertion: str | None = Header(
            default=None, alias="X-AuraClaw-Lease-Assertion"
        ),
    ) -> Any:
        request = HandsListRequest.model_validate(payload)
        trusted = await _trusted(authorization, lease_assertion)
        page = await gateway.list_resources(trusted, cursor=request.cursor)
        return page.model_dump(mode="json")

    @app.post(HANDS_RESOURCE_TEMPLATES_LIST)
    async def list_resource_templates(
        payload: dict[str, Any],
        authorization: str | None = Header(default=None),
        lease_assertion: str | None = Header(
            default=None, alias="X-AuraClaw-Lease-Assertion"
        ),
    ) -> Any:
        request = HandsListRequest.model_validate(payload)
        trusted = await _trusted(authorization, lease_assertion)
        page = await gateway.list_resource_templates(trusted, cursor=request.cursor)
        return page.model_dump(mode="json")

    @app.post(HANDS_RESOURCES_READ)
    async def read_resource(
        payload: dict[str, Any],
        authorization: str | None = Header(default=None),
        lease_assertion: str | None = Header(
            default=None, alias="X-AuraClaw-Lease-Assertion"
        ),
    ) -> Any:
        request = HandsReadResourceRequest.model_validate(payload)
        trusted = await _trusted(authorization, lease_assertion)
        contents = await gateway.read_resource(trusted, request.uri)
        return HandsReadResourceResponse(contents=contents).model_dump(mode="json")

    @app.post(HANDS_PROMPTS_LIST)
    async def list_prompts(
        payload: dict[str, Any],
        authorization: str | None = Header(default=None),
        lease_assertion: str | None = Header(
            default=None, alias="X-AuraClaw-Lease-Assertion"
        ),
    ) -> Any:
        request = HandsListRequest.model_validate(payload)
        trusted = await _trusted(authorization, lease_assertion)
        page = await gateway.list_prompts(trusted, cursor=request.cursor)
        return page.model_dump(mode="json")

    @app.post(HANDS_PROMPTS_GET)
    async def get_prompt(
        payload: dict[str, Any],
        authorization: str | None = Header(default=None),
        lease_assertion: str | None = Header(
            default=None, alias="X-AuraClaw-Lease-Assertion"
        ),
    ) -> Any:
        request = HandsGetPromptRequest.model_validate(payload)
        trusted = await _trusted(authorization, lease_assertion)
        result = await gateway.get_prompt(
            trusted, request.name, arguments=dict(request.arguments)
        )
        return HandsPromptResult.model_validate(result).model_dump(mode="json")

    @app.post(HANDS_INVOCATIONS_CANCEL)
    async def cancel_invocation(
        payload: dict[str, Any],
        authorization: str | None = Header(default=None),
        lease_assertion: str | None = Header(
            default=None, alias="X-AuraClaw-Lease-Assertion"
        ),
    ) -> Any:
        request = HandsCancelRequest.model_validate(payload)
        await _trusted(authorization, lease_assertion)
        result = await gateway.cancel_invocation(request.tool_invocation_id)
        return HandsCancelResponse(cancelled=result.cancelled).model_dump(mode="json")

    @app.exception_handler(AuraClawError)
    async def handle_auraclaw_error(_request: Request, exc: AuraClawError) -> JSONResponse:
        error = InternalError(
            code=_error_code(exc),
            message=exc.message,
            detail=exc.detail,
            retryable=exc.status_code >= 500,
        )
        return JSONResponse(status_code=exc.status_code, content=error.model_dump(mode="json"))

    @app.exception_handler(KeyError)
    async def handle_missing(_request: Request, exc: KeyError) -> JSONResponse:
        error = hands_error_from_exception(exc)
        return JSONResponse(
            status_code=404,
            content=InternalError(
                code=InternalErrorCode.NOT_FOUND,
                message=error.message,
                detail=error.detail,
            ).model_dump(mode="json"),
        )

    @app.exception_handler(ValidationError)
    async def handle_validation(_request: Request, exc: ValidationError) -> JSONResponse:
        return JSONResponse(
            status_code=422,
            content=InternalError(
                code=InternalErrorCode.INVALID_REQUEST,
                message="invalid Hands request",
                detail=str(exc),
            ).model_dump(mode="json"),
        )

    @app.exception_handler(ValueError)
    async def handle_value(_request: Request, exc: ValueError) -> JSONResponse:
        return JSONResponse(
            status_code=400,
            content=InternalError(
                code=InternalErrorCode.INVALID_REQUEST,
                message="invalid Hands request",
                detail=str(exc),
            ).model_dump(mode="json"),
        )

    return app


def _error_response(
    status: int,
    code: InternalErrorCode,
    message: str,
    *,
    detail: str | None = None,
) -> JSONResponse:
    return JSONResponse(
        status_code=status,
        content=InternalError(code=code, message=message, detail=detail).model_dump(
            mode="json"
        ),
    )
