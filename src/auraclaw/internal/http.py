from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping
from dataclasses import dataclass
from typing import Any, NoReturn, TypeVar

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import ValidationError
from starlette.responses import Response

from auraclaw.contracts.errors import (
    ApprovalValidationError,
    ArtifactAccessError,
    AuraClawError,
    AuthorizationError,
    CredentialAccessError,
    FencingTokenError,
    InvalidTransitionError,
    LeaseConflictError,
    NotFoundError,
    PolicyDeniedError,
    SchemaValidationError,
    VersionConflictError,
)
from auraclaw.contracts.internal import (
    INTERNAL_API_VERSION,
    ContractModel,
    InternalError,
    InternalErrorCode,
    ServiceIdentity,
)

RequestModel = TypeVar("RequestModel", bound=ContractModel)
ResponseModel = TypeVar("ResponseModel", bound=ContractModel)
ContractHandler = Callable[[ContractModel], Awaitable[ContractModel]]
StreamContractHandler = Callable[[ContractModel], AsyncIterator[ContractModel]]


@dataclass(frozen=True)
class ContractRoute:
    request_model: type[ContractModel]
    response_model: type[ContractModel]
    handler: ContractHandler


@dataclass(frozen=True)
class StreamContractRoute:
    request_model: type[ContractModel]
    event_model: type[ContractModel]
    handler: StreamContractHandler


def contract_route(
    request_model: type[RequestModel],
    response_model: type[ResponseModel],
    handler: Callable[[RequestModel], Awaitable[ResponseModel]],
) -> ContractRoute:
    async def invoke(request: ContractModel) -> ContractModel:
        parsed = request_model.model_validate(request)
        return await handler(parsed)

    return ContractRoute(
        request_model=request_model,
        response_model=response_model,
        handler=invoke,
    )


def stream_contract_route(
    request_model: type[RequestModel],
    event_model: type[ResponseModel],
    handler: Callable[[RequestModel], AsyncIterator[ResponseModel]],
) -> StreamContractRoute:
    async def invoke(request: ContractModel) -> AsyncIterator[ContractModel]:
        parsed = request_model.model_validate(request)
        async for event in handler(parsed):
            yield event_model.model_validate(event)

    return StreamContractRoute(
        request_model=request_model,
        event_model=event_model,
        handler=invoke,
    )


def _error_code(exc: AuraClawError) -> InternalErrorCode:
    aliases = {
        "authorization_denied": InternalErrorCode.FORBIDDEN,
        "lease_conflict": InternalErrorCode.LEASE_LOST,
        "credential_access_denied": InternalErrorCode.CREDENTIAL_DENIED,
        "artifact_access_denied": InternalErrorCode.ARTIFACT_DENIED,
        "invalid_transition": InternalErrorCode.INVALID_TRANSITION,
        "tool_schema_invalid": InternalErrorCode.INVALID_REQUEST,
    }
    try:
        return InternalErrorCode(exc.code)
    except ValueError:
        return aliases.get(exc.code, InternalErrorCode.INTERNAL_ERROR)


def raise_contract_error(response: httpx.Response) -> NoReturn:
    """Map an internal HTTP error payload onto the matching AuraClaw error type."""
    _raise_contract_error(response)


def _raise_contract_error(response: httpx.Response) -> NoReturn:
    try:
        error = InternalError.model_validate(response.json())
    except Exception as exc:
        raise AuraClawError(
            f"internal contract call failed with HTTP {response.status_code}",
            detail=response.text[:500] or None,
        ) from exc
    detail = f"{error.code.value}: {error.detail or ''}".rstrip(": ")
    mapping: dict[InternalErrorCode, type[AuraClawError]] = {
        InternalErrorCode.NOT_FOUND: NotFoundError,
        InternalErrorCode.VERSION_CONFLICT: VersionConflictError,
        InternalErrorCode.INVALID_TRANSITION: InvalidTransitionError,
        InternalErrorCode.FORBIDDEN: AuthorizationError,
        InternalErrorCode.UNAUTHORIZED: AuthorizationError,
        InternalErrorCode.LEASE_LOST: LeaseConflictError,
        InternalErrorCode.STALE_FENCING_TOKEN: FencingTokenError,
        InternalErrorCode.POLICY_DENIED: PolicyDeniedError,
        InternalErrorCode.APPROVAL_INVALID: ApprovalValidationError,
        InternalErrorCode.CREDENTIAL_DENIED: CredentialAccessError,
        InternalErrorCode.ARTIFACT_DENIED: ArtifactAccessError,
        InternalErrorCode.INVALID_REQUEST: SchemaValidationError,
    }
    exc_type = mapping.get(error.code, AuraClawError)
    raise exc_type(error.message, detail=detail)


def create_contract_app(
    service_name: str,
    routes: Mapping[str, ContractRoute],
    *,
    stream_routes: Mapping[str, StreamContractRoute] | None = None,
    workload_identities: Mapping[str, ServiceIdentity] | None = None,
    allow_unauthenticated: bool = False,
) -> FastAPI:
    app = FastAPI(title=f"AuraClaw {service_name} Internal API", version=INTERNAL_API_VERSION)

    @app.middleware("http")
    async def enforce_contract_version(
        raw_request: Request,
        call_next: Callable[[Request], Awaitable[Response]],
    ) -> Response:
        supplied = raw_request.headers.get("X-AuraClaw-Contract-Version")
        if supplied != INTERNAL_API_VERSION:
            error = InternalError(
                code=InternalErrorCode.INVALID_REQUEST,
                message="unsupported or missing internal contract version",
                detail=f"expected {INTERNAL_API_VERSION}",
            )
            return JSONResponse(status_code=426, content=error.model_dump(mode="json"))
        return await call_next(raw_request)

    @app.exception_handler(AuraClawError)
    async def handle_auraclaw_error(_request: Request, exc: AuraClawError) -> JSONResponse:
        error = InternalError(
            code=_error_code(exc),
            message=exc.message,
            detail=exc.detail,
            retryable=exc.status_code >= 500,
        )
        return JSONResponse(status_code=exc.status_code, content=error.model_dump(mode="json"))

    def _authenticate(request_model: ContractModel, raw_request: Request) -> None:
        if allow_unauthenticated:
            return
        authorization = raw_request.headers.get("Authorization", "")
        token = authorization.removeprefix("Bearer ")
        authenticated = (workload_identities or {}).get(token)
        context = getattr(request_model, "context", None)
        supplied = getattr(context, "service_identity", None)
        if authenticated is None or supplied != authenticated:
            error = InternalError(
                code=InternalErrorCode.UNAUTHORIZED,
                message="workload identity does not match request context",
            )
            raise InternalAuthenticationError(error)

    def make_endpoint(
        route: ContractRoute,
    ) -> Callable[[dict[str, Any], Request], Awaitable[dict[str, Any]]]:
        async def endpoint(payload: dict[str, Any], raw_request: Request) -> dict[str, Any]:
            try:
                request_model = route.request_model.model_validate(payload)
            except ValidationError as exc:
                raise ContractValidationError(str(exc)) from exc
            _authenticate(request_model, raw_request)
            response = await route.handler(request_model)
            validated = route.response_model.model_validate(response)
            return validated.model_dump(mode="json")

        return endpoint

    def make_stream_endpoint(
        route: StreamContractRoute,
    ) -> Callable[[dict[str, Any], Request], Awaitable[StreamingResponse]]:
        async def endpoint(
            payload: dict[str, Any], raw_request: Request
        ) -> StreamingResponse:
            try:
                request_model = route.request_model.model_validate(payload)
            except ValidationError as exc:
                raise ContractValidationError(str(exc)) from exc
            _authenticate(request_model, raw_request)

            async def event_stream() -> AsyncIterator[str]:
                async for event in route.handler(request_model):
                    validated = route.event_model.model_validate(event)
                    yield f"data: {validated.model_dump_json()}\n\n"
                yield "data: [DONE]\n\n"

            return StreamingResponse(
                event_stream(),
                media_type="text/event-stream",
                headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
            )

        return endpoint

    for path, route in routes.items():
        app.add_api_route(
            path,
            make_endpoint(route),
            methods=["POST"],
            response_model=route.response_model,
        )
    for path, stream_route in (stream_routes or {}).items():
        app.add_api_route(
            path,
            make_stream_endpoint(stream_route),
            methods=["POST"],
        )
    return app


class InternalAuthenticationError(AuraClawError):
    code = InternalErrorCode.UNAUTHORIZED.value
    status_code = 401

    def __init__(self, error: InternalError) -> None:
        super().__init__(error.message, detail=error.detail)


class ContractValidationError(AuraClawError):
    code = InternalErrorCode.INVALID_REQUEST.value
    status_code = 422


class InProcessContractClient:
    def __init__(self, routes: Mapping[str, ContractRoute]) -> None:
        self._routes = routes

    async def call(
        self,
        path: str,
        request: RequestModel,
        response_model: type[ResponseModel],
    ) -> ResponseModel:
        route = self._routes[path]
        parsed_request = route.request_model.model_validate(request)
        response = await route.handler(parsed_request)
        return response_model.model_validate(response)


class HttpContractClient:
    def __init__(
        self,
        client: httpx.AsyncClient,
        *,
        bearer_token: str | None = None,
        retry_attempts: int = 1,
        retry_backoff_seconds: float = 0.0,
    ) -> None:
        if retry_attempts < 1:
            raise ValueError("internal contract retry attempts must be positive")
        if retry_backoff_seconds < 0:
            raise ValueError("internal contract retry backoff cannot be negative")
        self._client = client
        self._bearer_token = bearer_token
        self._retry_attempts = retry_attempts
        self._retry_backoff_seconds = retry_backoff_seconds

    def _headers(self) -> dict[str, str]:
        headers = {"X-AuraClaw-Contract-Version": INTERNAL_API_VERSION}
        if self._bearer_token is not None:
            headers["Authorization"] = f"Bearer {self._bearer_token}"
        return headers

    async def call(
        self,
        path: str,
        request: RequestModel,
        response_model: type[ResponseModel],
    ) -> ResponseModel:
        response: httpx.Response | None = None
        for attempt in range(self._retry_attempts):
            try:
                response = await self._client.post(
                    path,
                    json=request.model_dump(mode="json"),
                    headers=self._headers(),
                )
            except httpx.TransportError:
                if attempt + 1 >= self._retry_attempts:
                    raise
            else:
                if response.status_code not in {502, 503, 504}:
                    break
                if attempt + 1 >= self._retry_attempts:
                    break
            if self._retry_backoff_seconds:
                await asyncio.sleep(self._retry_backoff_seconds * (attempt + 1))
        assert response is not None
        if response.is_error:
            _raise_contract_error(response)
        return response_model.model_validate(response.json())

    async def stream(
        self,
        path: str,
        request: RequestModel,
        event_model: type[ResponseModel],
    ) -> AsyncIterator[ResponseModel]:
        async with self._client.stream(
            "POST",
            path,
            json=request.model_dump(mode="json"),
            headers=self._headers(),
        ) as response:
            if response.is_error:
                await response.aread()
                _raise_contract_error(response)
            async for line in response.aiter_lines():
                if not line.startswith("data:"):
                    continue
                value = line[5:].strip()
                if not value or value == "[DONE]":
                    continue
                yield event_model.model_validate_json(value)
