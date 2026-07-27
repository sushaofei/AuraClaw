from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from typing import Any, NoReturn, TypeVar

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
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


@dataclass(frozen=True)
class ContractRoute:
    request_model: type[ContractModel]
    response_model: type[ContractModel]
    handler: ContractHandler


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


def _error_code(exc: AuraClawError) -> InternalErrorCode:
    aliases = {
        "authorization_denied": InternalErrorCode.FORBIDDEN,
        "lease_conflict": InternalErrorCode.LEASE_LOST,
        "credential_access_denied": InternalErrorCode.CREDENTIAL_DENIED,
        "artifact_access_denied": InternalErrorCode.ARTIFACT_DENIED,
        "invalid_transition": InternalErrorCode.INVALID_TRANSITION,
    }
    try:
        return InternalErrorCode(exc.code)
    except ValueError:
        return aliases.get(exc.code, InternalErrorCode.INTERNAL_ERROR)


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
    }
    exc_type = mapping.get(error.code, AuraClawError)
    raise exc_type(error.message, detail=detail)


def create_contract_app(
    service_name: str,
    routes: Mapping[str, ContractRoute],
    *,
    workload_identities: Mapping[str, ServiceIdentity] | None = None,
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

    def make_endpoint(
        route: ContractRoute,
    ) -> Callable[[dict[str, Any], Request], Awaitable[dict[str, Any]]]:
        async def endpoint(payload: dict[str, Any], raw_request: Request) -> dict[str, Any]:
            try:
                request_model = route.request_model.model_validate(payload)
            except ValidationError as exc:
                raise ContractValidationError(str(exc)) from exc
            if workload_identities is not None:
                authorization = raw_request.headers.get("Authorization", "")
                token = authorization.removeprefix("Bearer ")
                authenticated = workload_identities.get(token)
                context = getattr(request_model, "context", None)
                supplied = getattr(context, "service_identity", None)
                if authenticated is None or supplied != authenticated:
                    error = InternalError(
                        code=InternalErrorCode.UNAUTHORIZED,
                        message="workload identity does not match request context",
                    )
                    raise InternalAuthenticationError(error)
            response = await route.handler(request_model)
            validated = route.response_model.model_validate(response)
            return validated.model_dump(mode="json")

        return endpoint

    for path, route in routes.items():
        app.add_api_route(
            path,
            make_endpoint(route),
            methods=["POST"],
            response_model=route.response_model,
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
    def __init__(self, client: httpx.AsyncClient, *, bearer_token: str | None = None) -> None:
        self._client = client
        self._bearer_token = bearer_token

    async def call(
        self,
        path: str,
        request: RequestModel,
        response_model: type[ResponseModel],
    ) -> ResponseModel:
        headers = {"X-AuraClaw-Contract-Version": INTERNAL_API_VERSION}
        if self._bearer_token is not None:
            headers["Authorization"] = f"Bearer {self._bearer_token}"
        response = await self._client.post(
            path,
            json=request.model_dump(mode="json"),
            headers=headers,
        )
        if response.is_error:
            _raise_contract_error(response)
        return response_model.model_validate(response.json())
