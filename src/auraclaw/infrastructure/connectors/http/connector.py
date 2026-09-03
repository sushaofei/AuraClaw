from __future__ import annotations

import hashlib
import json
import re
from typing import Any
from urllib.parse import quote

from auraclaw.action.ports import CredentialInvoker, ResourcePolicyEvaluator
from auraclaw.contracts.capabilities import (
    CapabilityStatus,
    JavaApiOperationDefinition,
    JavaApiServerDefinition,
    McpServerDefinition,
)
from auraclaw.contracts.errors import PolicyDeniedError
from auraclaw.contracts.hands import (
    CapabilitySnapshot,
    HandsPromptResult,
    HandsResourceContent,
    HandsToolDescriptor,
    HandsToolResult,
    HandsTrustedContext,
)
from auraclaw.contracts.tools import PolicyDecision

_SAFE_PATH_VALUE = re.compile(r"^[A-Za-z0-9._:-]{1,128}$")
_FORBIDDEN_ARGUMENT_KEYS = {
    "access_token",
    "api_key",
    "authorization",
    "base_url",
    "credential",
    "credential_ref",
    "endpoint",
    "headers",
    "host",
    "method",
    "port",
    "scheme",
    "secret",
    "token",
    "url",
    "uri",
}


class ManagedJavaApiConnector:
    """Downstream Java API connector. Maps registered operations to Hands DTOs."""

    def __init__(
        self,
        server: JavaApiServerDefinition,
        *,
        credentials: CredentialInvoker,
        policy: ResourcePolicyEvaluator,
    ) -> None:
        if not server.enabled or server.status not in {
            CapabilityStatus.ACTIVE,
            CapabilityStatus.DEGRADED,
        }:
            raise ValueError("Java API server is not callable")
        if server.credential_ref is None:
            raise ValueError("Java API server requires a credential_ref")
        self._server = server
        self._credentials = credentials
        self._policy = policy
        self._operations = {item.tool_name: item for item in server.operations}

    @property
    def connector_id(self) -> str:
        return f"java-api:{self._server.server_id}"

    async def snapshot(self, trusted: HandsTrustedContext) -> CapabilitySnapshot:
        self._assert_tenant(trusted)
        return CapabilitySnapshot(
            connector_id=self.connector_id,
            tools=tuple(_tool_descriptor(item) for item in self._server.operations),
            extra={"connector_kind": "java-api"},
        )

    async def read_resource(
        self,
        trusted: HandsTrustedContext,
        uri: str,
    ) -> tuple[HandsResourceContent, ...]:
        del trusted, uri
        raise KeyError("Java API connector does not expose resources")

    async def get_prompt(
        self,
        trusted: HandsTrustedContext,
        name: str,
        *,
        arguments: dict[str, str] | None = None,
    ) -> HandsPromptResult:
        del trusted, name, arguments
        raise KeyError("Java API connector does not expose prompts")

    async def call_tool(
        self,
        trusted: HandsTrustedContext,
        *,
        name: str,
        arguments: dict[str, Any],
        invocation_id: str,
    ) -> HandsToolResult:
        self._assert_tenant(trusted)
        operation = self._operations.get(name)
        if operation is None:
            raise KeyError(f"Java API operation is not registered: {name}")
        path, query, body = _bind_arguments(operation, arguments)
        evaluation = await self._policy.evaluate_action(
            tenant_id=trusted.tenant_id,
            subject=trusted.runtime_id,
            action="java-api.remote.invoke",
            resource=self.connector_id,
            input_digest=_digest({"path": path, "query": query, "body": body}),
            correlation_id=trusted.run_id,
            attributes={
                "operation_id": operation.operation_id,
                "server_id": self._server.server_id,
            },
        )
        if evaluation.decision not in {
            PolicyDecision.ALLOW,
            PolicyDecision.ALLOW_WITH_CONSTRAINTS,
        }:
            raise PolicyDeniedError("Java API policy denied invocation")
        credential_ref = operation.credential_ref or self._server.credential_ref
        assert credential_ref is not None
        raw = await self._credentials.invoke(
            tenant_id=trusted.tenant_id,
            session_id=trusted.session_id,
            tool_name=self.connector_id,
            credential_ref=credential_ref,
            operation="http.invoke",
            request={
                "server_id": self._server.server_id,
                "operation_id": operation.operation_id,
                "path": path,
                "query": query,
                "body": body,
                "idempotency_key": invocation_id,
            },
            policy_decision_id=evaluation.decision_id,
        )
        if not isinstance(raw, dict):
            raise ValueError("Java API response is not an object")
        return HandsToolResult(status="success", content=dict(raw), summary="")

    async def aclose(self) -> None:
        return None

    def _assert_tenant(self, trusted: HandsTrustedContext) -> None:
        if (
            self._server.tenant_id is not None
            and self._server.tenant_id != trusted.tenant_id
        ):
            raise PolicyDeniedError("Java API server is outside tenant scope")


def catalog_server_definition(server: JavaApiServerDefinition) -> McpServerDefinition:
    return McpServerDefinition(
        server_id=server.server_id,
        tenant_id=server.tenant_id,
        title=server.title,
        endpoint=server.base_url,
        credential_ref=server.credential_ref,
        allowed_tool_prefixes=tuple(item.tool_name for item in server.operations),
        status=server.status,
        enabled=server.enabled,
        metadata={
            **server.metadata,
            "connector_kind": "java-api",
        },
    )


def _tool_descriptor(operation: JavaApiOperationDefinition) -> HandsToolDescriptor:
    return HandsToolDescriptor(
        name=operation.tool_name,
        version=operation.version,
        description=operation.description,
        input_schema=dict(operation.input_schema),
        output_schema=dict(operation.output_schema),
        read_only=operation.read_only,
        destructive=not operation.idempotent and not operation.read_only,
        risk_level=operation.risk_level,
    )


def _bind_arguments(
    operation: JavaApiOperationDefinition,
    arguments: dict[str, Any],
) -> tuple[str, dict[str, Any], dict[str, Any]]:
    declared = {binding.name: binding for binding in operation.argument_bindings}
    extra = set(arguments).difference(declared)
    if extra:
        raise ValueError("Java API arguments include undeclared fields")
    if any(str(key).lower() in _FORBIDDEN_ARGUMENT_KEYS for key in arguments):
        raise ValueError("Java API arguments may not override transport fields")
    path = operation.path_template
    query: dict[str, Any] = {}
    body: dict[str, Any] = {}
    for binding in operation.argument_bindings:
        if binding.name not in arguments:
            if binding.required:
                raise ValueError(f"Java API argument is required: {binding.name}")
            continue
        value = arguments[binding.name]
        if binding.location == "path":
            if not isinstance(value, (str, int)) or not _SAFE_PATH_VALUE.fullmatch(
                str(value)
            ):
                raise ValueError("Java API path argument is unsafe")
            path = path.replace("{" + binding.name + "}", quote(str(value), safe=""))
        elif binding.location == "query":
            query[binding.name] = value
        else:
            body[binding.name] = value
    if "{" in path or "}" in path:
        raise ValueError("Java API path template was not fully bound")
    return path, query, body


def _digest(value: dict[str, Any]) -> str:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()
