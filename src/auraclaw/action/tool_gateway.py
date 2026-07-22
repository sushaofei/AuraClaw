from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime, timedelta
from typing import Any, Protocol

from auraclaw.action.policy import PolicyEngine
from auraclaw.action.ports import (
    ArtifactWriter,
    CredentialAdapter,
    CredentialInvoker,
    HandsExecutor,
)
from auraclaw.contracts.errors import (
    ApprovalValidationError,
    CredentialAccessError,
    PolicyDeniedError,
    SandboxViolationError,
    SchemaValidationError,
)
from auraclaw.contracts.tools import (
    ApprovalRecord,
    ArtifactRef,
    PolicyDecision,
    ToolCapability,
    ToolInvocation,
    ToolPermission,
    ToolResult,
    ToolResultStatus,
)
from auraclaw.domain.approval import ApprovalAggregate, action_digest


class ApprovalReader(Protocol):
    async def get(self, tenant_id: str, approval_id: str) -> ApprovalRecord | None: ...

    async def find_approved(
        self, tenant_id: str, session_id: str, digest: str, policy_version: str
    ) -> ApprovalRecord | None: ...


class ToolRegistry:
    def __init__(self, capabilities: tuple[ToolCapability, ...] = ()) -> None:
        self._capabilities: dict[tuple[str, str], ToolCapability] = {}
        for capability in capabilities:
            self.register(capability)

    def register(self, capability: ToolCapability) -> None:
        key = (capability.name, capability.version)
        if key in self._capabilities:
            raise ValueError(f"Tool already registered: {capability.name}@{capability.version}")
        self._capabilities[key] = capability

    def get(self, name: str, version: str) -> ToolCapability:
        try:
            return self._capabilities[(name, version)]
        except KeyError as exc:
            raise PolicyDeniedError(f"Tool is not registered: {name}@{version}") from exc

    def discover(self, *, permissions: tuple[ToolPermission, ...] = ()) -> list[ToolCapability]:
        values = [
            item
            for item in self._capabilities.values()
            if not permissions or item.permission in permissions
        ]
        return sorted(values, key=lambda item: (item.name, item.version))


class JsonSchemaValidator:
    """Small strict JSON Schema subset used at the untrusted Tool boundary."""

    @classmethod
    def validate(cls, value: Any, schema: dict[str, Any], *, path: str = "$") -> None:
        expected_type = schema.get("type")
        if expected_type is not None and not cls._matches_type(value, str(expected_type)):
            raise SchemaValidationError(f"{path} must be {expected_type}")
        if "enum" in schema and value not in schema["enum"]:
            raise SchemaValidationError(f"{path} is not an allowed value")
        if isinstance(value, dict):
            properties = dict(schema.get("properties", {}))
            required = set(schema.get("required", ()))
            missing = sorted(required.difference(value))
            if missing:
                raise SchemaValidationError(f"{path} is missing required fields: {missing}")
            if schema.get("additionalProperties") is False:
                extras = sorted(set(value).difference(properties))
                if extras:
                    raise SchemaValidationError(f"{path} has unexpected fields: {extras}")
            for key, child in value.items():
                child_schema = properties.get(key)
                if isinstance(child_schema, dict):
                    cls.validate(child, child_schema, path=f"{path}.{key}")
        elif isinstance(value, list):
            item_schema = schema.get("items")
            if isinstance(item_schema, dict):
                for index, item in enumerate(value):
                    cls.validate(item, item_schema, path=f"{path}[{index}]")
        elif isinstance(value, str):
            if "minLength" in schema and len(value) < int(schema["minLength"]):
                raise SchemaValidationError(f"{path} is shorter than minLength")
            if "maxLength" in schema and len(value) > int(schema["maxLength"]):
                raise SchemaValidationError(f"{path} is longer than maxLength")
        elif isinstance(value, int | float) and not isinstance(value, bool):
            if "minimum" in schema and value < schema["minimum"]:
                raise SchemaValidationError(f"{path} is below minimum")
            if "maximum" in schema and value > schema["maximum"]:
                raise SchemaValidationError(f"{path} is above maximum")

    @staticmethod
    def _matches_type(value: Any, expected: str) -> bool:
        return {
            "object": isinstance(value, dict),
            "array": isinstance(value, list),
            "string": isinstance(value, str),
            "integer": isinstance(value, int) and not isinstance(value, bool),
            "number": isinstance(value, int | float) and not isinstance(value, bool),
            "boolean": isinstance(value, bool),
            "null": value is None,
        }.get(expected, False)



class ToolGateway:
    def __init__(
        self,
        *,
        registry: ToolRegistry,
        policy: PolicyEngine,
        approvals: ApprovalReader,
        hands: HandsExecutor,
        artifacts: ArtifactWriter,
        credential_proxy: CredentialInvoker | None = None,
        credential_adapters: dict[str, CredentialAdapter] | None = None,
        max_inline_bytes: int = 64 * 1024,
        approval_ttl: timedelta = timedelta(hours=1),
    ) -> None:
        self._registry = registry
        self._policy = policy
        self._approvals = approvals
        self._hands = hands
        self._artifacts = artifacts
        self._credential_proxy = credential_proxy
        self._credential_adapters = credential_adapters or {}
        self._max_inline_bytes = max_inline_bytes
        self._approval_ttl = approval_ttl
        self._results: dict[tuple[str, str], tuple[str, ToolResult]] = {}
        self._pending_approvals: dict[tuple[str, str, str], ApprovalRecord] = {}
        self._inflight: dict[str, asyncio.Task[Any]] = {}
        self._statuses: dict[str, str] = {}
        self._lock = asyncio.Lock()

    async def execute(self, invocation: ToolInvocation) -> ToolResult:
        task = asyncio.current_task()
        if task is not None:
            self._inflight[invocation.tool_invocation_id] = task
        self._statuses[invocation.tool_invocation_id] = "accepted"
        try:
            result = await self._execute_once(invocation)
        except asyncio.CancelledError:
            result = ToolResult(
                status=ToolResultStatus.CANCELLED,
                summary="tool invocation was cancelled",
                error_code="tool_cancelled",
                side_effect_status="unknown",
            )
        finally:
            self._inflight.pop(invocation.tool_invocation_id, None)
        self._statuses[invocation.tool_invocation_id] = result.status.value
        return result

    async def cancel(self, tool_invocation_id: str) -> bool:
        task = self._inflight.get(tool_invocation_id)
        if task is None or task.done():
            return False
        task.cancel()
        return True

    def get_status(self, tool_invocation_id: str) -> str | None:
        return self._statuses.get(tool_invocation_id)

    async def _execute_once(self, invocation: ToolInvocation) -> ToolResult:
        capability = self._registry.get(invocation.tool_name, invocation.tool_version)
        JsonSchemaValidator.validate(invocation.arguments, capability.input_schema)
        digest = action_digest(
            invocation.tool_name, invocation.tool_version, invocation.arguments
        )
        cache_key = (invocation.tenant_id, invocation.idempotency_key)
        async with self._lock:
            previous = self._results.get(cache_key)
            if previous is not None:
                previous_digest, previous_result = previous
                if previous_digest != digest:
                    return ToolResult(
                        status=ToolResultStatus.DENIED,
                        summary="idempotency key was already used for a different action",
                        error_code="idempotency_conflict",
                    )
                return previous_result

            decision = self._policy.evaluate(capability)
            if decision is PolicyDecision.DENY:
                result = ToolResult(
                    status=ToolResultStatus.DENIED,
                    summary="tool policy denied execution",
                    error_code="policy_denied",
                )
                self._results[cache_key] = (digest, result)
                return result
            if decision is PolicyDecision.REQUIRE_APPROVAL:
                approval = await self._resolve_approval(invocation, capability, digest)
                if approval is None:
                    pending = self._pending_approvals.get(
                        (invocation.tenant_id, invocation.session_id, digest)
                    )
                    if pending is None:
                        pending = ApprovalAggregate.request(
                            tenant_id=invocation.tenant_id,
                            session_id=invocation.session_id,
                            run_id=invocation.run_id,
                            digest=digest,
                            tool_name=invocation.tool_name,
                            redacted_arguments=self._redact(invocation.arguments),
                            risk=capability.risk_level,
                            reason=f"{capability.permission.value} action requires human approval",
                            expected_effect=invocation.expected_side_effect,
                            policy_version=self._policy.version,
                            ttl=self._approval_ttl,
                        )
                        self._pending_approvals[
                            (invocation.tenant_id, invocation.session_id, digest)
                        ] = pending
                    return ToolResult(
                        status=ToolResultStatus.DENIED,
                        summary="human approval is required before execution",
                        metadata={"approval_request": pending.as_event_payload()},
                        error_code="approval_required",
                    )

            result = await self._dispatch(invocation, capability)
            self._results[cache_key] = (digest, result)
            return result

    async def _resolve_approval(
        self,
        invocation: ToolInvocation,
        capability: ToolCapability,
        digest: str,
    ) -> ApprovalRecord | None:
        del capability
        record = None
        if invocation.approval_id is not None:
            record = await self._approvals.get(
                invocation.tenant_id, invocation.approval_id
            )
            if record is None:
                raise ApprovalValidationError("approval does not exist")
        else:
            record = await self._approvals.find_approved(
                invocation.tenant_id,
                invocation.session_id,
                digest,
                self._policy.version,
            )
        if record is None:
            return None
        ApprovalAggregate.validate(
            record,
            tenant_id=invocation.tenant_id,
            session_id=invocation.session_id,
            digest=digest,
            policy_version=self._policy.version,
        )
        return record

    async def _dispatch(
        self, invocation: ToolInvocation, capability: ToolCapability
    ) -> ToolResult:
        if invocation.deadline is not None and datetime.now(UTC) >= invocation.deadline:
            return ToolResult(
                status=ToolResultStatus.TIMEOUT,
                summary="tool deadline elapsed before dispatch",
                error_code="deadline_exceeded",
            )
        timeout = capability.timeout_seconds
        if invocation.deadline is not None:
            timeout = min(timeout, (invocation.deadline - datetime.now(UTC)).total_seconds())
        try:
            raw = await asyncio.wait_for(
                self._execute_adapter(invocation, capability), timeout=max(timeout, 0.001)
            )
        except TimeoutError:
            return ToolResult(
                status=ToolResultStatus.TIMEOUT,
                summary="tool execution timed out",
                error_code="tool_timeout",
                side_effect_status="unknown",
            )
        except (CredentialAccessError, PolicyDeniedError, SandboxViolationError) as exc:
            return ToolResult(
                status=ToolResultStatus.DENIED,
                summary="controlled execution boundary denied the tool call",
                error_code=exc.code,
                side_effect_status="not_started",
            )
        except Exception:
            return ToolResult(
                status=ToolResultStatus.ERROR,
                summary="tool adapter failed",
                error_code="tool_adapter_error",
                side_effect_status="unknown",
            )
        redacted = self._redact(raw)
        JsonSchemaValidator.validate(redacted, capability.output_schema)
        content = await self._extract_artifact(invocation, redacted)
        return ToolResult(
            status=ToolResultStatus.SUCCESS,
            content=content,
            summary=f"{invocation.tool_name} completed",
            metadata={"tool_version": capability.version},
            side_effect_status="completed",
        )

    async def _execute_adapter(
        self, invocation: ToolInvocation, capability: ToolCapability
    ) -> Any:
        if capability.runtime_location == "credential_proxy":
            if self._credential_proxy is None or invocation.credential_ref is None:
                raise PolicyDeniedError("credential proxy execution requires credential_ref")
            adapter = self._credential_adapters.get(invocation.tool_name)
            if adapter is None:
                raise PolicyDeniedError("credential proxy adapter is not registered")
            operation = invocation.expected_side_effect
            if capability.allowed_credential_operations:
                if operation not in capability.allowed_credential_operations:
                    raise PolicyDeniedError("tool operation is outside capability scope")
            return await self._credential_proxy.invoke(
                tenant_id=invocation.tenant_id,
                session_id=invocation.session_id,
                tool_name=invocation.tool_name,
                credential_ref=invocation.credential_ref,
                operation=operation,
                request=invocation.arguments,
                adapter=adapter,
            )
        return await self._hands.execute(invocation, capability)

    async def _extract_artifact(
        self, invocation: ToolInvocation, content: Any
    ) -> str | dict[str, Any] | ArtifactRef | None:
        serialized = json.dumps(
            content, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str
        ).encode()
        if len(serialized) <= self._max_inline_bytes:
            if content is None or isinstance(content, str | dict):
                return content
            return {"value": content}
        return await self._artifacts.put(
            tenant_id=invocation.tenant_id,
            root_session_id=invocation.root_session_id,
            session_id=invocation.session_id,
            content=serialized,
            artifact_type="tool-output",
            media_type="application/json",
            name=f"{invocation.tool_name}-{invocation.tool_invocation_id}.json",
            producer=f"tool:{invocation.tool_name}@{invocation.tool_version}",
        )

    def _redact(self, value: Any) -> Any:
        if self._credential_proxy is not None:
            return self._credential_proxy.redact(value)
        return value
