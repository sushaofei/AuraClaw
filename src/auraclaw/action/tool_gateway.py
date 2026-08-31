from __future__ import annotations

import asyncio
import inspect
import json
import secrets
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta
from typing import Any, Protocol

from auraclaw.action.policy import PolicyEngine as PolicyEngine
from auraclaw.action.ports import (
    ApprovalController,
    ArtifactWriter,
    CredentialAdapter,
    CredentialInvoker,
    HandsExecutor,
    InvocationStore,
    PolicyEvaluation,
    PolicyEvaluator,
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
    ApprovalStatus,
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
        self._discoverable: set[tuple[str, str]] = set()
        for capability in capabilities:
            self.register(capability)

    def register(self, capability: ToolCapability) -> None:
        key = (capability.name, capability.version)
        if key in self._capabilities:
            raise ValueError(f"Tool already registered: {capability.name}@{capability.version}")
        self._capabilities[key] = capability
        self._discoverable.add(key)

    def get(self, name: str, version: str) -> ToolCapability:
        try:
            return self._capabilities[(name, version)]
        except KeyError as exc:
            raise PolicyDeniedError(f"Tool is not registered: {name}@{version}") from exc

    def replace_owner(
        self,
        owner: str,
        capabilities: tuple[ToolCapability, ...],
    ) -> None:
        self._discoverable.difference_update(
            key
            for key, capability in self._capabilities.items()
            if capability.owner == owner
        )
        for capability in capabilities:
            key = (capability.name, capability.version)
            existing = self._capabilities.get(key)
            if existing is not None and existing != capability:
                raise ValueError(
                    f"Tool version changed without version bump: "
                    f"{capability.name}@{capability.version}"
                )
            if existing is None:
                self._capabilities[key] = capability
            self._discoverable.add(key)

    def revoke_owner(self, owner: str) -> None:
        removed = {
            key
            for key, capability in self._capabilities.items()
            if capability.owner == owner
        }
        self._capabilities = {
            key: capability
            for key, capability in self._capabilities.items()
            if key not in removed
        }
        self._discoverable.difference_update(removed)

    def discover(self, *, permissions: tuple[ToolPermission, ...] = ()) -> list[ToolCapability]:
        values = [
            item
            for key, item in self._capabilities.items()
            if key in self._discoverable
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



class _KeyedLocks:
    def __init__(self) -> None:
        self._guard = asyncio.Lock()
        self._entries: dict[tuple[str, str], tuple[asyncio.Lock, int]] = {}

    @asynccontextmanager
    async def hold(self, key: tuple[str, str]) -> AsyncIterator[None]:
        async with self._guard:
            lock, references = self._entries.get(key, (asyncio.Lock(), 0))
            self._entries[key] = (lock, references + 1)
        await lock.acquire()
        try:
            yield
        finally:
            lock.release()
            async with self._guard:
                current, references = self._entries[key]
                if references == 1:
                    self._entries.pop(key, None)
                else:
                    self._entries[key] = (current, references - 1)


class ToolGateway:
    def __init__(
        self,
        *,
        registry: ToolRegistry,
        policy: PolicyEvaluator,
        approvals: ApprovalReader,
        hands: HandsExecutor,
        artifacts: ArtifactWriter,
        credential_proxy: CredentialInvoker | None = None,
        credential_adapters: dict[str, CredentialAdapter] | None = None,
        invocation_store: InvocationStore | None = None,
        approval_controller: ApprovalController | None = None,
        max_inline_bytes: int = 64 * 1024,
        approval_ttl: timedelta = timedelta(hours=1),
        instance_id: str | None = None,
        execution_claim_ttl: timedelta = timedelta(seconds=30),
        cancellation_poll_interval: float = 1.0,
    ) -> None:
        if execution_claim_ttl <= timedelta(0):
            raise ValueError("execution_claim_ttl must be positive")
        if cancellation_poll_interval <= 0:
            raise ValueError("cancellation_poll_interval must be positive")
        self._registry = registry
        self._policy = policy
        self._approvals = approvals
        self._hands = hands
        self._artifacts = artifacts
        self._credential_proxy = credential_proxy
        self._credential_adapters = credential_adapters or {}
        self._invocation_store = invocation_store
        self._approval_controller = approval_controller
        self._max_inline_bytes = max_inline_bytes
        self._approval_ttl = approval_ttl
        self._instance_id = instance_id or f"hands-{secrets.token_hex(8)}"
        self._execution_claim_ttl = execution_claim_ttl
        self._cancellation_poll_interval = cancellation_poll_interval
        self._results: dict[tuple[str, str], tuple[str, ToolResult]] = {}
        self._pending_approvals: dict[tuple[str, str, str], ApprovalRecord] = {}
        self._inflight: dict[tuple[str, str], asyncio.Task[Any]] = {}
        self._claim_tokens: dict[tuple[str, str], str] = {}
        self._claim_monitors: dict[tuple[str, str], asyncio.Task[None]] = {}
        self._statuses: dict[tuple[str, str], str] = {}
        self._locks = _KeyedLocks()

    async def execute(self, invocation: ToolInvocation) -> ToolResult:
        task = asyncio.current_task()
        execution_key = (invocation.tenant_id, invocation.tool_invocation_id)
        if task is not None:
            self._inflight[execution_key] = task
        self._statuses[execution_key] = "accepted"
        try:
            result = await self._execute_once(invocation)
        except asyncio.CancelledError:
            result = ToolResult(
                status=ToolResultStatus.CANCELLED,
                summary="tool invocation was cancelled",
                error_code="tool_cancelled",
                side_effect_status="unknown",
            )
            if self._invocation_store is not None:
                claim_token = self._claim_tokens.get(execution_key)
                if claim_token is not None:
                    committed = await self._invocation_store.complete(
                        invocation, result, claim_token=claim_token
                    )
                    if not committed:
                        result = ToolResult(
                            status=ToolResultStatus.UNKNOWN,
                            summary="cancelled invocation could not commit its terminal state",
                            error_code="invocation_completion_uncommitted",
                            side_effect_status="unknown",
                        )
        finally:
            monitor = self._claim_monitors.pop(execution_key, None)
            if monitor is not None:
                monitor.cancel()
                await asyncio.gather(monitor, return_exceptions=True)
            self._claim_tokens.pop(execution_key, None)
            self._inflight.pop(execution_key, None)
        self._statuses[execution_key] = result.status.value
        return result

    async def cancel(
        self, tool_invocation_id: str, *, tenant_id: str | None = None
    ) -> bool:
        persisted = False
        if self._invocation_store is not None and tenant_id is not None:
            persisted = await self._invocation_store.request_cancel(
                tenant_id, tool_invocation_id
            )
        tasks = [
            task
            for (candidate_tenant, candidate_id), task in self._inflight.items()
            if candidate_id == tool_invocation_id
            and (tenant_id is None or candidate_tenant == tenant_id)
            and not task.done()
        ]
        for task in tasks:
            task.cancel()
        return persisted or bool(tasks)

    def get_status(self, tool_invocation_id: str) -> str | None:
        matches = [
            status
            for (_tenant_id, candidate_id), status in self._statuses.items()
            if candidate_id == tool_invocation_id
        ]
        return matches[0] if len(matches) == 1 else None

    async def get_authoritative_status(
        self, tenant_id: str, tool_invocation_id: str
    ) -> tuple[str, str, str | None, bool] | None:
        if self._invocation_store is not None:
            status = await self._invocation_store.get_status(
                tenant_id, tool_invocation_id
            )
            if status is None:
                return None
            return (
                status.status,
                status.side_effect_status,
                status.error_code,
                status.cancel_requested,
            )
        local = self._statuses.get((tenant_id, tool_invocation_id))
        return None if local is None else (local, "unknown", None, False)

    async def _execute_once(self, invocation: ToolInvocation) -> ToolResult:
        capability = self._registry.get(invocation.tool_name, invocation.tool_version)
        JsonSchemaValidator.validate(invocation.arguments, capability.input_schema)
        digest = action_digest(
            invocation.tool_name, invocation.tool_version, invocation.arguments
        )
        cache_key = (invocation.tenant_id, invocation.idempotency_key)
        async with self._locks.hold(cache_key):
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

            if self._invocation_store is not None:
                claim_token = secrets.token_urlsafe(24)
                persisted = await self._invocation_store.begin(
                    invocation,
                    digest,
                    owner=self._instance_id,
                    claim_token=claim_token,
                    claim_ttl=self._execution_claim_ttl,
                )
                if persisted.conflict:
                    return ToolResult(
                        status=ToolResultStatus.DENIED,
                        summary="idempotency key was already used for a different action",
                        error_code="idempotency_conflict",
                    )
                if isinstance(persisted.cached_result, ToolResult):
                    return persisted.cached_result
                if not persisted.acquired or persisted.claim_token is None:
                    return ToolResult(
                        status=ToolResultStatus.UNKNOWN,
                        summary="tool invocation execution claim was not acquired",
                        error_code="invocation_claim_failed",
                        side_effect_status="not_started",
                    )
                execution_key = (invocation.tenant_id, invocation.tool_invocation_id)
                self._claim_tokens[execution_key] = persisted.claim_token
                self._claim_monitors[execution_key] = asyncio.create_task(
                    self._monitor_claim(invocation, persisted.claim_token)
                )

            evaluated = self._policy.evaluate(capability, invocation)
            evaluation = await evaluated if inspect.isawaitable(evaluated) else evaluated
            if isinstance(evaluation, PolicyEvaluation):
                decision = evaluation.decision
                decision_id = evaluation.decision_id
                policy_version = evaluation.policy_version
            else:
                decision = evaluation
                decision_id = None
                policy_version = self._policy.version
            if decision is PolicyDecision.DENY:
                result = ToolResult(
                    status=ToolResultStatus.DENIED,
                    summary="tool policy denied execution",
                    error_code="policy_denied",
                )
                if self._invocation_store is not None:
                    if not await self._complete_claimed(invocation, result):
                        return self._claim_lost_result("before policy result was committed")
                self._results[cache_key] = (digest, result)
                return result
            if decision is PolicyDecision.REQUIRE_APPROVAL and not (
                capability.runtime_location == "remote-mcp"
                and capability.permission is ToolPermission.READ_ONLY
            ):
                approval = await self._resolve_approval(
                    invocation, capability, digest, policy_version
                )
                if not approval:
                    pending_key = (
                        invocation.tenant_id,
                        invocation.session_id,
                        digest,
                    )
                    pending = self._pending_approvals.get(pending_key)
                    if pending is not None and not await self._pending_approval_is_waiting(
                        pending
                    ):
                        self._pending_approvals.pop(pending_key, None)
                        pending = None
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
                            policy_version=policy_version,
                            ttl=self._approval_ttl,
                        )
                        self._pending_approvals[pending_key] = pending
                        if self._approval_controller is not None:
                            await self._approval_controller.request_approval(pending)
                    result = ToolResult(
                        status=ToolResultStatus.DENIED,
                        summary="human approval is required before execution",
                        metadata={"approval_request": pending.as_event_payload()},
                        error_code="approval_required",
                    )
                    if self._invocation_store is not None:
                        claim_token = self._claim_token(invocation)
                        parked = await self._invocation_store.wait_for_approval(
                            invocation, result, claim_token=claim_token
                        )
                        if not parked:
                            return self._claim_lost_result(
                                "before approval state was committed"
                            )
                    return result

            if self._invocation_store is not None:
                claim_token = self._claim_token(invocation)
                if not await self._invocation_store.mark_executing(
                    invocation, claim_token=claim_token
                ):
                    return self._claim_lost_result("before dispatch")
            result = await self._dispatch(
                invocation, capability, policy_decision_id=decision_id
            )
            if self._invocation_store is not None:
                if not await self._complete_claimed(invocation, result):
                    return ToolResult(
                        status=ToolResultStatus.UNKNOWN,
                        summary="tool invocation completed after its execution claim was lost",
                        error_code="invocation_completion_uncommitted",
                        side_effect_status="unknown",
                    )
            self._results[cache_key] = (digest, result)
            return result

    def _claim_token(self, invocation: ToolInvocation) -> str:
        return self._claim_tokens[(invocation.tenant_id, invocation.tool_invocation_id)]

    @staticmethod
    def _claim_lost_result(phase: str) -> ToolResult:
        return ToolResult(
            status=ToolResultStatus.UNKNOWN,
            summary=f"tool invocation claim expired {phase}",
            error_code="invocation_claim_lost",
            side_effect_status="not_started",
        )

    async def _complete_claimed(
        self, invocation: ToolInvocation, result: ToolResult
    ) -> bool:
        assert self._invocation_store is not None
        return await self._invocation_store.complete(
            invocation, result, claim_token=self._claim_token(invocation)
        )

    async def _monitor_claim(
        self, invocation: ToolInvocation, claim_token: str
    ) -> None:
        assert self._invocation_store is not None
        parent = self._inflight.get((invocation.tenant_id, invocation.tool_invocation_id))
        loop = asyncio.get_running_loop()
        next_renewal = loop.time() + self._execution_claim_ttl.total_seconds() / 3
        try:
            while True:
                await asyncio.sleep(self._cancellation_poll_interval)
                if await self._invocation_store.is_cancel_requested(
                    invocation, claim_token=claim_token
                ):
                    if parent is not None and not parent.done():
                        parent.cancel()
                    return
                if loop.time() >= next_renewal:
                    renewed = await self._invocation_store.renew(
                        invocation,
                        owner=self._instance_id,
                        claim_token=claim_token,
                        claim_ttl=self._execution_claim_ttl,
                    )
                    if not renewed:
                        if parent is not None and not parent.done():
                            parent.cancel()
                        return
                    next_renewal = (
                        loop.time() + self._execution_claim_ttl.total_seconds() / 3
                    )
        except Exception:
            if parent is not None and not parent.done():
                parent.cancel()

    async def _resolve_approval(
        self,
        invocation: ToolInvocation,
        capability: ToolCapability,
        digest: str,
        policy_version: str,
    ) -> bool:
        del capability
        if invocation.approval_id is not None:
            if self._approval_controller is not None:
                return await self._approval_controller.validate_approval(
                    tenant_id=invocation.tenant_id,
                    approval_id=invocation.approval_id,
                    session_id=invocation.session_id,
                    run_id=invocation.run_id,
                    action_digest=digest,
                    policy_version=policy_version,
                )
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
                policy_version,
            )
            if record is None:
                return False
        ApprovalAggregate.validate(
            record,
            tenant_id=invocation.tenant_id,
            session_id=invocation.session_id,
            digest=digest,
            policy_version=policy_version,
        )
        return True

    async def _pending_approval_is_waiting(self, pending: ApprovalRecord) -> bool:
        record = await self._approvals.get(pending.tenant_id, pending.approval_id)
        if record is None:
            return True
        return record.status in {ApprovalStatus.REQUESTED, ApprovalStatus.WAITING}

    async def _dispatch(
        self,
        invocation: ToolInvocation,
        capability: ToolCapability,
        *,
        policy_decision_id: str | None = None,
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
                self._execute_adapter(
                    invocation,
                    capability,
                    policy_decision_id=policy_decision_id,
                ),
                timeout=max(timeout, 0.001),
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
                summary=exc.message or "controlled execution boundary denied the tool call",
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
        self,
        invocation: ToolInvocation,
        capability: ToolCapability,
        *,
        policy_decision_id: str | None = None,
    ) -> Any:
        if capability.runtime_location == "credential_proxy":
            if self._credential_proxy is None or invocation.credential_ref is None:
                raise PolicyDeniedError("credential proxy execution requires credential_ref")
            adapter = self._credential_adapters.get(invocation.tool_name)
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
                policy_decision_id=policy_decision_id,
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
