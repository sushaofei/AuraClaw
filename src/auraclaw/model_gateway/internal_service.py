from __future__ import annotations

import asyncio
import hashlib
import json
import math
import time
from collections.abc import AsyncIterator
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Protocol
from uuid import uuid4

from auraclaw.action.ports import PolicyEvaluation
from auraclaw.contracts.errors import (
    AuthorizationError,
    BudgetExceededError,
    LeaseConflictError,
    ModelProviderError,
    PolicyDeniedError,
    VersionConflictError,
)
from auraclaw.contracts.internal import (
    ModelCancelRequest,
    ModelCancelResponse,
    ModelGenerateRequest,
    ModelGenerateResponse,
    ModelStreamEvent,
    ServiceIdentity,
)
from auraclaw.contracts.observability import MetricPoint
from auraclaw.contracts.tools import PolicyDecision
from auraclaw.model_gateway.ports import ModelCallReservation, ModelStateStore
from auraclaw.runtime.model_stream import iter_model_stream
from auraclaw.runtime.ports import (
    ModelClient,
    ModelPolicy,
    ModelRequest,
    ModelResponse,
    ProviderCancellationResult,
)


@dataclass
class _ExecutionMonitorState:
    cancel_requested: bool = False
    cancel_outcome: ProviderCancellationResult | None = None
    ownership_lost: bool = False


class ModelPolicyEnforcer(Protocol):
    async def evaluate_action(
        self,
        *,
        tenant_id: str,
        subject: str,
        action: str,
        resource: str,
        input_digest: str,
        correlation_id: str,
        attributes: dict[str, object],
    ) -> PolicyEvaluation: ...


class MetricWriter(Protocol):
    async def write_metric(self, metric: MetricPoint) -> None: ...


_RUNTIME_METRICS = frozenset(
    {
        "skill.runtime.active.count",
        "skill.runtime.prompt.bytes",
        "skill.runtime.prompt.estimated_tokens",
        "skill.runtime.content_cache.hit.count",
        "skill.runtime.content_cache.miss.count",
        "skill.runtime.trusted_messages.latency.seconds",
        "skill.runtime.prompt.rejected.count",
    }
)


class ModelGatewayInternalService:
    def __init__(
        self,
        model: ModelClient,
        *,
        policy: ModelPolicyEnforcer | None = None,
        state: ModelStateStore | None = None,
        tenant_token_limit: int = 1_000_000,
        gateway_id: str | None = None,
        claim_ttl: timedelta = timedelta(seconds=30),
        heartbeat_interval: float = 5.0,
        metric_writer: MetricWriter | None = None,
    ) -> None:
        self._model = model
        self._policy = policy
        self._state = state
        self._tenant_token_limit = tenant_token_limit
        self._gateway_id = gateway_id or f"model-gateway-{uuid4().hex}"
        self._claim_ttl = claim_ttl
        self._heartbeat_interval = heartbeat_interval
        self._metric_writer = metric_writer

    @staticmethod
    def _require_runtime(identity: ServiceIdentity) -> None:
        if identity is not ServiceIdentity.AGENT_RUNTIME:
            raise AuthorizationError("model calls are restricted to agent-runtime")

    async def generate(self, request: ModelGenerateRequest) -> ModelGenerateResponse:
        result: ModelGenerateResponse | None = None
        async for event in self.generate_stream(request):
            if event.type == "completed":
                result = ModelGenerateResponse.model_validate(event.payload)
        if result is None:
            raise ModelProviderError("model stream ended without a completed response")
        return result

    async def generate_stream(
        self, request: ModelGenerateRequest
    ) -> AsyncIterator[ModelStreamEvent]:
        self._require_runtime(request.context.service_identity)
        request_digest = self._request_digest(request)
        reservation = await self._prepare_stream(request, request_digest)
        if reservation is not None:
            if reservation.status == "completed":
                assert reservation.cached_response is not None
                async for event in self._cached_stream(reservation.cached_response):
                    yield event
                return
            if reservation.status == "conflict":
                raise VersionConflictError("model_call_id was reused with another request")
            if reservation.status == "in_progress":
                raise LeaseConflictError("model call is already in progress")
            if reservation.status == "quota_exceeded":
                raise BudgetExceededError("tenant model token quota is exhausted")
            if reservation.status == "cancelled":
                raise LeaseConflictError("model call was cancelled")
            if reservation.status == "reconciling":
                raise LeaseConflictError("model call outcome requires reconciliation")
        claim_token = reservation.claim_token if reservation is not None else None
        monitor_stop = asyncio.Event()
        monitor_state = _ExecutionMonitorState()
        parent_task = asyncio.current_task()
        monitor = (
            asyncio.create_task(
                self._monitor_execution(
                    request,
                    claim_token=claim_token,
                    stop=monitor_stop,
                    state=monitor_state,
                    parent_task=parent_task,
                )
            )
            if self._state is not None and claim_token is not None
            else None
        )
        sequence = 0
        response: ModelResponse | None = None
        provider_started = time.monotonic()
        first_output_recorded = False
        metric_tasks = [asyncio.create_task(self._emit_runtime_metrics(request))]
        try:
            async for chunk in iter_model_stream(
                self._model,
                ModelRequest(
                    model_call_id=request.model_call_id,
                    tenant_id=request.context.tenant_id,
                    run_id=request.run_id,
                    messages=request.messages,
                    session_id=request.session_id,
                    tools=request.tools,
                    policy=ModelPolicy(
                        capability=request.capability,
                        preferred_model=request.preferred_model,
                        allowed_providers=request.allowed_providers,
                        data_classification=request.data_classification,
                    ),
                    max_output_tokens=request.max_output_tokens,
                    runtime_metrics=request.runtime_metrics,
                    prompt_cache_key=request.prompt_cache_key,
                ),
            ):
                if chunk.kind == "delta":
                    if not chunk.delta:
                        continue
                    if not first_output_recorded:
                        first_output_recorded = True
                        metric_tasks.append(
                            asyncio.create_task(
                                self._emit_metric(
                                    "model.ttft.seconds",
                                    time.monotonic() - provider_started,
                                    request,
                                )
                            )
                        )
                    sequence += 1
                    yield ModelStreamEvent(
                        model_call_id=request.model_call_id,
                        sequence=sequence,
                        type="delta",
                        payload={"delta": chunk.delta},
                    )
                elif chunk.kind == "completed":
                    response = chunk.response
                    if response is not None:
                        metric_tasks.append(
                            asyncio.create_task(
                                self._emit_prompt_cache_metrics(response.usage, request)
                            )
                        )
                    if not first_output_recorded:
                        first_output_recorded = True
                        metric_tasks.append(
                            asyncio.create_task(
                                self._emit_metric(
                                    "model.ttft.seconds",
                                    time.monotonic() - provider_started,
                                    request,
                                )
                            )
                        )
        except asyncio.CancelledError:
            if self._state is not None and claim_token is not None:
                outcome = monitor_state.cancel_outcome
                if monitor_state.cancel_requested and outcome is not None and outcome.stopped:
                    if outcome.usage_final:
                        await self._state.mark_cancelled(
                            tenant_id=request.context.tenant_id,
                            model_call_id=request.model_call_id,
                            execution_owner=self._gateway_id,
                            claim_token=claim_token,
                            usage=outcome.usage,
                        )
                    else:
                        await self._state.mark_reconciling(
                            tenant_id=request.context.tenant_id,
                            model_call_id=request.model_call_id,
                            execution_owner=self._gateway_id,
                            claim_token=claim_token,
                            error_code="cancel_usage_unknown",
                        )
                else:
                    await self._state.mark_reconciling(
                        tenant_id=request.context.tenant_id,
                        model_call_id=request.model_call_id,
                        execution_owner=self._gateway_id,
                        claim_token=claim_token,
                        error_code=(
                            "execution_owner_lost"
                            if monitor_state.ownership_lost
                            else "consumer_disconnected"
                        ),
                    )
            raise
        except GeneratorExit:
            if self._state is not None and claim_token is not None:
                await self._state.mark_reconciling(
                    tenant_id=request.context.tenant_id,
                    model_call_id=request.model_call_id,
                    execution_owner=self._gateway_id,
                    claim_token=claim_token,
                    error_code="consumer_disconnected",
                )
            raise
        except Exception as exc:
            if self._state is not None:
                await self._state.fail(
                    tenant_id=request.context.tenant_id,
                    model_call_id=request.model_call_id,
                    error_code=type(exc).__name__,
                    claim_token=claim_token,
                )
            raise
        finally:
            monitor_stop.set()
            if monitor is not None:
                monitor.cancel()
                with suppress(asyncio.CancelledError):
                    await monitor
            await asyncio.gather(*metric_tasks, return_exceptions=True)
        if response is None:
            if self._state is not None:
                await self._state.fail(
                    tenant_id=request.context.tenant_id,
                    model_call_id=request.model_call_id,
                    error_code="missing_completed_response",
                    claim_token=claim_token,
                )
            raise ModelProviderError("model stream ended without a completed response")
        result = self._to_generate_response(response)
        if self._state is not None:
            try:
                await self._state.complete(
                    tenant_id=request.context.tenant_id,
                    model_call_id=request.model_call_id,
                    response=result,
                    claim_token=claim_token,
                )
            except LeaseConflictError:
                if claim_token is not None:
                    await self._state.mark_reconciling(
                        tenant_id=request.context.tenant_id,
                        model_call_id=request.model_call_id,
                        execution_owner=self._gateway_id,
                        claim_token=claim_token,
                        error_code="completion_claim_lost",
                    )
                raise
            except Exception:
                if claim_token is not None:
                    with suppress(Exception):
                        await self._state.mark_reconciling(
                            tenant_id=request.context.tenant_id,
                            model_call_id=request.model_call_id,
                            execution_owner=self._gateway_id,
                            claim_token=claim_token,
                            error_code="completion_persistence_failed",
                        )
                raise
        sequence += 1
        yield ModelStreamEvent(
            model_call_id=request.model_call_id,
            sequence=sequence,
            type="completed",
            payload=result.model_dump(mode="json"),
        )

    async def cancel(self, request: ModelCancelRequest) -> ModelCancelResponse:
        self._require_runtime(request.context.service_identity)
        provider_cancellable = callable(getattr(self._model, "cancel", None))
        if self._state is None:
            outcome = await self._cancel_provider(request.model_call_id)
            cancelled = outcome.stopped and outcome.usage_final
            return ModelCancelResponse(
                model_call_id=request.model_call_id,
                cancelled=cancelled,
                status=(
                    "cancelled"
                    if cancelled
                    else "cancel_usage_unknown"
                    if outcome.stopped
                    else "provider_not_cancellable"
                ),
                provider_cancellable=provider_cancellable,
            )
        cancellation = await self._state.request_cancel(
            tenant_id=request.context.tenant_id,
            model_call_id=request.model_call_id,
            run_id=request.run_id,
            actor=request.context.service_identity.value,
            correlation_id=request.context.correlation_id,
            causation_id=request.context.causation_id,
        )
        return ModelCancelResponse(
            model_call_id=request.model_call_id,
            cancelled=cancellation.status == "cancelled",
            status=cancellation.status,
            provider_cancellable=provider_cancellable,
        )

    async def _monitor_execution(
        self,
        request: ModelGenerateRequest,
        *,
        claim_token: str,
        stop: asyncio.Event,
        state: _ExecutionMonitorState,
        parent_task: asyncio.Task[object] | None,
    ) -> None:
        assert self._state is not None
        while not stop.is_set():
            try:
                await asyncio.wait_for(stop.wait(), timeout=self._heartbeat_interval)
                return
            except TimeoutError:
                pass
            try:
                execution = await self._state.heartbeat(
                    tenant_id=request.context.tenant_id,
                    model_call_id=request.model_call_id,
                    execution_owner=self._gateway_id,
                    claim_token=claim_token,
                    claim_ttl=self._claim_ttl,
                )
            except Exception:
                state.ownership_lost = True
                await self._cancel_provider(request.model_call_id)
                if parent_task is not None:
                    parent_task.cancel()
                return
            if not execution.owned:
                state.ownership_lost = True
                await self._cancel_provider(request.model_call_id)
                if parent_task is not None:
                    parent_task.cancel()
                return
            if execution.cancel_requested:
                state.cancel_requested = True
                state.cancel_outcome = await self._cancel_provider(request.model_call_id)
                if state.cancel_outcome.stopped:
                    return

    async def _cancel_provider(self, model_call_id: str) -> ProviderCancellationResult:
        cancel = getattr(self._model, "cancel", None)
        if not callable(cancel):
            return ProviderCancellationResult(stopped=False)
        result = await cancel(model_call_id)
        if isinstance(result, ProviderCancellationResult):
            return result
        return ProviderCancellationResult(stopped=bool(result))

    async def _prepare_stream(
        self, request: ModelGenerateRequest, request_digest: str
    ) -> ModelCallReservation | None:
        """Run policy and quota reservation concurrently when both are configured."""

        async def _no_reservation() -> None:
            return None

        policy_result, reservation = await asyncio.gather(
            self._enforce_policy(request),
            (
                self._state.reserve(
                    tenant_id=request.context.tenant_id,
                    model_call_id=request.model_call_id,
                    run_id=request.run_id,
                    request_digest=request_digest,
                    reserved_tokens=request.max_output_tokens,
                    token_limit=self._tenant_token_limit,
                    execution_owner=self._gateway_id,
                    provider_request_ref=request.model_call_id,
                    actor=request.context.service_identity.value,
                    correlation_id=request.context.correlation_id,
                    causation_id=request.context.causation_id,
                    claim_ttl=self._claim_ttl,
                )
                if self._state is not None
                else _no_reservation()
            ),
            return_exceptions=True,
        )
        if isinstance(reservation, BaseException):
            if isinstance(policy_result, BaseException):
                raise policy_result
            raise reservation
        if isinstance(policy_result, BaseException):
            if (
                self._state is not None
                and isinstance(reservation, ModelCallReservation)
                and reservation.status == "reserved"
            ):
                await self._state.fail(
                    tenant_id=request.context.tenant_id,
                    model_call_id=request.model_call_id,
                    error_code=type(policy_result).__name__,
                    claim_token=reservation.claim_token,
                )
            raise policy_result
        return reservation if isinstance(reservation, ModelCallReservation) else None

    async def _enforce_policy(self, request: ModelGenerateRequest) -> None:
        if self._policy is None:
            return
        encoded = json.dumps(request.messages, sort_keys=True, default=str).encode()
        evaluation = await self._policy.evaluate_action(
            tenant_id=request.context.tenant_id,
            subject=request.context.service_identity.value,
            action="model.generate",
            resource=request.capability,
            input_digest=hashlib.sha256(encoded).hexdigest(),
            correlation_id=request.run_id,
            attributes={
                "permission": "read-only",
                "risk_level": "medium",
                "data_classification": request.data_classification,
            },
        )
        if evaluation.decision not in {
            PolicyDecision.ALLOW,
            PolicyDecision.ALLOW_WITH_CONSTRAINTS,
        }:
            raise PolicyDeniedError("Model policy denied generation")

    async def _emit_runtime_metrics(self, request: ModelGenerateRequest) -> None:
        await asyncio.gather(
            *(
                self._emit_metric(name, float(value), request)
                for name, value in request.runtime_metrics.items()
                if name in _RUNTIME_METRICS
                and math.isfinite(float(value))
                and float(value) >= 0
            )
        )

    async def _emit_metric(
        self, name: str, value: float, request: ModelGenerateRequest
    ) -> None:
        if self._metric_writer is None:
            return
        try:
            await asyncio.wait_for(
                self._metric_writer.write_metric(
                    MetricPoint(
                        name=name,
                        value=value,
                        observed_at=datetime.now(UTC),
                        tenant_id=request.context.tenant_id,
                        session_id=request.session_id,
                        run_id=request.run_id,
                        deduplication_key=(
                            f"{request.context.tenant_id}:"
                            f"{request.model_call_id}:{name}"
                        ),
                    )
                ),
                timeout=0.1,
            )
        except Exception:
            return

    async def _emit_prompt_cache_metrics(
        self,
        usage: dict[str, int | float],
        request: ModelGenerateRequest,
    ) -> None:
        input_tokens = max(0.0, float(usage.get("input_tokens", 0)))
        cached_tokens = max(0.0, float(usage.get("cached_input_tokens", 0)))
        write_tokens = max(0.0, float(usage.get("cache_write_input_tokens", 0)))
        metrics = {
            "model.prompt_cache.cached_input_tokens": cached_tokens,
            "model.prompt_cache.write_input_tokens": write_tokens,
            "model.prompt_cache.hit_ratio": (
                min(1.0, cached_tokens / input_tokens) if input_tokens else 0.0
            ),
        }
        await asyncio.gather(
            *(self._emit_metric(name, value, request) for name, value in metrics.items())
        )

    @staticmethod
    def _request_digest(request: ModelGenerateRequest) -> str:
        return hashlib.sha256(
            json.dumps(
                {
                    "run_id": request.run_id,
                    "session_id": request.session_id,
                    "messages": request.messages,
                    "tools": request.tools,
                    "capability": request.capability,
                    "preferred_model": request.preferred_model,
                    "allowed_providers": request.allowed_providers,
                    "data_classification": request.data_classification,
                    "max_output_tokens": request.max_output_tokens,
                },
                sort_keys=True,
                default=str,
            ).encode()
        ).hexdigest()

    async def _cached_stream(
        self, cached: ModelGenerateResponse
    ) -> AsyncIterator[ModelStreamEvent]:
        sequence = 0
        for delta in cached.deltas:
            if not delta:
                continue
            sequence += 1
            yield ModelStreamEvent(
                model_call_id=cached.model_call_id,
                sequence=sequence,
                type="delta",
                payload={"delta": delta},
            )
        sequence += 1
        yield ModelStreamEvent(
            model_call_id=cached.model_call_id,
            sequence=sequence,
            type="completed",
            payload=cached.model_dump(mode="json"),
        )

    @staticmethod
    def _to_generate_response(response: ModelResponse) -> ModelGenerateResponse:
        return ModelGenerateResponse(
            model_call_id=response.model_call_id,
            provider=response.provider,
            model=response.model,
            completed_output=response.completed_output,
            deltas=response.deltas,
            tool_calls=tuple(
                {
                    "tool_invocation_id": call.tool_invocation_id,
                    "name": call.name,
                    "arguments": call.arguments,
                    "version": call.version,
                    "expected_side_effect": call.expected_side_effect,
                    "approval_id": call.approval_id,
                    "credential_ref": call.credential_ref,
                    "idempotency_key": call.idempotency_key,
                }
                for call in response.tool_calls
            ),
            finish_reason=response.finish_reason,
            usage=response.usage,
        )
