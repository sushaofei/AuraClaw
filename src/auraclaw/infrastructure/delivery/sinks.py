from __future__ import annotations

import hashlib
import hmac
import json
from datetime import UTC, datetime

import httpx

from auraclaw.contracts.commands import CommandContext
from auraclaw.contracts.delivery import (
    DeliveryJob,
    ResultSinkConfig,
    SinkResponse,
)
from auraclaw.contracts.errors import VersionConflictError
from auraclaw.contracts.events import Actor, NewEvent
from auraclaw.contracts.state import Visibility
from auraclaw.delivery.ports import DeliverySecretResolver
from auraclaw.session.ports import EventStore, OutboxRelayPort


class StaticDeliverySecretResolver:
    """Test adapter. Production implementations resolve refs through Credential Proxy."""

    def __init__(self, secrets: dict[tuple[str, str], str]) -> None:
        self._secrets = secrets

    async def resolve(self, tenant_id: str, credential_ref: str) -> str:
        return self._secrets[(tenant_id, credential_ref)]


class WebhookResultSink:
    sink_type = "webhook"

    def __init__(
        self,
        secrets: DeliverySecretResolver,
        *,
        client: httpx.AsyncClient | None = None,
        timeout: float = 10.0,
    ) -> None:
        self._secrets = secrets
        self._client = client
        self._timeout = timeout

    async def deliver(self, job: DeliveryJob, config: ResultSinkConfig) -> SinkResponse:
        if config.credential_ref is None:
            return SinkResponse(False, False, "webhook credential_ref is required")
        secret = await self._secrets.resolve(job.tenant_id, config.credential_ref)
        body = json.dumps(job.payload, separators=(",", ":"), sort_keys=True).encode()
        timestamp = str(int(datetime.now(UTC).timestamp()))
        signature = hmac.new(
            secret.encode(), timestamp.encode() + b"." + body, hashlib.sha256
        ).hexdigest()
        headers = {
            "Content-Type": "application/json",
            "Idempotency-Key": job.delivery_id,
            "X-AuraClaw-Timestamp": timestamp,
            "X-AuraClaw-Signature": f"sha256={signature}",
        }
        try:
            if self._client is not None:
                response = await self._client.post(
                    config.target_ref, content=body, headers=headers, timeout=self._timeout
                )
            else:
                async with httpx.AsyncClient() as client:
                    response = await client.post(
                        config.target_ref, content=body, headers=headers, timeout=self._timeout
                    )
        except (httpx.TimeoutException, httpx.NetworkError) as exc:
            return SinkResponse(False, True, type(exc).__name__)
        if 200 <= response.status_code < 300:
            return SinkResponse(True, summary=f"HTTP {response.status_code}")
        retryable = response.status_code == 429 or response.status_code >= 500
        return SinkResponse(False, retryable, f"HTTP {response.status_code}")


class ParentSessionResultSink:
    sink_type = "parent_session"

    def __init__(self, event_store: EventStore, relay: OutboxRelayPort) -> None:
        self._event_store = event_store
        self._relay = relay

    async def deliver(self, job: DeliveryJob, config: ResultSinkConfig) -> SinkResponse:
        for _ in range(5):
            parent_events = await self._event_store.load(job.tenant_id, config.target_ref)
            if not parent_events:
                return SinkResponse(False, False, "parent Session not found")
            first = parent_events[0]
            try:
                result = await self._event_store.append(
                    root_session_id=first.root_session_id,
                    session_id=config.target_ref,
                    run_id=first.run_id,
                    context=CommandContext(
                        command_id=f"parent-delivery:{job.delivery_id}",
                        tenant_id=job.tenant_id,
                        actor=Actor(type="delivery", id="parent-session-sink"),
                        correlation_id=job.delivery_id,
                        expected_version=len(parent_events),
                        operation="delivery.parent_session",
                    ),
                    events=[
                        NewEvent(
                            type="parent.result.received",
                            visibility=Visibility.INTERNAL,
                            payload={
                                "delivery_id": job.delivery_id,
                                "source_session_id": job.session_id,
                                "result_summary": job.payload.get("result_summary"),
                                "result_ref": job.payload.get("result_ref"),
                                "artifact_refs": job.payload.get("artifact_refs", []),
                            },
                        )
                    ],
                    command_result={"delivery_id": job.delivery_id},
                )
            except VersionConflictError:
                continue
            if not result.deduplicated:
                await self._relay.relay_once()
            return SinkResponse(True, summary="parent Session acknowledged")
        return SinkResponse(False, True, "parent Session write conflict")
