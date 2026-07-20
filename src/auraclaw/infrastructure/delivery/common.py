from __future__ import annotations

import hashlib

from auraclaw.contracts.delivery import DeliveryJob, DeliveryStatus, ResultSinkConfig
from auraclaw.contracts.events import CanonicalEvent, utc_now


def delivery_id_for(event_id: str, sink_id: str) -> str:
    digest = hashlib.sha256(f"{event_id}:{sink_id}".encode()).hexdigest()
    return f"del_{digest[:32]}"


def build_delivery_job(event: CanonicalEvent, sink: ResultSinkConfig) -> DeliveryJob:
    return DeliveryJob(
        delivery_id=delivery_id_for(event.event_id, sink.sink_id),
        event_id=event.event_id,
        tenant_id=event.tenant_id,
        root_session_id=event.root_session_id,
        session_id=event.session_id,
        run_id=event.run_id,
        sink_id=sink.sink_id,
        sink_type=sink.sink_type,
        sink_target_ref=sink.target_ref,
        payload={
            "delivery_id": delivery_id_for(event.event_id, sink.sink_id),
            "event_id": event.event_id,
            "tenant_id": event.tenant_id,
            "root_session_id": event.root_session_id,
            "session_id": event.session_id,
            "run_id": event.run_id,
            "event_type": event.type,
            "occurred_at": event.occurred_at.isoformat(),
            "result_summary": event.payload.get("result_summary"),
            "result_ref": event.payload.get("result_ref"),
            "artifact_refs": event.payload.get("artifact_refs", []),
            "error": event.payload.get("error"),
        },
        status=DeliveryStatus.PENDING,
        attempt_count=0,
        next_attempt_at=utc_now(),
        last_response_summary=None,
        created_at=utc_now(),
    )
