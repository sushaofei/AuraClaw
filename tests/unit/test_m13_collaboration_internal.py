import asyncio
from datetime import UTC, datetime, timedelta

import pytest

from auraclaw.contracts.commands import CommandContext
from auraclaw.contracts.errors import AuthorizationError
from auraclaw.contracts.events import Actor
from auraclaw.contracts.internal import (
    CollaborationCommandRequest,
    InternalRequestContext,
    LeaseAssertion,
    ServiceIdentity,
)
from auraclaw.gateways.task.admission import AllowAllAdmissionController
from auraclaw.infrastructure.clients.session import NoOpOutboxRelay
from auraclaw.infrastructure.persistence.memory_event_store import InMemoryEventStore
from auraclaw.internal.security import (
    InMemoryFencingTokenLedger,
    LeaseAssertionSigner,
    LeaseAssertionVerifier,
)
from auraclaw.projection.relay import OutboxRelay
from auraclaw.projection.task.projector import InMemoryTaskProjection
from auraclaw.session.collaboration_internal_service import (
    CollaborationInternalService,
)
from auraclaw.session.collaboration_service import CollaborationService
from auraclaw.session.task_service import TaskService


def test_internal_collaboration_commands_derive_actor_and_commit_child_terminal() -> None:
    async def scenario() -> None:
        key = b"m13-collaboration-lease-key-000000000000"
        signer = LeaseAssertionSigner(key_id="m13", signing_key=key)
        verifier = LeaseAssertionVerifier(
            {"m13": key},
            ledger=InMemoryFencingTokenLedger(),
            audience=("runtime", "session"),
        )
        events = InMemoryEventStore()
        projection = InMemoryTaskProjection()
        tasks = TaskService(
            event_store=events,
            relay=OutboxRelay(events, projection),
            reader=projection,
            admission=AllowAllAdmissionController(),
        )
        root = await tasks.create_task(
            goal="coordinate",
            context=CommandContext(
                command_id="root",
                tenant_id="tenant-m13",
                actor=Actor(type="user", id="user-m13"),
                correlation_id="corr-m13",
                expected_version=0,
                operation="create_task",
            ),
        )
        root_id = str(root["session_id"])
        root_run_id = str(root["run_id"])
        service = CollaborationInternalService(
            CollaborationService(
                event_store=events,
                relay=NoOpOutboxRelay(),
            ),
            events,
            lease_verifier=verifier,
        )

        def assertion(*, session_id: str, run_id: str, role: str) -> LeaseAssertion:
            return signer.sign(
                LeaseAssertion(
                    key_id="pending",
                    audience="runtime",
                    tenant_id="tenant-m13",
                    root_session_id=root_id,
                    session_id=session_id,
                    run_id=run_id,
                    runtime_id="runtime-m13",
                    role=role,
                    lease_id=f"lease-{session_id}",
                    fencing_token=1,
                    expires_at=datetime.now(UTC) + timedelta(minutes=5),
                    signature="",
                )
            )

        root_assertion = assertion(
            session_id=root_id, run_id=root_run_id, role="root"
        )
        request = CollaborationCommandRequest(
            context=InternalRequestContext(
                tenant_id="tenant-m13",
                service_identity=ServiceIdentity.AGENT_RUNTIME,
                request_id="create-child",
                correlation_id="corr-m13",
                causation_id="tool-call-create",
            ),
            lease_assertion=root_assertion,
            root_session_id=root_id,
            session_id=root_id,
            command_id="tool-call-create",
            operation="create_child",
            arguments={
                "spec": {
                    "task_key": "worker-one",
                    "role": "worker",
                    "goal": "produce one result",
                    "output_contract": {
                        "version": "1",
                        "required_fields": ["summary", "result_ref"],
                    },
                    "runtime_budget": {
                        "max_steps": 12,
                        "max_output_tokens": 2048,
                    },
                }
            },
        )
        created = await service.command(request)
        retried = await service.command(request)
        assert retried.result == created.result
        child_id = str(created.result["session_id"])
        child_events = await events.load("tenant-m13", child_id)
        assert [event.type for event in child_events] == [
            "child.created",
            "run.requested",
        ]
        assert child_events[0].actor == Actor(
            type="coordinator", id="runtime-m13"
        )
        assert child_events[0].payload["runtime_budget"]["max_steps"] == 12

        child_run_id = str(child_events[1].payload["run_id"])
        published = await service.command(
            CollaborationCommandRequest(
                context=request.context.model_copy(
                    update={"request_id": "publish", "causation_id": "tool-call-publish"}
                ),
                lease_assertion=assertion(
                    session_id=child_id,
                    run_id=child_run_id,
                    role="worker",
                ),
                root_session_id=root_id,
                session_id=child_id,
                command_id="tool-call-publish",
                operation="publish_result",
                arguments={
                    "summary": "done",
                    "result_ref": "result://worker-one",
                },
            )
        )
        assert published.result["status"] == "completed"
        child_events = await events.load("tenant-m13", child_id)
        assert [event.type for event in child_events[-2:]] == [
            "child.result_published",
            "run.completed",
        ]
        assert child_events[-1].run_id == child_run_id
        assert child_events[-1].actor == Actor(type="worker", id="runtime-m13")

        forged = root_assertion.model_copy(update={"role": "worker"})
        with pytest.raises(AuthorizationError, match="signature"):
            await service.command(request.model_copy(update={"lease_assertion": forged}))

    asyncio.run(scenario())
