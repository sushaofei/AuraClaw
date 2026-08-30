import asyncio
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from auraclaw.composition.services import RemoteRuntimeWorker, _runtime_instance_identity
from auraclaw.config import Settings
from auraclaw.contracts.errors import LeaseConflictError
from auraclaw.control.ports import RuntimeAssignment, RuntimeInstance, RuntimeLease
from auraclaw.infrastructure.persistence.memory_control_store import InMemoryControlStateStore

ROOT = Path(__file__).resolve().parents[2]


def _assignment(index: int) -> RuntimeAssignment:
    now = datetime.now(UTC)
    return RuntimeAssignment(
        tenant_id="tenant-57",
        root_session_id=f"session-{index}",
        session_id=f"session-{index}",
        run_id=f"run-{index}",
        runtime_id="runtime-57",
        lease_id=f"lease-{index}",
        fencing_token=1,
        role="agent",
        resource_profile={},
        execution_claim_token=f"claim-{index}",
        execution_claim_expires_at=now + timedelta(minutes=1),
    )


def test_production_runtime_identity_uses_instance_hostname(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("AURACLAW_RUNTIME_INSTANCE_UID", raising=False)
    monkeypatch.delenv("POD_UID", raising=False)
    monkeypatch.setattr("auraclaw.composition.services.socket.gethostname", lambda: "pod-a")
    settings = Settings.model_construct(
        deployment_profile="production", runtime_id=None, runtime_node_id="local"
    )
    assert _runtime_instance_identity(settings) == ("runtime-pod-a", "pod-a")


def test_production_manifests_do_not_share_a_fixed_runtime_identity() -> None:
    compose = (ROOT / "compose.prod.yml").read_text()
    runtime_block = compose.split("  agent-runtime:", 1)[1].split(
        "  model-gateway:", 1
    )[0]
    assert "replicas: 3" in runtime_block
    assert "AURACLAW_RUNTIME_ID" not in runtime_block
    kubernetes = (
        ROOT / "deploy/kubernetes/agent-runtime.patch.yaml"
    ).read_text()
    assert "AURACLAW_RUNTIME_INSTANCE_UID" in kubernetes
    assert "fieldPath: metadata.uid" in kubernetes


@pytest.mark.asyncio
async def test_duplicate_live_runtime_registration_fails_closed() -> None:
    store = InMemoryControlStateStore()
    first = RuntimeInstance(
        runtime_id="runtime-fixed",
        runtime_type="agent",
        role="agent",
        node_id="node-a",
        capabilities={},
        capacity=1,
        registration_id="incarnation-a",
    )
    await store.register_runtime(first)
    with pytest.raises(LeaseConflictError, match="already registered"):
        await store.register_runtime(
            RuntimeInstance(
                **{
                    **first.__dict__,
                    "node_id": "node-b",
                    "registration_id": "incarnation-b",
                }
            )
        )


@pytest.mark.asyncio
async def test_runtime_capacity_is_real_harness_concurrency() -> None:
    release = asyncio.Event()
    entered = asyncio.Event()
    active = 0
    maximum = 0

    class Control:
        capacity = 3

        async def register(self) -> None:
            return None

        async def heartbeat(self) -> None:
            return None

        async def claim(self, *, limit: int = 1) -> list[RuntimeAssignment]:
            assert limit == 3
            return [_assignment(index) for index in range(3)]

    class Harness:
        async def execute(self, assignment: RuntimeAssignment) -> None:
            nonlocal active, maximum
            del assignment
            active += 1
            maximum = max(maximum, active)
            if maximum == 3:
                entered.set()
            await release.wait()
            active -= 1

        async def record_failure(
            self, assignment: RuntimeAssignment, exc: Exception
        ) -> None:
            del assignment, exc

    worker = RemoteRuntimeWorker(Control(), Harness())  # type: ignore[arg-type]
    tick = asyncio.create_task(worker.tick())
    await asyncio.wait_for(entered.wait(), timeout=1)
    assert maximum == 3
    release.set()
    assert await tick == 3


@pytest.mark.asyncio
async def test_assignment_claim_renewal_extends_lease_and_excludes_old_owner() -> None:
    store = InMemoryControlStateStore()
    store.execution_claim_ttl = timedelta(milliseconds=50)
    store.assignment_lease_ttl = timedelta(seconds=1)
    runtime = RuntimeInstance(
        runtime_id="runtime-57",
        runtime_type="agent",
        role="agent",
        node_id="node-57",
        capabilities={},
        capacity=1,
        registration_id="incarnation-57",
    )
    await store.register_runtime(runtime)
    assignment = _assignment(1)
    assignment.execution_claim_token = None
    assignment.execution_claim_expires_at = None
    resource_id = "session:tenant-57:session-1"
    lease = RuntimeLease(
        resource_id=resource_id,
        lease_id=assignment.lease_id,
        owner="orchestrator",
        fencing_token=1,
        expires_at=datetime.now(UTC) + timedelta(milliseconds=100),
    )
    store._leases[resource_id] = lease
    store._assignments["tenant-57:session-1:run-1"] = (assignment, "assigned")
    claimed = await store.claim_assignments(
        runtime.runtime_id,
        runtime.role,
        registration_id=runtime.registration_id,
    )
    assert len(claimed) == 1
    active = claimed[0].assignment
    await store.renew_assignment_claim(
        claimed[0].task_id,
        runtime_id=runtime.runtime_id,
        registration_id=runtime.registration_id,
        execution_claim_token=active.execution_claim_token or "",
        lease_id=active.lease_id,
        fencing_token=active.fencing_token,
    )
    assert store._leases[resource_id].expires_at > lease.expires_at
    assert await store.claim_assignments(
        runtime.runtime_id,
        runtime.role,
        registration_id=runtime.registration_id,
    ) == []
