from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

from auraclaw.contracts.events import NewEvent
from auraclaw.runtime.ports import (
    ControlStateStore,
    RunnableItem,
    RuntimeAssignment,
    RuntimeBudget,
    RuntimeInstance,
    RuntimeLease,
    RuntimeProvisioner,
    SessionClient,
)


class ManagedOrchestrator:
    """Resource scheduler; semantic decomposition remains outside this component."""

    def __init__(
        self,
        *,
        orchestrator_id: str,
        control_store: ControlStateStore,
        session: SessionClient,
        provisioner: RuntimeProvisioner,
        lease_ttl: timedelta = timedelta(seconds=30),
    ) -> None:
        self._id = orchestrator_id
        self._control = control_store
        self._session = session
        self._provisioner = provisioner
        self._lease_ttl = lease_ttl

    async def watch(self, tasks: list[dict[str, Any]]) -> int:
        enqueued = 0
        for task in tasks:
            if task.get("status") not in {"pending", "runnable"}:
                continue
            if task.get("runnable") is False:
                continue
            tenant_id = str(task["tenant_id"])
            session_id = str(task["session_id"])
            run_id = str(task["run_id"])
            item = RunnableItem(
                task_id=self.task_id(tenant_id, session_id, run_id),
                tenant_id=tenant_id,
                root_session_id=str(task.get("root_session_id", session_id)),
                session_id=session_id,
                run_id=run_id,
                source_version=int(task["projection_version"]),
                priority=int(task.get("priority", 0)),
                queue_partition=str(task.get("queue_partition", tenant_id)),
                role=str(task.get("role", "root")),
                required_capability=dict(task.get("required_capability", {})),
                budget=RuntimeBudget(
                    max_steps=int(task.get("max_steps", 16)),
                    max_output_tokens=int(task.get("max_output_tokens", 8192)),
                ),
            )
            enqueued += int(await self._control.enqueue(item))
        return enqueued

    async def schedule_once(self) -> RuntimeAssignment | None:
        claimed = await self._control.claim(self._id, limit=1)
        if not claimed:
            return None
        item = claimed[0].item
        previous = await self._control.get_assignment(item.task_id)
        resource_id = f"session:{item.tenant_id}:{item.session_id}"
        lease = await self._control.acquire_lease(resource_id, self._id, ttl=self._lease_ttl)
        if lease is None:
            await self._control.reschedule(item.task_id)
            return None
        try:
            runtime = await self._provisioner.provision(item, lease)
            await self._control.register_runtime(runtime)
            assignment = RuntimeAssignment(
                tenant_id=item.tenant_id,
                root_session_id=item.root_session_id,
                session_id=item.session_id,
                run_id=item.run_id,
                runtime_id=runtime.runtime_id,
                lease_id=lease.lease_id,
                fencing_token=lease.fencing_token,
                role=item.role,
                resource_profile=item.required_capability,
                deadline=item.deadline,
                budget=item.budget,
            )
            if not await self._control.assign(item.task_id, assignment):
                await self._control.release_lease(lease)
                return None
            lifecycle_events = (
                [
                    NewEvent(
                        type="runtime.failed",
                        payload={
                            "run_id": item.run_id,
                            "runtime_id": previous.runtime_id,
                            "error": {
                                "code": "runtime_lease_expired",
                                "category": "orchestration",
                                "retryable": True,
                            },
                        },
                    ),
                    NewEvent(
                        type="runtime.reprovisioned",
                        payload={
                            "run_id": item.run_id,
                            "previous_runtime_id": previous.runtime_id,
                            "runtime_id": runtime.runtime_id,
                            "lease_id": lease.lease_id,
                            "fencing_token": lease.fencing_token,
                        },
                    ),
                ]
                if previous is not None
                else [
                    NewEvent(
                        type="run.scheduled",
                        payload={
                            "run_id": item.run_id,
                            "runtime_id": runtime.runtime_id,
                            "lease_id": lease.lease_id,
                            "fencing_token": lease.fencing_token,
                        },
                    )
                ]
            )
            await self._session.append(
                assignment,
                lifecycle_events,
                command_id=f"orchestrator:schedule:{item.run_id}:{lease.fencing_token}",
                operation="orchestrator.schedule",
            )
            return assignment
        except Exception:
            await self._control.release_lease(lease)
            await self._control.reschedule(item.task_id)
            raise

    async def cancel(self, assignment: RuntimeAssignment) -> None:
        await self._control.request_cancel(
            assignment.tenant_id, assignment.session_id, assignment.run_id
        )
        await self._provisioner.cancel(assignment.runtime_id)

    async def heartbeat(self, assignment: RuntimeAssignment) -> RuntimeLease:
        renewed = await self._control.renew_lease(
            RuntimeLease(
                resource_id=f"session:{assignment.tenant_id}:{assignment.session_id}",
                lease_id=assignment.lease_id,
                owner=self._id,
                fencing_token=assignment.fencing_token,
                expires_at=datetime.now(UTC),
            ),
            ttl=self._lease_ttl,
        )
        await self._control.heartbeat(assignment.runtime_id, assignment.fencing_token)
        return renewed

    async def reconcile(self) -> None:
        await self._control.recover_expired()
        await self.schedule_once()

    @staticmethod
    def task_id(tenant_id: str, session_id: str, run_id: str) -> str:
        return f"{tenant_id}:{session_id}:{run_id}"


class LocalRuntimeProvisioner:
    def __init__(self, node_id: str = "local") -> None:
        self._node_id = node_id
        self._counter = 0
        self.cancelled: set[str] = set()

    async def provision(self, item: RunnableItem, lease: RuntimeLease) -> RuntimeInstance:
        del lease
        self._counter += 1
        return RuntimeInstance(
            runtime_id=f"runtime-{self._node_id}-{self._counter}",
            runtime_type="agent",
            role=item.role,
            node_id=self._node_id,
            capabilities=dict(item.required_capability),
            capacity=1,
        )

    async def cancel(self, runtime_id: str) -> None:
        self.cancelled.add(runtime_id)
