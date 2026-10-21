"""Opt-in, isolated evaluation object graph. Production builders never import this module."""

from __future__ import annotations

import hashlib
import json
import math
import time
from dataclasses import asdict
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import uuid4

from auraclaw.contracts.errors import BudgetExceededError
from auraclaw.contracts.events import NewEvent
from auraclaw.control.ports import RuntimeAssignment, RuntimeBudget
from auraclaw.evaluation.contracts import UPSTREAMS, BenchmarkEnvironment, EvaluationConfig
from auraclaw.evaluation.runtime import (
    ActionBudgetExceeded,
    BenchmarkModel,
    EnvironmentTools,
    FixedToolsController,
    ModelAdmissionRejected,
)
from auraclaw.infrastructure.persistence.memory_control_store import InMemoryControlStateStore
from auraclaw.infrastructure.persistence.memory_event_store import InMemoryEventStore
from auraclaw.runtime.clients import FencedSessionClient, InMemoryRuntimeEventBus
from auraclaw.runtime.harness import AgentHarness
from auraclaw.runtime.ports import ModelClient, ModelPolicy


async def _run_benchmark_episode(
    env: BenchmarkEnvironment,
    model: ModelClient,
    config: EvaluationConfig,
    *,
    task_id: str,
    trial: int,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    started = time.monotonic()
    key = "eval_" + uuid4().hex
    control, events = InMemoryControlStateStore(), InMemoryEventStore()
    lease = await control.acquire_lease(
        f"session:benchmark:{key}",
        key,
        ttl=timedelta(seconds=config.timeout_seconds + 60),
    )
    assert lease is not None
    tools = EnvironmentTools(env, config)
    bridge = BenchmarkModel(model, tools)
    assignment = RuntimeAssignment(
        tenant_id="benchmark",
        root_session_id=key,
        session_id=key,
        run_id=key,
        runtime_id=key,
        lease_id=lease.lease_id,
        fencing_token=lease.fencing_token,
        role="root",
        resource_profile={
            "required_capabilities": [
                {"capability_id": name, "version": "1"} for name in tools.descriptors
            ]
        },
        deadline=datetime.now(UTC) + timedelta(seconds=config.timeout_seconds),
        budget=RuntimeBudget(
            max_steps=2 * config.max_actions + 2,
            max_output_tokens=config.max_output_tokens,
            policy_version="1",
        ),
    )
    session = FencedSessionClient(events, control)
    await session.append(
        assignment,
        [
            NewEvent(
                type="session.created",
                payload={
                    "goal": env.observation,
                },
            )
        ],
        command_id=key,
        operation="benchmark.initialize",
    )
    harness = AgentHarness(
        control_store=control,
        session=session,
        model=bridge,
        tools=tools,
        runtime_events=InMemoryRuntimeEventBus(),
        model_policy=ModelPolicy(preferred_model=config.model),
        capability_controller=FixedToolsController(tools),
        per_turn_output_tokens=config.per_turn_output_tokens,
    )
    status, error_type, reward = "completed", None, None
    try:
        try:
            await harness.execute(assignment)
        except ModelAdmissionRejected as error:
            status, error_type = "infra_error", type(error).__name__
            await harness.record_failure(assignment, error)
        except (ActionBudgetExceeded, BudgetExceededError) as error:
            status, error_type = "budget_exhausted", type(error).__name__
            await harness.record_failure(assignment, error)
        except Exception as error:
            status, error_type = "runtime_error", type(error).__name__
            await harness.record_failure(assignment, RuntimeError(error_type))
            # Do not score a crashed adapter as a successful task.
        if status == "completed" and not env.done:
            status = "incomplete"
        if status in {"completed", "budget_exhausted", "incomplete"}:
            try:
                reward = env.evaluate()
                if not math.isfinite(reward) or not 0 <= reward <= 1:
                    raise ValueError("Invalid official reward")
            except Exception as error:
                status, error_type, reward = "evaluation_error", type(error).__name__, None
        facts = await session.load(assignment)
        record = {
            "schema_version": 1,
            "run_id": key,
            "task_id": task_id,
            "trial": trial,
            "config": asdict(config),
            "upstream_revision": UPSTREAMS[config.benchmark][1],
            "profile": "auraclaw-fixed-tools-single-action-v1",
            "validation_scope": "adapter-run",
            "status": status,
            "error_type": error_type,
            "official_reward": reward,
            "success": status == "completed" and reward == 1.0,
            "environment_done": env.done,
            "model_calls": bridge.calls,
            "tool_calls": tools.calls,
            "usage": bridge.usage,
            "cost": bridge.usage.get("cost") if bridge.cost_known else None,
            "cost_scope": "agent_model_only",
            "total_cost": None,
            "elapsed_seconds": time.monotonic() - started,
            "public_input_digest": hashlib.sha256(
                json.dumps(
                    [env.instructions, env.observation, env.tools],
                    sort_keys=True,
                ).encode()
            ).hexdigest(),
        }
        return record, [event.as_dict() for event in facts]
    finally:
        await control.release_lease(lease)


async def run_benchmark_episode(
    env: BenchmarkEnvironment,
    model: ModelClient,
    config: EvaluationConfig,
    *,
    task_id: str,
    trial: int,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    try:
        return await _run_benchmark_episode(env, model, config, task_id=task_id, trial=trial)
    finally:
        env.close()
