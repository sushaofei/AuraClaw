from __future__ import annotations

import json
from typing import Any

import pytest

from auraclaw.composition.evaluation import run_benchmark_episode
from auraclaw.evaluation.components.longmemeval import LongMemEvalEnvironment, MemoryCase
from auraclaw.evaluation.components.mcp_universe import McpUniverseEnvironment
from auraclaw.evaluation.components.plan import build_plan, select_tasks
from auraclaw.evaluation.components.skillsbench import SkillsBenchEnvironment
from auraclaw.evaluation.contracts import EvaluationConfig
from auraclaw.runtime.ports import ModelResponse, ToolCall


def memory_case():
    return MemoryCase.from_official(
        {
            "question_id": "q1",
            "question_type": "single-session-user",
            "question": "What city?",
            "question_date": "2026-10-22",
            "answer": "private-gold",
            "haystack_session_ids": ["s1"],
            "haystack_dates": ["2026-01-01"],
            "haystack_sessions": [
                [
                    {
                        "role": "user",
                        "content": "I live in Paris.",
                        "has_answer": True,
                        "private_annotation": "private-gold",
                    }
                ]
            ],
            "answer_session_ids": ["s1"],
        }
    )


class Memory:
    sessions: list[dict[str, Any]]
    closed = False

    async def ingest(self, sessions):
        self.sessions = sessions

    async def search(self, query):
        return json.dumps(self.sessions)

    async def close(self):
        self.closed = True


class Sandbox:
    closed = False

    async def execute(self, command):
        return "executed"

    async def close(self):
        self.closed = True


class Skills:
    reads = 0

    async def list_skills(self):
        return [{"name": "parser", "description": "Parse input"}]

    async def read(self, name, path):
        self.reads += 1
        return "public Skill instructions"


class Mcp(Sandbox):
    calls = []

    async def call(self, server, tool, arguments):
        self.calls.append((server, tool, arguments))
        return "tool result"


class ScriptModel:
    def __init__(self, calls):
        self.calls = iter(calls)
        self.messages = []

    async def generate(self, request):
        self.messages.append(request.messages)
        name, args = next(self.calls)
        return ModelResponse(
            request.model_call_id,
            "fixture",
            "scripted",
            "",
            tool_calls=(ToolCall(request.model_call_id, name, args),),
            usage={"output_tokens": 1},
        )


async def grade_memory(case, answer):
    assert case.answer == "private-gold"
    return float(answer == "Paris")


async def verify():
    return 1.0


async def grade_mcp(answer):
    return float(answer == "done")


def config(benchmark):
    return EvaluationConfig(
        benchmark,
        {"longmemeval": "s_cleaned", "skillsbench": "registry-v1.1", "mcp-universe": "mcpuniverse"}[
            benchmark
        ],
        "scripted",
        "a" * 40,
        "fixture",
        max_actions=5,
    )


@pytest.mark.asyncio
async def test_longmemeval_gold_is_never_in_memory_or_canonical_model_input():
    memory = Memory()
    env = LongMemEvalEnvironment(memory_case(), memory=memory, grade=grade_memory)
    await env.initialize()
    assert "has_answer" not in json.dumps(memory.sessions)
    assert "private_annotation" not in json.dumps(memory.sessions)
    assert "private-gold" not in json.dumps(memory.sessions)
    model = ScriptModel(
        [("memory_search", {"query": "city"}), ("submit_answer", {"answer": "Paris"})]
    )
    record, trace = await run_benchmark_episode(
        env, model, config("longmemeval"), task_id="q1", trial=0, profile="auraclaw-memory-port-v1"
    )
    assert record["success"] and record["tool_calls"] == 2 and memory.closed
    assert record["profile"] == "auraclaw-memory-port-v1"
    assert "private-gold" not in json.dumps(model.messages)
    assert "private-gold" not in json.dumps(trace)
    assert any(e["type"] == "tool.call.completed" for e in trace)


@pytest.mark.asyncio
async def test_no_memory_control_does_not_receive_history_or_search_tool():
    env = LongMemEvalEnvironment(memory_case(), memory=None, grade=grade_memory)
    await env.initialize()
    assert "Paris" not in env.observation and "Paris" not in env.instructions
    assert [t["function"]["name"] for t in env.tools] == ["submit_answer"]
    with pytest.raises(ValueError):
        await env.invoke("memory_search", {"query": "city"})


def test_full_history_reference_uses_only_sanitized_sessions():
    env = LongMemEvalEnvironment(
        memory_case(), memory=None, grade=grade_memory, condition="full-history"
    )
    assert "Paris" in env.observation and "private-gold" not in env.observation
    assert "has_answer" not in env.observation
    with pytest.raises(ValueError):
        LongMemEvalEnvironment(
            memory_case(), memory=Memory(), grade=grade_memory, condition="no-memory"
        )


@pytest.mark.asyncio
async def test_skill_pair_uses_actual_runtime_calls_and_lazy_parts():
    skills, sandbox = Skills(), Sandbox()
    env = SkillsBenchEnvironment(
        "public task", sandbox=sandbox, skills=skills, verify=verify, oracle_verified=True
    )
    assert skills.reads == 0
    model = ScriptModel(
        [
            ("skill_read", {"name": "parser", "path": "SKILL.md"}),
            ("sandbox_execute", {"command": "task"}),
            ("finish_task", {}),
        ]
    )
    record, trace = await run_benchmark_episode(
        env, model, config("skillsbench"), task_id="task", trial=0, profile="auraclaw-skill-port-v1"
    )
    assert record["success"] and record["tool_calls"] == 3 and skills.reads == 1 and sandbox.closed
    assert any(e["type"] == "run.completed" for e in trace)
    control = SkillsBenchEnvironment(
        "public task", sandbox=Sandbox(), skills=None, verify=verify, oracle_verified=True
    )
    assert all(not t["function"]["name"].startswith("skill_") for t in control.tools)
    with pytest.raises(ValueError):
        await control.invoke("skill_read", {"name": "parser", "path": "SKILL.md"})


@pytest.mark.parametrize(
    "path", ["/oracle/solve.sh", "../verifier/test.sh", "a/../../x", "a\\b", ""]
)
@pytest.mark.asyncio
async def test_skill_path_escape_rejected(path):
    skills = Skills()
    env = SkillsBenchEnvironment(
        "task", sandbox=Sandbox(), skills=skills, verify=verify, oracle_verified=True
    )
    with pytest.raises(ValueError):
        await env.invoke("skill_read", {"name": "parser", "path": path})
    assert skills.reads == 0


def test_unverified_oracle_blocks_skill_execution():
    with pytest.raises(ValueError, match="oracle"):
        SkillsBenchEnvironment(
            "task", sandbox=Sandbox(), skills=None, verify=verify, oracle_verified=False
        )


@pytest.mark.asyncio
async def test_mcp_aliases_route_to_only_admitted_server_and_tool():
    gateway = Mcp()
    tools = [
        {
            "server": s,
            "name": "read",
            "description": "Read",
            "inputSchema": {"type": "object", "properties": {}, "additionalProperties": False},
        }
        for s in ["one", "two"]
    ]
    env = McpUniverseEnvironment("public task", tools=tools, gateway=gateway, grade=grade_mcp)
    aliases = [t["function"]["name"] for t in env.tools]
    assert aliases[0] != aliases[1]
    with pytest.raises(ValueError):
        await env.invoke("unadmitted", {})
    model = ScriptModel([(aliases[1], {}), ("submit_answer", {"answer": "done"})])
    record, _ = await run_benchmark_episode(
        env, model, config("mcp-universe"), task_id="task", trial=0, profile="auraclaw-mcp-port-v1"
    )
    assert record["success"] and gateway.closed
    assert gateway.calls[-1] == ("two", "read", {})
    with pytest.raises(RuntimeError):
        await env.invoke(aliases[0], {})


@pytest.mark.asyncio
async def test_grader_failure_is_unscored_error_and_closes_environment():
    async def bad_grade(case, answer):
        raise RuntimeError("grader unavailable")

    memory = Memory()
    env = LongMemEvalEnvironment(memory_case(), memory=memory, grade=bad_grade)
    await env.initialize()
    record, _ = await run_benchmark_episode(
        env,
        ScriptModel([("submit_answer", {"answer": "Paris"})]),
        config("longmemeval"),
        task_id="q1",
        trial=0,
    )
    assert record["status"] == "evaluation_error" and record["official_reward"] is None
    assert not record["success"] and memory.closed


def test_plan_is_deterministic_stratified_and_never_claims_a_score():
    groups = {"abstention": ["b", "a"], "temporal": ["c", "d"]}
    snapshot = json.dumps(groups, sort_keys=True)
    selected = select_tasks(groups, 3)
    assert selected == select_tasks(dict(reversed(list(groups.items()))), 3)
    assert any(x in selected for x in ["b", "a"]) and any(x in selected for x in ["c", "d"])
    assert json.dumps(groups, sort_keys=True) == snapshot
    plan = build_plan("skillsbench", selected)
    assert plan["expected_attempts"] == 18 and plan["score"] is None
    assert plan["execution_state"] == "not_started"
    with pytest.raises(ValueError):
        select_tasks({"a": ["duplicate"], "b": ["duplicate"]}, 2)
    with pytest.raises(ValueError):
        build_plan("skillsbench", ["a", "a"])


@pytest.mark.asyncio
async def test_reference_memory_is_question_local_and_ranks_history_without_gold():
    from auraclaw.evaluation.components.reference_memory import LexicalReferenceMemory

    memory = LexicalReferenceMemory(top_k=1)
    sessions = [
        {"session_id": "unrelated", "turns": [{"role": "user", "content": "banana"}]},
        {"session_id": "relevant", "turns": [{"role": "user", "content": "Paris city"}]},
    ]
    await memory.ingest(sessions)
    sessions[1]["turns"][0]["content"] = "mutated"
    result = json.loads(await memory.search("city"))
    assert len(result) == 1 and result[0]["session_id"] == "relevant"
    assert result[0]["turns"][0]["content"] == "Paris city"
    with pytest.raises(RuntimeError):
        await memory.ingest(sessions)
    await memory.close()
    assert await memory.search("city") == "[]"


def test_official_repeated_session_ids_are_preserved_without_deduplication():
    raw = {
        "question_id": "q1",
        "question_type": "multi-session",
        "question": "Question?",
        "question_date": "2026-01-01",
        "answer": "hidden",
        "haystack_session_ids": ["repeat", "repeat"],
        "haystack_dates": ["2025-01-01", "2025-01-01"],
        "haystack_sessions": [[{"role": "user", "content": "history", "has_answer": True}]] * 2,
    }
    case = MemoryCase.from_official(raw)
    assert len(case.sessions) == 2 and case.sessions[0] == case.sessions[1]
    assert "has_answer" not in json.dumps(case.sessions)
    raw["haystack_dates"].pop()
    with pytest.raises(ValueError, match="coverage"):
        MemoryCase.from_official(raw)
