"""One process per real LongMemEval canary. Uses Runtime, Gateway and a declared custom judge."""

from __future__ import annotations

import argparse
import ast
import asyncio
import hashlib
import json
import os
from dataclasses import asdict
from pathlib import Path
from typing import Any
from uuid import uuid4

from plan_components import checked_checkout

from auraclaw.composition.evaluation import run_benchmark_episode
from auraclaw.evaluation.components.catalog import (
    COMPONENT_BENCHMARKS,
    LONGMEMEVAL_DATA_SHA256,
)
from auraclaw.evaluation.components.longmemeval import LongMemEvalEnvironment, MemoryCase
from auraclaw.evaluation.components.reference_memory import LexicalReferenceMemory
from auraclaw.evaluation.contracts import EvaluationConfig
from auraclaw.infrastructure.clients.model import RemoteModelClient
from auraclaw.runtime.ports import ModelPolicy, ModelRequest


def prompt_builder(root: Path) -> Any:
    checked_checkout(root, COMPONENT_BENCHMARKS["longmemeval"].revision)
    source = root / "src/evaluation/evaluate_qa.py"
    # Extract the pinned pure prompt function; avoid importing the SDK's direct Provider client.
    tree = ast.parse(source.read_text())
    fn = next(
        n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "get_anscheck_prompt"
    )
    namespace: dict[str, Any] = {}
    exec(compile(ast.Module(body=[fn], type_ignores=[]), str(source), "exec"), namespace)
    return namespace["get_anscheck_prompt"]


def write_private(path: Path, value: Any) -> None:
    with open(path, "x", encoding="utf-8", opener=lambda p, flags: os.open(p, flags, 0o600)) as f:
        json.dump(value, f, ensure_ascii=False, allow_nan=False, indent=2)
        f.write("\n")


async def execute(args: argparse.Namespace) -> None:
    raw = args.dataset.read_bytes()
    if hashlib.sha256(raw).hexdigest() != LONGMEMEVAL_DATA_SHA256:
        raise ValueError("Data does not match pinned cleaned S")
    cases = json.loads(raw)
    case = MemoryCase.from_official(next(c for c in cases if c["question_id"] == args.question_id))
    build_prompt = prompt_builder(args.upstream_root)
    config = EvaluationConfig(
        "longmemeval",
        "s_cleaned",
        args.model,
        args.revision,
        "sha256:" + LONGMEMEVAL_DATA_SHA256,
        max_actions=30,
        max_output_tokens=24576,
        per_turn_output_tokens=4096,
        timeout_seconds=600,
        seed=20261022,
    )
    memory = LexicalReferenceMemory() if args.condition == "retrieval-memory" else None
    client = RemoteModelClient(
        args.gateway_url, bearer_token=os.environ["AURACLAW_EVAL_GATEWAY_TOKEN"], timeout=180
    )
    judge_usage: dict[str, Any] = {}

    async def grade(item: MemoryCase, answer: str) -> float:
        prompt = build_prompt(
            item.question_type,
            item.question,
            item.answer,
            answer,
            abstention=item.question_id.endswith("_abs"),
        )
        key = "judge_" + uuid4().hex
        response = await client.generate(
            ModelRequest(
                key,
                "benchmark",
                key,
                ({"role": "user", "content": prompt},),
                policy=ModelPolicy(preferred_model=args.judge),
                max_output_tokens=64,
            )
        )
        judge_usage.update(response.usage)
        label = response.completed_output.strip().lower().rstrip(".")
        if label not in {"yes", "no"}:
            raise ValueError("Judge did not return a binary verdict")
        return float(label == "yes")

    env = LongMemEvalEnvironment(case, memory=memory, grade=grade, condition=args.condition)
    # Byte cap is an admission guard, not truncation or a claim to count model tokens.
    if len(env.observation.encode()) > args.max_input_bytes:
        await env.close()
        await client.aclose()
        raise ValueError("Full input exceeds declared byte admission cap; no model called")
    try:
        await env.initialize()
        record, trace = await asyncio.wait_for(
            run_benchmark_episode(
                env,
                client,
                config,
                task_id=case.question_id,
                trial=0,
                profile="auraclaw-longmemeval-reference-" + args.condition + "-v1",
            ),
            timeout=600,
        )
        record.update(
            {
                "validation_scope": "reference-adapter",
                "dataset_revision_verified": True,
                "condition": args.condition,
                "judge": args.judge,
                "judge_usage": judge_usage,
                "judge_scope": "custom-judge-not-official-comparable",
                "question_type": case.question_type,
                "config": asdict(config),
            }
        )
        write_private(args.output.with_suffix(".trace.json"), trace)
        write_private(args.output, record)
        print(
            json.dumps(
                {
                    k: record[k]
                    for k in (
                        "status",
                        "success",
                        "official_reward",
                        "condition",
                        "validation_scope",
                    )
                }
            )
        )
    finally:
        await env.close()
        await client.aclose()


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    for name in ("dataset", "upstream-root", "output"):
        p.add_argument("--" + name, type=Path, required=True)
    for name in ("question-id", "model", "judge", "revision", "gateway-url"):
        p.add_argument("--" + name, required=True)
    p.add_argument(
        "--condition", choices=COMPONENT_BENCHMARKS["longmemeval"].conditions, required=True
    )
    p.add_argument("--max-input-bytes", type=int, default=800000)
    asyncio.run(execute(p.parse_args()))


if __name__ == "__main__":
    main()
