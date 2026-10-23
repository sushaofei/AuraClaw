import importlib.util
import json
import sys
from pathlib import Path

import pytest


@pytest.fixture
def batch(monkeypatch):
    root = Path(__file__).parents[2] / "benchmarks"
    monkeypatch.syspath_prepend(str(root))
    spec = importlib.util.spec_from_file_location(
        "component_batch", root / "run_component_batch.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.mark.asyncio
async def test_failed_worker_retains_denominator_and_is_not_replayed(batch, tmp_path):
    target = tmp_path / "attempt_0.json"
    job = {
        "output": str(target),
        "command": [sys.executable, "-c", "raise SystemExit(7)"],
        "process_timeout": 10,
        "record": {"task_id": "one", "trial": 0, "config": {}, "condition": "with-skill"},
    }
    await batch.run_job(job)
    record = json.loads(target.read_text())
    assert record["status"] == "infra_error"
    assert record["official_reward"] is None
    assert record["dataset_revision_verified"] is False
    before = target.read_bytes()
    await batch.run_job(job)
    assert target.read_bytes() == before


@pytest.mark.asyncio
async def test_uncertain_dispatch_requires_review_not_silent_retry(batch, tmp_path):
    target = tmp_path / "attempt_0.json"
    target.with_suffix(".started.json").write_text("{}")
    with pytest.raises(RuntimeError, match="Unresolved dispatched"):
        await batch.run_job({"output": str(target)})
