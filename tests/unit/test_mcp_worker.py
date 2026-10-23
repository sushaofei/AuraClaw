"""MCP grader transport errors must not become model failures."""

import importlib.util
import json
import sys
from pathlib import Path

import pytest

pytest.importorskip("mcp")
BENCHMARKS = Path(__file__).parents[2] / "benchmarks"


@pytest.fixture
def worker(monkeypatch):
    monkeypatch.syspath_prepend(str(BENCHMARKS))
    spec = importlib.util.spec_from_file_location("mcp_worker", BENCHMARKS / "run_mcp_universe.py")
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, "mcp_worker", module)
    spec.loader.exec_module(module)
    return module


@pytest.mark.asyncio
async def test_transport_failure_is_not_zero_score(worker, tmp_path):
    port = worker.BlenderTransport([], "sha256:" + "a" * 64, tmp_path / "attempt.json")

    async def broken(*args):
        raise RuntimeError("SSH disconnected")

    port.process = broken
    with pytest.raises(RuntimeError, match="SSH disconnected"):
        await port.grade(tmp_path, 2)
    assert not (tmp_path / "attempt.grader.json").exists()


@pytest.mark.asyncio
async def test_missing_artifact_records_official_file_check_failure(worker, tmp_path):
    port = worker.BlenderTransport([], "sha256:" + "a" * 64, tmp_path / "attempt.json")

    async def absent(*args):
        return b'{"exists":false,"artifact":null}'

    port.process = absent
    assert await port.grade(tmp_path, 2) == 0.0
    assert json.loads((tmp_path / "attempt.grader.json").read_text()) == {"file_exists": False}


def test_public_assets_do_not_grant_grader_network_or_inherit_host_proxy(worker, tmp_path):
    port = worker.BlenderTransport([], "sha256:" + "a" * 64, tmp_path / "attempt.json")
    flags = port.agent_isolation("eval-assets", "http://192.168.254.1:8080")
    assert "--network=eval-assets" in flags
    assert "--http-proxy=false" in flags
    assert port.isolation()[0] == "--network=none"
    with pytest.raises(ValueError):
        port.agent_isolation("eval-assets", "http://secret@192.168.254.1:8080")
    with pytest.raises(ValueError):
        port.agent_isolation("eval-assets", None)
