from __future__ import annotations

import importlib.util
import json
import os
import subprocess
import sys
from dataclasses import asdict, replace
from pathlib import Path
from types import SimpleNamespace

import pytest

from auraclaw.composition.evaluation import run_benchmark_episode
from auraclaw.evaluation.contracts import EvaluationConfig
from auraclaw.evaluation.environments import AppWorldEnvironment, TauEnvironment
from auraclaw.evaluation.fixtures import FixtureModel, FixtureTau, FixtureWorld, fixture_environment
from auraclaw.evaluation.report import summarize
from auraclaw.evaluation.runtime import EnvironmentTools
from auraclaw.runtime.ports import ModelResponse, ToolCall

ROOT = Path(__file__).resolve().parents[2]


def config(benchmark="tau-bench", **kwargs):
    return replace(
        EvaluationConfig(
            benchmark=benchmark,
            split="dev",
            model="scripted",
            harness_revision="a" * 40,
            dataset_revision="fixture",
            domain="retail" if benchmark == "tau-bench" else None,
            user_model="scripted",
            user_provider="fixture",
            max_actions=5,
        ),
        **kwargs,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("benchmark", ["tau-bench", "appworld"])
async def test_real_runtime_runs_adapter_without_upstream_dependencies(benchmark):
    class Capture(FixtureModel):
        seen = []

        async def generate(self, request):
            self.seen.append(request)
            return await super().generate(request)

    model = Capture()
    record, trace = await run_benchmark_episode(
        fixture_environment(benchmark),
        model,
        config(benchmark),
        task_id="0",
        trial=0,
    )
    assert record["success"] and record["environment_done"]
    assert record["cost"] is None
    assert record["tool_calls"] == (2 if benchmark == "tau-bench" else 1)
    assert record["model_calls"] == (2 if benchmark == "tau-bench" else 1)
    assert any(event["type"] == "run.completed" for event in trace)
    assert any(event["type"] == "tool.call.completed" for event in trace)
    assert "never expose" not in json.dumps(trace)
    for request in model.seen:
        assert all(not tool["function"]["name"].startswith("auraclaw.") for tool in request.tools)
    if benchmark == "tau-bench":
        assert model.seen[-1].messages[-1]["content"] == "found"


@pytest.mark.asyncio
async def test_tau_hidden_info_is_never_read_and_user_dialog_remains_multi_turn():
    class Hidden:
        def __str__(self):
            raise AssertionError("Hidden task info leaked")

    class Env(FixtureTau):
        spoken = 0

        def reset(self, **kwargs):
            return SimpleNamespace(observation="public goal", info=Hidden())

        def step(self, action):
            self.spoken += action.name == "respond"
            return SimpleNamespace(
                observation="public user reply",
                info=Hidden(),
                done=self.spoken == 2,
                reward=float(self.spoken == 2),
            )

    class Model:
        calls = 0

        async def generate(self, request):
            self.calls += 1
            if self.calls == 2:
                assert request.messages[-1] == {"role": "user", "content": "public user reply"}
                assert request.messages[-2] == {"role": "assistant", "content": "hello"}
            return ModelResponse(
                request.model_call_id, "fixture", "scripted", "hello", usage={"output_tokens": 1}
            )

    env = TauEnvironment(Env(), 0, lambda **kw: SimpleNamespace(**kw))
    record, _ = await run_benchmark_episode(env, Model(), config(), task_id="0", trial=0)
    assert record["success"] and record["model_calls"] == 2


@pytest.mark.asyncio
async def test_action_budget_failure_is_not_official_success():
    class NeverDone(FixtureWorld):
        def execute(self, code):
            return "not yet"

        def evaluate(self, **kwargs):
            return SimpleNamespace(success=False, num_tests=1)

    world = NeverDone()
    record, trace = await run_benchmark_episode(
        AppWorldEnvironment(world),
        FixtureModel(),
        config("appworld", max_actions=1),
        task_id="task",
        trial=0,
    )
    assert world.closed
    assert record["status"] == "budget_exhausted" and not record["success"]
    assert record["official_reward"] == 0
    assert any(event["type"] == "run.failed" for event in trace)


@pytest.mark.asyncio
async def test_evaluator_failure_is_separate_from_task_failure_and_closes_world():
    class BrokenGrader(FixtureWorld):
        def evaluate(self, **kwargs):
            raise ConnectionError("unavailable")

    world = BrokenGrader()
    record, _ = await run_benchmark_episode(
        AppWorldEnvironment(world),
        FixtureModel(),
        config("appworld"),
        task_id="task",
        trial=0,
    )
    assert record["status"] == "evaluation_error" and record["official_reward"] is None
    assert world.closed


@pytest.mark.asyncio
async def test_missing_output_usage_stops_before_environment_dispatch():
    class MissingUsage:
        async def generate(self, request):
            return ModelResponse(
                request.model_call_id,
                "fixture",
                "scripted",
                "",
                tool_calls=(
                    ToolCall(
                        "call", "appworld_execute", {"code": "apis.supervisor.complete_task()"}
                    ),
                ),
            )

    world = FixtureWorld()
    record, _ = await run_benchmark_episode(
        AppWorldEnvironment(world),
        MissingUsage(),
        config("appworld"),
        task_id="task",
        trial=0,
    )
    assert record["status"] == "runtime_error" and record["official_reward"] is None
    assert not world.completed and world.closed


@pytest.mark.asyncio
async def test_invocation_identity_is_idempotent_but_rebinding_and_unknown_tools_fail():
    world = FixtureWorld()
    tools = EnvironmentTools(AppWorldEnvironment(world), config("appworld"))
    call = ToolCall("call", "appworld_execute", {"code": "apis.supervisor.complete_task()"})
    first = await tools.execute(None, call)
    assert await tools.execute(None, call) == first and tools.calls == 1
    with pytest.raises(ValueError, match="identity"):
        await tools.execute(None, replace(call, arguments={"code": "other"}))
    denied = await tools.execute(None, ToolCall("unknown", "host_shell", {}))
    assert denied["side_effect_status"] == "not_started"


@pytest.mark.parametrize(
    "kwargs",
    [
        {"max_actions": 0},
        {"max_actions": True},
        {"seed": 1.5},
        {"timeout_seconds": float("inf")},
        {"split": "invalid"},
    ],
)
def test_invalid_evaluation_config_is_rejected(kwargs):
    with pytest.raises(ValueError):
        config(**kwargs)


def rows():
    return [
        {
            "config": asdict(config()),
            "task_id": "0",
            "trial": trial,
            "success": trial < 2,
            "status": "completed",
            "official_reward": float(trial < 2),
            "cost": None,
        }
        for trial in range(3)
    ]


def test_pass_power_k_uses_all_success_estimator_and_never_converts_unknown_cost_to_zero():
    report = summarize(rows(), trials=3)
    assert report["pass_power_k"] == {"1": 2 / 3, "2": 1 / 3, "3": 0.0}
    assert report["cost"] is None
    assert summarize(rows()[:2], trials=3)["pass_power_k"] == {}


def test_duplicate_attempts_and_mixed_experiments_are_not_silently_averaged():
    with pytest.raises(ValueError, match="Duplicate"):
        summarize([rows()[0], rows()[0]], trials=3)
    mixed = rows()
    mixed[0]["config"] = asdict(config(max_actions=2))
    with pytest.raises(ValueError, match="different"):
        summarize(mixed, trials=3)


@pytest.mark.parametrize("benchmark", ["tau-bench", "appworld"])
def test_cli_smoke_is_process_isolated_and_labels_fixture_results(tmp_path, benchmark):
    completed = subprocess.run(
        [
            sys.executable,
            str(ROOT / "benchmarks/run_agents.py"),
            "smoke",
            "--benchmark",
            benchmark,
            "--output",
            str(tmp_path),
        ],
        env={**os.environ, "PYTHONPATH": str(ROOT / "src")},
        capture_output=True,
        text=True,
        check=True,
        timeout=30,
    )
    report = json.loads(completed.stdout)
    assert report["validation_scope"] == "fixture"
    assert report["attempts"] == 2 and report["success_rate"] == 1
    directory = Path(report["output"])
    artifacts = list(directory.glob("attempt_*.json"))
    assert len(artifacts) == 4  # two summaries and two private canonical traces
    assert all(path.stat().st_mode & 0o077 == 0 for path in artifacts)
    attempts = [json.loads(path.read_text()) for path in artifacts if ".trace." not in path.name]
    assert attempts[0]["run_id"] != attempts[1]["run_id"]


@pytest.mark.asyncio
async def test_failed_later_model_call_does_not_report_partial_cost_as_total():
    class NeverDone(FixtureWorld):
        def execute(self, code):
            return "continue"

    class Model(FixtureModel):
        async def generate(self, request):
            if self.calls:
                raise ConnectionError("provider status unknown")
            response = await super().generate(request)
            return replace(response, usage={"output_tokens": 1, "cost": 0.01})

    record, _ = await run_benchmark_episode(
        AppWorldEnvironment(NeverDone()),
        Model(),
        config("appworld"),
        task_id="task",
        trial=0,
    )
    assert record["usage"]["cost"] == 0.01
    assert record["cost"] is None and record["total_cost"] is None
    assert record["status"] == "runtime_error"


@pytest.mark.asyncio
@pytest.mark.parametrize("remote_docker", [True, False])
async def test_appworld_worker_preserves_remote_paths_and_official_experiment_identity(
    tmp_path, monkeypatch, remote_docker
):
    spec = importlib.util.spec_from_file_location(
        "evaluation_cli_test", ROOT / "benchmarks/run_agents.py"
    )
    cli = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(cli)
    captured = {}

    def create_world(**kwargs):
        captured.update(kwargs)
        return FixtureWorld()

    class Gateway(FixtureModel):
        def __init__(self, *args, **kwargs):
            super().__init__()

        async def aclose(self):
            pass

    sdk = SimpleNamespace(load_task_ids=lambda split: ["sample"], AppWorld=create_world)
    monkeypatch.setattr(cli, "upstream_module", lambda *args: sdk)
    monkeypatch.setattr(cli, "RemoteModelClient", Gateway)
    monkeypatch.setattr(cli, "verify_dataset", lambda *args: None)
    monkeypatch.setattr(
        cli,
        "importlib",
        SimpleNamespace(
            import_module=lambda name: SimpleNamespace(
                path_store=SimpleNamespace(data=str(tmp_path))
            )
        ),
    )
    monkeypatch.setenv("AURACLAW_EVAL_GATEWAY_TOKEN", "fixture-token")
    manifest = tmp_path / "manifest.json"
    manifest.write_text(
        json.dumps(
            {
                "config": asdict(config("appworld")),
                "upstream_root": str(tmp_path),
                "model_gateway_url": "http://127.0.0.1:18005",
                "appworld_environment_url": "http://127.0.0.1:18000",
                "appworld_remote_docker": remote_docker,
            }
        )
    )
    result = tmp_path / "result.json"
    await cli.worker(
        SimpleNamespace(manifest=manifest, smoke=False, task_id="sample", trial=0, result=result)
    )
    record = json.loads(result.read_text())
    assert record["success"]
    assert captured["remote_docker"] is remote_docker
    assert captured["raise_on_unsafe_syntax"] and captured["raise_on_unsafe_execution"]
    assert record["environment_experiment_name"] == captured["experiment_name"]
    assert captured["experiment_name"].startswith("auraclaw_")


@pytest.mark.parametrize(
    ("profile", "owner", "sql", "kafka", "provider_key", "reason"),
    [
        ("production", "agent-runtime", False, True, None, "development profile"),
        ("development", "task-api", False, True, None, "SQL storage"),
        ("development", "agent-runtime", False, False, None, "Kafka"),
        ("development", "agent-runtime", False, True, "provider-key", "Provider credentials"),
    ],
)
def test_evaluation_deployment_rejects_wrong_profile_or_missing_boundaries(
    monkeypatch, profile, owner, sql, kafka, provider_key, reason
):
    spec = importlib.util.spec_from_file_location(
        "evaluation_service_test", ROOT / "benchmarks/deploy/run_service.py"
    )
    entry = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(entry)
    monkeypatch.setattr(sys, "argv", ["run_service.py", "runtime"])
    monkeypatch.setattr(
        entry,
        "get_settings",
        lambda: SimpleNamespace(
            deployment_profile=profile,
            sql_storage_enabled=sql,
            kafka_enabled=kafka,
            model_api_key=provider_key,
        ),
    )
    monkeypatch.setattr(entry, "service_spec", lambda *args: SimpleNamespace(name=owner))
    monkeypatch.setattr(entry, "_run_service_process", lambda *args: pytest.fail("must not start"))
    with pytest.raises(SystemExit, match=reason):
        entry.main()


def test_official_dataset_verification_binds_bytes_and_rejects_links(tmp_path):
    import hashlib

    spec = importlib.util.spec_from_file_location(
        "evaluation_provenance_test", ROOT / "benchmarks/run_agents.py"
    )
    cli = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(cli)
    (tmp_path / "input.json").write_bytes(b"official-data")
    digest = hashlib.sha256(b"input.json\0" + hashlib.sha256(b"official-data").digest())
    declared = "sha256:" + digest.hexdigest()
    cli.verify_dataset(tmp_path, declared)
    (tmp_path / "__pycache__").mkdir()
    (tmp_path / "__pycache__/generated.pyc").write_bytes(b"not-input")
    cli.verify_dataset(tmp_path, declared)
    (tmp_path / "input.json").write_bytes(b"modified")
    with pytest.raises(ValueError, match="do not match"):
        cli.verify_dataset(tmp_path, declared)
    (tmp_path / "input.json").write_bytes(b"official-data")
    (tmp_path / "linked.json").symlink_to(tmp_path / "input.json")
    with pytest.raises(ValueError, match="symlinks"):
        cli.verify_dataset(tmp_path, declared)


def test_evaluation_config_uses_one_app_login_and_keeps_migration_separate(tmp_path):
    import tarfile
    from urllib.parse import urlsplit

    from dotenv import dotenv_values

    model_env = tmp_path / "model.env"
    model_env.write_text(
        "AURACLAW_MODEL_API_KEY=fixture-only\n"
        "AURACLAW_MODEL_BASE_URL=https://fixture.invalid/v1\n"
        "AURACLAW_MODEL_NAME=fixture-model\n"
        "AURACLAW_MODEL_PROVIDER=openai_compatible\n"
        "UNRELATED_SECRET=must-not-copy\n"
    )
    out = tmp_path / "private"
    command = [
        sys.executable,
        str(ROOT / "benchmarks/deploy/prepare_master.py"),
        "--model-env",
        str(model_env),
        "--output-dir",
        str(out),
        "--image",
        "auraclaw:fixture",
        "--nginx-image",
        "nginx@sha256:" + "a" * 64,
    ]
    for _ in range(2):
        result = subprocess.run(command, capture_output=True, text=True, check=True)
        assert "fixture-only" not in result.stdout + result.stderr
        with tarfile.open(out / "deployment-config.tar") as tar:
            raw = tar.extractfile(".env.test").read().decode()
            assert "must-not-copy" not in raw
            env = dotenv_values(stream=__import__("io").StringIO(raw))
            app = urlsplit(env["AURACLAW_DATABASE_URL"])
            migration = urlsplit(env["AURACLAW_MIGRATION_DATABASE_URL"])
            assert app.username == "auraclaw_app"
            assert migration.username == "auraclaw_migration"
            assert app.password != migration.password
            assert not any(
                k.endswith("_DATABASE_URL")
                and k not in {"AURACLAW_DATABASE_URL", "AURACLAW_MIGRATION_DATABASE_URL"}
                for k in env
            )
        state = (out / "deployment-credentials.json").read_text()
        if _ == 0:
            first = state
        else:
            assert state == first
    assert (out.stat().st_mode & 0o777) == 0o700
    assert ((out / "deployment-credentials.json").stat().st_mode & 0o777) == 0o600


@pytest.mark.asyncio
async def test_model_gateway_budget_rejection_is_infrastructure_not_task_failure():
    from auraclaw.contracts.errors import BudgetExceededError

    class Rejected:
        async def generate(self, request):
            raise BudgetExceededError("tenant model token quota is exhausted")

    record, _ = await run_benchmark_episode(
        fixture_environment("tau-bench"),
        Rejected(),
        config(),
        task_id="0",
        trial=0,
    )
    assert record["status"] == "infra_error"
    assert record["error_type"] == "ModelAdmissionRejected"
    assert record["official_reward"] is None and not record["success"]
    assert record["tool_calls"] == 0 and record["model_calls"] == 1


def test_e3_stops_on_infrastructure_failure_and_keeps_full_planned_denominator(
    tmp_path, monkeypatch
):
    spec = importlib.util.spec_from_file_location(
        "evaluation_cli", ROOT / "benchmarks/run_agents.py"
    )
    cli = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(cli)
    manifest = {
        "config": asdict(config()),
        "task_ids": ["0", "1"],
        "trials": 3,
        "protocol": "e3-heldout-v1",
        "upstream_root": str(tmp_path),
        "model_gateway_url": "http://localhost:8005",
    }
    path = tmp_path / "plan.json"
    path.write_text(json.dumps(manifest))
    monkeypatch.setattr(cli, "revision", lambda root: "a" * 40)

    def failed_worker(command, **kwargs):
        target = Path(command[command.index("--result") + 1])
        target.write_text(
            json.dumps(
                {
                    "config": manifest["config"],
                    "task_id": "0",
                    "trial": 0,
                    "status": "infra_error",
                    "success": False,
                    "official_reward": None,
                    "cost": None,
                }
            )
        )
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr(cli.subprocess, "run", failed_worker)
    with pytest.raises(SystemExit) as error:
        cli.run(SimpleNamespace(command="run", manifest=path, output=tmp_path / "results"))
    assert error.value.code == 2
    report = json.loads(next((tmp_path / "results").glob("*/summary.json")).read_text())
    assert report["attempts"] == 1 and report["expected_attempts"] == 6
    assert report["error_attempts"] == 1 and not report["complete"]
    assert report["pass_power_k"] == {}
