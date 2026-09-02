from __future__ import annotations

import ast
from pathlib import Path

from auraclaw.composition.services import SERVICE_BUILDERS

ROOT = Path(__file__).resolve().parents[2]
BUILDERS = ROOT / "src" / "auraclaw" / "composition" / "builders"


def test_every_production_service_has_an_independent_builder_module() -> None:
    expected_modules = {
        "artifact",
        "credential_proxy",
        "delivery",
        "hands",
        "model_gateway",
        "orchestrator",
        "policy",
        "projection",
        "runtime",
        "session",
        "streaming",
        "task_api",
    }

    assert {path.stem for path in BUILDERS.glob("*.py") if path.stem != "__init__"} == (
        expected_modules
    )
    assert set(SERVICE_BUILDERS) == {
        "task-api",
        "streaming-gateway",
        "session",
        "action-hands",
        "model-gateway",
        "policy",
        "credential-proxy",
        "artifact-service",
        "delivery-worker",
        "orchestrator",
        "agent-runtime",
        "projection-worker",
        "default",
    }


def test_service_builders_do_not_depend_on_each_other() -> None:
    for path in BUILDERS.glob("*.py"):
        tree = ast.parse(path.read_text())
        imported_modules = {
            node.module
            for node in ast.walk(tree)
            if isinstance(node, ast.ImportFrom) and node.module is not None
        }
        assert not any(
            module.startswith("auraclaw.composition.builders.")
            for module in imported_modules
        ), path.name


def test_services_module_is_only_the_dispatch_and_lifecycle_facade() -> None:
    services_path = ROOT / "src" / "auraclaw" / "composition" / "services.py"
    tree = ast.parse(services_path.read_text())
    function_names = {
        node.name for node in tree.body if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    }

    assert not function_names.intersection(
        {
            "_orchestrator_app",
            "_hands_app",
            "_policy_app",
            "_credential_proxy_app",
            "_artifact_app",
            "_delivery_app",
            "_projection_app",
            "_model_gateway_app",
            "_runtime_app",
        }
    )
