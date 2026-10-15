from __future__ import annotations

import ast
import subprocess
import sys
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


def test_service_builders_import_without_loading_the_dispatcher() -> None:
    # A fresh interpreter catches transitive and import-time dependencies hidden
    # by the dispatcher already being present in this test process's sys.modules.
    script = """
import importlib
import importlib.abc
import sys
from pathlib import Path

sys.path.insert(0, sys.argv[1])

class RejectDispatcher(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path, target=None):
        if fullname == "auraclaw.composition.services":
            raise AssertionError("builder imported the service dispatcher")
        return None

sys.meta_path.insert(0, RejectDispatcher())
for path in sorted(Path(sys.argv[2]).glob("*.py")):
    if path.stem != "__init__":
        importlib.import_module("auraclaw.composition.builders." + path.stem)
"""
    result = subprocess.run(
        [sys.executable, "-c", script, str(ROOT / "src"), str(BUILDERS)],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, result.stdout + result.stderr


def test_services_module_is_only_the_dispatch_facade() -> None:
    services_path = ROOT / "src" / "auraclaw" / "composition" / "services.py"
    tree = ast.parse(services_path.read_text())
    function_names = {
        node.name for node in tree.body if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    }

    assert function_names == {
        "create_service_app",
        *("_" + name + "_builder" for name in (
            "task_api", "streaming_gateway", "session", "orchestrator", "runtime",
            "model_gateway", "hands", "policy", "credential_proxy", "artifact_service",
            "delivery", "projection", "base",
        )),
    }
    assert not any(isinstance(node, ast.ClassDef) for node in tree.body)
