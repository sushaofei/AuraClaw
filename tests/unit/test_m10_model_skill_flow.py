from __future__ import annotations

import asyncio
import json
import time
from pathlib import Path
from unittest.mock import patch

from fastapi.testclient import TestClient

from auraclaw.action.hands import HandsGateway
from auraclaw.action.mcp_primitives import McpResourceRegistry
from auraclaw.action.model_skill_compiler import (
    ModelSkillCompiler,
    ModelSkillPublisher,
)
from auraclaw.action.skill_packages import (
    HmacSkillSignatureVerifier,
    SkillPackageRegistry,
    skill_package_digest,
)
from auraclaw.action.tool_gateway import ToolRegistry
from auraclaw.composition.services import create_service_app
from auraclaw.config import Settings
from auraclaw.contracts.model_skills import ModelSkillSnapshot
from auraclaw.control.ports import RuntimeAssignment
from auraclaw.infrastructure.artifacts.store import (
    ArtifactStore,
    InMemoryObjectStorage,
)
from auraclaw.internal.hands import InProcessHandsClient
from auraclaw.runtime.hands_adapter import HandsRuntimeAdapter

PRICE_INSIGHT_MODEL_CONFIG = (
    Path(__file__).parents[2]
    / "config"
    / "model-skills"
    / "procurement-price-insight.json"
)


class _SnapshotSource:
    def __init__(self, *snapshots: ModelSkillSnapshot) -> None:
        self.snapshots = snapshots

    async def load_snapshots(self) -> tuple[ModelSkillSnapshot, ...]:
        return self.snapshots


class _LocalArtifactWriter(ArtifactStore):
    def __init__(self, *args: object, **kwargs: object) -> None:
        del args, kwargs
        super().__init__(
            InMemoryObjectStorage(), signing_key=b"auraclaw-s3-artifact-key"
        )

    async def aclose(self) -> None:
        return None


class _ConcurrentSource(_SnapshotSource):
    def __init__(self, *snapshots: ModelSkillSnapshot) -> None:
        super().__init__(*snapshots)
        self.active = 0
        self.max_active = 0

    async def load_snapshots(self) -> tuple[ModelSkillSnapshot, ...]:
        self.active += 1
        self.max_active = max(self.max_active, self.active)
        await asyncio.sleep(0)
        self.active -= 1
        return self.snapshots


class _UnusedGateway:
    async def execute(self, invocation: object) -> object:
        raise AssertionError(f"unexpected Tool call: {invocation}")

    async def cancel(self, tool_invocation_id: str) -> bool:
        del tool_invocation_id
        return False


def _snapshot() -> ModelSkillSnapshot:
    return ModelSkillSnapshot(
        tenant_id="1",
        model={
            "id": 2,
            "model_code": "supplier_score",
            "model_name": "供应商综合评分模型",
            "model_type": "SCORE",
            "target_type": "SUPPLIER",
            "business_domain": "SUPPLY_SCORE",
            "status": "DRAFT",
            "description": "综合五类指标形成供应商评分。",
        },
        version={
            "id": 2,
            "version_no": "1.0.0",
            "status": "DRAFT",
        },
        sections={
            "input_features": [
                {
                    "id": 1,
                    "feature_code": "quality_score",
                    "feature_name": "质量表现",
                    "feature_data_type": "NUMBER",
                    "required_flag": False,
                }
            ],
            "output_schemas": [],
            "weights": [
                {
                    "id": 1,
                    "feature_code": "quality_score",
                    "weight_value": "1.000000",
                }
            ],
        },
        source_revision="mysql:2:2:0123456789abcdef",
        source_digest=f"sha256:{'a' * 64}",
    )


def _price_insight_snapshot() -> ModelSkillSnapshot:
    spec = json.loads(PRICE_INSIGHT_MODEL_CONFIG.read_text())
    return ModelSkillSnapshot(
        tenant_id="1",
        model={
            "id": 3,
            **spec["model"],
            "status": "ENABLED",
        },
        version={
            "id": 4,
            **spec["version"],
            "status": "PUBLISHED",
            "config_snapshot_json": spec,
        },
        sections={
            "input_sources": spec["input_sources"],
            "input_features": spec["input_features"],
            "output_schemas": spec["output_schemas"],
            "weights": spec["weights"],
            "tags": spec["tags"],
            "switches": [spec["switch"]],
        },
        source_revision="mysql:3:4:fedcba9876543210",
        source_digest=f"sha256:{'b' * 64}",
    )


def _assignment() -> RuntimeAssignment:
    return RuntimeAssignment(
        tenant_id="development",
        root_session_id="session-root",
        session_id="session-child",
        run_id="run-1",
        runtime_id="runtime-a",
        lease_id="lease-1",
        fencing_token=1,
        role="worker",
        resource_profile={},
    )


def _publisher_fixture(
    source: _SnapshotSource,
) -> tuple[ModelSkillPublisher, McpResourceRegistry]:
    signer = HmacSkillSignatureVerifier(
        {"ct-model": b"model-skill-test-signing-key"}
    )
    resources = McpResourceRegistry()
    registry = SkillPackageRegistry(
        artifacts=ArtifactStore(
            InMemoryObjectStorage(),
            signing_key=b"model-skill-test-artifact-key",
        ),
        signature_verifier=signer,
        resources=resources,
    )
    return (
        ModelSkillPublisher(
            source,
            ModelSkillCompiler(signer),
            registry,
            target_tenant_id="development",
        ),
        resources,
    )


def test_model_config_flows_through_skill_mcp_to_runtime_client() -> None:
    async def scenario() -> None:
        signer = HmacSkillSignatureVerifier(
            {"ct-model": b"model-skill-test-signing-key"}
        )
        resources = McpResourceRegistry()
        registry = SkillPackageRegistry(
            artifacts=ArtifactStore(
                InMemoryObjectStorage(),
                signing_key=b"model-skill-test-artifact-key",
            ),
            signature_verifier=signer,
            resources=resources,
        )
        compiler = ModelSkillCompiler(signer)
        package = compiler.compile(_snapshot())
        assert package.manifest.name == "model.supplier-score"
        assert package.manifest.version == "1.0.0-draft.2"
        assert package.manifest.required_tools == ()
        assert skill_package_digest(package) == skill_package_digest(
            compiler.compile(_snapshot())
        )

        publisher = ModelSkillPublisher(
            _SnapshotSource(_snapshot()),
            compiler,
            registry,
            target_tenant_id="development",
        )
        publications = await publisher.reconcile()
        assert len(publications) == 1

        client = HandsRuntimeAdapter(
            InProcessHandsClient(
                HandsGateway(
                    registry=ToolRegistry(),
                    gateway=_UnusedGateway(),  # type: ignore[arg-type]
                    resources=resources,
                )
            )
        )
        manifest = await client.load_skill_manifest(
            _assignment(),
            publisher="ct-model",
            name="model.supplier-score",
            version="1.0.0-draft.2",
        )
        assert manifest["input_schema"]["properties"]["quality_score"]["type"] == "number"

        instructions = await client.load_skill_part(
            _assignment(),
            publisher="ct-model",
            name="model.supplier-score",
            version="1.0.0-draft.2",
            path="SKILL.md",
        )
        assert "不提供权威计算或业务回写" in instructions[0]["text"]

        config = await client.load_skill_part(
            _assignment(),
            publisher="ct-model",
            name="model.supplier-score",
            version="1.0.0-draft.2",
            path="references/config.json",
        )
        parsed = json.loads(config[0]["text"])
        assert parsed["model"]["model_code"] == "supplier_score"
        assert parsed["source_digest"] == f"sha256:{'a' * 64}"

    asyncio.run(scenario())


def test_published_model_config_compiles_executable_atomic_price_skill() -> None:
    signer = HmacSkillSignatureVerifier(
        {"ct-model": b"model-skill-test-signing-key"}
    )
    package = ModelSkillCompiler(signer).compile(_price_insight_snapshot())

    assert package.manifest.name == "procurement.price-insight.generate"
    assert package.manifest.version == "4.0.0"
    assert package.manifest.required_tools == ()
    assert [skill.name for skill in package.manifest.required_skills] == [
        "procurement.price-data.validate",
        "procurement.price-metrics.analyze",
    ]
    instructions = package.files["SKILL.md"].decode()
    assert "由 `ct_model_*` 已发布配置生成" in instructions
    assert "`history_dev_pct`" in instructions
    assert "`dwd_pr_price_insight_rule_di`" in instructions
    assert "模型参数权重仅控制解读和呈现优先级" in instructions
    assert any(
        "价格管理控制塔" in applies_when
        for applies_when in package.manifest.applies_when
    )


def test_executable_model_config_rejects_table_scope_expansion() -> None:
    snapshot = _price_insight_snapshot()
    configured = dict(snapshot.version["config_snapshot_json"])
    skill = dict(configured["auraclaw_skill"])
    skill["data_tables"] = [*skill["data_tables"], "unrelated_business_table"]
    configured["auraclaw_skill"] = skill
    invalid = snapshot.model_copy(
        update={
            "version": {
                **snapshot.version,
                "config_snapshot_json": configured,
            }
        }
    )
    signer = HmacSkillSignatureVerifier(
        {"ct-model": b"model-skill-test-signing-key"}
    )
    try:
        ModelSkillCompiler(signer).compile(invalid)
    except Exception as exc:
        assert "invalid DWD table scope" in str(exc)
    else:
        raise AssertionError("out-of-scope DWD tables must be rejected")


def test_executable_model_config_rejects_weight_semantic_changes() -> None:
    snapshot = _price_insight_snapshot()
    sections = {
        name: [dict(row) for row in rows]
        for name, rows in snapshot.sections.items()
    }
    sections["weights"][0]["weight_value"] = "0.200000"
    invalid = snapshot.model_copy(update={"sections": sections})
    signer = HmacSkillSignatureVerifier(
        {"ct-model": b"model-skill-test-signing-key"}
    )

    try:
        ModelSkillCompiler(signer).compile(invalid)
    except Exception as exc:
        assert "weight semantics are invalid" in str(exc)
    else:
        raise AssertionError("weight semantic changes must fail closed")


def test_executable_model_config_rejects_unregistered_capability_tag() -> None:
    snapshot = _price_insight_snapshot()
    sections = {
        name: [dict(row) for row in rows]
        for name, rows in snapshot.sections.items()
    }
    sections["tags"][0]["tag_code"] = "free_form_prompt"
    invalid = snapshot.model_copy(update={"sections": sections})
    signer = HmacSkillSignatureVerifier(
        {"ct-model": b"model-skill-test-signing-key"}
    )

    try:
        ModelSkillCompiler(signer).compile(invalid)
    except Exception as exc:
        assert "discovery tags are incomplete" in str(exc)
    else:
        raise AssertionError("unregistered capability tags must fail closed")


def test_model_skill_reconcile_is_idempotent() -> None:
    async def scenario() -> None:
        publisher, resources = _publisher_fixture(_SnapshotSource(_snapshot()))

        first = await publisher.reconcile()
        second = await publisher.reconcile()

        assert second == first
        assert len(resources.discover_resources("development")) == 4
        assert publisher.last_errors == {}

    asyncio.run(scenario())


def test_model_skill_reconcile_publishes_new_snapshot() -> None:
    async def scenario() -> None:
        source = _SnapshotSource(_snapshot())
        publisher, resources = _publisher_fixture(source)
        await publisher.reconcile()
        second = _snapshot().model_copy(
            update={
                "model": {
                    **_snapshot().model,
                    "id": 3,
                    "model_code": "supplier_risk",
                    "model_name": "供应商风险模型",
                },
                "version": {
                    **_snapshot().version,
                    "id": 3,
                },
                "source_revision": "mysql:3:3:bbbbbbbbbbbbbbbb",
                "source_digest": f"sha256:{'b' * 64}",
            }
        )
        source.snapshots = (_snapshot(), second)

        publications = await publisher.reconcile()

        assert [item.manifest.name for item in publications] == [
            "model.supplier-risk",
            "model.supplier-score",
        ]
        assert len(resources.discover_resources("development")) == 8

    asyncio.run(scenario())


def test_model_skill_reconcile_revokes_and_reactivates_removed_snapshot() -> None:
    async def scenario() -> None:
        source = _SnapshotSource(_snapshot())
        publisher, resources = _publisher_fixture(source)
        first = await publisher.reconcile()
        assert len(first) == 1

        source.snapshots = ()
        assert await publisher.reconcile() == ()
        assert resources.discover_resources("development") == []

        source.snapshots = (_snapshot(),)
        reactivated = await publisher.reconcile()
        assert len(reactivated) == 1
        assert reactivated[0].artifact_ref == first[0].artifact_ref
        assert len(resources.discover_resources("development")) == 4

    asyncio.run(scenario())


def test_model_skill_reconcile_isolates_invalid_snapshot_and_recovers() -> None:
    async def scenario() -> None:
        valid = _snapshot()
        invalid = valid.model_copy(
            update={
                "model": {
                    **valid.model,
                    "id": 4,
                    "model_code": "",
                    "model_name": "无效模型",
                },
                "version": {**valid.version, "id": 4},
                "source_revision": "mysql:4:4:cccccccccccccccc",
                "source_digest": f"sha256:{'c' * 64}",
            }
        )
        source = _SnapshotSource(valid, invalid)
        publisher, resources = _publisher_fixture(source)

        publications = await publisher.reconcile()
        assert len(publications) == 1
        assert publisher.last_errors == {
            invalid.source_revision: "SchemaValidationError"
        }

        repaired = invalid.model_copy(
            update={
                "model": {
                    **invalid.model,
                    "model_code": "repaired_model",
                },
                "source_revision": "mysql:4:4:dddddddddddddddd",
                "source_digest": f"sha256:{'d' * 64}",
            }
        )
        source.snapshots = (valid, repaired)
        recovered = await publisher.reconcile()

        assert len(recovered) == 2
        assert publisher.last_errors == {}
        assert len(resources.discover_resources("development")) == 8

    asyncio.run(scenario())


def test_model_skill_reconcile_serializes_overlapping_scans() -> None:
    async def scenario() -> None:
        source = _ConcurrentSource(_snapshot())
        publisher, _resources = _publisher_fixture(source)

        await asyncio.gather(
            publisher.reconcile(),
            publisher.reconcile(),
        )

        assert source.max_active == 1

    asyncio.run(scenario())


def test_action_hands_schedules_periodic_model_skill_reconciliation() -> None:
    settings = Settings(
        _env_file=None,
        MYSQL_DB_HOST="model-db",
        MYSQL_DB_USER="model-reader",
        MYSQL_DB_PWD="not-a-real-secret",
        MYSQL_DB_NAME="models",
        model_skill_reconcile_interval_seconds=17,
    )

    app = create_service_app("hands", settings)

    assert settings.model_skill_source_configured
    assert app.state.tick is not None
    assert app.state.worker_interval == 17


def test_action_hands_lifespan_reconciles_and_revokes_model_skills() -> None:
    source = _SnapshotSource(_snapshot())
    settings = Settings(
        _env_file=None,
        MYSQL_DB_HOST="model-db",
        MYSQL_DB_USER="model-reader",
        MYSQL_DB_PWD="not-a-real-secret",
        MYSQL_DB_NAME="models",
        model_skill_reconcile_interval_seconds=5,
    )

    with (
        patch(
            "auraclaw.composition.services.MySqlModelSkillSource",
            return_value=source,
        ),
        patch(
            "auraclaw.composition.services.RemoteArtifactWriter",
            return_value=_LocalArtifactWriter(),
        ),
    ):
        app = create_service_app("hands", settings)

    with TestClient(app):
        publisher = app.state.model_skill_publisher
        assert publisher is not None
        assert len(publisher.publications) == 1

        source.snapshots = ()
        deadline = time.monotonic() + 7
        while time.monotonic() < deadline:
            if not publisher.publications:
                break
            time.sleep(0.05)

        assert publisher.publications == ()

    assert app.state.stopping
