from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest
from fastapi import FastAPI

from auraclaw.action.skill_admin_queries import (
    SkillAdminManagementQueryService,
    SkillAdminPublisherQueryService,
)
from auraclaw.action.skill_admission_query import (
    SkillAdmissionMetricsPolicy,
    SkillAdmissionMetricsService,
)
from auraclaw.action.skill_lifecycle import SkillAdmissionMetricRecord
from auraclaw.api.routes.admin_skills import create_skill_admin_router
from auraclaw.contracts.errors import NotFoundError

NOW = datetime(2026, 10, 15, 4, 0, tzinfo=UTC)


def normalize_openapi(schema):
    operations = {
        f"{method.upper()} {path}": operation
        for path, methods in schema["paths"].items()
        for method, operation in methods.items()
    }
    # Intern repeated headers and response shapes while retaining the full wire contract.
    encoded_parameters = sorted(
        {
            json.dumps(item, sort_keys=True)
            for operation in operations.values()
            for item in operation.get("parameters", [])
        }
    )
    encoded_responses = sorted(
        {json.dumps(operation["responses"], sort_keys=True) for operation in operations.values()}
    )
    parameter_ids = {value: f"parameter_{index}" for index, value in enumerate(encoded_parameters)}
    response_ids = {value: f"response_{index}" for index, value in enumerate(encoded_responses)}
    return {
        "schemas": schema["components"]["schemas"],
        "parameters": {parameter_ids[value]: json.loads(value) for value in encoded_parameters},
        "responses": {response_ids[value]: json.loads(value) for value in encoded_responses},
        "operations": {
            key: {
                **operation,
                "parameters": [
                    parameter_ids[json.dumps(item, sort_keys=True)]
                    for item in operation.get("parameters", [])
                ],
                "responses": response_ids[json.dumps(operation["responses"], sort_keys=True)],
            }
            for key, operation in operations.items()
        },
    }


def test_split_admin_routes_preserve_pre_refactor_openapi_contract():
    app = FastAPI()
    app.include_router(create_skill_admin_router(object()))
    expected = json.loads(
        (Path(__file__).parents[1] / "fixtures" / "skill_admin_openapi_v1.json").read_text()
    )
    assert normalize_openapi(app.openapi()) == expected


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "accepted,quarantined,status",
    [
        (0, 0, "insufficient_data"),
        (0, 19, "insufficient_data"),
        (15, 5, "ok"),
        (14, 6, "firing"),
    ],
)
async def test_admission_alert_boundaries_and_tenant_window(accepted, quarantined, status):
    class Reader:
        async def admission_metrics(self, tenant_id, *, since):
            assert tenant_id == "tenant-a"
            assert since == NOW - timedelta(hours=8)
            return (
                SkillAdmissionMetricRecord("accepted", "policy-a", accepted, 10.0),
                SkillAdmissionMetricRecord("quarantined", "policy-b", quarantined, 40.0),
            )

    service = SkillAdmissionMetricsService(
        Reader(), SkillAdmissionMetricsPolicy(), clock=lambda: NOW
    )
    report = await service.metrics("tenant-a", window_hours=8)
    assert report["window"] == {
        "hours": 8,
        "since": (NOW - timedelta(hours=8)).isoformat(),
        "observed_at": NOW.isoformat(),
    }
    alert = report["alerts"][0]
    assert alert["status"] == status
    assert alert["sample_count"] == accepted + quarantined
    assert alert["threshold"] == 0.25 and alert["minimum_samples"] == 20
    assert report["metrics"][1]["value"] == 10.0
    assert report["metrics"][3]["value"] == 40.0
    assert report["metrics"][-1]["value"] == alert["value"]


@pytest.mark.parametrize(
    "arguments",
    [
        {"window_hours": 0},
        {"window_hours": 2161},
        {"quarantine_alert_ratio": -0.1},
        {"quarantine_alert_ratio": 1.1},
        {"quarantine_alert_min_samples": 0},
    ],
)
def test_admission_policy_rejects_invalid_configuration(arguments):
    with pytest.raises(ValueError):
        SkillAdmissionMetricsPolicy(**arguments)


@pytest.mark.asyncio
@pytest.mark.parametrize("window", [0, -1, 2161])
async def test_admission_service_rejects_unbounded_window_before_querying(window):
    class Reader:
        async def admission_metrics(self, *_args, **_kwargs):
            raise AssertionError("invalid window must not query storage")

    service = SkillAdmissionMetricsService(Reader(), SkillAdmissionMetricsPolicy())
    with pytest.raises(ValueError, match="window"):
        await service.metrics("tenant-a", window_hours=window)


@pytest.mark.asyncio
async def test_management_view_joins_only_requested_skill_and_preserves_missing_packages():
    publication = SimpleNamespace(publisher="platform", name="release", version="1.0.0")
    missing_package = SimpleNamespace(publisher="platform", name="release", version="2.0.0")
    unrelated = SimpleNamespace(publisher="other", name="release", version="1.0.0")
    package = SimpleNamespace(manifest=publication)
    upgrade = SimpleNamespace(phase="completed")

    class Reader:
        async def list_publications(self, tenant_id):
            assert tenant_id == "tenant-a"
            return publication, missing_package, unrelated

        async def list_packages(self, tenant_id):
            assert tenant_id == "tenant-a"
            return package, SimpleNamespace(manifest=unrelated)

        async def get_installation(self, tenant_id, publisher, name):
            assert (tenant_id, publisher, name) == ("tenant-a", "platform", "release")
            raise NotFoundError("not installed")

        async def get_upgrade_state(self, tenant_id, publisher, name):
            assert (tenant_id, publisher, name) == ("tenant-a", "platform", "release")
            return upgrade

    service = SkillAdminManagementQueryService(Reader())
    view = await service.management_view("tenant-a", "platform", "release")
    assert view.installation is None
    assert view.versions == ((publication, package), (missing_package, None))
    assert view.upgrade is upgrade


@pytest.mark.asyncio
async def test_management_filters_preserve_legal_hold_false_and_publication_status():
    def package(held):
        return SimpleNamespace(
            manifest=SimpleNamespace(publisher="platform", name="release"),
            retention_status=SimpleNamespace(value="retained"),
            legal_hold=held,
        )

    held, free = package(True), package(False)
    active = SimpleNamespace(
        publisher="platform", name="release", status=SimpleNamespace(value="active")
    )
    revoked = SimpleNamespace(
        publisher="platform", name="release", status=SimpleNamespace(value="revoked")
    )

    class Reader:
        async def list_packages(self, tenant_id):
            assert tenant_id == "tenant-a"
            return held, free

        async def list_publications(self, tenant_id):
            assert tenant_id == "tenant-a"
            return active, revoked

    service = SkillAdminManagementQueryService(Reader())
    assert await service.list_packages("tenant-a", publisher="platform", legal_hold=False) == (
        free,
    )
    assert await service.list_packages("tenant-a", retention_status="purged") == ()
    assert await service.list_publications("tenant-a", status="active", name="release") == (active,)
    assert await service.list_publications("tenant-a", publisher="another") == ()


@pytest.mark.asyncio
async def test_publisher_filters_are_case_insensitive_and_preserve_key_pairing():
    record = SimpleNamespace(
        publisher="platform", display_name="Release Team", status=SimpleNamespace(value="active")
    )
    keys = (SimpleNamespace(key_id="public-key-1"),)

    class Reader:
        async def list_publishers(self, tenant_id):
            assert tenant_id == "tenant-a"
            return ((record, keys),)

    service = SkillAdminPublisherQueryService(Reader())
    assert await service.list_publishers("tenant-a", text="RELEASE", status="active") == (
        (record, keys),
    )
    assert await service.list_publishers("tenant-a", text="other") == ()
    assert await service.list_publishers("tenant-a", status="suspended") == ()
