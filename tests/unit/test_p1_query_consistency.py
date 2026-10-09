from fastapi.testclient import TestClient

from auraclaw.composition.api import create_app


def test_session_queries_share_min_version_and_conditional_get_contract() -> None:
    with TestClient(create_app(profile="task-api")) as client:
        tenant_headers = {"X-Tenant-ID": "tenant-query-consistency"}
        created = client.post(
            "/v1/tasks",
            headers={**tenant_headers, "Idempotency-Key": "create-query-contract"},
            json={"goal": "verify the query consistency contract"},
        )
        assert created.status_code == 202
        session_id = created.json()["session_id"]
        task = client.get(f"/v1/tasks/{session_id}", headers=tenant_headers)
        projection_version = int(task.json()["projection_version"])
        etag = f'W/"{projection_version}"'

        endpoints = (
            f"/v1/tasks/{session_id}",
            f"/v1/tasks/{session_id}/children",
            f"/v1/tasks/{session_id}/transcript",
            f"/v1/tasks/{session_id}/activity",
            f"/v1/operations/sessions/{session_id}/timeline",
        )
        for endpoint in endpoints:
            stale = client.get(
                endpoint,
                headers=tenant_headers,
                params={"min_version": projection_version + 1},
            )
            assert stale.status_code == 202, endpoint
            assert stale.headers["retry-after"] == "1", endpoint
            assert stale.headers["etag"] == etag, endpoint
            assert stale.headers["x-projection-version"] == str(projection_version), endpoint
            assert stale.json()["projection_version"] == projection_version, endpoint

            unchanged = client.get(
                endpoint,
                headers={**tenant_headers, "If-None-Match": etag},
                params={"min_version": projection_version},
            )
            assert unchanged.status_code == 304, endpoint
            assert unchanged.headers["etag"] == etag, endpoint
            assert unchanged.headers["x-projection-version"] == str(
                projection_version
            ), endpoint


def test_result_query_exposes_projection_version_header_while_pending() -> None:
    with TestClient(create_app(profile="task-api")) as client:
        tenant_headers = {"X-Tenant-ID": "tenant-result-consistency"}
        created = client.post(
            "/v1/tasks",
            headers={**tenant_headers, "Idempotency-Key": "create-result-contract"},
            json={"goal": "verify result version headers"},
        )
        session_id = created.json()["session_id"]

        result = client.get(f"/v1/tasks/{session_id}/result", headers=tenant_headers)
        assert result.status_code == 202
        assert result.headers["etag"] == f'W/"{result.json()["projection_version"]}"'
        assert result.headers["x-projection-version"] == str(
            result.json()["projection_version"]
        )

        waited = client.get(
            f"/v1/tasks/{session_id}/result",
            headers=tenant_headers,
            params={"wait": "true", "timeout_seconds": 1, "min_version": 999},
        )
        assert waited.status_code == 202
        assert waited.headers["retry-after"] == "1"
        assert waited.headers["x-projection-version"] == str(
            waited.json()["projection_version"]
        )

        invalid = client.get(
            f"/v1/tasks/{session_id}/transcript",
            headers=tenant_headers,
            params={"min_version": -1},
        )
        assert invalid.status_code == 422
