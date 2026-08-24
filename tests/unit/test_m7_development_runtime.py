from fastapi.testclient import TestClient

from auraclaw.config import Settings, get_settings
from auraclaw.main import create_app


def test_local_browser_origin_passes_cors_preflight() -> None:
    settings = get_settings()
    previous = settings.cors_allow_origins
    settings.cors_allow_origins = "http://127.0.0.1:3000"
    try:
        with TestClient(create_app(profile="task-api")) as client:
            response = client.options(
                "/v1/tasks",
                headers={
                    "Origin": "http://127.0.0.1:3000",
                    "Access-Control-Request-Method": "POST",
                    "Access-Control-Request-Headers": "content-type,idempotency-key,x-tenant-id",
                },
            )
        assert response.status_code == 200
        assert response.headers["access-control-allow-origin"] == "http://127.0.0.1:3000"
        assert "idempotency-key" in response.headers["access-control-allow-headers"].lower()
    finally:
        settings.cors_allow_origins = previous


def test_runtime_logic_is_independent_of_resource_backends() -> None:
    settings = Settings(
        _env_file=None,
        storage_backend="postgres",
        runtime_event_backend="kafka",
    )
    assert settings.postgres_enabled is True
    assert settings.kafka_enabled is True
