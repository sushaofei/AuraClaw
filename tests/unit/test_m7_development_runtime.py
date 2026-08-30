from fastapi.testclient import TestClient

from auraclaw.config import Settings, get_settings
from auraclaw.main import create_app


class _NoOpObservability:
    async def record_span(self, **kwargs: object) -> None:
        del kwargs

    async def metric(self, *args: object, **kwargs: object) -> None:
        del args, kwargs


def test_local_browser_origin_passes_cors_preflight() -> None:
    settings = get_settings()
    previous = settings.cors_allow_origins
    settings.cors_allow_origins = "http://127.0.0.1:3000"
    try:
        app = create_app(profile="task-api")
        app.state.observability_service = _NoOpObservability()
        with TestClient(app) as client:
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
            lan = client.options(
                "/v1/tasks",
                headers={
                    "Origin": "http://10.244.16.10:1420",
                    "Access-Control-Request-Method": "POST",
                    "Access-Control-Request-Headers": "content-type,idempotency-key,x-tenant-id",
                },
            )
            assert lan.status_code == 200
            assert lan.headers["access-control-allow-origin"] == "http://10.244.16.10:1420"
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
