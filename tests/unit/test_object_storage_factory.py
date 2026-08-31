import pytest

from auraclaw.composition.object_storage import build_object_storage
from auraclaw.config import Settings


def test_resolved_artifact_backend_prefers_obs_endpoint_in_auto(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    for name in (
        "AURACLAW_ARTIFACT_BACKEND",
        "OBS_ENDPOINT",
        "SEAWEEDFS_HOST",
        "OBS_AK",
        "OBS_SK",
        "SEAWEEDFS_ACCESS_KEY",
        "SEAWEEDFS_SECRET_KEY",
    ):
        monkeypatch.delenv(name, raising=False)

    monkeypatch.setenv("OBS_ENDPOINT", "obsv3.example.com")
    monkeypatch.setenv("OBS_AK", "obs-ak")
    monkeypatch.setenv("OBS_SK", "obs-sk")
    monkeypatch.setenv("SEAWEEDFS_HOST", "seaweed.example")
    settings = Settings(_env_file=None)
    assert settings.resolved_artifact_backend == "obs"
    assert settings.object_storage_enabled is True
    assert settings.seaweedfs_enabled is False
    assert settings.obs_enabled is True


def test_build_object_storage_selects_obs_backend(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("AURACLAW_ARTIFACT_BACKEND", "obs")
    monkeypatch.setenv("OBS_ENDPOINT", "obsv3.example.com")
    monkeypatch.setenv("OBS_BUCKET", "auraclaw-dev")
    monkeypatch.setenv("OBS_AK", "obs-ak")
    monkeypatch.setenv("OBS_SK", "obs-sk")
    monkeypatch.setenv("OBS_REGION", "cn-north-1")
    settings = Settings(_env_file=None)
    bundle = build_object_storage(settings)
    assert bundle.backend == "obs"
    assert bundle.verifier is not None
    assert bundle.multipart is not None
    url, _ = bundle.presigner.presign("PUT", "tenant/object")
    assert "obsv3.example.com" in url
    assert "auraclaw-dev" in url


def test_build_object_storage_local_has_no_verifier(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("AURACLAW_ARTIFACT_BACKEND", "local")
    settings = Settings(_env_file=None)
    bundle = build_object_storage(settings)
    assert bundle.backend == "local"
    assert bundle.verifier is None
    assert bundle.multipart is None


def test_production_object_storage_rejects_local_and_missing_credentials() -> None:
    with pytest.raises(ValueError, match="persistent object storage"):
        build_object_storage(
            Settings(
                _env_file=None,
                deployment_profile="production",
                artifact_backend="local",
            )
        )
    with pytest.raises(ValueError, match="SeaweedFS backend requires"):
        build_object_storage(
            Settings(
                _env_file=None,
                deployment_profile="production",
                artifact_backend="seaweedfs",
                seaweedfs_host="seaweed.test",
            )
        )


def test_obs_backend_requires_credentials(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AURACLAW_ARTIFACT_BACKEND", "obs")
    monkeypatch.setenv("OBS_ENDPOINT", "obsv3.example.com")
    monkeypatch.delenv("OBS_SK", raising=False)
    with pytest.raises(ValueError, match="OBS_SK"):
        Settings(_env_file=None)
