from __future__ import annotations

import os
from pathlib import Path

import pytest

from auraclaw.config import (
    Settings,
    _validate_local_dev_storage,
    apply_local_dev_proxy_env,
    get_settings,
)


def test_apply_local_dev_proxy_env_only_for_env_dev(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("HTTP_PROXY", "http://corp-proxy.example:8080")
    monkeypatch.setenv("https_proxy", "http://corp-proxy.example:8080")
    monkeypatch.delenv("AURACLAW_MODEL_USE_PROXY", raising=False)

    test_env = tmp_path / ".env.test"
    test_env.write_text("AURACLAW_DEPLOYMENT_PROFILE=production\n")
    apply_local_dev_proxy_env(test_env)
    assert os.environ.get("HTTP_PROXY") == "http://corp-proxy.example:8080"

    prod_env = tmp_path / ".env.prod"
    prod_env.write_text("AURACLAW_DEPLOYMENT_PROFILE=production\n")
    apply_local_dev_proxy_env(prod_env)
    assert os.environ.get("HTTP_PROXY") == "http://corp-proxy.example:8080"

    dev_env = tmp_path / ".env.dev"
    dev_env.write_text(
        "NO_PROXY=10.244.16.132,127.0.0.1\n"
        "no_proxy=10.244.16.132,127.0.0.1\n"
    )
    apply_local_dev_proxy_env(dev_env)
    assert os.environ.get("NO_PROXY") == "10.244.16.132,127.0.0.1"
    assert os.environ.get("no_proxy") == "10.244.16.132,127.0.0.1"
    assert "HTTP_PROXY" not in os.environ
    assert "https_proxy" not in os.environ


def test_apply_local_dev_proxy_env_respects_use_proxy_flag(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("HTTP_PROXY", "http://corp-proxy.example:8080")
    monkeypatch.setenv("AURACLAW_MODEL_USE_PROXY", "1")
    dev_env = tmp_path / ".env.dev"
    dev_env.write_text("NO_PROXY=127.0.0.1\n")
    apply_local_dev_proxy_env(dev_env)
    assert os.environ.get("HTTP_PROXY") == "http://corp-proxy.example:8080"
    assert os.environ.get("NO_PROXY") == "127.0.0.1"


def test_get_settings_skips_proxy_for_prod_env_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    get_settings.cache_clear()
    monkeypatch.delenv("AURACLAW_DISABLE_ENV_FILE", raising=False)
    monkeypatch.setenv("HTTP_PROXY", "http://corp-proxy.example:8080")
    monkeypatch.delenv("AURACLAW_MODEL_USE_PROXY", raising=False)

    prod = tmp_path / ".env.prod"
    prod.write_text(
        "AURACLAW_DEPLOYMENT_PROFILE=production\n"
        "AURACLAW_STORAGE_BACKEND=memory\n"
        "AURACLAW_ALLOW_INSECURE_IDENTITY_HEADERS=false\n"
    )
    monkeypatch.setenv("AURACLAW_ENV_FILE", str(prod))
    get_settings.cache_clear()
    try:
        get_settings()
        assert os.environ.get("HTTP_PROXY") == "http://corp-proxy.example:8080"
    finally:
        get_settings.cache_clear()


def test_local_dev_env_rejects_memory_storage(tmp_path: Path) -> None:
    env = tmp_path / ".env.dev"
    env.write_text(
        "AURACLAW_DEPLOYMENT_PROFILE=development\n"
        "AURACLAW_STORAGE_BACKEND=memory\n"
    )
    settings = Settings(
        _env_file=env,
        deployment_profile="development",
        storage_backend="memory",
    )
    with pytest.raises(ValueError, match="requires SQL storage"):
        _validate_local_dev_storage(settings, env)
