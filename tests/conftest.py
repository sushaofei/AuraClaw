"""Keep tests from picking up repo-root .env.debug / .env."""

from __future__ import annotations

import pytest

from auraclaw.config import get_settings


@pytest.fixture(autouse=True)
def _disable_repo_env_files(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AURACLAW_DISABLE_ENV_FILE", "1")
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()
