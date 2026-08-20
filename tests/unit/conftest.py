"""Isolate unit tests from ambient developer shell / .env exports."""

from __future__ import annotations

import os

import pytest

from auraclaw.composition import providers
from auraclaw.config import get_settings

# Unit tests construct Settings(_env_file=None, ...) with explicit kwargs.
# A shell that sourced .env exports AURACLAW_*/MYSQL_DB_*/DB_* into the process;
# pydantic-settings still reads those env vars and breaks production/hands unit tests.
_AMBIENT_PREFIXES = (
    "AURACLAW_",
    "MYSQL_DB_",
    "DB_",
    "SEAWEEDFS_",
    "REDIS_",
    "KAFKA_",
)


@pytest.fixture(autouse=True)
def _clear_ambient_settings_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in list(os.environ):
        if name.startswith(_AMBIENT_PREFIXES) or name.endswith("_DATABASE_URL"):
            monkeypatch.delenv(name, raising=False)
    get_settings.cache_clear()
    for value in vars(providers).values():
        cache_clear = getattr(value, "cache_clear", None)
        if callable(cache_clear):
            cache_clear()
    get_settings().allow_insecure_identity_headers = True
