from functools import lru_cache
from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_prefix="AURACLAW_", extra="ignore")

    env: str = "development"
    host: str = "127.0.0.1"
    port: int = 8000
    log_level: str = "INFO"
    storage_backend: str = "memory"
    database_url: str = "postgresql+asyncpg://auraclaw:auraclaw@localhost:5432/auraclaw"
    artifact_root: Path = Path(".data/artifacts")


@lru_cache
def get_settings() -> Settings:
    return Settings()
