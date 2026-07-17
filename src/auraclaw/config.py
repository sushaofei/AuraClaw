from functools import lru_cache
from pathlib import Path
from typing import Literal
from urllib.parse import quote

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_prefix="AURACLAW_", extra="ignore")

    env: str = "development"
    host: str = "127.0.0.1"
    port: int = 8000
    log_level: str = "INFO"
    storage_backend: Literal["auto", "memory", "postgres"] = "auto"
    database_url: str = "postgresql+asyncpg://auraclaw:auraclaw@localhost:5432/auraclaw"
    db_host: str | None = Field(default=None, validation_alias="DB_HOST")
    db_port: int = Field(default=5432, validation_alias="DB_PORT")
    db_user: str | None = Field(default=None, validation_alias="DB_USER")
    db_password: str | None = Field(default=None, validation_alias="DB_PWD")
    db_name: str | None = Field(default=None, validation_alias="DB_NAME")
    db_name_dev: str | None = Field(default=None, validation_alias="DB_NAME_DEV")
    db_name_pro: str | None = Field(default=None, validation_alias="DB_NAME_PRO")
    artifact_root: Path = Path(".data/artifacts")

    @property
    def resolved_database_url(self) -> str:
        database_name = self.selected_database_name
        if self.db_host and self.db_user and self.db_password is not None and database_name:
            user = quote(self.db_user, safe="")
            password = quote(self.db_password, safe="")
            database = quote(database_name, safe="")
            return (
                f"postgresql+asyncpg://{user}:{password}@{self.db_host}:{self.db_port}/{database}"
            )
        return self.database_url

    @property
    def selected_database_name(self) -> str | None:
        if self.env.lower() in {"production", "prod"}:
            return self.db_name_pro or self.db_name
        return self.db_name_dev or self.db_name

    @property
    def postgres_enabled(self) -> bool:
        if self.storage_backend == "memory":
            return False
        if self.storage_backend == "postgres":
            return True
        return bool(self.db_host and self.db_user and self.selected_database_name)


@lru_cache
def get_settings() -> Settings:
    return Settings()
