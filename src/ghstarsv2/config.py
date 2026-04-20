from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore")

    app_name: str = "ghstars"
    api_prefix: str = "/api/v1"

    database_url: str = "postgresql+psycopg://ghstars:ghstars@db:5432/ghstars"
    data_dir: Path = Path("data")
    raw_fetch_dir_name: str = "raw"
    export_dir_name: str = "exports"
    frontend_dist_dir: Path = Path("frontend/dist")

    default_categories: str = "cs.CV"

    github_token: str = ""
    huggingface_token: str = ""
    alphaxiv_token: str = ""

    huggingface_enabled: bool = True
    alphaxiv_enabled: bool = True

    arxiv_api_min_interval: float = 0.5
    huggingface_min_interval: float = 0.5
    github_min_interval: float = 0.5

    worker_poll_seconds: float = 1.0
    job_timeout_seconds: int = 1800

    public_export_downloads: bool = True
    cors_origins: list[str] = Field(default_factory=lambda: ["*"])

    @property
    def raw_fetch_dir(self) -> Path:
        return self.data_dir / self.raw_fetch_dir_name

    @property
    def export_dir(self) -> Path:
        return self.data_dir / self.export_dir_name

    @property
    def default_categories_list(self) -> list[str]:
        return [item.strip() for item in self.default_categories.split(",") if item.strip()]


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()


def clear_settings_cache() -> None:
    get_settings.cache_clear()
