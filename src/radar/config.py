"""Application settings loaded from environment variables."""

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    database_url: str = "sqlite:///./radar.db"
    log_level: str = "INFO"
    clist_api_key: str | None = None
    clist_username: str | None = None


def get_settings() -> Settings:
    return Settings()
