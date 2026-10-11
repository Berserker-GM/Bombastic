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
    api_token: str | None = None
    clist_api_key: str | None = None
    clist_username: str | None = None
    google_token_path: str = "token.json"
    google_token_json: str | None = None
    google_calendar_id: str = "primary"
    llm_provider: str | None = None
    llm_model: str | None = None
    anthropic_api_key: str | None = None
    openai_api_key: str | None = None


def get_settings() -> Settings:
    return Settings()
