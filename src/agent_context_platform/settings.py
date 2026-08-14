from typing import Literal

from pydantic_settings import BaseSettings, SettingsConfigDict

Environment = Literal["development", "test", "production"]


class Settings(BaseSettings):
    """Process settings loaded from the Agent Context environment namespace."""

    model_config = SettingsConfigDict(
        env_prefix="AGENT_CONTEXT_",
        extra="forbid",
        frozen=True,
        strict=True,
        validate_default=True,
    )

    environment: Environment = "development"
