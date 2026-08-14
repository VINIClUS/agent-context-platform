from typing import Literal

from fastapi import FastAPI

from agent_context_platform.settings import Settings


def create_app(settings: Settings | None = None) -> FastAPI:
    """Create an isolated platform application with no external resources."""
    resolved_settings = Settings() if settings is None else settings
    application = FastAPI(title="Agent Context Platform")
    application.state.settings = resolved_settings

    @application.get("/health/live", include_in_schema=True)
    def health_live() -> dict[str, Literal["ok"]]:
        return {"status": "ok"}

    return application
