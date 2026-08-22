from __future__ import annotations

from typing import Annotated, Literal, Self

from pydantic import (
    AnyHttpUrl,
    AnyUrl,
    BaseModel,
    ConfigDict,
    Field,
    PostgresDsn,
    Secret,
    SecretStr,
    UrlConstraints,
    field_validator,
    model_validator,
)
from pydantic_settings import BaseSettings, SettingsConfigDict

Environment = Literal["development", "test", "production"]
Neo4jUri = Annotated[
    AnyUrl,
    UrlConstraints(
        allowed_schemes=[
            "neo4j",
            "neo4j+s",
            "neo4j+ssc",
            "bolt",
            "bolt+s",
            "bolt+ssc",
        ],
        host_required=True,
    ),
]

_SECTION_CONFIG = ConfigDict(
    extra="forbid",
    frozen=True,
    strict=True,
    validate_default=True,
    hide_input_in_errors=True,
)


class PostgreSQLSettings(BaseModel):
    """PostgreSQL connection settings for the async psycopg driver."""

    model_config = _SECTION_CONFIG

    dsn: Secret[PostgresDsn] | None = Field(default=None, repr=False)

    @field_validator("dsn")
    @classmethod
    def require_async_psycopg_database(
        cls, dsn: Secret[PostgresDsn] | None
    ) -> Secret[PostgresDsn] | None:
        if dsn is None:
            return None

        value = dsn.get_secret_value()
        if value.scheme != "postgresql+psycopg":
            raise ValueError("PostgreSQL DSN must use postgresql+psycopg")
        if value.path in (None, "", "/"):
            raise ValueError("PostgreSQL DSN must include a database")
        return dsn


class Neo4jSettings(BaseModel):
    """Neo4j driver settings, populated when graph projection is enabled."""

    model_config = _SECTION_CONFIG

    uri: Neo4jUri | None = None
    username: str | None = Field(default=None, repr=False)
    password: SecretStr | None = Field(default=None, repr=False)
    database: str = "neo4j"
    connection_timeout: float = Field(default=5.0, gt=0, allow_inf_nan=False)
    connection_acquisition_timeout: float = Field(default=10.0, gt=0, allow_inf_nan=False)
    max_transaction_retry_time: float = Field(default=15.0, gt=0, allow_inf_nan=False)
    transaction_timeout: float = Field(default=10.0, gt=0, allow_inf_nan=False)
    schema_timeout: float = Field(default=30.0, gt=0, allow_inf_nan=False)

    @field_validator("uri")
    @classmethod
    def reject_embedded_credentials(cls, uri: Neo4jUri | None) -> Neo4jUri | None:
        if uri is not None and (uri.username is not None or uri.password is not None):
            raise ValueError("Neo4j URI must not contain credentials")
        return uri

    @model_validator(mode="after")
    def require_acquisition_timeout_to_exceed_connection_timeout(self) -> Self:
        if self.connection_acquisition_timeout <= self.connection_timeout:
            raise ValueError("connection_acquisition_timeout must exceed connection_timeout")
        return self


class S3Settings(BaseModel):
    """Product-neutral settings for the S3-compatible blob store."""

    model_config = _SECTION_CONFIG

    endpoint_url: AnyHttpUrl | None = None
    region_name: str = "garage"
    bucket_name: str = "agent-context-content"
    access_key_id: SecretStr | None = Field(default=None, repr=False)
    secret_access_key: SecretStr | None = Field(default=None, repr=False)
    addressing_style: Literal["path"] = "path"


class MCPSettings(BaseModel):
    """Authentication and transport invariants for the read-only MCP server."""

    model_config = _SECTION_CONFIG

    bearer_token_verifier: SecretStr | None = Field(default=None, repr=False)
    stateless_http: Literal[True] = True


class Settings(BaseSettings):
    """Single process boundary for the Agent Context environment namespace."""

    model_config = SettingsConfigDict(
        env_prefix="AGENT_CONTEXT_",
        env_nested_delimiter="__",
        extra="forbid",
        frozen=True,
        strict=True,
        validate_default=True,
        hide_input_in_errors=True,
    )

    environment: Environment = "development"
    postgresql: PostgreSQLSettings = Field(default_factory=PostgreSQLSettings)
    neo4j: Neo4jSettings = Field(default_factory=Neo4jSettings)
    s3: S3Settings = Field(default_factory=S3Settings)
    mcp: MCPSettings = Field(default_factory=MCPSettings)

    @model_validator(mode="after")
    def require_complete_production_settings(self) -> Self:
        if self.environment != "production":
            return self

        neo4j_password = (
            None if self.neo4j.password is None else self.neo4j.password.get_secret_value()
        )
        s3_access_key_id = (
            None if self.s3.access_key_id is None else self.s3.access_key_id.get_secret_value()
        )
        s3_secret_access_key = (
            None
            if self.s3.secret_access_key is None
            else self.s3.secret_access_key.get_secret_value()
        )
        bearer_token_verifier = (
            None
            if self.mcp.bearer_token_verifier is None
            else self.mcp.bearer_token_verifier.get_secret_value()
        )
        missing = [
            name
            for name, value in (
                ("postgresql.dsn", self.postgresql.dsn),
                ("neo4j.uri", self.neo4j.uri),
                ("neo4j.username", self.neo4j.username),
                ("neo4j.password", neo4j_password),
                ("neo4j.database", self.neo4j.database),
                ("s3.endpoint_url", self.s3.endpoint_url),
                ("s3.region_name", self.s3.region_name),
                ("s3.bucket_name", self.s3.bucket_name),
                ("s3.access_key_id", s3_access_key_id),
                ("s3.secret_access_key", s3_secret_access_key),
                ("mcp.bearer_token_verifier", bearer_token_verifier),
            )
            if value is None or (isinstance(value, str) and not value.strip())
        ]
        if missing:
            raise ValueError(f"Missing production settings: {', '.join(missing)}")
        return self
