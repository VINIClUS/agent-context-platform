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

from agent_context_platform.operations.faults import FaultLabel

# Where the indexer image mounts the checkout being indexed (containers/indexer/Dockerfile).
DEFAULT_CHECKOUT_ROOTS = ("/work",)
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
    # Least-privilege login roles for the operator CLI (members of `agent_context_projector` and
    # `agent_context_api`); each falls back to `dsn` when unset. `admin_dsn` is the operator
    # connection that provisions `operations.registered_producers` and `operations.mcp_tokens`
    # (the API and projector roles may not write them). `migration_dsn` is the schema owner that
    # `agent-context db upgrade` runs Alembic as (the platform-migrate role, never the API).
    projector_dsn: Secret[PostgresDsn] | None = Field(default=None, repr=False)
    api_dsn: Secret[PostgresDsn] | None = Field(default=None, repr=False)
    admin_dsn: Secret[PostgresDsn] | None = Field(default=None, repr=False)
    migration_dsn: Secret[PostgresDsn] | None = Field(default=None, repr=False)

    @property
    def effective_projector_dsn(self) -> Secret[PostgresDsn] | None:
        return self.projector_dsn or self.dsn

    @property
    def effective_api_dsn(self) -> Secret[PostgresDsn] | None:
        return self.api_dsn or self.dsn

    @property
    def effective_admin_dsn(self) -> Secret[PostgresDsn] | None:
        return self.admin_dsn or self.dsn

    @property
    def effective_migration_dsn(self) -> Secret[PostgresDsn] | None:
        return self.migration_dsn or self.dsn

    @field_validator("dsn", "projector_dsn", "api_dsn", "admin_dsn", "migration_dsn")
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
    write_mode: Literal["content_addressed", "if_none_match"] = "content_addressed"
    max_compressed_bytes: int = 67_108_864
    max_uncompressed_bytes: int = 268_435_456
    connect_timeout_seconds: float = 5.0
    read_timeout_seconds: float = 30.0
    max_attempts: int = 3


class MCPSettings(BaseModel):
    """Authentication and transport invariants for the read-only MCP server."""

    model_config = _SECTION_CONFIG

    # Server key for the principal cache's HMAC(token) keys. Required in production;
    # other environments fall back to a random per-process key (the cache is in-memory).
    token_hmac_key: SecretStr | None = Field(default=None, repr=False)
    # Upper bound on how long a revocation can go unnoticed on one replica.
    principal_cache_ttl_seconds: float = Field(default=30.0, gt=0, le=300, allow_inf_nan=False)
    principal_cache_max_entries: int = Field(default=1024, gt=0)
    # Per-principal, per-replica token bucket.
    rate_limit_per_second: float = Field(default=5.0, gt=0, allow_inf_nan=False)
    rate_limit_burst: int = Field(default=20, gt=0)
    stateless_http: Literal[True] = True
    # A trailing ":*" accepts any 1-5 digit port. A Host or Origin with no port does
    # not match a ":*" entry, so list it explicitly if needed. Origin is only checked
    # when present; a duplicated Host or Origin header is rejected.
    allowed_hosts: tuple[str, ...] = Field(
        default=("127.0.0.1:*", "localhost:*", "[::1]:*"), strict=False
    )
    allowed_origins: tuple[str, ...] = Field(
        default=("http://127.0.0.1:*", "http://localhost:*", "http://[::1]:*"), strict=False
    )
    max_request_body_bytes: int = Field(default=1_048_576, gt=0)

    @field_validator("token_hmac_key")
    @classmethod
    def require_long_hmac_key(cls, key: SecretStr | None) -> SecretStr | None:
        # Blank is reported as missing in production; a short non-blank key is never valid.
        if key is not None and 0 < len(key.get_secret_value().strip()) < 32:
            raise ValueError("MCP token HMAC key must be at least 32 characters")
        return key


class IngestionSettings(BaseModel):
    """Batch ingestion limits and the Argon2id cost used for the unknown-prefix dummy hash.

    Stored verifiers carry their own parameters, so the cost settings only shape the
    dummy verifier that keeps unknown-prefix requests as slow as real ones.
    """

    model_config = _SECTION_CONFIG

    # Counted while streaming; Content-Length is never trusted. Sized for 500 events
    # plus base64 content items (base64 inflates raw bytes by 4/3).
    max_request_body_bytes: int = Field(default=33_554_432, gt=0)
    argon2_time_cost: int = Field(default=3, gt=0)
    argon2_memory_cost_kib: int = Field(default=65_536, gt=0)
    argon2_parallelism: int = Field(default=4, gt=0)
    # Bounds concurrent Argon2 verifications (each holds memory_cost KiB).
    argon2_max_concurrency: int = Field(default=4, gt=0)
    # Verifications allowed to wait for a slot; beyond it requests fail fast with 503.
    argon2_max_queue_depth: int = Field(default=16, gt=0)


class FaultInjectionSettings(BaseModel):
    """Crash-test fault points (docs/operations/fault-injection.md); inert by default.

    Armed only when ``enabled`` is true and ``crash_at`` names a label; ``after`` fires on
    that label's n-th hit. ``Settings`` refuses ``enabled`` in production.
    """

    model_config = _SECTION_CONFIG

    enabled: bool = False
    crash_at: FaultLabel | None = None
    after: int = Field(default=1, ge=1)


# --- observability (PLATFORM-050) ---------------------------------------------------------------
class ObservabilitySettings(BaseModel):
    """OpenTelemetry export (docs/operations/telemetry.md); disabled while no endpoint is set.

    ``otlp_endpoint`` is the OTLP/HTTP base URL; when unset the standard
    ``OTEL_EXPORTER_OTLP_*ENDPOINT`` variables apply. Export headers (for example access tokens)
    come only from the standard ``OTEL_EXPORTER_OTLP_*HEADERS`` variables, never from settings.
    """

    model_config = _SECTION_CONFIG

    enabled: bool = True
    otlp_endpoint: AnyHttpUrl | None = None
    service_instance_id: str | None = Field(default=None, min_length=1, max_length=128)
    export_timeout_seconds: float = Field(default=5.0, gt=0, le=60, allow_inf_nan=False)
    max_queue_size: int = Field(default=2048, gt=0, le=65_536)
    max_export_batch_size: int = Field(default=512, gt=0, le=65_536)
    schedule_delay_seconds: float = Field(default=5.0, gt=0, le=300, allow_inf_nan=False)
    metric_export_interval_seconds: float = Field(default=30.0, ge=1, le=3600, allow_inf_nan=False)
    shutdown_timeout_seconds: float = Field(default=5.0, gt=0, le=60, allow_inf_nan=False)
    projection_sample_seconds: float = Field(default=15.0, ge=1, le=3600, allow_inf_nan=False)

    @field_validator("otlp_endpoint")
    @classmethod
    def reject_embedded_credentials(cls, url: AnyHttpUrl | None) -> AnyHttpUrl | None:
        if url is not None and (url.username is not None or url.password is not None):
            raise ValueError("OTLP endpoint must not contain credentials")
        return url

    @model_validator(mode="after")
    def require_batch_within_queue(self) -> Self:
        if self.max_export_batch_size > self.max_queue_size:
            raise ValueError("max_export_batch_size must not exceed max_queue_size")
        return self


# --- end observability ---------------------------------------------------------------------------


# --- Codex OTLP trace receiver (PLATFORM-053) ----------------------------------------------------
class OtelReceiverSettings(BaseModel):
    """Limits of ``POST /v1/otel/traces`` (docs/data/codex-otel-mapping.md, "Receiver").

    ``idempotency_hmac_key`` keys the mapper's idempotency digest; it must stay stable across
    restarts and replicas or re-exported records stop deduplicating, so the receiver answers 503
    until it is set (there is no per-process fallback).
    """

    model_config = _SECTION_CONFIG

    idempotency_hmac_key: SecretStr | None = Field(default=None, repr=False)
    # Counted while streaming, before decompression.
    max_request_body_bytes: int = Field(default=4_194_304, gt=0)
    # Bound on gzip output: a decompression bomb fails as 413 without being inflated.
    max_decompressed_bytes: int = Field(default=16_777_216, gt=0)
    # Spans per request; also bounds the mapped records (one ingestion batch holds 500 events).
    max_spans: int = Field(default=500, gt=0, le=500)
    # Per-producer, per-replica token bucket.
    rate_limit_per_second: float = Field(default=5.0, gt=0, allow_inf_nan=False)
    rate_limit_burst: int = Field(default=20, gt=0)

    @field_validator("idempotency_hmac_key")
    @classmethod
    def require_long_hmac_key(cls, key: SecretStr | None) -> SecretStr | None:
        if key is not None and len(key.get_secret_value().strip()) < 32:
            raise ValueError("OTel receiver HMAC key must be at least 32 characters")
        return key


# --- end Codex OTLP trace receiver ---------------------------------------------------------------


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
    ingestion: IngestionSettings = Field(default_factory=IngestionSettings)
    fault_injection: FaultInjectionSettings = Field(default_factory=FaultInjectionSettings)
    observability: ObservabilitySettings = Field(default_factory=ObservabilitySettings)
    otel_receiver: OtelReceiverSettings = Field(default_factory=OtelReceiverSettings)
    # Structural indexer (PLATFORM-032b). Adapters are confined with Landlock and the runner
    # refuses to run them when the kernel cannot enforce it; this is the dev-only escape hatch
    # (AGENT_CONTEXT_INDEXER_ALLOW_UNCONFINED_ADAPTERS). Never enable it in production.
    indexer_allow_unconfined_adapters: bool = False
    # Directories being indexed; no path readable by an adapter may equal, contain or lie in one.
    # Never empty (an empty list would disable the check), default the image's mount.
    indexer_checkout_roots: tuple[str, ...] = Field(
        default=DEFAULT_CHECKOUT_ROOTS, strict=False, min_length=1
    )

    @model_validator(mode="after")
    def require_complete_production_settings(self) -> Self:
        if self.environment != "production":
            return self
        if self.fault_injection.enabled:
            raise ValueError("fault injection must not be enabled in production")

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
        token_hmac_key = (
            None if self.mcp.token_hmac_key is None else self.mcp.token_hmac_key.get_secret_value()
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
                ("mcp.token_hmac_key", token_hmac_key),
            )
            if value is None or (isinstance(value, str) and not value.strip())
        ]
        if missing:
            raise ValueError(f"Missing production settings: {', '.join(missing)}")
        return self
