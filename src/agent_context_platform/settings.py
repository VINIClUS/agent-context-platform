from __future__ import annotations

import json
import os
import stat
import unicodedata
from pathlib import Path
from typing import Annotated, Any, Final, Literal, Self

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
from pydantic.fields import FieldInfo
from pydantic_settings import (
    BaseSettings,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
)

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


class SearchSettings(BaseModel):
    """Local embedding model location and bounds (PLATFORM-042).

    The service never downloads the model: `agent-context models fetch --dest <dir>` does, and
    the service loads `model_dir`, failing closed on any digest mismatch. With no `model_dir`
    the search projector cannot embed: the worker refuses to start unless it runs lexical-only
    (`projection run --lexical-only`: rows without embeddings, counted), and replays refuse unless
    `--without-search` skips the projector.
    """

    model_config = _SECTION_CONFIG

    model_dir: Path | None = Field(default=None, strict=False)
    embedding_batch_size: int = Field(default=16, ge=1, le=256)
    embedding_threads: int = Field(default=1, ge=1, le=16)


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
    search: SearchSettings = Field(default_factory=SearchSettings)
    fault_injection: FaultInjectionSettings = Field(default_factory=FaultInjectionSettings)
    observability: ObservabilitySettings = Field(default_factory=ObservabilitySettings)
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

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        # Precedence, first wins: explicit arguments, the environment, then secret files.
        return (
            init_settings,
            env_settings,
            dotenv_settings,
            _SecretFilesSource(settings_cls),
            file_secret_settings,
        )


# --- file-based secrets (PLATFORM-054) ------------------------------------------------------------
# `AGENT_CONTEXT_SECRETS_DIR` names a directory of files, each named after the variable it replaces
# (`AGENT_CONTEXT_POSTGRESQL__API_DSN`) and holding only the value (one trailing newline is
# dropped). INFRA mounts them 0400 instead of exporting secrets into the process environment,
# where `/proc/<pid>/environ` shows them. Only the secret fields have file names
# (`SECRET_FILE_NAMES`). A variable set BOTH in the environment (even empty, in any letter case)
# and as a file refuses to start: a stale variable left after a rotation would otherwise keep the
# old value in use and visible. Everything is refused, never skipped, when it is not as expected:
# a relative, non-canonical or missing directory, a symlink anywhere in its path, a directory others
# can write, a file that is a symlink or not regular, an owner other than this process's user or
# root, a file with any group or other permission bit, a value that is empty, has more than one
# line, surrounding whitespace, a byte-order mark or another control or separator character, a
# file over 64 KiB (the final newline included), and an `AGENT_CONTEXT_*` file that is not a secret
# setting (it would never apply). Only upper-case names are ours; other files are ignored. Errors
# name the file or variable, never a value.

SECRETS_DIR_ENV: Final = "AGENT_CONTEXT_SECRETS_DIR"
_SECRET_FILE_MAX_BYTES: Final = 65_536
_SECRET_FILE_PREFIX: Final = "AGENT_CONTEXT_"


class SecretFileError(ValueError):
    """A secrets directory or file is not safe to read; the message names it, never its value."""


def _secret_fields() -> dict[str, tuple[str, str]]:
    """`AGENT_CONTEXT_<SECTION>__<FIELD>` -> (section, field) for every secret-typed field."""
    names: dict[str, tuple[str, str]] = {}
    for section, info in Settings.model_fields.items():
        model = info.annotation
        if not (isinstance(model, type) and issubclass(model, BaseModel)):
            continue
        for field, sub in model.model_fields.items():
            if "Secret" in repr(sub.annotation):
                names[f"{_SECRET_FILE_PREFIX}{section.upper()}__{field.upper()}"] = (section, field)
    return names


def _accepted_owners() -> frozenset[int]:
    return frozenset({os.geteuid(), 0})


def _refuse_unsafe(name: str, info: os.stat_result, *, kind: str) -> None:
    if info.st_uid not in _accepted_owners():
        raise SecretFileError(f"{name}: the {kind} owner must be this process's user or root")


def read_secret_files(directory: str) -> dict[str, dict[str, str]]:
    """The secret settings in `directory`, nested by section (see the section comment above)."""
    path = Path(directory)
    if not path.is_absolute():
        raise SecretFileError(f"{SECRETS_DIR_ENV} must be an absolute path")
    if os.path.normpath(directory) != directory.rstrip("/") or ".." in path.parts:
        raise SecretFileError(f"{SECRETS_DIR_ENV} must be a canonical path (no `.`, `..`, `//`)")
    # realpath() checks every ancestor; the open below refuses a symlink in the last component
    # (O_NOFOLLOW). An ancestor swapped for a symlink between the two needs write access to that
    # ancestor, which already means control over what the process reads: accepted.
    if os.path.realpath(directory) != os.path.normpath(directory):
        raise SecretFileError(f"{SECRETS_DIR_ENV} must not go through a symlink")
    try:
        directory_fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW | os.O_CLOEXEC)
    except OSError:
        raise SecretFileError(
            f"{SECRETS_DIR_ENV} must name an existing directory (not a symlink)"
        ) from None
    try:
        info = os.fstat(directory_fd)
        _refuse_unsafe(SECRETS_DIR_ENV, info, kind="directory")
        if info.st_mode & 0o022:
            raise SecretFileError(
                f"{SECRETS_DIR_ENV}: the directory is writable by group or others"
            )
        values: dict[str, dict[str, str]] = {}
        for name in sorted(os.listdir(directory_fd)):
            if not name.startswith(_SECRET_FILE_PREFIX):
                continue  # not ours (a mount may hold other files)
            target = SECRET_FILE_NAMES.get(name)
            if target is None:
                raise SecretFileError(f"{name}: not a secret setting, so a file would never apply")
            section, field = target
            values.setdefault(section, {})[field] = _read_secret_file(directory_fd, name)
        return values
    finally:
        os.close(directory_fd)


def _read_secret_file(directory_fd: int, name: str) -> str:
    try:
        fd = os.open(
            name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK | os.O_CLOEXEC, dir_fd=directory_fd
        )
    except OSError:
        raise SecretFileError(f"{name}: must be a readable regular file, not a symlink") from None
    try:
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode):
            raise SecretFileError(f"{name}: must be a readable regular file, not a symlink")
        _refuse_unsafe(name, info, kind="file")
        if info.st_mode & 0o077:
            raise SecretFileError(f"{name}: readable or writable by group or others (use 0400)")
        data = os.read(fd, _SECRET_FILE_MAX_BYTES + 1)
    finally:
        os.close(fd)
    if len(data) > _SECRET_FILE_MAX_BYTES:
        raise SecretFileError(f"{name}: larger than {_SECRET_FILE_MAX_BYTES} bytes")
    try:
        text = data.decode("utf-8")
    except UnicodeDecodeError:
        raise SecretFileError(f"{name}: not UTF-8 text") from None
    value = text[:-2] if text.endswith("\r\n") else text.removesuffix("\n")
    if not value or any(
        unicodedata.category(character)[0] in "CZ" and character != " " for character in value
    ):
        raise SecretFileError(
            f"{name}: must hold one non-empty line without control characters or a BOM"
        )
    if value != value.strip():
        raise SecretFileError(f"{name}: the value must not start or end with whitespace")
    return value


def _section_sets(exported: dict[str, str], section: str, field: str) -> bool:
    """Whether the whole-section variable `AGENT_CONTEXT_<SECTION>` (JSON) may set `field`.

    Fail closed: a value that is not a JSON object might hold the secret, so it counts as set.
    """
    raw = exported.get(f"{_SECRET_FILE_PREFIX}{section.upper()}")
    if raw is None:
        return False
    try:
        parsed = json.loads(raw)
    except ValueError:
        return True
    if not isinstance(parsed, dict):
        return True
    return any(isinstance(key, str) and key.lower() == field for key in parsed)


class _SecretFilesSource(PydanticBaseSettingsSource):
    """Reads `read_secret_files(AGENT_CONTEXT_SECRETS_DIR)` when that variable is set."""

    def get_field_value(self, field: FieldInfo, field_name: str) -> tuple[Any, str, bool]:
        return None, field_name, False  # `__call__` reads the whole directory at once

    def __call__(self) -> dict[str, Any]:
        directory = os.environ.get(SECRETS_DIR_ENV)
        if directory is None:
            return {}
        values = read_secret_files(directory)
        exported = {name.upper(): value for name, value in os.environ.items()}
        both = sorted(
            name
            for name, (section, field) in SECRET_FILE_NAMES.items()
            if field in values.get(section, {})
            and (name in exported or _section_sets(exported, section, field))
        )
        if both:
            raise SecretFileError(
                f"{', '.join(both)}: set both in the environment and in {SECRETS_DIR_ENV}; "
                "unset the variable"
            )
        return values


SECRET_FILE_NAMES: Final = _secret_fields()
