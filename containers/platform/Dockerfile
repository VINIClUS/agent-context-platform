# Platform service image (PLATFORM-049): one immutable image for the INFRA-040 roles.
#
#   platform-migrate    agent-context db upgrade --expect-head <io.agent-context.alembic.head>
#   platform-api        (default command) uvicorn on 0.0.0.0:8000, /health/live and /health/ready
#   platform-projector  agent-context projection run
#   platform-indexer    agent-context index ... (run as --user 10001:10001; see README)
#
# Every role runs with the same hardening, and the image needs nothing more:
#
#   docker run --read-only --tmpfs /tmp:rw,noexec,nosuid,size=64m --cap-drop ALL \
#     --security-opt no-new-privileges --pids-limit 256 --memory 1g <image> [command]
#
# Nothing is added to the base but the locked runtime environment, the Alembic scripts and a
# stdlib health probe: no compilers, curl or shell tooling. The command is exec-form, so no
# shell runs at start-up, and HEALTHCHECK is left to the API role's compose service
# (`python /app/healthcheck.py /health/ready`) because the other roles serve no HTTP.
# Semantic-search model files are not baked in: they are provisioned into a read-only volume.
#
# Build from the repository root (the private SDK wheel must be in build/sdk/). CI passes the
# label values; when one is set the build fails unless it matches what the image contains:
#   docker build -f containers/platform/Dockerfile \
#     --build-arg REVISION=<git sha> --build-arg VERSION=<project version> \
#     --build-arg SDK_VERSION=<sdk version> --build-arg ALEMBIC_HEAD=<alembic head> \
#     -t agent-context-platform .

# Base pinned by digest (python:3.12-slim); bump deliberately, together with the lockfile.
FROM python:3.12-slim@sha256:f77ac9e44ae96ef2c90b8053ea08c31f8be030f824196b0ae4db6d462c84e51f AS builder

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never \
    UV_PROJECT_ENVIRONMENT=/opt/venv \
    PIP_DISABLE_PIP_VERSION_CHECK=1

RUN pip install --no-cache-dir uv==0.11.28

WORKDIR /app
# The private SDK wheel(s) that [tool.uv.sources] in pyproject.toml points at (any version).
COPY build/sdk build/sdk
COPY pyproject.toml uv.lock README.md ./
COPY src src
RUN uv sync --frozen --no-dev --no-editable --python /usr/local/bin/python
COPY alembic.ini ./
COPY alembic alembic

ARG VERSION=""
ARG SDK_VERSION=""
ARG ALEMBIC_HEAD=""
# The labels are a deploy contract (infra pins --expect-head from them): verify every one set.
RUN VERSION="$VERSION" SDK_VERSION="$SDK_VERSION" ALEMBIC_HEAD="$ALEMBIC_HEAD" \
    AGENT_CONTEXT_ALEMBIC_CONFIG=/app/alembic.ini /opt/venv/bin/python -c "\
import os, sys; from importlib.metadata import version; \
from agent_context_platform.operations.migrate import alembic_config, image_head; \
actual = {'VERSION': version('agent-context-platform'), 'SDK_VERSION': version('agent-context-sdk'), \
'ALEMBIC_HEAD': image_head(alembic_config())}; \
given = {k: os.environ[k].removeprefix('v') if k == 'VERSION' else os.environ[k] for k in actual}; \
wrong = [k for k, v in actual.items() if given[k] and given[k] != v]; \
print('image contents:', actual); \
sys.exit('build arguments do not match the image: ' + ', '.join(wrong) if wrong else 0)"

FROM python:3.12-slim@sha256:f77ac9e44ae96ef2c90b8053ea08c31f8be030f824196b0ae4db6d462c84e51f AS runtime

ARG REVISION=unknown
ARG VERSION=dev
ARG SDK_VERSION=unknown
ARG ALEMBIC_HEAD=unknown
LABEL org.opencontainers.image.title="agent-context-platform" \
      org.opencontainers.image.source="https://github.com/VINIClUS/agent-context-platform" \
      org.opencontainers.image.revision="${REVISION}" \
      org.opencontainers.image.version="${VERSION}" \
      io.agent-context.sdk.version="${SDK_VERSION}" \
      io.agent-context.alembic.head="${ALEMBIC_HEAD}"

# Numeric, dedicated, non-root UID, distinct from the indexer image's 10001.
RUN groupadd --system --gid 10002 platform \
    && useradd --system --uid 10002 --gid 10002 --no-create-home \
        --home-dir /nonexistent --shell /usr/sbin/nologin platform

COPY --from=builder /opt/venv /opt/venv
COPY --from=builder /app/alembic.ini /app/alembic.ini
COPY --from=builder /app/alembic /app/alembic
COPY containers/platform/healthcheck.py /app/healthcheck.py

ENV PATH=/opt/venv/bin:$PATH \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    AGENT_CONTEXT_ALEMBIC_CONFIG=/app/alembic.ini

USER 10002:10002
WORKDIR /app
EXPOSE 8000
CMD ["uvicorn", "agent_context_platform.app:create_app", "--factory", \
     "--host", "0.0.0.0", "--port", "8000", "--no-access-log", "--no-server-header"]
