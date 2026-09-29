# Indexer image (PLATFORM-032). It hosts the structural-index runner, which starts each
# tree-sitter adapter as an rlimit-bound subprocess (see indexing/tree_sitter/runner.py).
# Network and filesystem isolation of that subprocess come from how this image is run:
#
#   docker run --read-only --network none --cap-drop ALL \
#     --security-opt no-new-privileges --pids-limit 64 --memory 1g \
#     --mount type=bind,src=<checkout>,dst=/work,readonly <image> ...
#
# INFRA-040 encodes the same flags in compose (read_only, network_mode: none,
# cap_drop: [ALL], no-new-privileges, pids_limit, mem_limit). The image itself installs
# nothing but the locked runtime dependencies: no compilers, git or shell tooling are
# added, and the entrypoint is exec-form so no shell runs at start-up.
#
# Build from the repository root (the private SDK wheel must be in build/sdk/):
#   docker build -f containers/indexer/Dockerfile -t agent-context-indexer .

# Base pinned by digest (python:3.12-slim); bump deliberately, together with the lockfile.
FROM python:3.12-slim@sha256:f77ac9e44ae96ef2c90b8053ea08c31f8be030f824196b0ae4db6d462c84e51f AS builder

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never \
    UV_PROJECT_ENVIRONMENT=/opt/venv \
    PIP_DISABLE_PIP_VERSION_CHECK=1

RUN pip install --no-cache-dir uv==0.11.28

WORKDIR /app
# Keep the wheel name in step with [tool.uv.sources] in pyproject.toml.
COPY build/sdk/agent_context_sdk-0.3.2-py3-none-any.whl build/sdk/
COPY pyproject.toml uv.lock README.md ./
COPY src src
RUN uv sync --frozen --no-dev --no-editable --python /usr/local/bin/python

FROM python:3.12-slim@sha256:f77ac9e44ae96ef2c90b8053ea08c31f8be030f824196b0ae4db6d462c84e51f AS runtime

# Numeric, dedicated, non-root UID: RLIMIT_NPROC is per UID, so nothing else may share it.
RUN groupadd --system --gid 10001 indexer \
    && useradd --system --uid 10001 --gid 10001 --no-create-home \
        --home-dir /nonexistent --shell /usr/sbin/nologin indexer

COPY --from=builder /opt/venv /opt/venv

ENV PATH=/opt/venv/bin:$PATH \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

USER 10001:10001
WORKDIR /
ENTRYPOINT ["python"]
