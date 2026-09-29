"""Run a structural adapter in a resource-limited subprocess and validate its output.

Sandbox design. The indexer already runs as a hardened container, so there is no
nested Docker and no privileged syscall filter here. Two layers cooperate:

Container (``containers/indexer/Dockerfile`` and the INFRA-040 compose service),
what only a container can provide:

- ``network_mode: none`` (no network for the parser),
- ``read_only: true`` root filesystem, the checkout mounted read-only,
- a non-root numeric UID dedicated to the indexer worker, ``cap_drop: [ALL]``,
  ``no-new-privileges``,
- ``pids_limit``, ``mem_limit`` and ``cpus`` as cgroup backstops.

This runner, per adapter run:

- spawns ``command`` (an absolute argv, never a shell, never untrusted text) as a
  subprocess in its own session/process group, through an inline ``python -I -S``
  trampoline that sets the rlimits and ``execv``s the adapter. ``preexec_fn`` is
  avoided because it is unsafe in threaded parents;
- rlimits (``Limits``): ``RLIMIT_AS`` 512 MiB (``RLIMIT_RSS`` is a no-op on Linux),
  ``RLIMIT_CPU`` 10 s, ``RLIMIT_NOFILE`` 32, ``RLIMIT_FSIZE`` 0, ``RLIMIT_NPROC`` 16,
  ``RLIMIT_CORE`` 0 (a core would hold source text);
- a wall-clock deadline (default 20 s) that also covers hangs a CPU limit cannot;
- an environment allowlist (``LANG``/``LC_ALL`` plus what the caller passes), so no
  secret of the worker leaks in; the working directory is a fresh empty temporary
  directory, removed afterwards (``/`` when no temporary directory can be created,
  which is the read-only container: nothing is writable there anyway, so INFRA-040
  needs no writable ``/tmp`` for the indexer);
- the request is written to a pipe that is then closed; stdout is bounded
  (``MAX_OUTPUT_BYTES``) and stderr is bounded and discarded (tracebacks carry
  source). Overflow kills the whole process group. Reads and the write are
  multiplexed, so an adapter that never reads its input cannot deadlock the runner;
- on every exit path the process group is killed and the child reaped.

Known limits (be honest about them):

- ``RLIMIT_NPROC`` is per real UID and counts threads of every process of that UID.
  It is a fork-bomb brake, not an isolation boundary, and only meaningful for a
  non-root UID (root with ``CAP_SYS_RESOURCE`` ignores it, but the container drops
  capabilities). With a low limit and a shared UID the adapter simply cannot fork,
  which is the intent. The backstop is the compose ``pids_limit``.
- ``RLIMIT_FSIZE`` 0 stops data being written, yet creating empty files, unlinking
  or renaming is still possible where the filesystem allows it; only the read-only
  root filesystem and the read-only checkout mount prevent that. Outside the
  container the empty temporary working directory contains the damage.
- ``RLIMIT_AS`` bounds address space, so it is deliberately generous; the cgroup
  ``mem_limit`` bounds real memory.
- No seccomp or namespaces: they would need privileges the container does not have,
  and the container flags already remove the network and filesystem writes.

Validation (``base.validate_module``) is the second half of the boundary: whatever
the adapter prints is untrusted data. Errors are ``StructuralError`` with a code only.

``serve`` is the adapter-side entry point: read one request from stdin, call the
parse function, print one module. Adapters (PLATFORM-033 to 035) call it from
``__main__``.
"""

from __future__ import annotations

import contextlib
import json
import os
import selectors
import shutil
import signal
import subprocess
import sys
import tempfile
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import IO, Any, Final

from pydantic import ValidationError

from agent_context_platform.indexing.tree_sitter.base import (
    MAX_OUTPUT_BYTES,
    MAX_REQUEST_BYTES,
    ParsedModule,
    ParseRequest,
    StructuralError,
    StructuralErrorCode,
    parser_fingerprint,
    validate_module,
)

MAX_STDERR_BYTES: Final = 64 * 1024
_CHUNK: Final = 65_536
_FALLBACK_CWD: Final = "/"
_DEFAULT_ENV_ALLOWLIST: Final = ("LANG", "LC_ALL")

# Applies the rlimits, then replaces itself with the adapter. Constant source; the
# limits (JSON) and argv are passed as arguments, never interpolated.
_TRAMPOLINE: Final = (
    "import json,os,resource,sys\n"
    "for name,value in json.loads(sys.argv[1]).items():\n"
    "    resource.setrlimit(getattr(resource,name),(value,value))\n"
    "os.execv(sys.argv[2],sys.argv[2:])\n"
)


@dataclass(frozen=True, slots=True)
class Limits:
    """Per-run resource bounds; every value must be positive (FSIZE and CORE are fixed at 0)."""

    address_space_bytes: int = 512 * 1024 * 1024
    cpu_seconds: int = 10
    open_files: int = 32
    processes: int = 16
    wall_seconds: float = 20.0
    max_output_bytes: int = MAX_OUTPUT_BYTES
    max_stderr_bytes: int = MAX_STDERR_BYTES

    def __post_init__(self) -> None:
        for name in (
            "address_space_bytes",
            "cpu_seconds",
            "open_files",
            "processes",
            "wall_seconds",
            "max_output_bytes",
            "max_stderr_bytes",
        ):
            if getattr(self, name) <= 0:
                raise ValueError(f"{name} must be positive")

    def rlimits(self) -> dict[str, int]:
        return {
            "RLIMIT_AS": self.address_space_bytes,
            "RLIMIT_CPU": self.cpu_seconds,
            "RLIMIT_NOFILE": self.open_files,
            "RLIMIT_NPROC": self.processes,
            "RLIMIT_FSIZE": 0,
            "RLIMIT_CORE": 0,
        }


class SandboxedAdapter:
    """``StructuralAdapter`` that runs ``command`` in the sandbox and validates the answer."""

    def __init__(
        self,
        command: Sequence[str],
        *,
        language: str,
        parser_name: str,
        parser_version: str,
        parser_config: Mapping[str, Any] | None = None,
        limits: Limits | None = None,
        env: Mapping[str, str] | None = None,
    ) -> None:
        if not command or not os.path.isabs(command[0]):
            raise ValueError("command must be an argv whose first item is an absolute path")
        self._command = tuple(command)
        self._language = language
        self._limits = limits or Limits()
        self._env = _child_env(env)
        self._fingerprint = parser_fingerprint(parser_name, parser_version, parser_config)

    @property
    def language(self) -> str:
        return self._language

    @property
    def fingerprint(self) -> str:
        return self._fingerprint

    def parse(self, request: ParseRequest) -> ParsedModule:
        if any(item.language != self._language for item in request.files):
            raise StructuralError(StructuralErrorCode.LANGUAGE_MISMATCH)
        payload = request.model_dump_json().encode("utf-8")
        if len(payload) > MAX_REQUEST_BYTES:
            raise StructuralError(StructuralErrorCode.INPUT_TOO_LARGE)
        raw = _run(self._command, payload, self._limits, self._env)
        try:
            module = ParsedModule.model_validate_json(raw)
        except ValidationError:
            raise StructuralError(StructuralErrorCode.SCHEMA_VIOLATION) from None
        except ValueError:
            raise StructuralError(StructuralErrorCode.MALFORMED_OUTPUT) from None
        return validate_module(request, module, expected_fingerprint=self._fingerprint)


def _child_env(extra: Mapping[str, str] | None) -> dict[str, str]:
    env = {name: os.environ[name] for name in _DEFAULT_ENV_ALLOWLIST if name in os.environ}
    env.update(extra or {})
    return env


def _run(command: Sequence[str], payload: bytes, limits: Limits, env: dict[str, str]) -> bytes:
    workdir = _make_workdir()
    try:
        argv = [
            sys.executable,
            "-I",
            "-S",
            "-c",
            _TRAMPOLINE,
            json.dumps(limits.rlimits()),
            *command,
        ]
        try:
            process = subprocess.Popen(
                argv,
                stdin=subprocess.PIPE,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                cwd=workdir,
                env=env,
                close_fds=True,
                start_new_session=True,
            )
        except OSError:
            raise StructuralError(StructuralErrorCode.SPAWN_FAILED) from None
        try:
            deadline = time.monotonic() + limits.wall_seconds
            output = _communicate(process, payload, limits, deadline)
            status = process.wait(timeout=max(deadline - time.monotonic(), 0.01))
        except subprocess.TimeoutExpired:
            raise StructuralError(StructuralErrorCode.TIMEOUT) from None
        finally:
            _reap(process)
        if status != 0:
            raise StructuralError(StructuralErrorCode.NONZERO_EXIT)
        return output
    finally:
        if workdir != _FALLBACK_CWD:
            shutil.rmtree(workdir, ignore_errors=True)


def _make_workdir() -> str:
    """Fresh empty temporary directory; ``/`` when nothing is writable (read-only container)."""
    try:
        return tempfile.mkdtemp(prefix="agent-context-sandbox-")
    except OSError:
        return _FALLBACK_CWD


def _reap(process: subprocess.Popen[bytes]) -> None:
    with contextlib.suppress(ProcessLookupError, PermissionError):
        os.killpg(process.pid, signal.SIGKILL)
    for stream in (process.stdin, process.stdout, process.stderr):
        if stream is not None:
            with contextlib.suppress(OSError):
                stream.close()
    with contextlib.suppress(subprocess.TimeoutExpired):
        process.wait(timeout=5)


def _communicate(
    process: subprocess.Popen[bytes], payload: bytes, limits: Limits, deadline: float
) -> bytes:
    """Feed ``payload`` and collect bounded stdout before the monotonic ``deadline``."""
    assert process.stdin is not None
    assert process.stdout is not None
    assert process.stderr is not None
    stdin, stdout, stderr = process.stdin.fileno(), process.stdout.fileno(), process.stderr.fileno()
    for fd in (stdin, stdout, stderr):
        os.set_blocking(fd, False)
    out = bytearray()
    err_total = 0
    sent = 0
    with selectors.DefaultSelector() as selector:
        selector.register(stdin, selectors.EVENT_WRITE)
        selector.register(stdout, selectors.EVENT_READ)
        selector.register(stderr, selectors.EVENT_READ)
        while selector.get_map():
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise StructuralError(StructuralErrorCode.TIMEOUT)
            for key, _ in selector.select(remaining):
                fd = key.fd
                if fd == stdin:
                    sent = _feed(selector, process.stdin, payload, sent)
                    continue
                chunk = os.read(fd, _CHUNK)
                if not chunk:
                    selector.unregister(fd)
                elif fd == stdout:
                    out += chunk
                    if len(out) > limits.max_output_bytes:
                        raise StructuralError(StructuralErrorCode.OUTPUT_TOO_LARGE)
                else:
                    err_total += len(chunk)  # discarded: only counted
                    if err_total > limits.max_stderr_bytes:
                        raise StructuralError(StructuralErrorCode.STDERR_TOO_LARGE)
    return bytes(out)


def _feed(selector: selectors.BaseSelector, stream: IO[bytes], payload: bytes, sent: int) -> int:
    """Write what the pipe accepts; close stdin (EOF) when done or when the child hung up."""
    try:
        sent += os.write(stream.fileno(), payload[sent : sent + _CHUNK])
    except BlockingIOError:
        return sent
    except OSError:
        sent = len(payload)
    if sent >= len(payload):
        selector.unregister(stream.fileno())
        with contextlib.suppress(OSError):
            stream.close()
    return sent


def serve(
    parse: Callable[[ParseRequest], ParsedModule],
    stdin: IO[bytes] | None = None,
    stdout: IO[bytes] | None = None,
) -> int:
    """Adapter-side entry point: one bounded request in, one module out; exit status.

    Prints nothing to stderr and never the input: on any failure the process
    exits non-zero and the runner reports a code.
    """
    source = stdin if stdin is not None else sys.stdin.buffer
    sink = stdout if stdout is not None else sys.stdout.buffer
    raw = source.read(MAX_REQUEST_BYTES + 1)
    if len(raw) > MAX_REQUEST_BYTES:
        return 2
    try:
        request = ParseRequest.model_validate_json(raw)
        module = parse(request)
    except (ValidationError, ValueError, RecursionError, MemoryError, StructuralError):
        return 3
    sink.write(module.model_dump_json().encode("utf-8"))
    sink.flush()
    return 0
