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
  subprocess in its own session/process group, under a per-run supervisor (an inline
  ``python -I -S`` program, ``_SUPERVISOR``). The supervisor is a Linux child
  subreaper (``PR_SET_CHILD_SUBREAPER``; other platforms fail closed): it forks the
  adapter with the rlimits, and when the adapter exits, or on SIGTERM, it SIGKILLs and
  reaps every descendant, including double-forked and ``setsid`` daemons that left
  the process group, before exiting. ``PDEATHSIG`` is SIGTERM so it still cleans up if
  the runner dies. ``preexec_fn`` is avoided because it is unsafe in threaded parents;
- rlimits (``Limits``): ``RLIMIT_AS`` 512 MiB (``RLIMIT_RSS`` is a no-op on Linux),
  ``RLIMIT_CPU`` 10 s, ``RLIMIT_NOFILE`` 32, ``RLIMIT_FSIZE`` 0, ``RLIMIT_NPROC`` 16,
  ``RLIMIT_CORE`` 0 (a core would hold source text);
- Landlock read confinement (``landlock.py``), applied in the adapter process itself right
  before its ``execv`` (a trampoline the supervisor execs, so the supervisor keeps ``/proc``):
  the adapter can read only its interpreter prefix and stdlib, its virtualenv
  (site-packages: bindings and grammar ``.so`` files), the adapter script, the system library
  dirs, ``/etc/ld.so.cache``, ``/dev/null`` and ``/dev/urandom``; it can write, create,
  truncate, rename and unlink nothing anywhere, and from ABI 4 it cannot bind or connect TCP.
  The restriction is inherited by every descendant and cannot be lifted. Fails closed: when
  Landlock is unavailable (ENOSYS, EOPNOTSUPP, ABI 0, seccomp, unknown architecture) the
  runner raises ``SandboxUnavailable`` instead of running the adapter, unless
  ``Limits.allow_unconfined`` (setting ``AGENT_CONTEXT_INDEXER_ALLOW_UNCONFINED_ADAPTERS``,
  default false, DEV ONLY, logged once as a WARN) is on. A read set that touches a checkout
  root (``Limits.checkout_roots``) is refused with ``UNSAFE_READ_SET``;
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
- on every exit path (success, timeout, overflow, error) the supervisor is sent SIGTERM
  and the process group SIGKILL, both before the supervisor is reaped (``waitid`` with
  ``WNOWAIT``), so a recycled pgid can never be hit.

Known limits (be honest about them):

- ``RLIMIT_NPROC`` is per real UID and counts threads of every process of that UID.
  It is a fork-bomb brake, not an isolation boundary, and only meaningful for a
  non-root UID (root with ``CAP_SYS_RESOURCE`` ignores it, but the container drops
  capabilities). The count includes the indexer worker's own threads when they share
  the UID: with the default of 16 the adapter can fork only while the UID has fewer
  than 16 tasks, otherwise it simply cannot fork, which fails closed. Size it as
  (worker threads + adapters in flight + a small margin) for the deployment, or give
  the adapter runs a dedicated UID. Because the supervisor sweeps descendants,
  survivors of earlier runs no longer accumulate against it. The backstop is the
  compose ``pids_limit``.
- The adapter runs as the same UID as the supervisor, so a compromised parser can SIGKILL
  the supervisor. The runner is therefore ALSO a child subreaper (``prctl`` once,
  process-wide), so the orphans of a dead supervisor re-parent to it, and after every
  run (and at the start of the next) ``_sweep_orphans`` kills and reaps them. It touches
  only children of the runner that are not live supervisors: live ones must carry the
  ``AGENT_CONTEXT_SANDBOX_RUN`` key in their environment with ANY token (a finished run's
  token is dead), zombies must be session leaders or in a session no live supervisor owns.
  Concurrent runs' supervisors, and so their descendants, are excluded; sweep, spawn and
  registration of a supervisor share one lock, so no live supervisor is ever unregistered
  while a sweep runs.
  Side effects and residuals: orphans of any other code in the worker process also
  re-parent to it (subreaper is process-wide), and a zombie sweep can in theory reap a
  foreign ``start_new_session`` child that exited during the run, so the worker should not
  run such children concurrently.
- Won't fix here: same-UID signalling between concurrent runs, and a live orphan that
  scrubbed its environment (``execve`` with an empty one) after killing the supervisor.
  Both are contained by the INFRA-040 per-job PID namespace (a per-job container with
  ``--init`` and ``pids_limit``, so every descendant dies with the job).
- Read isolation is NOT provided by the container (the adapter shares the indexer's
  filesystem view); Landlock provides it. Requirements: Landlock enabled in the kernel's
  LSM list (Ubuntu 22.04+ and GitHub ``ubuntu-latest`` have it) and a container seccomp
  profile that allows the ``landlock_*`` syscalls (Docker's default does, since 23.0;
  otherwise INFRA-040 must add them, and the runner refuses to run until then).
  Residual: the adapter can still read the file it was given (it is on stdin), so what it
  can leak is structural output about that file, which ``base.validate_module`` bounds
  (only text that occurs in the parsed file itself is accepted). Landlock does not hide
  process-level information (``/proc`` is denied to the adapter, but same-UID signalling
  is not) and has no rights newer than ABI 5 for filesystem objects; PLATFORM-037 and
  INFRA-040 should still mount only the checkout being indexed, read-only, and no secrets.
- With the fallback ``Limits.allow_unconfined`` there is NO read isolation (dev only).
- ``RLIMIT_FSIZE`` 0 stops data being written, and Landlock denies creating, truncating,
  renaming and unlinking anywhere, so the adapter cannot alter the filesystem when it is
  enforced. In the dev fallback only the read-only root filesystem and checkout mount
  prevent that, and the empty temporary working directory contains the damage.
- ``RLIMIT_AS`` bounds address space, so it is deliberately generous; the cgroup
  ``mem_limit`` bounds real memory.
- No seccomp filter or namespaces of our own: they would need privileges the container
  does not have; the container flags remove the network and Landlock the filesystem.

Validation (``base.validate_module``) is the second half of the boundary: whatever
the adapter prints is untrusted data. Errors are ``StructuralError`` with a code only.

``serve`` is the adapter-side entry point: read one request from stdin, call the
parse function, print one module. Adapters (PLATFORM-033 to 035) call it from
``__main__``.
"""

from __future__ import annotations

import contextlib
import ctypes
import json
import logging
import os
import selectors
import shutil
import signal
import subprocess
import sys
import tempfile
import threading
import time
import uuid
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import IO, Any, Final

from pydantic import ValidationError

from agent_context_platform.indexing.tree_sitter import landlock
from agent_context_platform.indexing.tree_sitter.base import (
    MAX_OUTPUT_BYTES,
    MAX_REQUEST_BYTES,
    ParsedModule,
    ParseRequest,
    SandboxUnavailable,
    StructuralError,
    StructuralErrorCode,
    parser_fingerprint,
    validate_module,
)
from agent_context_platform.settings import Settings

_LOG = logging.getLogger(__name__)
MAX_STDERR_BYTES: Final = 64 * 1024
_CHUNK: Final = 65_536
_FALLBACK_CWD: Final = "/"
_RUN_KEY: Final = "AGENT_CONTEXT_SANDBOX_RUN"
_DEFAULT_ENV_ALLOWLIST: Final = ("LANG", "LC_ALL")

# Per-run supervisor (constant source; limits JSON and argv arrive as arguments, never
# interpolated). It becomes a child subreaper, so every descendant of the adapter that
# is orphaned (double fork, setsid daemon) is re-parented to it instead of escaping;
# forks the adapter with the rlimits; and, once the adapter exits or on SIGTERM, kills
# and reaps every descendant before it exits itself. With a trampoline (``landlock.py``) the
# adapter child execs it instead of the command: it confines itself, then execs the command. PDEATHSIG is SIGTERM (not SIGKILL)
# so that the supervisor still runs that cleanup if the runner dies.
_SUPERVISOR: Final = r"""
import ctypes, json, os, resource, signal, sys, time
libc = ctypes.CDLL(None, use_errno=True)
if libc.prctl(36, 1, 0, 0, 0) != 0 or libc.prctl(1, int(signal.SIGTERM), 0, 0, 0) != 0:
    os._exit(111)
me = os.getpid()
if os.getppid() != int(sys.argv[1]):  # runner already gone (PDEATHSIG race)
    os._exit(111)
main_status = [None]

def children():
    found = []
    for name in os.listdir("/proc"):
        if not name.isdigit():
            continue
        try:
            with open("/proc/" + name + "/stat", "rb") as handle:
                data = handle.read()
        except OSError:
            continue
        if int(data[data.rindex(b")") + 2:].split()[1]) == me:
            found.append(int(name))
    return found

def sweep():
    while True:
        for pid in children():
            try:
                os.kill(pid, signal.SIGKILL)
            except OSError:
                pass
        try:
            while True:
                pid, status = os.waitpid(-1, os.WNOHANG)
                if pid == 0:
                    break
                if pid == main_child:
                    main_status[0] = status
        except ChildProcessError:
            return
        time.sleep(0.005)

def on_term(signum, frame):
    sweep()
    os._exit(143)

signal.signal(signal.SIGTERM, on_term)
limits = json.loads(sys.argv[2])
trampoline, confine_paths = sys.argv[3], sys.argv[4]
command = sys.argv[5:]
main_child = os.fork()
if main_child == 0:
    try:
        for name, value in limits.items():
            resource.setrlimit(getattr(resource, name), (value, value))
        if trampoline:  # Landlock is applied in the adapter process, right before its execv
            command = [sys.executable, "-I", "-S", trampoline, confine_paths, *command]
            os.execv(sys.executable, command)
        os.execv(command[0], command)
    except BaseException:
        os._exit(120)
while True:
    try:
        _, status = os.waitpid(main_child, 0)
        break
    except InterruptedError:
        continue
main_status[0] = status
sweep()
code = os.waitstatus_to_exitcode(main_status[0])
os._exit(code if code >= 0 else 128 - code)
"""
_TERM_GRACE_SECONDS: Final = 5.0


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
    # Landlock. ``checkout_roots``: directories being indexed, never readable by the adapter.
    # ``extra_read_paths``: the only way to widen the computed read set. ``allow_unconfined``
    # runs adapters without Landlock when the kernel lacks it (DEV ONLY, logged once).
    checkout_roots: tuple[str, ...] = ()
    extra_read_paths: tuple[str, ...] = ()
    allow_unconfined: bool = False

    @classmethod
    def from_settings(cls, settings: Settings, **overrides: Any) -> Limits:
        """Limits carrying the indexer settings (escape hatch and checkout roots)."""
        return cls(
            checkout_roots=settings.indexer_checkout_roots,
            allow_unconfined=settings.indexer_allow_unconfined_adapters,
            **overrides,
        )

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
    if not sys.platform.startswith("linux"):
        raise StructuralError(StructuralErrorCode.SPAWN_FAILED)  # subreaper is Linux-only
    _ensure_subreaper()
    confined = _confinement(command, limits)
    env = {**env, _RUN_KEY: uuid.uuid4().hex}
    workdir = _make_workdir()
    try:
        argv = [
            sys.executable,
            "-I",
            "-S",
            "-c",
            _SUPERVISOR,
            str(os.getpid()),
            json.dumps(limits.rlimits()),
            landlock.__file__ if confined is not None else "",
            json.dumps({"paths": confined or []}),
            *command,
        ]
        with _LIVE_LOCK:  # sweep, spawn and registration are atomic against other sweeps
            _sweep_orphans()
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
            _LIVE_SUPERVISORS.add(process.pid)
        try:
            deadline = time.monotonic() + limits.wall_seconds
            output = _communicate(process, payload, limits, deadline)
            if not _exited(process, deadline):
                raise StructuralError(StructuralErrorCode.TIMEOUT)
        finally:
            _reap(process)
        if confined is not None and process.returncode == landlock.CONFINE_FAILED_EXIT:
            raise SandboxUnavailable  # the kernel refused the ruleset: nothing ran
        if process.returncode != 0:
            raise StructuralError(StructuralErrorCode.NONZERO_EXIT)
        return output
    finally:
        if workdir != _FALLBACK_CWD:
            shutil.rmtree(workdir, ignore_errors=True)


_WARN_LOCK = threading.Lock()
_warned_unconfined = False


def _confinement(command: Sequence[str], limits: Limits) -> tuple[str, ...] | None:
    """Read set to confine the adapter to, or ``None`` only under the dev escape hatch."""
    if landlock.abi_version() < 1:
        if not limits.allow_unconfined:
            raise SandboxUnavailable
        _warn_unconfined()
        return None
    paths = landlock.read_set(command, limits.extra_read_paths)
    try:
        landlock.check_read_set(paths, limits.checkout_roots)
    except landlock.UnsafeReadSet:
        raise StructuralError(StructuralErrorCode.UNSAFE_READ_SET) from None
    return paths


def _warn_unconfined() -> None:
    global _warned_unconfined
    with _WARN_LOCK:
        if _warned_unconfined:
            return
        _warned_unconfined = True
    _LOG.warning(
        "Landlock is unavailable and unconfined structural adapters are allowed "
        "(AGENT_CONTEXT_INDEXER_ALLOW_UNCONFINED_ADAPTERS): adapters can read every file "
        "this process can. Development only."
    )


def _exited(process: subprocess.Popen[bytes], deadline: float) -> bool:
    """Wait for the supervisor to exit *without reaping it*, so its pgid cannot be reused."""
    while True:
        try:
            if os.waitid(os.P_PID, process.pid, os.WEXITED | os.WNOHANG | os.WNOWAIT) is not None:
                return True
        except ChildProcessError:
            return True
        if time.monotonic() >= deadline:
            return False
        time.sleep(0.005)


def _reap(process: subprocess.Popen[bytes]) -> None:
    """Stop every process of the run on every exit path, then reap the supervisor.

    SIGTERM goes to the supervisor alone (not the group) so it can sweep descendants that
    left the process group; a group SIGKILL afterwards, before the supervisor is reaped,
    is the fallback and cannot hit a recycled pgid.
    """
    if not _exited(process, time.monotonic()):
        with contextlib.suppress(ProcessLookupError, PermissionError):
            os.kill(process.pid, signal.SIGTERM)
        _exited(process, time.monotonic() + _TERM_GRACE_SECONDS)
    with contextlib.suppress(ProcessLookupError, PermissionError):
        os.killpg(process.pid, signal.SIGKILL)
    for stream in (process.stdin, process.stdout, process.stderr):
        if stream is not None:
            with contextlib.suppress(OSError):
                stream.close()
    try:
        with _LIVE_LOCK:
            _sweep_orphans(process.pid)
    finally:
        process.wait()
        with _LIVE_LOCK:
            _LIVE_SUPERVISORS.discard(process.pid)


_LIVE_SUPERVISORS: set[int] = set()
_LIVE_LOCK = threading.RLock()  # spawn + registration + every sweep run under it
_SUBREAPER_LOCK = threading.Lock()
_subreaper_set = False
_ORPHAN_SWEEP_SECONDS: Final = 3.0


def _ensure_subreaper() -> None:
    """Make this process a child subreaper (once): orphans of a dead supervisor come here."""
    global _subreaper_set
    with _SUBREAPER_LOCK:
        if _subreaper_set:
            return
        try:
            libc = ctypes.CDLL(None, use_errno=True)
            failed = libc.prctl(36, 1, 0, 0, 0) != 0  # PR_SET_CHILD_SUBREAPER
        except (OSError, AttributeError):
            failed = True
        if failed:
            raise StructuralError(StructuralErrorCode.SPAWN_FAILED)
        _subreaper_set = True


@dataclass(frozen=True, slots=True)
class _Stat:
    state: str
    ppid: int
    sid: int
    starttime: int


def _stat(pid: int) -> _Stat | None:
    try:
        with open(f"/proc/{pid}/stat", "rb") as handle:
            data = handle.read()
        fields = data[data.rindex(b")") + 2 :].split()
        return _Stat(fields[0].decode(), int(fields[1]), int(fields[3]), int(fields[19]))
    except (OSError, ValueError, IndexError):
        return None


def _has_run_key(pid: int) -> bool:
    """True when the process environment carries ``_RUN_KEY`` with ANY token."""
    try:
        with open(f"/proc/{pid}/environ", "rb") as handle:
            prefix = f"{_RUN_KEY}=".encode()
            return any(item.startswith(prefix) for item in handle.read().split(b"\0"))
    except OSError:
        return False


def _sweep_orphans(supervisor: int | None = None) -> None:
    """Kill and reap every orphan of a finished run (supervisors SIGKILLed by the adapter).

    The runner is a subreaper, so those orphans are its children. What is touched: children
    of the runner that are not a live supervisor (``_LIVE_SUPERVISORS``, so concurrent runs
    stay untouched) and either carry ``_RUN_KEY`` in their environment with ANY token (the
    token of a finished run is dead, and a live run's processes are its supervisor's
    descendants, not our children until their supervisor dies), or are zombies (nothing to
    read) that led a session or sat in a session not owned by a live supervisor. Other
    children of the worker are never touched. Runs at the start and end of every run and
    repeats until nothing is left, because killing an orphan re-parents its own children.
    """
    me = os.getpid()
    deadline = time.monotonic() + _ORPHAN_SWEEP_SECONDS
    while time.monotonic() < deadline:
        with _LIVE_LOCK:
            live = set(_LIVE_SUPERVISORS) - {supervisor}
        victims: list[int] = []
        for name in os.listdir("/proc"):
            if not name.isdigit():
                continue
            pid = int(name)
            info = None if pid in live or pid == supervisor else _stat(pid)
            if info is None or info.ppid != me:
                continue
            if info.state == "Z":
                if info.sid == pid or (info.sid not in live and info.sid != os.getsid(0)):
                    victims.append(pid)
            elif _has_run_key(pid):
                victims.append(pid)
        if not victims:
            return
        for pid in victims:
            with contextlib.suppress(ProcessLookupError, PermissionError):
                os.kill(pid, signal.SIGKILL)
        for pid in victims:
            with contextlib.suppress(ChildProcessError):
                for _ in range(200):
                    if os.waitpid(pid, os.WNOHANG)[0] != 0:
                        break
                    time.sleep(0.005)


def _make_workdir() -> str:
    """Fresh empty temporary directory; ``/`` when nothing is writable (read-only container)."""
    try:
        return tempfile.mkdtemp(prefix="agent-context-sandbox-")
    except OSError:
        return _FALLBACK_CWD


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
