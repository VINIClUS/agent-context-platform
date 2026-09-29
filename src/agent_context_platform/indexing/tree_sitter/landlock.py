"""Landlock read confinement for structural adapters (PLATFORM-032b).

A compromised native parser must not read the checkout, other checkouts, ``.env`` files,
``/etc`` secrets or ``$HOME``: the source arrives on stdin, so the adapter never needs the
filesystem beyond its own interpreter. This module computes the small set of paths the
adapter needs and confines the process to it with the Linux Landlock LSM (unprivileged,
inherited across ``execve``, irreversible).

Standalone on purpose (stdlib only, no package imports): it is also the *trampoline* that
the runner's supervisor executes as ``python -I -S landlock.py <json> <command...>`` in
the forked adapter process. The trampoline applies the ruleset and then ``execv``s the
adapter, so the restriction covers the adapter and everything it spawns, and never the
supervisor (which needs ``/proc`` for its sweep).

Design:

- every filesystem access right of the detected ABI is *handled*, so everything is denied
  unless a rule allows it; the rules grant only ``read_file``/``read_dir``/``execute``
  (never write, ``make_*`` or ``remove_*``), so the sandbox also cannot create, truncate,
  rename or unlink anything;
- from ABI 4 the TCP bind/connect rights are handled too (defense in depth: the container
  has no network);
- the allowed set is computed in ONE place (``read_set``) from the adapter command and can
  only be widened through ``Limits.extra_read_paths``. ``check_read_set`` refuses a set that
  breaks the absolute floor (never ``/``, ``$HOME``, ``/etc``, ``/tmp``, ... or any other
  path of depth 1 except the fixed system library directories) or that touches a checkout
  root (equal, ancestor or inside);
- the interpreter's prefix is granted only after the interpreter has been positively
  identified (its real path has ``lib/pythonX.Y`` beside it, or it is the host's
  ``sys.executable``), never by position: a direct-executable adapter gets its own file only;
- ``python -m <module>`` is supported: the parent resolves the top-level package with
  ``find_spec`` and grants that package directory only. Because the import system lists the
  directory it searches, the package's parent directory gets ``read_dir`` alone (names, no
  file content), unless it is already covered;
- the trampoline confirms the ruleset on a dedicated status pipe (one byte, written after
  ``landlock_restrict_self`` succeeded, close-on-exec before the adapter starts), so an
  adapter's own exit status can never be mistaken for a confinement failure;
- when the kernel cannot enforce it the runner fails closed (``LandlockUnavailable``), and a
  kernel refusal in the adapter process fails closed even with the dev escape hatch on.

Kernel requirement: Landlock must be enabled in the LSM list (``CONFIG_SECURITY_LANDLOCK``
and ``landlock`` in ``/sys/kernel/security/lsm`` or the ``lsm=`` boot parameter), and the
container's seccomp profile must allow ``landlock_create_ruleset``, ``landlock_add_rule``
and ``landlock_restrict_self`` (Docker's default profile does since 23.0; ``prctl`` is
allowed everywhere).

Known limits: rights added by ABIs newer than the ones listed here cannot be named, so they
stay unhandled (the newest fs right known is ``IOCTL_DEV``, ABI 5); scopes (ABI 6) are not
used. Structural output about the input file itself is the residual channel and is what
``base.validate_module`` bounds.
"""

from __future__ import annotations

import ctypes
import importlib.util
import json
import os
import platform
import re
import stat
import struct
import sys
from collections.abc import Iterable, Sequence
from typing import Final, NamedTuple

# Syscall numbers, ``landlock_create_ruleset``/``landlock_add_rule``/``landlock_restrict_self``.
# Kernel UAPI: arch/x86/entry/syscalls/syscall_64.tbl (444, 445, 446) and
# include/uapi/asm-generic/unistd.h (aarch64 uses the generic table: 444, 445, 446).
# Any other architecture fails closed until it is added here and tested.
SYSCALLS: Final[dict[str, tuple[int, int, int]]] = {
    "x86_64": (444, 445, 446),
    "aarch64": (444, 445, 446),
}

PR_SET_NO_NEW_PRIVS: Final = 38
LANDLOCK_CREATE_RULESET_VERSION: Final = 1 << 0
LANDLOCK_RULE_PATH_BENEATH: Final = 1

FS_EXECUTE: Final = 1 << 0
FS_WRITE_FILE: Final = 1 << 1
FS_READ_FILE: Final = 1 << 2
FS_READ_DIR: Final = 1 << 3
FS_REMOVE_DIR: Final = 1 << 4
FS_REMOVE_FILE: Final = 1 << 5
FS_MAKE_CHAR: Final = 1 << 6
FS_MAKE_DIR: Final = 1 << 7
FS_MAKE_REG: Final = 1 << 8
FS_MAKE_SOCK: Final = 1 << 9
FS_MAKE_FIFO: Final = 1 << 10
FS_MAKE_BLOCK: Final = 1 << 11
FS_MAKE_SYM: Final = 1 << 12
FS_REFER: Final = 1 << 13  # ABI 2
FS_TRUNCATE: Final = 1 << 14  # ABI 3
FS_IOCTL_DEV: Final = 1 << 15  # ABI 5
NET_BIND_TCP: Final = 1 << 0  # ABI 4
NET_CONNECT_TCP: Final = 1 << 1

_FS_V1: Final = (1 << 13) - 1  # bits 0..12
_FS_ABI2: Final = _FS_V1 | FS_REFER
_FS_ABI3: Final = _FS_ABI2 | FS_TRUNCATE  # ABI 4 adds only network rights
_FS_ABI5: Final = _FS_ABI3 | FS_IOCTL_DEV
_NET_ALL: Final = NET_BIND_TCP | NET_CONNECT_TCP

DIR_RIGHTS: Final = FS_EXECUTE | FS_READ_FILE | FS_READ_DIR
FILE_RIGHTS: Final = FS_EXECUTE | FS_READ_FILE
DEVICE_RIGHTS: Final = FS_READ_FILE

# Exit statuses of the trampoline. The runner does NOT read them as a signal (an adapter can
# exit with any status); it reads the handshake byte the trampoline writes on the status pipe.
CONFINE_FAILED_EXIT: Final = 121  # informational: the status pipe is the signal, not this
EXEC_FAILED_EXIT: Final = 120
HANDSHAKE: Final = b"1"

SYSTEM_LIBRARY_DIRS: Final = ("/lib", "/lib64", "/usr/lib", "/usr/lib64", "/usr/local/lib")
SYSTEM_FILES: Final = ("/etc/ld.so.cache", "/dev/null", "/dev/urandom")
# Trees no read rule may equal or contain (paths below them, such as a venv under /home, are
# fine). ``$HOME`` is added at check time.
FORBIDDEN_TREES: Final = (
    "/home",
    "/root",
    "/etc",
    "/srv",
    "/var",
    "/mnt",
    "/media",
    "/tmp",
    "/proc",
    "/sys",
    "/run",
)


class LandlockUnavailable(Exception):
    """The kernel cannot enforce a Landlock ruleset (content-free)."""


class UnsafeReadSet(Exception):
    """An allowed path is, contains or lies inside a checkout root (content-free)."""


def fs_mask(abi: int) -> int:
    """Every filesystem access right known to ``abi`` (ABI above 5 add no new named right)."""
    if abi < 1:
        return 0
    if abi >= 5:
        return _FS_ABI5
    return _FS_V1 if abi == 1 else _FS_ABI2 if abi == 2 else _FS_ABI3


def net_mask(abi: int) -> int:
    return _NET_ALL if abi >= 4 else 0


def _libc() -> ctypes.CDLL:
    return ctypes.CDLL(None, use_errno=True)


def _numbers() -> tuple[int, int, int]:
    try:
        return SYSCALLS[platform.machine()]
    except KeyError:
        raise LandlockUnavailable from None


def abi_version() -> int:
    """Landlock ABI of the running kernel; 0 when unavailable (ENOSYS, EOPNOTSUPP, seccomp)."""
    try:
        create, _, _ = _numbers()
        libc = _libc()
        result = int(libc.syscall(create, None, 0, LANDLOCK_CREATE_RULESET_VERSION))
    except (LandlockUnavailable, OSError, AttributeError):
        return 0
    return result if result > 0 else 0


def _ruleset_attr(abi: int) -> bytes:
    size = 8 if abi < 4 else 16
    return struct.pack("<QQ", fs_mask(abi), net_mask(abi))[:size]


def restrict(
    paths: Iterable[str],
    abi: int | None = None,
    listing: Iterable[str] = (),
) -> None:
    """Confine THIS process (and its future children) to read/execute below ``paths``.

    ``listing`` directories get ``read_dir`` only (entry names, never file content).
    Irreversible. Raises ``LandlockUnavailable`` and applies nothing when any step fails.
    """
    level = abi_version() if abi is None else abi
    if level < 1:
        raise LandlockUnavailable
    create, add_rule, restrict_self = _numbers()
    libc = _libc()
    attr = _ruleset_attr(level)
    ruleset = int(libc.syscall(create, attr, len(attr), 0))
    if ruleset < 0:
        raise LandlockUnavailable
    try:
        for path in paths:
            _allow(libc, add_rule, ruleset, path, None)
        for path in listing:
            _allow(libc, add_rule, ruleset, path, FS_READ_DIR)
        if libc.prctl(PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0) != 0:
            raise LandlockUnavailable
        if libc.syscall(restrict_self, ruleset, 0) != 0:
            raise LandlockUnavailable
    finally:
        os.close(ruleset)


def _allow(libc: ctypes.CDLL, add_rule: int, ruleset: int, path: str, only: int | None) -> None:
    try:
        fd = os.open(path, os.O_PATH | os.O_CLOEXEC)
    except OSError:
        raise LandlockUnavailable from None
    try:
        mode = os.fstat(fd).st_mode
        if stat.S_ISDIR(mode):
            rights = DIR_RIGHTS if only is None else only
        elif stat.S_ISREG(mode):
            rights = FILE_RIGHTS
        else:
            rights = DEVICE_RIGHTS  # a device or another special file: read only
        beneath = struct.pack("=Qi", rights, fd)  # packed: u64 allowed_access, s32 parent_fd
        if libc.syscall(add_rule, ruleset, LANDLOCK_RULE_PATH_BENEATH, beneath, 0) != 0:
            raise LandlockUnavailable
    finally:
        os.close(fd)


class ReadSet(NamedTuple):
    """What an adapter may touch: ``read`` (read/execute below) and ``listing`` (names only)."""

    read: tuple[str, ...]
    listing: tuple[str, ...] = ()


_PYTHON_NAME = re.compile(r"^python(\d+(\.\d+)*)?$")


def _real(path: str) -> str:
    return os.path.realpath(path)


def _python_prefix(executable: str) -> str | None:
    """Prefix of ``executable`` when it is positively a Python interpreter, else ``None``.

    Positive means: the real path is the host's ``sys.executable``, or it is named
    ``pythonX[.Y]`` with a ``lib/python<ver>`` directory beside it. Never decided by position.
    """
    real = _real(executable)
    prefix = os.path.dirname(os.path.dirname(real))
    if real == _real(sys.executable) and os.path.isdir(os.path.join(prefix, "lib")):
        return prefix
    if not _PYTHON_NAME.match(os.path.basename(real)):
        return None
    lib = os.path.join(prefix, "lib")
    try:
        if any(name.startswith("python3") for name in os.listdir(lib)):
            return prefix
    except OSError:
        return None
    return None


def _module_locations(module: str) -> tuple[list[str], list[str]]:
    """(package dirs or module files, their parent dirs) of the top-level of ``module``."""
    try:
        spec = importlib.util.find_spec(module.split(".")[0])
    except (ImportError, ValueError, AttributeError):
        return [], []
    if spec is None:
        return [], []
    places = list(spec.submodule_search_locations or [])
    if not places and spec.origin and os.path.isfile(spec.origin):
        places = [spec.origin]
    real = [_real(item) for item in places]
    return real, [os.path.dirname(item) for item in real]


def read_set(command: Sequence[str], extra: Iterable[str] = ()) -> ReadSet:
    """The only paths an adapter started with ``command`` may read (real paths, sorted).

    The single place that decides it:

    - the executable file, plus, only when it is positively a Python interpreter, its
      ``lib`` directory (stdlib, extension modules) and the virtualenv it runs from
      (site-packages: the tree-sitter bindings and grammar ``.so`` files, ``pyvenv.cfg``);
    - the adapter script when ``command[1]`` is one (the file only, never its directory), or
      the top-level package of ``command[1:3] == ["-m", module]`` resolved by ``find_spec``
      in this process (the package directory only, its parent listable);
    - the system library directories, ``/etc/ld.so.cache`` and two device nodes;
    - ``extra`` (``Limits.extra_read_paths``).

    Paths that do not exist are left out. ``check_read_set`` validates the result.
    """
    found: set[str] = set()
    interpreter = command[0]
    found.add(_real(interpreter))
    prefix = _python_prefix(interpreter)
    if prefix is not None:
        found.add(os.path.join(prefix, "lib"))
        venv = os.path.dirname(os.path.dirname(os.path.abspath(interpreter)))
        if os.path.isfile(os.path.join(venv, "pyvenv.cfg")):
            found.add(_real(venv))
    listing: set[str] = set()
    if len(command) > 2 and command[1] == "-m":
        places, parents = _module_locations(command[2])
        found.update(places)
        listing.update(parents)
    elif len(command) > 1 and os.path.isabs(command[1]) and os.path.isfile(command[1]):
        found.add(_real(command[1]))  # the script; later arguments are data, never granted
    found.update(_real(item) for item in (*SYSTEM_LIBRARY_DIRS, *SYSTEM_FILES, *extra))
    read = tuple(sorted(item for item in found if os.path.lexists(item)))
    uncovered = (item for item in listing if os.path.isdir(item) and not _covered(item, read))
    return ReadSet(read, tuple(sorted(uncovered)))


def _covered(path: str, roots: Iterable[str]) -> bool:
    return any(_within(path, root) for root in roots)


def _within(path: str, root: str) -> bool:
    return path == root or path.startswith(root.rstrip("/") + "/")


def _depth(path: str) -> int:
    return len([part for part in path.split("/") if part])


def _floor_violation(path: str) -> bool:
    """True for ``/``, any depth-1 path but the fixed library dirs, and any forbidden tree."""
    fixed = {*SYSTEM_LIBRARY_DIRS, *(_real(item) for item in SYSTEM_LIBRARY_DIRS)}
    if _depth(path) <= 1 and path not in fixed:
        return True
    trees = (*FORBIDDEN_TREES, _real(os.path.expanduser("~")))
    return any(_within(tree, path) for tree in trees)  # equal to, or an ancestor of, a tree


def check_read_set(rules: ReadSet | Iterable[str], checkout_roots: Iterable[str]) -> None:
    """Refuse a set that breaks the floor or has a path equal to, above or in a checkout root.

    An empty ``checkout_roots`` is refused too: the overlap check must never be disabled by
    omission.
    """
    roots = [_real(root) for root in checkout_roots]
    if not roots:
        raise UnsafeReadSet
    paths = (*rules.read, *rules.listing) if isinstance(rules, ReadSet) else tuple(rules)
    for path in paths:
        real = _real(path)
        if _floor_violation(real):
            raise UnsafeReadSet
        if any(_within(real, root) or _within(root, real) for root in roots):
            raise UnsafeReadSet


def _trampoline(argv: Sequence[str]) -> None:
    """``landlock.py <json {"read": [], "listing": [], "status_fd": n}> <command...>``.

    Confines, writes the handshake byte on the status pipe, makes the pipe close-on-exec (so
    the adapter never holds it and cannot forge the byte) and execs the adapter.
    """
    try:
        plan = json.loads(argv[1])
        restrict(plan["read"], listing=plan["listing"])
        os.write(plan["status_fd"], HANDSHAKE)
        os.set_inheritable(plan["status_fd"], False)
    except BaseException:
        os._exit(CONFINE_FAILED_EXIT)
    command = list(argv[2:])
    try:
        os.execv(command[0], command)
    except BaseException:
        os._exit(EXEC_FAILED_EXIT)


if __name__ == "__main__":
    _trampoline(sys.argv)
