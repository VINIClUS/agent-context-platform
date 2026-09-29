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
  only be widened through ``Limits.extra_read_paths``; ``check_read_set`` refuses a set
  that touches a checkout root (equal, ancestor or inside);
- when the kernel cannot enforce it the runner fails closed (``LandlockUnavailable``).

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
import json
import os
import platform
import stat
import struct
import sys
from collections.abc import Iterable, Sequence
from typing import Final

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

# Exit statuses of the trampoline (the runner maps CONFINE_FAILED to SandboxUnavailable).
CONFINE_FAILED_EXIT: Final = 121
EXEC_FAILED_EXIT: Final = 120

SYSTEM_LIBRARY_DIRS: Final = ("/lib", "/lib64", "/usr/lib", "/usr/lib64", "/usr/local/lib")
SYSTEM_FILES: Final = ("/etc/ld.so.cache", "/dev/null", "/dev/urandom")


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


def restrict(paths: Iterable[str], abi: int | None = None) -> None:
    """Confine THIS process (and its future children) to read/execute below ``paths``.

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
            _allow(libc, add_rule, ruleset, path)
        if libc.prctl(PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0) != 0:
            raise LandlockUnavailable
        if libc.syscall(restrict_self, ruleset, 0) != 0:
            raise LandlockUnavailable
    finally:
        os.close(ruleset)


def _allow(libc: ctypes.CDLL, add_rule: int, ruleset: int, path: str) -> None:
    try:
        fd = os.open(path, os.O_PATH | os.O_CLOEXEC)
    except OSError:
        raise LandlockUnavailable from None
    try:
        mode = os.fstat(fd).st_mode
        if stat.S_ISDIR(mode):
            rights = DIR_RIGHTS
        elif stat.S_ISREG(mode):
            rights = FILE_RIGHTS
        else:
            rights = DEVICE_RIGHTS  # a device or another special file: read only
        beneath = struct.pack("=Qi", rights, fd)  # packed: u64 allowed_access, s32 parent_fd
        if libc.syscall(add_rule, ruleset, LANDLOCK_RULE_PATH_BENEATH, beneath, 0) != 0:
            raise LandlockUnavailable
    finally:
        os.close(fd)


def _real(path: str) -> str:
    return os.path.realpath(path)


def read_set(command: Sequence[str], extra: Iterable[str] = ()) -> tuple[str, ...]:
    """The only paths an adapter started with ``command`` may read (real paths, sorted).

    The single place that decides it: the interpreter's real prefix (its stdlib and
    extension modules), the virtualenv it runs from (site-packages: the tree-sitter bindings
    and grammar ``.so`` files, ``pyvenv.cfg``), the adapter script when ``command[1]`` is
    one (the file only, not its directory; later arguments are data, never granted), the system library directories,
    ``/etc/ld.so.cache`` and two device nodes, plus ``extra``. Paths that do not exist are
    left out.
    """
    found: set[str] = set()
    interpreter = command[0]
    found.add(_real(os.path.dirname(os.path.dirname(_real(interpreter)))))
    found.add(_real(interpreter))
    bin_dir = os.path.dirname(os.path.abspath(interpreter))
    venv = os.path.dirname(bin_dir)
    if os.path.isfile(os.path.join(venv, "pyvenv.cfg")):
        found.add(_real(venv))
    if len(command) > 1 and os.path.isabs(command[1]) and os.path.isfile(command[1]):
        found.add(_real(command[1]))  # the script; later arguments are never granted
    found.update(_real(item) for item in (*SYSTEM_LIBRARY_DIRS, *SYSTEM_FILES, *extra))
    return tuple(sorted(item for item in found if os.path.lexists(item)))


def _within(path: str, root: str) -> bool:
    return path == root or path.startswith(root.rstrip("/") + "/")


def check_read_set(paths: Iterable[str], checkout_roots: Iterable[str]) -> None:
    """Refuse a set with a path equal to, above or inside any checkout root."""
    roots = [_real(root) for root in checkout_roots]
    for path in paths:
        real = _real(path)
        if any(_within(real, root) or _within(root, real) for root in roots):
            raise UnsafeReadSet


def _trampoline(argv: Sequence[str]) -> None:
    """``landlock.py <json {"paths": [...]}> <command...>``: confine, then exec the adapter."""
    try:
        restrict(json.loads(argv[1])["paths"])
    except BaseException:
        os._exit(CONFINE_FAILED_EXIT)
    command = list(argv[2:])
    try:
        os.execv(command[0], command)
    except BaseException:
        os._exit(EXEC_FAILED_EXIT)


if __name__ == "__main__":
    _trampoline(sys.argv)
