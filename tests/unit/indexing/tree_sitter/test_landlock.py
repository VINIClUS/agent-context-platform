"""Landlock confinement of structural adapters (PLATFORM-032b).

Tests that need kernel support are skipped only when the kernel lacks Landlock; the skip
reason names it, and ``conftest.py`` prints the probed ABI in every run's summary.
"""

from __future__ import annotations

import json
import logging
import os
import platform
import resource
import sys
from pathlib import Path
from typing import Any

import pytest
from pydantic import ValidationError

from agent_context_platform.indexing.tree_sitter import landlock, runner
from agent_context_platform.indexing.tree_sitter.base import (
    ParsedModule,
    ParseRequest,
    SandboxUnavailable,
    SourceFile,
    StructuralError,
    StructuralErrorCode,
    parser_fingerprint,
)
from agent_context_platform.indexing.tree_sitter.runner import Limits, SandboxedAdapter
from agent_context_platform.settings import Settings

from .conftest import PACKAGE_READ_PATHS

pytestmark = pytest.mark.unit

FAKE = str(Path(__file__).with_name("fake_adapter.py"))
FINGERPRINT = parser_fingerprint("fake", "1.0", {"grammar": "x"})
ABI = landlock.abi_version()
requires_landlock = pytest.mark.skipif(
    ABI < 1,
    reason="kernel lacks Landlock (ABI 0: not in the LSM list, ENOSYS/EOPNOTSUPP or seccomp)",
)


def request() -> ParseRequest:
    return ParseRequest(files=(SourceFile.from_bytes("pkg/mod.py", "python", b"def a(): pass\n"),))


def command(mode: str, *args: str) -> list[str]:
    return [sys.executable, FAKE, mode, *args]


def limits(**over: Any) -> Limits:
    base = {"wall_seconds": 10.0, "cpu_seconds": 5, "extra_read_paths": PACKAGE_READ_PATHS}
    extra = tuple(over.pop("extra_read_paths", ()))
    return Limits(**{**base, **over, "extra_read_paths": (*PACKAGE_READ_PATHS, *extra)})


def adapter(mode: str, *args: str, **over: Any) -> SandboxedAdapter:
    return SandboxedAdapter(
        command(mode, *args),
        language="python",
        parser_name="fake",
        parser_version="1.0",
        parser_config={"grammar": "x"},
        limits=limits(**over),
        env={"FAKE_FINGERPRINT": FINGERPRINT},
    )


def outcome(mode: str, *args: str, **over: Any) -> StructuralErrorCode | ParsedModule:
    try:
        return adapter(mode, *args, **over).parse(request())
    except StructuralError as error:
        return error.code


def probe(paths: list[Path | str], **over: Any) -> dict[str, str]:
    """Raw stdout of the fake's ``read_other_file`` mode: {attempt: errno name or "ok"}."""
    joined = os.pathsep.join(str(item) for item in paths)
    raw = runner._run(
        command("read_other_file", joined),
        request().model_dump_json().encode(),
        limits(**over),
        {},
    )
    result: dict[str, str] = json.loads(raw)["probe"]
    return result


@pytest.fixture
def unconfined(monkeypatch: pytest.MonkeyPatch) -> None:
    """Run without Landlock, as the dev-only escape hatch does (the control of denial tests)."""
    monkeypatch.setattr(landlock, "abi_version", lambda: 0)
    monkeypatch.setattr(runner, "_warned_unconfined", True)


@pytest.fixture
def victims(tmp_path: Path) -> tuple[Path, Path, Path]:
    sibling = tmp_path / "sibling.txt"
    sibling.write_text("secret sibling")
    checkout = tmp_path / "checkout"
    (checkout / "src").mkdir(parents=True)
    inside = checkout / "src" / "a.py"
    inside.write_text("SECRET = 1\n")
    return sibling, checkout, inside


# --- static parts: no kernel support needed ------------------------------------------------


def test_syscall_numbers_match_the_kernel_uapi() -> None:
    # arch/x86/entry/syscalls/syscall_64.tbl and include/uapi/asm-generic/unistd.h (aarch64)
    assert landlock.SYSCALLS == {"x86_64": (444, 445, 446), "aarch64": (444, 445, 446)}
    assert landlock.PR_SET_NO_NEW_PRIVS == 38
    assert landlock.LANDLOCK_CREATE_RULESET_VERSION == 1
    assert landlock.LANDLOCK_RULE_PATH_BENEATH == 1


def test_unknown_architecture_fails_closed(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(platform, "machine", lambda: "riscv64")
    assert landlock.abi_version() == 0
    with pytest.raises(landlock.LandlockUnavailable):
        landlock.restrict([])


@pytest.mark.parametrize(
    ("abi", "fs_bits", "net"),
    [(1, 13, 0), (2, 14, 0), (3, 15, 0), (4, 15, 3), (5, 16, 3), (6, 16, 3), (8, 16, 3)],
)
def test_every_access_right_of_the_abi_is_handled(abi: int, fs_bits: int, net: int) -> None:
    mask = landlock.fs_mask(abi)
    assert bin(mask).count("1") == fs_bits
    assert mask & (landlock.FS_WRITE_FILE | landlock.FS_MAKE_REG | landlock.FS_REMOVE_FILE)
    assert (abi >= 2) == bool(mask & landlock.FS_REFER)
    assert (abi >= 3) == bool(mask & landlock.FS_TRUNCATE)
    assert (abi >= 5) == bool(mask & landlock.FS_IOCTL_DEV)
    assert landlock.net_mask(abi) == net
    assert len(landlock._ruleset_attr(abi)) == (8 if abi < 4 else 16)
    assert landlock.fs_mask(0) == 0


def test_granted_rights_never_write() -> None:
    granted = landlock.DIR_RIGHTS | landlock.FILE_RIGHTS | landlock.DEVICE_RIGHTS
    assert granted == landlock.FS_EXECUTE | landlock.FS_READ_FILE | landlock.FS_READ_DIR
    assert granted & ~landlock.fs_mask(1) == 0


def test_read_set_is_minimal_and_computed_from_the_command() -> None:
    rules = landlock.read_set(command("ok"))
    paths = rules.read
    real = os.path.realpath
    prefix = os.path.dirname(os.path.dirname(real(sys.executable)))
    assert real(sys.executable) in paths
    assert os.path.join(prefix, "lib") in paths  # the stdlib, not the whole prefix
    assert prefix not in paths
    assert real(FAKE) in paths  # the script file itself ...
    assert real(os.path.dirname(FAKE)) not in paths  # ... never its directory
    assert real(os.path.expanduser("~")) not in paths
    assert real("/etc") not in paths
    assert paths == tuple(sorted(paths))
    assert rules.listing == ()
    extra = str(Path(sys.prefix))
    assert real(extra) in landlock.read_set(command("ok"), [extra]).read
    landlock.check_read_set(rules, ["/work"])


def test_read_set_of_a_venv_includes_pyvenv_cfg_directory() -> None:
    if not (Path(sys.prefix) / "pyvenv.cfg").is_file():
        pytest.skip("tests do not run from a virtualenv")
    rules = landlock.read_set([str(Path(sys.prefix) / "bin/python")])
    assert os.path.realpath(sys.prefix) in rules.read


def test_the_interpreter_prefix_needs_a_positive_identification(tmp_path: Path) -> None:
    """Bot P1 (a): a direct-executable adapter never gets its grandparent directory."""
    app = tmp_path / "app"
    app.mkdir()
    binary = app / "adapter"
    binary.write_text("#!/bin/sh\n")
    binary.chmod(0o755)
    rules = landlock.read_set([str(binary)])
    assert str(binary) in rules.read
    assert str(tmp_path) not in rules.read
    assert str(app) not in rules.read
    assert not any(item.startswith(str(tmp_path)) and item != str(binary) for item in rules.read)
    assert landlock._python_prefix(str(binary)) is None
    # A file merely named python, with no lib/python3* beside it, is not an interpreter.
    fake = tmp_path / "bin" / "python3"
    fake.parent.mkdir()
    fake.write_text("")
    assert landlock._python_prefix(str(fake)) is None
    (tmp_path / "lib" / "python3.12").mkdir(parents=True)
    assert landlock._python_prefix(str(fake)) == str(tmp_path)
    assert landlock._python_prefix(sys.executable) is not None
    # ``/`` would be the grandparent of /bin/true; it is a file, so only itself is granted.
    assert "/" not in landlock.read_set(["/bin/true"]).read


@pytest.mark.parametrize(
    "bad",
    [
        *("/", "/etc", "/tmp", "/home", "/var", "/srv", "/root", "/mnt", "/media", "/run"),
        *("/proc", "/sys", "/usr", "/opt", "~"),
    ],
)
def test_the_absolute_floor_refuses_forbidden_and_shallow_paths(bad: str) -> None:
    """Bot P1 (b): whatever computes the set, these can never be granted."""
    target = os.path.expanduser(bad)
    with pytest.raises(landlock.UnsafeReadSet):
        landlock.check_read_set([target], ["/work"])
    with pytest.raises(landlock.UnsafeReadSet):
        landlock.check_read_set(landlock.ReadSet((), (target,)), ["/work"])
    if ABI >= 1:
        assert outcome("ok", extra_read_paths=(target,)) is StructuralErrorCode.UNSAFE_READ_SET


def test_the_floor_allows_the_fixed_system_paths_and_deep_paths() -> None:
    fixed = [*landlock.SYSTEM_LIBRARY_DIRS, *landlock.SYSTEM_FILES]
    landlock.check_read_set([item for item in fixed if os.path.lexists(item)], ["/work"])
    landlock.check_read_set(
        ["/etc/ld.so.cache", "/tmp/x/venv", os.path.expanduser("~/proj")], ["/w"]
    )


def test_an_empty_checkout_root_list_never_disables_the_check(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Bot P1 (c): refused at every layer, and the default is the indexer's mount."""
    with pytest.raises(landlock.UnsafeReadSet):
        landlock.check_read_set(["/usr/lib"], [])
    with pytest.raises(ValueError, match="checkout_roots"):
        Limits(checkout_roots=())
    assert Limits().checkout_roots == ("/work",)
    assert Settings().indexer_checkout_roots == ("/work",)
    monkeypatch.setenv("AGENT_CONTEXT_INDEXER_CHECKOUT_ROOTS", "[]")
    with pytest.raises(ValidationError):
        Settings()


def test_read_set_equal_above_or_inside_a_checkout_is_refused(tmp_path: Path) -> None:
    checkout = tmp_path / "checkout"
    (checkout / "deep").mkdir(parents=True)
    landlock.check_read_set([str(tmp_path / "elsewhere")], [str(checkout)])
    landlock.check_read_set([str(tmp_path / "checkout2")], [str(checkout)])  # not a prefix match
    for bad in (checkout, tmp_path, checkout / "deep", Path("/")):
        with pytest.raises(landlock.UnsafeReadSet):
            landlock.check_read_set([str(bad)], [str(checkout)])
    link = tmp_path / "link"
    link.symlink_to(checkout)
    with pytest.raises(landlock.UnsafeReadSet):
        landlock.check_read_set([str(link)], [str(checkout)])


@pytest.mark.parametrize("where", ["equal", "above", "inside"])
def test_runner_refuses_to_start_when_the_read_set_touches_the_checkout(
    tmp_path: Path, where: str
) -> None:
    if ABI < 1:
        pytest.skip("kernel lacks Landlock: the read set is not computed")
    checkout = tmp_path / "checkout"
    (checkout / "sub").mkdir(parents=True)
    extra = {"equal": checkout, "above": tmp_path, "inside": checkout / "sub"}[where]
    result = outcome("ok", checkout_roots=(str(checkout),), extra_read_paths=(str(extra),))
    assert result is StructuralErrorCode.UNSAFE_READ_SET


def test_an_interpreter_lib_holding_the_checkout_is_refused() -> None:
    if ABI < 1:
        pytest.skip("kernel lacks Landlock: the read set is not computed")
    prefix = os.path.dirname(os.path.dirname(os.path.realpath(sys.executable)))
    result = outcome("ok", checkout_roots=(os.path.join(prefix, "lib"),))
    assert result is StructuralErrorCode.UNSAFE_READ_SET


# --- python -m <module> --------------------------------------------------------------------


@pytest.fixture
def module_adapter(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> tuple[Path, Path]:
    """A fake ``-m`` adapter package outside any checkout, found through PYTHONPATH."""
    root = tmp_path / "site"
    package = root / "acfake"
    package.mkdir(parents=True)
    (package / "__init__.py").write_text("")
    (package / "sibling.py").write_text("SECRET = 1\n")
    (package / "run.py").write_text(
        "import json, os, sys\n"
        "from acfake import sibling\n"
        "here = os.path.dirname(os.path.dirname(__file__))\n"
        "try:\n"
        "    open(os.path.join(here, 'other_pkg', '__init__.py')).read()\n"
        "except PermissionError:\n"
        "    pass\n"
        "else:\n"
        "    sys.exit(9)\n"
        "request = json.loads(sys.stdin.read())\n"
        "file = request['files'][0]\n"
        "sys.stdout.write(json.dumps({'protocol_version': 1, 'files': [{\n"
        "    'path': file['path'], 'language': file['language'],\n"
        "    'parser_fingerprint': os.environ['FAKE_FINGERPRINT'],\n"
        "    'symbols': [], 'relations': []}]}))\n"
    )
    other = root / "other_pkg"
    other.mkdir()
    (other / "__init__.py").write_text("")
    monkeypatch.syspath_prepend(str(root))
    return root, package


def test_dash_m_grants_the_top_level_package_and_lists_its_parent(
    module_adapter: tuple[Path, Path],
) -> None:
    root, package = module_adapter
    rules = landlock.read_set([sys.executable, "-m", "acfake.run"])
    assert os.path.realpath(package) in rules.read
    assert os.path.realpath(root) not in rules.read
    assert rules.listing == (os.path.realpath(root),)
    assert os.path.realpath(root / "other_pkg") not in rules.read
    assert landlock.read_set([sys.executable, "-m", "no_such_top_level_pkg"]).listing == ()
    assert landlock.read_set([sys.executable, "-m"]).listing == ()


@requires_landlock
def test_dash_m_adapter_runs_confined_and_cannot_read_its_siblings(
    module_adapter: tuple[Path, Path], tmp_path: Path
) -> None:
    root, _ = module_adapter
    checkout = tmp_path / "checkout"
    checkout.mkdir()
    env = {"FAKE_FINGERPRINT": FINGERPRINT, "PYTHONPATH": str(root)}
    made = SandboxedAdapter(
        [sys.executable, "-m", "acfake.run"],
        language="python",
        parser_name="fake",
        parser_version="1.0",
        parser_config={"grammar": "x"},
        limits=limits(checkout_roots=(str(checkout),)),
        env=env,
    )
    # The adapter imported through the listable parent, and its own run exits 9 if it can
    # read the sibling package (denied: only the listing right covers the parent).
    assert isinstance(made.parse(request()), ParsedModule)


@requires_landlock
def test_editable_install_under_src_works_and_is_refused_inside_the_checkout() -> None:
    """The adapter package of this repo (a path-based editable install in dev/CI)."""
    module = "agent_context_platform.indexing.tree_sitter.base"
    cmd = [sys.executable, "-m", module]
    raw = runner._run(cmd, b"", limits(checkout_roots=("/work",)), {})
    assert raw == b""  # imported (through src/, confined) and ran to completion
    package = os.path.realpath(os.path.dirname(os.path.dirname(landlock.__file__ + "/..")))
    repo = os.path.dirname(os.path.dirname(package))
    for root in (package, os.path.dirname(package), repo):
        with pytest.raises(StructuralError) as caught:
            runner._run(cmd, b"", limits(checkout_roots=(root,)), {})
        assert caught.value.code is StructuralErrorCode.UNSAFE_READ_SET


# --- fail closed ---------------------------------------------------------------------------


def test_runner_refuses_adapters_when_landlock_is_unavailable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(landlock, "abi_version", lambda: 0)
    with pytest.raises(SandboxUnavailable) as caught:
        adapter("ok").parse(request())
    assert isinstance(caught.value, StructuralError)
    assert caught.value.code is StructuralErrorCode.SANDBOX_UNAVAILABLE
    assert str(caught.value) == "sandbox_unavailable"
    assert Limits().allow_unconfined is False


def test_escape_hatch_runs_unconfined_and_warns_once(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    monkeypatch.setattr(landlock, "abi_version", lambda: 0)
    monkeypatch.setattr(runner, "_warned_unconfined", False)
    with caplog.at_level(logging.WARNING, logger=runner.__name__):
        assert isinstance(adapter("ok", allow_unconfined=True).parse(request()), ParsedModule)
        assert isinstance(adapter("ok", allow_unconfined=True).parse(request()), ParsedModule)
    warnings = [item for item in caplog.records if item.levelno == logging.WARNING]
    assert len(warnings) == 1
    assert "UNCONFINED_ADAPTERS" in warnings[0].getMessage()
    assert "Development only" in warnings[0].getMessage()


def test_a_ruleset_the_kernel_refuses_fails_closed(monkeypatch: pytest.MonkeyPatch) -> None:
    """The probe passes but applying fails in the adapter process: nothing runs."""
    monkeypatch.setattr(landlock, "abi_version", lambda: 8)
    monkeypatch.setattr(
        landlock, "read_set", lambda *a, **k: landlock.ReadSet(("/nonexistent/ac/path",))
    )
    assert outcome("ok") is StructuralErrorCode.SANDBOX_UNAVAILABLE


# --- kernel enforcement --------------------------------------------------------------------


@requires_landlock
def test_confined_adapter_cannot_read_write_or_list_anything_else(
    victims: tuple[Path, Path, Path],
) -> None:
    sibling, checkout, inside = victims
    hostname = "/etc/hostname"
    result = probe([sibling, inside, hostname], checkout_roots=(str(checkout),))
    denied = {key: value for key, value in result.items() if key != "tcp"}
    assert set(denied.values()) == {"EACCES"}, result
    for path in (sibling, inside, hostname):
        assert result[f"read:{path}"] == "EACCES"
    assert result["listdir:/proc"] == "EACCES"
    assert result["create:cwd"] == "EACCES"
    if ABI >= 4:
        assert result["tcp"] == "EACCES"
    assert sibling.read_text() == "secret sibling"
    assert not sibling.with_name("sibling.txt.new").exists()


@requires_landlock
def test_the_same_attempts_succeed_without_confinement(
    victims: tuple[Path, Path, Path], unconfined: None
) -> None:
    """Control: proves the denials above come from Landlock, not from ENOENT or permissions."""
    sibling, _, inside = victims
    result = probe([sibling, inside], allow_unconfined=True)
    for path in (sibling, inside):
        assert result[f"read:{path}"] == "ok"
        assert result[f"list:{path.parent}"] == "ok"
    assert result["create:cwd"] == "ok"
    assert result[f"create:{sibling}"] == "ok"
    assert result["listdir:/"] == "ok"
    assert "EACCES" not in result.values()


@requires_landlock
def test_confined_adapter_still_parses_with_its_compiled_dependencies() -> None:
    """Positive: the interpreter, ``ssl``/``_ctypes`` and a site-packages extension load."""
    assert isinstance(outcome("deps"), ParsedModule)
    assert isinstance(outcome("ok"), ParsedModule)


@requires_landlock
def test_confined_adapter_cannot_write_a_file(tmp_path: Path) -> None:
    target = tmp_path / "escaped"
    assert outcome("write", str(target)) is StructuralErrorCode.NONZERO_EXIT
    assert not target.exists()


@requires_landlock
def test_the_confinement_reaches_grandchildren(tmp_path: Path) -> None:
    """A child the adapter spawns inherits the ruleset: it cannot read the sibling either."""
    secret = tmp_path / "secret.txt"
    secret.write_text("x")
    code = (
        "import subprocess, sys\n"
        "r = subprocess.run([sys.executable, '-c', 'open(__import__(\"sys\").argv[1]).read()',"
        " sys.argv[1]])\n"
        "sys.exit(0 if r.returncode else 9)\n"
    )
    hard = resource.getrlimit(resource.RLIMIT_NPROC)[1]
    headroom = 100_000 if hard == resource.RLIM_INFINITY else hard
    # RLIMIT_NPROC counts every task of this UID, so a dev machine needs headroom to fork.
    raw = runner._run(
        [sys.executable, "-c", code, str(secret)], b"", limits(processes=headroom), {}
    )
    assert raw == b""


def test_limits_carry_the_indexer_settings(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("AGENT_CONTEXT_INDEXER_ALLOW_UNCONFINED_ADAPTERS", "true")
    monkeypatch.setenv("AGENT_CONTEXT_INDEXER_CHECKOUT_ROOTS", json.dumps([str(tmp_path)]))
    built = Limits.from_settings(Settings(), wall_seconds=3.0)
    assert built.allow_unconfined is True
    assert built.checkout_roots == (str(tmp_path),)
    assert built.wall_seconds == 3.0


@requires_landlock
def test_an_adapter_exiting_121_is_an_ordinary_adapter_failure() -> None:
    """Reviewer NB1: the confinement result travels on a status pipe, never on exit codes."""
    assert landlock.CONFINE_FAILED_EXIT == 121
    assert outcome("exit121") is StructuralErrorCode.NONZERO_EXIT


def test_an_adapter_exiting_121_is_not_a_sandbox_failure_under_the_hatch(
    unconfined: None,
) -> None:
    assert outcome("exit121", allow_unconfined=True) is StructuralErrorCode.NONZERO_EXIT


@requires_landlock
def test_a_refused_ruleset_fails_closed_even_with_the_escape_hatch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Reviewer NB4: the hatch covers a kernel without Landlock, not a refusal in the child."""
    monkeypatch.setattr(
        landlock, "read_set", lambda *a, **k: landlock.ReadSet(("/nonexistent/ac/path",))
    )
    assert outcome("ok", allow_unconfined=True) is StructuralErrorCode.SANDBOX_UNAVAILABLE


def test_legacy_sandbox_tests_run_unconfined_and_say_so_on_a_host_without_landlock() -> None:
    """Bot P2: with ABI 0 the pre-existing sandbox tests pass (escape hatch), reason visible."""
    program = (
        "import sys, pytest\n"
        "from agent_context_platform.indexing.tree_sitter import landlock\n"
        "landlock.abi_version = lambda: 0\n"
        "target = sys.argv[1] + '::test_well_behaved_adapter_round_trips'\n"
        "sys.exit(pytest.main(['-q', '--no-cov', '-p', 'no:cacheprovider', target]))\n"
    )
    import subprocess

    legacy = str(Path(__file__).with_name("test_base.py"))
    done = subprocess.run(
        [sys.executable, "-c", program, legacy],
        capture_output=True,
        text=True,
        timeout=120,
        check=False,
        cwd=str(Path(__file__).parents[4]),
    )
    assert done.returncode == 0, done.stdout[-1500:]
    assert "ABI 0 (UNAVAILABLE" in done.stdout
    assert "UNCONFINED" in done.stdout
