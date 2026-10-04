from __future__ import annotations

from collections.abc import Iterator
from typing import get_args

import pytest
from pydantic import ValidationError

from agent_context_platform.operations import faults
from agent_context_platform.operations.faults import FaultLabel, fault_point
from agent_context_platform.settings import FaultInjectionSettings, Settings

pytestmark = pytest.mark.unit

LABEL: FaultLabel = "ledger.before_commit"


class _Exit(BaseException):
    pass


@pytest.fixture(autouse=True)
def exits(monkeypatch: pytest.MonkeyPatch) -> Iterator[list[int]]:
    codes: list[int] = []

    def fake_exit(code: int) -> None:
        codes.append(code)
        raise _Exit

    monkeypatch.setattr(faults.os, "_exit", fake_exit)
    yield codes
    faults.configure(FaultInjectionSettings())


def test_disabled_is_a_no_op(exits: list[int], capsys: pytest.CaptureFixture[str]) -> None:
    faults.configure(FaultInjectionSettings(enabled=False, crash_at=LABEL))
    fault_point(LABEL)
    assert exits == []
    assert capsys.readouterr().err == ""


def test_enabled_without_a_label_is_a_no_op(exits: list[int]) -> None:
    faults.configure(FaultInjectionSettings(enabled=True))
    fault_point(LABEL)
    assert exits == []


def test_other_labels_do_not_fire_or_count(exits: list[int]) -> None:
    faults.configure(FaultInjectionSettings(enabled=True, crash_at=LABEL, after=2))
    fault_point("content.before_s3_put")
    fault_point("content.before_s3_put")
    fault_point(LABEL)
    assert exits == []


def test_first_hit_crashes_with_a_content_free_line(
    exits: list[int], capsys: pytest.CaptureFixture[str]
) -> None:
    faults.configure(FaultInjectionSettings(enabled=True, crash_at=LABEL))
    with pytest.raises(_Exit):
        fault_point(LABEL)
    assert exits == [137]
    assert capsys.readouterr().err == f"fault_injected label={LABEL}\n"


def test_nth_hit_crashes(exits: list[int]) -> None:
    faults.configure(FaultInjectionSettings(enabled=True, crash_at=LABEL, after=3))
    fault_point(LABEL)
    fault_point(LABEL)
    assert exits == []
    with pytest.raises(_Exit):
        fault_point(LABEL)
    assert exits == [137]


def test_reconfigure_disarms(exits: list[int]) -> None:
    faults.configure(FaultInjectionSettings(enabled=True, crash_at=LABEL))
    faults.configure(FaultInjectionSettings())
    fault_point(LABEL)
    assert exits == []


def test_settings_load_from_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("AGENT_CONTEXT_FAULT_INJECTION__ENABLED", "true")
    monkeypatch.setenv("AGENT_CONTEXT_FAULT_INJECTION__CRASH_AT", "projection.during_mutation")
    monkeypatch.setenv("AGENT_CONTEXT_FAULT_INJECTION__AFTER", "2")
    fault_injection = Settings().fault_injection
    assert fault_injection.enabled is True
    assert fault_injection.crash_at == "projection.during_mutation"
    assert fault_injection.after == 2


def test_settings_default_to_disabled() -> None:
    assert Settings().fault_injection == FaultInjectionSettings(enabled=False)


@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("CRASH_AT", "ledger.nope"),
        ("AFTER", "0"),
        ("ENABLED", "maybe"),
    ],
)
def test_invalid_fault_settings_fail_at_load(
    monkeypatch: pytest.MonkeyPatch, name: str, value: str
) -> None:
    monkeypatch.setenv(f"AGENT_CONTEXT_FAULT_INJECTION__{name}", value)
    with pytest.raises(ValidationError):
        Settings()


def test_production_refuses_enabled_fault_injection(
    complete_production_environment: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("AGENT_CONTEXT_FAULT_INJECTION__ENABLED", "true")
    with pytest.raises(ValidationError, match="fault injection must not be enabled"):
        Settings()


def test_production_accepts_disabled_fault_injection(
    complete_production_environment: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("AGENT_CONTEXT_FAULT_INJECTION__CRASH_AT", LABEL)
    Settings()


def test_label_set_matches_the_documentation() -> None:
    from pathlib import Path

    doc = (Path(__file__).parents[2] / "docs/operations/fault-injection.md").read_text()
    for label in get_args(FaultLabel):
        assert f"`{label}`" in doc
