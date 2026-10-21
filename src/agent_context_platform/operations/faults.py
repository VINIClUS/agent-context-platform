"""Documented crash-test fault points; inert unless explicitly armed.

A fault point is a named boundary where an end-to-end crash test may kill the process.
``fault_point(label)`` does nothing unless the process was configured (``configure``,
called once from the settings loaders) with fault injection enabled and armed on that
label. When the configured hit arrives it writes ``fault_injected label=<label>`` to
stderr and calls ``os._exit(137)``: no cleanup, no ``finally`` blocks, like SIGKILL.

The labels are a closed set documented in ``docs/operations/fault-injection.md``;
that file is the contract the E2E crash scenarios consume. Settings refuse to load
with fault injection enabled in production. A disabled hook costs one ``None`` check.
"""

from __future__ import annotations

import os
import sys
from dataclasses import dataclass
from typing import TYPE_CHECKING, Final, Literal

if TYPE_CHECKING:
    from agent_context_platform.settings import FaultInjectionSettings

FaultLabel = Literal[
    "content.before_s3_put",
    "content.after_s3_put",
    "content.after_head_verification",
    "ledger.before_db_transaction",
    "ledger.after_event_before_outbox",
    "ledger.before_commit",
    "ledger.after_commit_before_response",
    "projection.during_mutation",
    "projection.before_checkpoint",
]

CRASH_EXIT_CODE: Final = 137


@dataclass(slots=True)
class _Arming:
    label: str
    remaining: int


_armed: _Arming | None = None


def configure(settings: FaultInjectionSettings) -> None:
    """Arm (or disarm) the process-wide fault point from validated settings."""
    global _armed
    if settings.enabled and settings.crash_at is not None:
        _armed = _Arming(label=settings.crash_at, remaining=settings.after)
    else:
        _armed = None


def fault_point(label: FaultLabel) -> None:
    """Crash the process here when armed on ``label`` and this is the configured hit."""
    armed = _armed
    if armed is None or armed.label != label:
        return
    armed.remaining -= 1
    if armed.remaining > 0:
        return
    sys.stderr.write(f"fault_injected label={label}\n")
    sys.stderr.flush()
    os._exit(CRASH_EXIT_CODE)
