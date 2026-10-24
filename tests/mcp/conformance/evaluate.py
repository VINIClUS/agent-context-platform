"""Evaluate the conformance suite's results against the applicability manifest (PLATFORM-047).

The pinned suite (`conformance.lock`) writes one ``<out>/server-<scenario>-<timestamp>/checks.json``
per scenario. A profile passes only when every SCORED check that failed is accounted for by
`applicability.toml`, and the manifest itself is current:

- a failed check (``FAILURE`` or ``ERROR``) of a scored scenario must be listed for that profile,
  either as a capability exclusion (the check exercises only a capability the platform does not
  advertise) or, on the shipping profile only, as fixture coverage (the check needs one of the
  suite's fixture tools and MUST then pass on the fixture profile);
- every listed check must have run and failed: a listed check that passes is a stale entry, one
  that did not run is a missing entry, and both fail the run;
- a scenario the results or the manifest name that the requirement set does not know fails the
  run; a scored scenario that did not run fails it too;
- unscored scenarios (the requirement set's ``not_scored``) are reported and never gate; listing
  them is an error;
- a capability exclusion may not touch a protected scenario (stateless/session behaviour,
  transport security, headers, authorization), and every citation must name a scenario and a
  literal that exist in the pinned package's ``dist/index.js``.

It writes one aggregated ``checks.json`` per profile and a ``summary.json`` of counts, and prints
counts and the ids of offending checks, never check details. Exit status 0 when both profiles
pass, 1 otherwise.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import tomllib
from collections import Counter
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Final

import yaml

PROFILES: Final = ("shipping", "fixture")
FAILED: Final = frozenset({"FAILURE", "ERROR"})
STATUSES: Final = frozenset({"SUCCESS", "FAILURE", "ERROR", "WARNING", "SKIPPED", "INFO"})
# Scenarios whose checks may never be excluded for a capability: their behaviour is ours.
PROTECTED: Final = re.compile(r"stateless|session|auth|dns-rebinding|header|security|sse")
_RESULT_DIR: Final = re.compile(r"^server-(?P<scenario>.+)-\d{4}-\d{2}-\d{2}T[\d-]+Z$")


class ManifestError(ValueError):
    """The manifest is malformed or breaks the applicability policy."""


@dataclass(frozen=True, slots=True)
class Capability:
    name: str
    discover_key: str | None
    methods: tuple[str, ...]
    description: str


@dataclass(frozen=True, slots=True)
class Entry:
    kind: str  # "capability" or "fixture"
    scenario: str
    checks: tuple[str, ...]
    profiles: tuple[str, ...]
    reason: str  # the capability name, or the fixture tool the check needs
    justification: str
    source: str
    source_literal: str


@dataclass(frozen=True, slots=True)
class Manifest:
    capabilities: Mapping[str, Capability]
    entries: tuple[Entry, ...]

    def listed(self, profile: str) -> dict[tuple[str, str], Entry]:
        return {
            (entry.scenario, check): entry
            for entry in self.entries
            if profile in entry.profiles
            for check in entry.checks
        }


@dataclass(frozen=True, slots=True)
class Requirements:
    scored: frozenset[str]
    unscored: frozenset[str]

    @property
    def known(self) -> frozenset[str]:
        return self.scored | self.unscored


@dataclass(slots=True)
class ProfileReport:
    profile: str
    passed: int = 0
    failed: int = 0
    excluded: int = 0
    covered: int = 0
    other: int = 0
    scenarios: int = 0
    unscored_scenarios: int = 0
    unscored_failed_scenarios: int = 0
    errors: list[str] = field(default_factory=list)

    def as_dict(self) -> dict[str, Any]:
        return {
            "profile": self.profile,
            "scored_scenarios": self.scenarios,
            "scored_checks_passed": self.passed,
            "scored_checks_failed_unlisted": self.failed,
            "scored_checks_excluded": self.excluded,
            "scored_checks_covered_by_fixture": self.covered,
            "scored_checks_other_status": self.other,
            "unscored_scenarios": self.unscored_scenarios,
            "unscored_scenarios_failing": self.unscored_failed_scenarios,
            "errors": len(self.errors),
        }


# --- loading ------------------------------------------------------------------------------------


def _strings(value: Any, where: str) -> tuple[str, ...]:
    if not isinstance(value, list) or not value or not all(isinstance(v, str) and v for v in value):
        raise ManifestError(f"{where}: expected a non-empty list of strings")
    if len(set(value)) != len(value):
        raise ManifestError(f"{where}: duplicate values")
    return tuple(value)


def _text(table: Mapping[str, Any], key: str, where: str) -> str:
    value = table.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ManifestError(f"{where}: `{key}` must be a non-empty string")
    return value


def load_manifest(path: Path) -> Manifest:
    data = tomllib.loads(path.read_text(encoding="utf-8"))
    unknown = set(data) - {"capability", "exclusion", "fixture"}
    if unknown:
        raise ManifestError(f"unknown top-level keys: {sorted(unknown)}")
    capabilities: dict[str, Capability] = {}
    for name, table in data.get("capability", {}).items():
        where = f"capability.{name}"
        key = table.get("discover_key")
        if key is not None and not isinstance(key, str):
            raise ManifestError(f"{where}: `discover_key` must be a string")
        capabilities[name] = Capability(
            name=name,
            discover_key=key,
            methods=_strings(table.get("methods"), f"{where}.methods"),
            description=_text(table, "description", where),
        )
    entries: list[Entry] = []
    for kind, rows in (
        ("capability", data.get("exclusion", [])),
        ("fixture", data.get("fixture", [])),
    ):
        for index, row in enumerate(rows):
            where = f"{'exclusion' if kind == 'capability' else 'fixture'}[{index}]"
            allowed = {
                "scenario",
                "checks",
                "profiles",
                "capability" if kind == "capability" else "tool",
                "justification",
                "source",
                "source_literal",
            }
            extra = set(row) - allowed
            if extra:
                raise ManifestError(f"{where}: unknown keys {sorted(extra)}")
            scenario = _text(row, "scenario", where)
            profiles = (
                _strings(row.get("profiles"), f"{where}.profiles")
                if kind == "capability"
                else ("shipping",)
            )
            if not set(profiles) <= set(PROFILES):
                raise ManifestError(f"{where}: unknown profile in {profiles}")
            reason = _text(row, "capability" if kind == "capability" else "tool", where)
            if kind == "capability":
                if reason not in capabilities:
                    raise ManifestError(f"{where}: unknown capability {reason!r}")
                if PROTECTED.search(scenario):
                    raise ManifestError(f"{where}: {scenario!r} is protected; it must pass")
            entries.append(
                Entry(
                    kind=kind,
                    scenario=scenario,
                    checks=_strings(row.get("checks"), f"{where}.checks"),
                    profiles=profiles,
                    reason=reason,
                    justification=_text(row, "justification", where),
                    source=_text(row, "source", where),
                    source_literal=_text(row, "source_literal", where),
                )
            )
    manifest = Manifest(capabilities=capabilities, entries=tuple(entries))
    for profile in PROFILES:
        seen: Counter[tuple[str, str]] = Counter(
            (entry.scenario, check)
            for entry in manifest.entries
            if profile in entry.profiles
            for check in entry.checks
        )
        duplicated = [key for key, count in seen.items() if count > 1]
        if duplicated:
            raise ManifestError(f"{profile}: checks listed twice: {sorted(duplicated)}")
    used = {entry.reason for entry in manifest.entries if entry.kind == "capability"}
    if set(capabilities) - used:
        raise ManifestError(f"capabilities never used: {sorted(set(capabilities) - used)}")
    return manifest


def load_requirements(path: Path) -> Requirements:
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    scored = frozenset(data["server"])
    unscored = frozenset(
        row["scenario"] for row in data.get("not_scored", []) if row.get("leg") == "server"
    )
    return Requirements(scored=scored, unscored=unscored)


def load_results(directory: Path) -> dict[str, list[dict[str, Any]]]:
    """``{scenario: checks}`` from the suite's per-scenario result directories."""
    results: dict[str, list[dict[str, Any]]] = {}
    for child in sorted(directory.iterdir()):
        match = _RESULT_DIR.match(child.name)
        if not child.is_dir() or match is None:
            continue
        scenario = match["scenario"]
        if scenario in results:
            raise ManifestError(f"scenario {scenario!r} has two result directories")
        checks = json.loads((child / "checks.json").read_text(encoding="utf-8"))
        if not isinstance(checks, list):
            raise ManifestError(f"{child.name}/checks.json is not a list")
        results[scenario] = checks
    return results


# --- evaluation ---------------------------------------------------------------------------------


def check_sources(manifest: Manifest, package_source: str) -> list[str]:
    """Every citation names a scenario and a literal of the pinned package."""
    errors = []
    for entry in manifest.entries:
        if f"this.name=`{entry.scenario}`" not in package_source and (
            f"this.name = `{entry.scenario}`" not in package_source
        ):
            errors.append(f"citation: scenario {entry.scenario!r} not in the pinned package")
        if entry.source_literal not in package_source:
            errors.append(
                f"citation: literal {entry.source_literal!r} of {entry.scenario!r} not in the "
                "pinned package"
            )
    return errors


def _status(check: Mapping[str, Any], where: str, errors: list[str]) -> str:
    status = check.get("status")
    if status not in STATUSES:
        errors.append(f"{where}: unknown status {status!r}")
        return "ERROR"
    return str(status)


def _grouped(
    scenario: str, checks: Sequence[Mapping[str, Any]], errors: list[str]
) -> dict[str, str]:
    """One status per check id: a scenario may report an id once per variant it probes.

    The id failed if any of its reports failed, passed if all passed, else it is the first
    other status (a warning, a skip or information).
    """
    statuses: dict[str, list[str]] = {}
    for check in checks:
        check_id = check.get("id")
        if not isinstance(check_id, str) or not check_id:
            errors.append(f"results: {scenario} has a check without an id")
            continue
        statuses.setdefault(check_id, []).append(_status(check, f"{scenario}/{check_id}", errors))
    grouped = {}
    for check_id, values in statuses.items():
        failed = [value for value in values if value in FAILED]
        others = [value for value in values if value != "SUCCESS"]
        grouped[check_id] = failed[0] if failed else (others[0] if others else "SUCCESS")
    return grouped


def evaluate_profile(
    profile: str,
    results: Mapping[str, Sequence[Mapping[str, Any]]],
    manifest: Manifest,
    requirements: Requirements,
) -> ProfileReport:
    report = ProfileReport(profile)
    listed = manifest.listed(profile)
    for entry in manifest.entries:
        if profile in entry.profiles and entry.scenario not in requirements.known:
            report.errors.append(f"manifest: unknown scenario {entry.scenario!r}")
        elif profile in entry.profiles and entry.scenario in requirements.unscored:
            report.errors.append(f"manifest: {entry.scenario!r} is not scored; do not list it")
    for scenario in sorted(set(results) - requirements.known):
        report.errors.append(f"results: unknown scenario {scenario!r}")
    for scenario in sorted(requirements.scored - set(results)):
        report.errors.append(f"results: scored scenario {scenario!r} did not run")
    seen: set[tuple[str, str]] = set()
    for scenario in sorted(set(results) & requirements.known):
        checks = results[scenario]
        if scenario in requirements.unscored:
            report.unscored_scenarios += 1
            if any(_status(c, scenario, report.errors) in FAILED for c in checks):
                report.unscored_failed_scenarios += 1
            continue
        report.scenarios += 1
        for check_id, status in _grouped(scenario, checks, report.errors).items():
            key = (scenario, check_id)
            seen.add(key)
            entry = listed.get(key)
            if status in FAILED:
                if entry is None:
                    report.failed += 1
                    report.errors.append(f"unexpected failure: {scenario}/{check_id}")
                elif entry.kind == "fixture":
                    report.covered += 1
                else:
                    report.excluded += 1
            elif entry is not None:
                report.errors.append(f"stale entry: {scenario}/{check_id} now {status}")
            elif status == "SUCCESS":
                report.passed += 1
            else:
                report.other += 1
    for scenario, check_id in sorted(set(listed) - seen):
        if scenario in requirements.known and scenario not in requirements.unscored:
            report.errors.append(f"missing: listed check {scenario}/{check_id} did not run")
    return report


def check_fixture_coverage(
    manifest: Manifest, fixture: Mapping[str, Sequence[Mapping[str, Any]]]
) -> list[str]:
    """A check the shipping profile leaves to the fixture tools must pass on the fixture profile."""
    errors = []
    for entry in manifest.entries:
        if entry.kind != "fixture":
            continue
        statuses = _grouped(entry.scenario, fixture.get(entry.scenario, ()), errors)
        for check_id in entry.checks:
            if statuses.get(check_id) != "SUCCESS":
                errors.append(
                    f"fixture coverage: {entry.scenario}/{check_id} is "
                    f"{statuses.get(check_id, 'missing')} on the fixture profile"
                )
    return errors


def evaluate(
    manifest: Manifest,
    requirements: Requirements,
    results: Mapping[str, Mapping[str, Sequence[Mapping[str, Any]]]],
    package_source: str | None = None,
) -> tuple[list[ProfileReport], list[str]]:
    reports = [
        evaluate_profile(profile, results[profile], manifest, requirements) for profile in PROFILES
    ]
    errors = check_fixture_coverage(manifest, results["fixture"])
    if package_source is not None:
        errors.extend(check_sources(manifest, package_source))
    return reports, errors


def _aggregate(results: Mapping[str, Sequence[Mapping[str, Any]]], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(results, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--requirements", type=Path, required=True)
    parser.add_argument("--package-source", type=Path, required=True)
    parser.add_argument("--shipping", type=Path, required=True)
    parser.add_argument("--fixture", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True, help="<out>/<profile>/checks.json")
    arguments = parser.parse_args(None if argv is None else list(argv))

    try:
        manifest = load_manifest(arguments.manifest)
        requirements = load_requirements(arguments.requirements)
        results = {
            "shipping": load_results(arguments.shipping),
            "fixture": load_results(arguments.fixture),
        }
    except (ManifestError, OSError, ValueError, KeyError) as error:
        print(f"conformance: cannot evaluate: {error}", file=sys.stderr)
        return 1
    for profile in PROFILES:
        _aggregate(results[profile], arguments.out / profile / "checks.json")
    reports, errors = evaluate(
        manifest,
        requirements,
        results,
        arguments.package_source.read_text(encoding="utf-8"),
    )
    summary = {
        "manifest_entries": len(manifest.entries),
        "manifest_checks": sum(len(entry.checks) for entry in manifest.entries),
        "profiles": [report.as_dict() for report in reports],
        "errors": len(errors) + sum(len(report.errors) for report in reports),
    }
    (arguments.out / "summary.json").write_text(
        json.dumps(summary, indent=2) + "\n", encoding="utf-8"
    )
    for report in reports:
        counts = report.as_dict()
        print(
            f"conformance[{report.profile}]: scored scenarios {counts['scored_scenarios']}, "
            f"checks passed {report.passed}, excluded {report.excluded}, covered by fixture "
            f"{report.covered}, other {report.other}, unexpected failures {report.failed}; "
            f"unscored scenarios {report.unscored_scenarios} "
            f"({report.unscored_failed_scenarios} failing, not gating)"
        )
        for error in report.errors:
            print(f"conformance[{report.profile}]: {error}", file=sys.stderr)
    for error in errors:
        print(f"conformance: {error}", file=sys.stderr)
    return 0 if summary["errors"] == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
