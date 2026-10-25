"""Evaluate the conformance suite's results against the applicability manifest (PLATFORM-047).

The pinned suite (`conformance.lock`) writes one ``<out>/server-<scenario>-<timestamp>/checks.json``
per scenario. A profile passes only when every SCORED check that failed is accounted for by
`applicability.toml`, and the manifest itself is current and honest:

- a failed check (``FAILURE`` or ``ERROR``) of a scored scenario must be listed for that profile,
  either as a capability exclusion (the check exercises only a capability the platform does not
  advertise) or, on the shipping profile only, as fixture coverage (the check needs one of the
  suite's fixture tools and MUST then pass on the fixture profile);
- a listed failure counts only with the suite's own not-applicable evidence in its report
  (`EVIDENCE`: "Method not found" for an unserved method, "Unknown tool" or "Not testable" for a
  missing fixture tool, and so on); a listed check that failed for any other reason is an
  unexpected failure, so an entry can never mask a regression of what it does not describe;
- only allowlisted scenarios (and, where a scenario mixes applicable checks, allowlisted check
  ids) may be listed at all (`EXCLUDABLE`, `FIXTURE_COVERABLE`): stateless and session behaviour,
  transport security, headers, authorization and integrity are never excludable;
- every listed check must have run and failed: a listed check that passes is a stale entry, one
  that did not run is a missing entry, and both fail the run;
- a scenario the results or the manifest name that the requirement set does not know fails the
  run; a scored scenario that did not run, or reported no check, fails it too;
- unscored scenarios (the requirement set's ``not_scored``) are reported and never gate; listing
  them is an error;
- every citation is checked inside the cited scenario's own class in the pinned package's
  ``dist/index.js``: the cited literal and every listed check id must occur there.

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
_RESULT_DIR: Final = re.compile(r"^server-(?P<scenario>.+)-\d{4}-\d{2}-\d{2}T[\d-]+Z$")
_INPUT_REQUIRED_RESULT: Final = tuple(
    f"input-required-result-{name}"
    for name in (
        "basic-elicitation",
        "basic-sampling",
        "basic-list-roots",
        "request-state",
        "multiple-input-requests",
        "multi-round",
        "non-tool-request",
        "result-type",
        # It tampers with the requestState of the server's own InputRequiredResult: a server that
        # never asks the client for input issues no requestState, so nothing can be tampered with.
        # The server's other integrity checks are in scenarios that are not excludable.
        "tampered-state",
        "capability-check",
    )
)
# The only scenarios a capability exclusion may name, per capability; `None` admits every check of
# the scenario, a set only those check ids (the scenario's other checks apply and must pass).
EXCLUDABLE: Final[Mapping[str, Mapping[str, frozenset[str] | None]]] = {
    "resources": {
        "resources-list": None,
        "resources-read-text": None,
        "resources-read-binary": None,
        "resources-templates-read": None,
        "caching": frozenset(
            {
                "sep-2549-resources-list-caching-hints",
                "sep-2549-resources-templates-list-caching-hints",
            }
        ),
    },
    "prompts": {
        "prompts-list": None,
        "prompts-get-simple": None,
        "prompts-get-with-args": None,
        "prompts-get-embedded-resource": None,
        "prompts-get-with-image": None,
        "caching": frozenset({"sep-2549-prompts-list-caching-hints"}),
    },
    "completions": {"completion-complete": None},
    "input_requests": dict.fromkeys(_INPUT_REQUIRED_RESULT),
    "response_stream": {"tools-call-with-progress": None},
}
# The only scenarios (and check ids) fixture coverage may name.
FIXTURE_COVERABLE: Final[Mapping[str, frozenset[str] | None]] = {
    "tools-call-simple-text": None,
    "tools-call-image": None,
    "tools-call-audio": None,
    "tools-call-embedded-resource": None,
    "tools-call-mixed-content": None,
    "tools-call-error": None,
    # Only the four diagnostic checks: every other server-stateless check must pass on shipping.
    "server-stateless": frozenset(
        {
            "sep-2575-server-rejects-undeclared-capability",
            "sep-2575-missing-capability-http-400",
            "sep-2575-http-server-no-independent-requests-on-stream",
            "sep-2575-server-no-log-without-loglevel",
        }
    ),
}
# The suite's own words for "this does not apply here", per reason and profile: the check's
# `errorMessage` must match one of them in full. Only the message counts, never `details`, which
# echoes what the server sent. A listed failure whose message matches none failed for another
# reason, and so does one that matches the phrasing of another profile (the fixture profile serves
# the progress tool, so "Unknown tool" there is a regression, not evidence).
_UNSERVED: Final = (
    r"Failed: Method not found",
    r"(?:prompts|resources|resources/templates)/list returned JSON-RPC error -32601: "
    r"Method not found",
)
_NO_INPUT_REQUESTS: Final = (
    r"JSON-RPC error: Method not found",  # the non-tool scenario's prompts/get
    r"Prerequisite failed: could not get initial InputRequiredResult with requestState",
)
_EVIDENCE_TEXT: Final[Mapping[tuple[str, str], tuple[str, ...]]] = {
    **{
        (capability, profile): _UNSERVED
        for capability in ("resources", "prompts", "completions")
        for profile in PROFILES
    },
    ("input_requests", "shipping"): (
        *_NO_INPUT_REQUESTS,
        r"JSON-RPC error: Unknown tool",
        # Only with no result at all (`_evidence_holds`): the tool call was refused.
        r"Expected InputRequiredResult with inputRequests and requestState",
    ),
    # On the fixture profile an unknown tool is a complete tool-error result; the suite then says
    # it got a complete result, and the result itself must be the SDK's unknown-tool error for the
    # cited tool (`_evidence_holds`).
    ("input_requests", "fixture"): (
        *_NO_INPUT_REQUESTS,
        r"Expected InputRequiredResult(?: .+)?",
        r'Expected resultType "input_required", got "complete"',
        r"Server returned complete result; expected InputRequiredResult.+",
    ),
    ("response_stream", "shipping"): (r"Failed: Unknown tool",),
    ("response_stream", "fixture"): (r"No progress notifications received",),
    ("fixture", "shipping"): (
        r"Failed: Unknown tool",
        r"Not testable: server does not list the diagnostic tool '(?P<tool>[A-Za-z0-9_]+)'.*",
    ),
}
EVIDENCE: Final[Mapping[tuple[str, str], tuple[re.Pattern[str], ...]]] = {
    key: tuple(re.compile(pattern, re.DOTALL) for pattern in patterns)
    for key, patterns in _EVIDENCE_TEXT.items()
}
# Checks that must be reported on both profiles whatever else changes.
ALWAYS_REPORTED: Final[Mapping[str, frozenset[str]]] = {
    "dns-rebinding-protection": frozenset(
        {"localhost-host-rebinding-rejected", "localhost-host-valid-accepted"}
    ),
}
# The only scored checks that may end neither passed nor failed, with the status they have now:
# a success turning into a skip (or any other drift) fails the run, and so does a pinned check
# that now passes (update the pin).
OTHER_STATUSES: Final[Mapping[str, frozenset[tuple[str, str, str]]]] = {
    profile: frozenset(
        {
            ("caching", "sep-2549-resources-read-caching-hints", "SKIPPED"),
            (
                "input-required-result-missing-input-response",
                "sep-2322-missing-response-rerequests",
                "WARNING",
            ),
            ("sep-2164-resource-not-found", "sep-2164-error-code", "WARNING"),
            ("sep-2164-resource-not-found", "sep-2164-data-uri", "WARNING"),
            ("server-sse-multiple-streams", "server-sse-streams-functional", "INFO"),
            *(
                ("server-stateless", f"sep-2575-server-{check}", "SKIPPED")
                for check in (
                    "sends-subscription-ack",
                    "tags-subscription-id",
                    "honors-notification-filter",
                    "sends-prompts-list-changed-on-subscription",
                    "sends-tools-list-changed-on-subscription",
                )
            ),
            (
                "input-required-result-ignore-extra-params"
                if profile == "shipping"
                else "input-required-result-validate-input",
                "sep-2322-ignore-unexpected-params"
                if profile == "shipping"
                else "sep-2322-error-on-protocol-error",
                "WARNING",
            ),
        }
    )
    for profile in PROFILES
}
MIN_LITERAL_CHARS: Final = 6


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

    def evidence(self, profile: str) -> tuple[re.Pattern[str], ...]:
        return EVIDENCE[("fixture" if self.kind == "fixture" else self.reason, profile)]

    @property
    def tool(self) -> str | None:
        """The tool a not-applicable report may name: the fixture or cited suite tool."""
        if self.kind == "fixture":
            return self.reason
        literal = self.source_literal.strip("`")
        return literal if literal.startswith("test_") else None


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


def _check_allowed(entry: Entry, where: str) -> None:
    """Only allowlisted scenarios and check ids may be listed; everything else must pass."""
    allowlist = FIXTURE_COVERABLE if entry.kind == "fixture" else EXCLUDABLE[entry.reason]
    if entry.scenario not in allowlist:
        raise ManifestError(
            f"{where}: {entry.scenario!r} may not be listed"
            + ("" if entry.kind == "fixture" else f" for {entry.reason!r}")
            + "; it must pass"
        )
    allowed_checks = allowlist[entry.scenario]
    if allowed_checks is not None and not set(entry.checks) <= allowed_checks:
        extra = sorted(set(entry.checks) - allowed_checks)
        raise ManifestError(f"{where}: checks {extra} of {entry.scenario!r} must pass")


def load_manifest(path: Path) -> Manifest:
    data = tomllib.loads(path.read_text(encoding="utf-8"))
    unknown = set(data) - {"capability", "exclusion", "fixture"}
    if unknown:
        raise ManifestError(f"unknown top-level keys: {sorted(unknown)}")
    capabilities: dict[str, Capability] = {}
    for name, table in data.get("capability", {}).items():
        where = f"capability.{name}"
        if name not in EXCLUDABLE:
            raise ManifestError(f"{where}: not an excludable capability")
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
            if kind == "capability" and reason not in capabilities:
                raise ManifestError(f"{where}: unknown capability {reason!r}")
            entry = Entry(
                kind=kind,
                scenario=scenario,
                checks=_strings(row.get("checks"), f"{where}.checks"),
                profiles=profiles,
                reason=reason,
                justification=_text(row, "justification", where),
                source=_text(row, "source", where),
                source_literal=_text(row, "source_literal", where),
            )
            if len(entry.source_literal.strip("`")) < MIN_LITERAL_CHARS:
                raise ManifestError(f"{where}: `source_literal` is too short to cite anything")
            _check_allowed(entry, where)
            entries.append(entry)
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


def scenario_source(package_source: str, scenario: str) -> str | None:
    """The source of the scenario's own class in the bundle, or ``None`` when it is not there once.

    The bundle declares each scenario as a class whose constructor sets ``this.name``: the span
    runs from the class keyword before that assignment to the class keyword before the next
    scenario's assignment.
    """
    markers = [f"this.name=`{scenario}`", f"this.name = `{scenario}`"]
    positions = [index for marker in markers if (index := package_source.find(marker)) != -1]
    if len(positions) != 1 or any(package_source.count(marker) > 1 for marker in markers):
        return None
    position = positions[0]
    start = package_source.rfind("class", 0, position)
    following = [
        index
        for marker in ("this.name=`", "this.name = `")
        if (index := package_source.find(marker, position + 1)) != -1
    ]
    end = len(package_source)
    if following:
        next_name = min(following)
        class_start = package_source.rfind("class", position, next_name)
        end = class_start if class_start != -1 else next_name
    return package_source[max(start, 0) : end]


def check_sources(manifest: Manifest, package_source: str) -> list[str]:
    """Each citation's literal and listed check ids occur in the cited scenario's own class."""
    errors = []
    for entry in manifest.entries:
        source = scenario_source(package_source, entry.scenario)
        if source is None:
            errors.append(f"citation: scenario {entry.scenario!r} not in the pinned package")
            continue
        if entry.source_literal not in source:
            errors.append(
                f"citation: literal {entry.source_literal!r} not in scenario {entry.scenario!r}"
            )
        for check in entry.checks:
            if f"`{check}`" not in source:
                errors.append(f"citation: check {check!r} not in scenario {entry.scenario!r}")
    return errors


def _status(check: Mapping[str, Any], where: str, errors: list[str]) -> str:
    status = check.get("status")
    if status not in STATUSES:
        errors.append(f"{where}: unknown status {status!r}")
        return "ERROR"
    return str(status)


@dataclass(slots=True)
class _Group:
    status: str
    failures: list[Mapping[str, Any]]


def _grouped(
    scenario: str, checks: Sequence[Mapping[str, Any]], errors: list[str]
) -> dict[str, _Group]:
    """One status per check id: a scenario may report an id once per variant it probes.

    The id failed if any of its reports failed, passed if all passed, else it is the first
    other status (a warning, a skip or information). Failed reports are kept for their evidence.
    """
    reports: dict[str, list[tuple[str, Mapping[str, Any]]]] = {}
    for check in checks:
        check_id = check.get("id")
        if not isinstance(check_id, str) or not check_id:
            errors.append(f"results: {scenario} has a check without an id")
            continue
        status = _status(check, f"{scenario}/{check_id}", errors)
        reports.setdefault(check_id, []).append((status, check))
    grouped = {}
    for check_id, values in reports.items():
        failures = [check for status, check in values if status in FAILED]
        others = [status for status, _ in values if status != "SUCCESS"]
        status = (
            next(status for status, _ in values if status in FAILED)
            if failures
            else (others[0] if others else "SUCCESS")
        )
        grouped[check_id] = _Group(status, failures)
    return grouped


def _unknown_tool_result(report: Mapping[str, Any], tool: str | None) -> bool:
    """The report's result is the SDK's unknown-tool error for exactly ``tool``."""
    details = report.get("details")
    result = details.get("result") if isinstance(details, Mapping) else None
    return (
        tool is not None
        and isinstance(result, Mapping)
        and result.get("isError") is True
        and result.get("content") == [{"text": f"Unknown tool: {tool}", "type": "text"}]
    )


def _evidence_holds(entry: Entry, profile: str, match: re.Match[str], report: Any) -> bool:
    """What the message alone cannot show: which tool was missing, and that nothing answered."""
    named = match.groupdict().get("tool")
    if named is not None:
        return named == entry.tool
    message = match.string
    if (
        entry.reason == "input_requests"
        and profile == "fixture"
        and message.startswith(("Expected", "Server returned"))
    ):
        return _unknown_tool_result(report, entry.tool)
    if entry.reason == "input_requests" and message.startswith("Expected"):
        details = report.get("details")
        return not (isinstance(details, Mapping) and details.get("result"))
    return True


def has_evidence(entry: Entry, profile: str, report: Mapping[str, Any]) -> bool:
    """The failed report's message says, in the suite's exact words, that the entry applies."""
    message = report.get("errorMessage")
    if not isinstance(message, str):
        return False
    for pattern in entry.evidence(profile):
        match = pattern.fullmatch(message)
        if match is not None:
            return _evidence_holds(entry, profile, match, report)
    return False


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
        if not checks:
            report.errors.append(f"results: scored scenario {scenario!r} reported no check")
        for check_id, group in _grouped(scenario, checks, report.errors).items():
            key = (scenario, check_id)
            seen.add(key)
            entry = listed.get(key)
            if group.status in FAILED:
                if entry is None:
                    report.failed += 1
                    report.errors.append(f"unexpected failure: {scenario}/{check_id}")
                elif not all(has_evidence(entry, profile, failure) for failure in group.failures):
                    report.failed += 1
                    report.errors.append(
                        f"unexpected failure: {scenario}/{check_id} (listed for "
                        f"{entry.reason!r}, but the report shows no such reason)"
                    )
                elif entry.kind == "fixture":
                    report.covered += 1
                else:
                    report.excluded += 1
            elif entry is not None:
                report.errors.append(f"stale entry: {scenario}/{check_id} now {group.status}")
            elif (scenario, check_id, group.status) in OTHER_STATUSES[profile]:
                report.other += 1
            elif group.status == "SUCCESS":
                report.passed += 1
                if any(key == (s, c) for s, c, _ in OTHER_STATUSES[profile]):
                    report.errors.append(
                        f"stale status pin: {scenario}/{check_id} now SUCCESS (update "
                        "OTHER_STATUSES)"
                    )
            else:
                report.other += 1
                report.errors.append(f"unexpected status: {scenario}/{check_id} is {group.status}")
    for scenario, required in ALWAYS_REPORTED.items():
        for check_id in sorted(required - {c for s, c in seen if s == scenario}):
            report.errors.append(f"missing: required check {scenario}/{check_id} did not run")
    for scenario, check_id in sorted(set(listed) - seen):
        if scenario in requirements.known and scenario not in requirements.unscored:
            report.errors.append(f"missing: listed check {scenario}/{check_id} did not run")
    return report


def check_fixture_coverage(
    manifest: Manifest, fixture: Mapping[str, Sequence[Mapping[str, Any]]]
) -> list[str]:
    """A check the shipping profile leaves to the fixture tools must pass on the fixture profile."""
    errors: list[str] = []
    for entry in manifest.entries:
        if entry.kind != "fixture":
            continue
        groups = _grouped(entry.scenario, fixture.get(entry.scenario, ()), errors)
        for check_id in entry.checks:
            status = groups[check_id].status if check_id in groups else "missing"
            if status != "SUCCESS":
                errors.append(
                    f"fixture coverage: {entry.scenario}/{check_id} is {status} on the fixture "
                    "profile"
                )
    return errors


def check_same_checks(
    requirements: Requirements,
    results: Mapping[str, Mapping[str, Sequence[Mapping[str, Any]]]],
) -> list[str]:
    """Both profiles report the same check ids for every scored scenario: none was dropped."""
    errors = []
    for scenario in sorted(requirements.scored):
        ids = [
            {check.get("id") for check in results[profile].get(scenario, ())}
            for profile in PROFILES
        ]
        if ids[0] != ids[1]:
            errors.append(
                f"results: {scenario!r} reports other check ids on the shipping and fixture "
                f"profiles: {sorted(map(str, ids[0] ^ ids[1]))}"
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
    errors.extend(check_same_checks(requirements, results))
    if package_source is not None:
        errors.extend(check_sources(manifest, package_source))
    return reports, errors


def _aggregate(results: Mapping[str, Any], path: Path) -> None:
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

    # Results first, so the artifact has per-profile checks even when nothing else loads.
    results: dict[str, dict[str, list[dict[str, Any]]]] = {}
    problem: Exception | None = None
    for profile, directory in (("shipping", arguments.shipping), ("fixture", arguments.fixture)):
        try:
            results[profile] = load_results(directory)
            _aggregate(results[profile], arguments.out / profile / "checks.json")
        except (ManifestError, OSError, ValueError) as error:
            problem = problem or error
            _aggregate({"error": type(error).__name__}, arguments.out / profile / "checks.json")
    try:
        if problem is not None:
            raise problem
        manifest = load_manifest(arguments.manifest)
        requirements = load_requirements(arguments.requirements)
    except (ManifestError, OSError, ValueError, KeyError) as error:
        summary_stub = {"error": type(error).__name__, "errors": 1}
        (arguments.out / "summary.json").write_text(json.dumps(summary_stub) + "\n", "utf-8")
        print(f"conformance: cannot evaluate: {error}", file=sys.stderr)
        return 1
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
    (arguments.out / "summary.json").write_text(json.dumps(summary, indent=2) + "\n", "utf-8")
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
