"""The conformance evaluator on synthetic suite results (PLATFORM-047).

Every rule that can fail a run is exercised here without Node or a network: unlisted scored
failures, stale and missing entries, unknown scenarios, listing an unscored or a protected
scenario, fixture coverage that does not pass, and citations the pinned package does not hold.
"""

from __future__ import annotations

import json
import textwrap
from pathlib import Path
from typing import Any

import evaluate
import pytest

pytestmark = pytest.mark.unit

MANIFEST = Path(__file__).with_name("applicability.toml")
PACKAGE = (
    "this.name=`resources-list` this.name=`tools-call-simple-text` "
    "this.name=`server-stateless` `resources/list` `test_simple_text` `test_missing_capability`"
)


def _manifest(tmp_path: Path, body: str) -> evaluate.Manifest:
    path = tmp_path / "manifest.toml"
    path.write_text(textwrap.dedent(body), encoding="utf-8")
    return evaluate.load_manifest(path)


BASE = """
    [capability.resources]
    discover_key = "resources"
    methods = ["resources/list"]
    description = "Server resources."

    [[exclusion]]
    scenario = "resources-list"
    checks = ["resources-list"]
    profiles = ["shipping", "fixture"]
    capability = "resources"
    justification = "Calls only resources/list."
    source = "dist/index.js scenario `resources-list`"
    source_literal = '`resources/list`'

    [[fixture]]
    scenario = "tools-call-simple-text"
    checks = ["tools-call-simple-text"]
    tool = "test_simple_text"
    justification = "A fixture tool."
    source = "dist/index.js scenario `tools-call-simple-text`"
    source_literal = '`test_simple_text`'
"""
REQUIREMENTS = evaluate.Requirements(
    scored=frozenset({"resources-list", "tools-call-simple-text", "server-stateless"}),
    unscored=frozenset({"tasks-lifecycle"}),
)


def _check(check_id: str, status: str = "SUCCESS") -> dict[str, Any]:
    return {"id": check_id, "status": status, "errorMessage": "detail never printed"}


def _results(**overrides: list[dict[str, Any]]) -> dict[str, dict[str, list[dict[str, Any]]]]:
    shipping = {
        "resources-list": [_check("resources-list", "FAILURE")],
        "tools-call-simple-text": [_check("tools-call-simple-text", "FAILURE")],
        "server-stateless": [_check("sep-2575-a"), _check("sep-2575-b", "WARNING")],
        "tasks-lifecycle": [_check("tasks-x", "FAILURE")],
    }
    fixture = {
        "resources-list": [_check("resources-list", "FAILURE")],
        "tools-call-simple-text": [_check("tools-call-simple-text")],
        "server-stateless": [_check("sep-2575-a"), _check("sep-2575-b")],
        "tasks-lifecycle": [_check("tasks-x")],
    }
    for name, checks in overrides.items():
        profile, _, scenario = name.partition("__")
        target = shipping if profile == "shipping" else fixture
        target[scenario.replace("_", "-")] = checks
    return {"shipping": shipping, "fixture": fixture}


def _errors(
    manifest: evaluate.Manifest,
    results: dict[str, dict[str, list[dict[str, Any]]]],
    package: str | None = PACKAGE,
) -> list[str]:
    reports, errors = evaluate.evaluate(manifest, REQUIREMENTS, results, package)
    return [error for report in reports for error in report.errors] + errors


def test_listed_failures_and_fixture_coverage_pass(tmp_path: Path) -> None:
    manifest = _manifest(tmp_path, BASE)
    reports, errors = evaluate.evaluate(manifest, REQUIREMENTS, _results(), PACKAGE)

    assert errors == []
    shipping, fixture = reports
    assert shipping.errors == fixture.errors == []
    assert (shipping.excluded, shipping.covered, shipping.passed, shipping.other) == (1, 1, 1, 1)
    assert (fixture.excluded, fixture.covered, fixture.passed) == (1, 0, 3)
    assert (shipping.unscored_scenarios, shipping.unscored_failed_scenarios) == (1, 1)
    assert fixture.unscored_failed_scenarios == 0


def test_an_unlisted_scored_failure_fails_the_run(tmp_path: Path) -> None:
    manifest = _manifest(tmp_path, BASE)
    results = _results(shipping__server_stateless=[_check("sep-2575-a", "FAILURE")])
    assert _errors(manifest, results) == ["unexpected failure: server-stateless/sep-2575-a"]


def test_an_error_status_is_a_failure_too(tmp_path: Path) -> None:
    manifest = _manifest(tmp_path, BASE)
    results = _results(fixture__server_stateless=[_check("sep-2575-a", "ERROR")])
    assert _errors(manifest, results) == ["unexpected failure: server-stateless/sep-2575-a"]


def test_a_listed_check_that_passes_is_stale(tmp_path: Path) -> None:
    manifest = _manifest(tmp_path, BASE)
    results = _results(fixture__resources_list=[_check("resources-list")])
    assert _errors(manifest, results) == ["stale entry: resources-list/resources-list now SUCCESS"]


def test_a_listed_check_that_did_not_run_is_missing(tmp_path: Path) -> None:
    manifest = _manifest(tmp_path, BASE)
    results = _results(shipping__resources_list=[_check("other-check", "FAILURE")])
    assert _errors(manifest, results) == [
        "unexpected failure: resources-list/other-check",
        "missing: listed check resources-list/resources-list did not run",
    ]


def test_a_scored_scenario_that_did_not_run_fails_the_run(tmp_path: Path) -> None:
    manifest = _manifest(tmp_path, BASE)
    results = _results()
    del results["fixture"]["server-stateless"]
    assert _errors(manifest, results) == ["results: scored scenario 'server-stateless' did not run"]


def test_unknown_scenarios_fail_the_run(tmp_path: Path) -> None:
    manifest = _manifest(tmp_path, BASE.replace('"resources-list"', '"resources-lst"', 1))
    results = _results()
    results["shipping"]["brand-new-scenario"] = [_check("x")]
    errors = _errors(manifest, results, None)
    assert "results: unknown scenario 'brand-new-scenario'" in errors
    assert "manifest: unknown scenario 'resources-lst'" in errors


def test_an_unscored_scenario_is_never_listed(tmp_path: Path) -> None:
    manifest = _manifest(
        tmp_path,
        BASE.replace('scenario = "resources-list"', 'scenario = "tasks-lifecycle"', 1),
    )
    errors = _errors(manifest, _results(), None)
    assert "manifest: 'tasks-lifecycle' is not scored; do not list it" in errors


def test_a_fixture_covered_check_must_pass_on_the_fixture_profile(tmp_path: Path) -> None:
    manifest = _manifest(tmp_path, BASE)
    results = _results(
        fixture__tools_call_simple_text=[_check("tools-call-simple-text", "FAILURE")]
    )
    assert (
        "fixture coverage: tools-call-simple-text/tools-call-simple-text is FAILURE on the "
        "fixture profile" in _errors(manifest, results)
    )


def test_a_repeated_check_id_fails_if_any_report_failed(tmp_path: Path) -> None:
    manifest = _manifest(tmp_path, BASE)
    repeated = [_check("sep-2575-a"), _check("sep-2575-a", "FAILURE"), _check("sep-2575-b")]
    assert _errors(manifest, _results(shipping__server_stateless=repeated)) == [
        "unexpected failure: server-stateless/sep-2575-a"
    ]
    reports, _ = evaluate.evaluate(
        manifest,
        REQUIREMENTS,
        _results(fixture__server_stateless=[_check("sep-2575-a")] * 3),
        PACKAGE,
    )
    assert reports[1].passed == 2  # one per id, not per report


def test_unknown_statuses_and_ids_are_errors(tmp_path: Path) -> None:
    manifest = _manifest(tmp_path, BASE)
    results = _results(
        fixture__server_stateless=[_check("sep-2575-a", "MAYBE"), {"status": "SUCCESS"}]
    )
    errors = _errors(manifest, results)
    assert "server-stateless/sep-2575-a: unknown status 'MAYBE'" in errors
    assert "results: server-stateless has a check without an id" in errors


def test_citations_must_exist_in_the_pinned_package(tmp_path: Path) -> None:
    manifest = _manifest(tmp_path, BASE.replace("'`resources/list`'", "'`resources/lisst`'"))
    errors = _errors(manifest, _results(), PACKAGE.replace("this.name=`resources-list`", ""))
    assert "citation: scenario 'resources-list' not in the pinned package" in errors
    assert (
        "citation: literal '`resources/lisst`' of 'resources-list' not in the pinned package"
        in errors
    )


@pytest.mark.parametrize(
    "scenario",
    [
        "server-stateless",
        "dns-rebinding-protection",
        "http-header-validation",
        "auth/x",
        "caching-session",
    ],
)
def test_a_protected_scenario_can_never_be_excluded(tmp_path: Path, scenario: str) -> None:
    body = BASE.replace('scenario = "resources-list"', f'scenario = "{scenario}"', 1)
    with pytest.raises(evaluate.ManifestError, match="protected"):
        _manifest(tmp_path, body)


@pytest.mark.parametrize(
    ("old", "new", "message"),
    [
        ('capability = "resources"', 'capability = "unicorns"', "unknown capability"),
        ('profiles = ["shipping", "fixture"]', 'profiles = ["staging"]', "unknown profile"),
        ('justification = "Calls only resources/list."', 'justification = " "', "justification"),
        ('checks = ["resources-list"]', "checks = []", "non-empty list"),
        ('checks = ["resources-list"]', 'checks = ["a", "a"]', "duplicate"),
        ("[[fixture]]", "[[waiver]]", "unknown top-level keys"),
        ('tool = "test_simple_text"', 'tool = "test_simple_text"\nnote = "x"', "unknown keys"),
        ('discover_key = "resources"', "discover_key = 1", "discover_key"),
    ],
)
def test_a_malformed_manifest_is_refused(tmp_path: Path, old: str, new: str, message: str) -> None:
    assert old in BASE
    with pytest.raises(evaluate.ManifestError, match=message):
        _manifest(tmp_path, BASE.replace(old, new, 1))


def test_a_check_listed_twice_and_an_unused_capability_are_refused(tmp_path: Path) -> None:
    twice = BASE + BASE[BASE.index("    [[exclusion]]") : BASE.index("    [[fixture]]")]
    with pytest.raises(evaluate.ManifestError, match="listed twice"):
        _manifest(tmp_path, twice)
    unused = BASE + '\n    [capability.prompts]\n    methods = ["prompts/list"]\n'
    unused += '    description = "Prompts."\n'
    with pytest.raises(evaluate.ManifestError, match="never used"):
        _manifest(tmp_path, unused)


def test_the_shipped_manifest_is_valid_and_covers_only_known_capabilities() -> None:
    manifest = evaluate.load_manifest(MANIFEST)

    assert set(manifest.capabilities) == {
        "resources",
        "prompts",
        "completions",
        "input_requests",
        "response_stream",
    }
    assert all(entry.justification and entry.source for entry in manifest.entries)
    covered = {entry.scenario for entry in manifest.entries if entry.kind == "fixture"}
    assert covered == {
        "tools-call-simple-text",
        "tools-call-image",
        "tools-call-audio",
        "tools-call-embedded-resource",
        "tools-call-mixed-content",
        "tools-call-error",
        "server-stateless",
    }


def _suite_output(root: Path, profile: str, scenarios: dict[str, list[dict[str, Any]]]) -> Path:
    directory = root / profile / "results"
    for index, (scenario, checks) in enumerate(scenarios.items()):
        result = directory / f"server-{scenario}-2026-10-24T15-25-3{index}-437Z"
        result.mkdir(parents=True)
        (result / "checks.json").write_text(json.dumps(checks), encoding="utf-8")
    (directory / "not-a-result").mkdir()
    return directory


def _requirements_file(tmp_path: Path) -> Path:
    path = tmp_path / "2026-07-28.yaml"
    path.write_text(
        "server:\n  - resources-list\n  - tools-call-simple-text\n  - server-stateless\n"
        "client:\n  - tools_call\n"
        "not_scored:\n  - scenario: tasks-lifecycle\n    leg: server\n    reason: extension\n"
        "  - scenario: auth/dpop\n    leg: client\n    reason: extension\n",
        encoding="utf-8",
    )
    return path


def _run_main(tmp_path: Path, results: dict[str, dict[str, list[dict[str, Any]]]]) -> int:
    manifest = tmp_path / "manifest.toml"
    manifest.write_text(textwrap.dedent(BASE), encoding="utf-8")
    package = tmp_path / "index.js"
    package.write_text(PACKAGE, encoding="utf-8")
    return evaluate.main(
        [
            "--manifest",
            str(manifest),
            "--requirements",
            str(_requirements_file(tmp_path)),
            "--package-source",
            str(package),
            "--shipping",
            str(_suite_output(tmp_path, "shipping", results["shipping"])),
            "--fixture",
            str(_suite_output(tmp_path, "fixture", results["fixture"])),
            "--out",
            str(tmp_path / "out"),
        ]
    )


def test_main_writes_per_profile_checks_and_a_count_only_summary(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    assert _run_main(tmp_path, _results()) == 0

    summary = json.loads((tmp_path / "out" / "summary.json").read_text(encoding="utf-8"))
    assert summary["errors"] == 0
    assert summary["manifest_entries"] == 2
    assert [profile["profile"] for profile in summary["profiles"]] == ["shipping", "fixture"]
    shipping = json.loads((tmp_path / "out" / "shipping" / "checks.json").read_text("utf-8"))
    assert set(shipping) == {
        "resources-list",
        "tools-call-simple-text",
        "server-stateless",
        "tasks-lifecycle",
    }
    output = capsys.readouterr()
    assert "detail never printed" not in output.out + output.err
    assert "conformance[shipping]: scored scenarios 3" in output.out


def test_main_fails_on_an_unexpected_failure(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    results = _results(shipping__server_stateless=[_check("sep-2575-a", "FAILURE")])
    assert _run_main(tmp_path, results) == 1
    assert "unexpected failure: server-stateless/sep-2575-a" in capsys.readouterr().err


def test_main_refuses_unreadable_inputs(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    (tmp_path / "manifest.toml").write_text("[[waiver]]\n", encoding="utf-8")
    code = evaluate.main(
        [
            "--manifest",
            str(tmp_path / "manifest.toml"),
            "--requirements",
            str(tmp_path / "missing.yaml"),
            "--package-source",
            str(tmp_path / "index.js"),
            "--shipping",
            str(tmp_path),
            "--fixture",
            str(tmp_path),
            "--out",
            str(tmp_path / "out"),
        ]
    )
    assert code == 1
    assert "cannot evaluate" in capsys.readouterr().err
