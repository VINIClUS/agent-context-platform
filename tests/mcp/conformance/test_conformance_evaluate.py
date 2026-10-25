"""The conformance evaluator on synthetic suite results (PLATFORM-047).

Every rule that can fail a run is exercised here without Node or a network: unlisted scored
failures, listed failures without the suite's not-applicable evidence, stale and missing entries,
unknown scenarios, listing an unscored or a non-allowlisted scenario, fixture coverage that does
not pass, and citations outside the cited scenario. The review's mutants M2b and M2c (a bogus but
well-formed entry masking a real regression) are among them.
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
# A bundle in the pinned package's shape: one class per scenario, `this.name` in its constructor.
PACKAGE = (
    "var a=class{constructor(){this.name=`resources-list`}run(){r(`resources/list`);"
    "c(`resources-list`)}},"
    "b=class{constructor(){this.name=`tools-call-simple-text`}run(){t(`test_simple_text`);"
    "c(`tools-call-simple-text`)}},"
    "d=class{constructor(){this.name=`tools-list`}run(){r(`tools/list`);c(`tools-list`)}},"
    "e=class{constructor(){this.name=`server-stateless`}run(){r(`prompts/list`);"
    "t(`test_missing_capability`);c(`sep-2575-a`);c(`sep-2575-b`)}}"
)
CANARY = "detail never printed"


# The shipped pins, before the synthetic tests replace them.
SHIPPED_ALWAYS_REPORTED = evaluate.ALWAYS_REPORTED
SHIPPED_OTHER_STATUSES = evaluate.OTHER_STATUSES


@pytest.fixture(autouse=True)
def synthetic_pins(monkeypatch: pytest.MonkeyPatch) -> None:
    """The synthetic results' own pins (the shipped ones are tested against the real manifest)."""
    monkeypatch.setattr(evaluate, "ALWAYS_REPORTED", {})
    monkeypatch.setattr(
        evaluate,
        "OTHER_STATUSES",
        {
            "shipping": frozenset({("server-stateless", "sep-2575-b", "WARNING")}),
            "fixture": frozenset(),
        },
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


def _check(check_id: str, status: str = "SUCCESS", message: str = "") -> dict[str, Any]:
    return {"id": check_id, "status": status, "errorMessage": message, "details": {"x": CANARY}}


UNSERVED = "Failed: Method not found"
UNKNOWN_TOOL = "Failed: Unknown tool"


def _results(**overrides: list[dict[str, Any]]) -> dict[str, dict[str, list[dict[str, Any]]]]:
    shipping = {
        "resources-list": [_check("resources-list", "FAILURE", UNSERVED)],
        "tools-call-simple-text": [_check("tools-call-simple-text", "FAILURE", UNKNOWN_TOOL)],
        "server-stateless": [_check("sep-2575-a"), _check("sep-2575-b", "WARNING")],
        "tasks-lifecycle": [_check("tasks-x", "FAILURE")],
    }
    fixture = {
        "resources-list": [_check("resources-list", "FAILURE", UNSERVED)],
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
    failed = [_check("sep-2575-a", "FAILURE"), _check("sep-2575-b", "WARNING")]
    results = _results(shipping__server_stateless=failed)
    assert _errors(manifest, results) == ["unexpected failure: server-stateless/sep-2575-a"]


def test_an_error_status_is_a_failure_too(tmp_path: Path) -> None:
    manifest = _manifest(tmp_path, BASE)
    results = _results(
        fixture__server_stateless=[_check("sep-2575-a", "ERROR"), _check("sep-2575-b")]
    )
    assert _errors(manifest, results) == ["unexpected failure: server-stateless/sep-2575-a"]


def test_a_listed_check_that_passes_is_stale(tmp_path: Path) -> None:
    manifest = _manifest(tmp_path, BASE)
    results = _results(fixture__resources_list=[_check("resources-list")])
    assert _errors(manifest, results) == ["stale entry: resources-list/resources-list now SUCCESS"]


def test_a_listed_check_that_did_not_run_is_missing(tmp_path: Path) -> None:
    manifest = _manifest(tmp_path, BASE)
    results = _results(shipping__resources_list=[_check("other-check", "FAILURE")])
    errors = _errors(manifest, results)
    assert errors[:2] == [
        "unexpected failure: resources-list/other-check",
        "missing: listed check resources-list/resources-list did not run",
    ]
    assert "other check ids on the shipping and fixture profiles" in errors[2]


def test_a_scored_scenario_that_did_not_run_fails_the_run(tmp_path: Path) -> None:
    manifest = _manifest(tmp_path, BASE)
    results = _results()
    del results["fixture"]["server-stateless"]
    errors = _errors(manifest, results)
    assert errors[0] == "results: scored scenario 'server-stateless' did not run"
    assert "other check ids" in errors[1]


def test_unknown_scenarios_fail_the_run(tmp_path: Path) -> None:
    manifest = _manifest(tmp_path, BASE)
    results = _results()
    results["shipping"]["brand-new-scenario"] = [_check("x")]
    # An allowlisted scenario the requirement set no longer knows.
    requirements = evaluate.Requirements(
        scored=REQUIREMENTS.scored - {"resources-list"}, unscored=REQUIREMENTS.unscored
    )
    reports, _ = evaluate.evaluate(manifest, requirements, results, None)
    errors = [error for report in reports for error in report.errors]
    assert "results: unknown scenario 'brand-new-scenario'" in errors
    assert "manifest: unknown scenario 'resources-list'" in errors


def test_an_unscored_scenario_is_never_listed(tmp_path: Path) -> None:
    manifest = _manifest(tmp_path, BASE)
    requirements = evaluate.Requirements(
        scored=REQUIREMENTS.scored - {"resources-list"},
        unscored=REQUIREMENTS.unscored | {"resources-list"},
    )
    reports, _ = evaluate.evaluate(manifest, requirements, _results(), None)
    errors = [error for report in reports for error in report.errors]
    assert "manifest: 'resources-list' is not scored; do not list it" in errors


def test_a_scored_scenario_without_checks_fails_the_run(tmp_path: Path) -> None:
    manifest = _manifest(tmp_path, BASE)
    errors = _errors(manifest, _results(fixture__server_stateless=[]))
    assert "results: scored scenario 'server-stateless' reported no check" in errors


def test_a_fixture_covered_check_must_pass_on_the_fixture_profile(tmp_path: Path) -> None:
    manifest = _manifest(tmp_path, BASE)
    results = _results(
        fixture__tools_call_simple_text=[_check("tools-call-simple-text", "FAILURE", UNKNOWN_TOOL)]
    )
    assert (
        "fixture coverage: tools-call-simple-text/tools-call-simple-text is FAILURE on the "
        "fixture profile" in _errors(manifest, results)
    )


def test_a_repeated_check_id_fails_if_any_report_failed(tmp_path: Path) -> None:
    manifest = _manifest(tmp_path, BASE)
    repeated = [
        _check("sep-2575-a"),
        _check("sep-2575-a", "FAILURE"),
        _check("sep-2575-b", "WARNING"),
    ]
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


def test_citations_must_exist_in_the_cited_scenario(tmp_path: Path) -> None:
    manifest = _manifest(tmp_path, BASE.replace("'`resources/list`'", "'`resources/lisst`'"))
    errors = _errors(manifest, _results())
    assert "citation: literal '`resources/lisst`' not in scenario 'resources-list'" in errors

    errors = _errors(_manifest(tmp_path, BASE), _results(), PACKAGE.replace("`resources-list`", ""))
    assert "citation: scenario 'resources-list' not in the pinned package" in errors


def test_a_literal_from_another_scenario_is_not_a_citation(tmp_path: Path) -> None:
    # `prompts/list` occurs in the bundle (server-stateless), but not in tools-call-simple-text.
    manifest = _manifest(tmp_path, BASE.replace("'`test_simple_text`'", "'`prompts/list`'"))
    errors = _errors(manifest, _results())
    assert "citation: literal '`prompts/list`' not in scenario 'tools-call-simple-text'" in errors


def test_a_listed_check_id_must_belong_to_the_cited_scenario(tmp_path: Path) -> None:
    body = BASE.replace('checks = ["tools-call-simple-text"]', 'checks = ["tools-list"]')
    errors = _errors(_manifest(tmp_path, body), _results())
    assert "citation: check 'tools-list' not in scenario 'tools-call-simple-text'" in errors


def test_a_scenario_named_twice_in_the_bundle_is_not_a_citation(tmp_path: Path) -> None:
    doubled = PACKAGE + "class{constructor(){this.name=`resources-list`}}"
    errors = _errors(_manifest(tmp_path, BASE), _results(), doubled)
    assert "citation: scenario 'resources-list' not in the pinned package" in errors


@pytest.mark.parametrize(
    "scenario",
    [
        "server-stateless",
        "dns-rebinding-protection",
        "http-header-validation",
        "tools-list",
        "auth/x",
        "integrity-tamper",
        "resources-list-session",
    ],
)
def test_only_allowlisted_scenarios_can_be_excluded(tmp_path: Path, scenario: str) -> None:
    body = BASE.replace('scenario = "resources-list"', f'scenario = "{scenario}"', 1)
    with pytest.raises(evaluate.ManifestError, match="may not be listed for 'resources'; it must"):
        _manifest(tmp_path, body)


def test_a_capability_may_exclude_only_its_own_scenarios(tmp_path: Path) -> None:
    body = BASE.replace(
        'scenario = "resources-list"\n    checks = ["resources-list"]',
        'scenario = "completion-complete"\n    checks = ["completion-complete"]',
    )
    with pytest.raises(evaluate.ManifestError, match="'completion-complete' may not be listed"):
        _manifest(tmp_path, body)


def test_a_mixed_scenario_admits_only_its_allowlisted_checks(tmp_path: Path) -> None:
    body = BASE.replace(
        'scenario = "resources-list"\n    checks = ["resources-list"]',
        'scenario = "caching"\n    checks = ["sep-2549-tools-list-caching-hints"]',
    )
    with pytest.raises(evaluate.ManifestError, match="sep-2549-tools-list-caching-hints"):
        _manifest(tmp_path, body)


@pytest.mark.parametrize(
    ("scenario", "check"),
    [
        ("server-stateless", "sep-2575-http-server-method-not-found-404"),
        ("dns-rebinding-protection", "dns-rebinding-protection"),
        ("tools-list", "tools-list"),
    ],
)
def test_fixture_coverage_is_allowlisted_too(tmp_path: Path, scenario: str, check: str) -> None:
    body = BASE.replace(
        'scenario = "tools-call-simple-text"\n    checks = ["tools-call-simple-text"]',
        f'scenario = "{scenario}"\n    checks = ["{check}"]',
    )
    with pytest.raises(evaluate.ManifestError, match="must pass"):
        _manifest(tmp_path, body)


def _append(body: str) -> str:
    return MANIFEST.read_text(encoding="utf-8") + textwrap.dedent(body)


def test_mutant_m2c_a_capability_exclusion_on_tools_list_is_refused(tmp_path: Path) -> None:
    """M2c: a tools/list regression masked by a well-formed `prompts` exclusion."""
    mutant = _append(
        """
        [[exclusion]]
        scenario = "tools-list"
        checks = ["tools-list"]
        profiles = ["shipping"]
        capability = "prompts"
        justification = "Bogus."
        source = "dist/index.js scenario `tools-list`"
        source_literal = '`prompts/list`'
        """
    )
    (tmp_path / "m.toml").write_text(mutant, encoding="utf-8")
    with pytest.raises(evaluate.ManifestError, match="'tools-list' may not be listed"):
        evaluate.load_manifest(tmp_path / "m.toml")


def test_mutant_m2b_fixture_coverage_of_server_stateless_routing_is_refused(
    tmp_path: Path,
) -> None:
    """M2b: a 404->200 regression masked by fixture coverage of the routing checks."""
    checks = ", ".join(
        f'"sep-2575-http-server-method-not-found-404{suffix}"'
        for suffix in (
            "",
            "-initialize",
            "-ping",
            "-logging-setlevel",
            "-resources-subscribe",
            "-resources-unsubscribe",
        )
    )
    mutant = _append(
        f"""
        [[fixture]]
        scenario = "server-stateless"
        checks = [{checks}]
        tool = "test_missing_capability"
        justification = "Bogus."
        source = "dist/index.js scenario `server-stateless`"
        source_literal = '`test_missing_capability`'
        """
    )
    (tmp_path / "m.toml").write_text(mutant, encoding="utf-8")
    with pytest.raises(evaluate.ManifestError, match="must pass"):
        evaluate.load_manifest(tmp_path / "m.toml")


def test_a_listed_failure_without_not_applicable_evidence_is_unexpected(tmp_path: Path) -> None:
    """Even an allowlisted entry masks nothing but its own reason (M2b/M2c's second line)."""
    manifest = _manifest(tmp_path, BASE)
    regression = "Expected HTTP 404 and code -32602, got HTTP 200"
    results = _results(
        shipping__resources_list=[_check("resources-list", "FAILURE", "Missing resources array")],
        shipping__tools_call_simple_text=[_check("tools-call-simple-text", "FAILURE", regression)],
    )
    assert _errors(manifest, results) == [
        "unexpected failure: resources-list/resources-list (listed for 'resources', but the "
        "report shows no such reason)",
        "unexpected failure: tools-call-simple-text/tools-call-simple-text (listed for "
        "'test_simple_text', but the report shows no such reason)",
    ]


def test_every_report_of_a_repeated_failed_check_needs_evidence(tmp_path: Path) -> None:
    manifest = _manifest(tmp_path, BASE)
    reports = [
        _check("resources-list", "FAILURE", UNSERVED),
        _check("resources-list", "FAILURE", "Resource 0: missing uri"),
    ]
    errors = _errors(manifest, _results(fixture__resources_list=reports))
    assert errors == [
        "unexpected failure: resources-list/resources-list (listed for 'resources', but the "
        "report shows no such reason)"
    ]


def test_evidence_naming_another_tool_does_not_count(tmp_path: Path) -> None:
    manifest = _manifest(tmp_path, BASE)
    other = "Not testable: server does not list the diagnostic tool 'test_logging_tool'"
    results = _results(
        shipping__tools_call_simple_text=[_check("tools-call-simple-text", "FAILURE", other)]
    )
    assert len(_errors(manifest, results)) == 1
    own = "Not testable: server does not list the diagnostic tool 'test_simple_text'"
    results = _results(
        shipping__tools_call_simple_text=[_check("tools-call-simple-text", "FAILURE", own)]
    )
    assert _errors(manifest, results) == []


def test_evidence_must_be_the_message_not_the_echoed_details(tmp_path: Path) -> None:
    """P5: `details` echo what the server sent; a server-sent "Method not found" is no evidence."""
    manifest = _manifest(tmp_path, BASE)
    echoed = _check("resources-list", "FAILURE", "Missing resources array")
    echoed["details"] = {"result": {"error": "Method not found", "code": -32601}}
    assert _errors(manifest, _results(shipping__resources_list=[echoed])) == [
        "unexpected failure: resources-list/resources-list (listed for 'resources', but the "
        "report shows no such reason)"
    ]


def test_evidence_is_the_suites_exact_phrasing() -> None:
    """P10: the marker must be the whole message, not a word inside a 500's text."""
    entries = {e.scenario: e for e in evaluate.load_manifest(MANIFEST).entries}
    mrtr = entries["input-required-result-basic-elicitation"]
    for message in (
        "JSON-RPC error: Unknown tool",
        "Prerequisite failed: could not get initial InputRequiredResult with requestState",
    ):
        assert evaluate.has_evidence(mrtr, "shipping", {"errorMessage": message})
    for message in (
        "HTTP 500: InputRequiredResult handler crashed",
        "JSON-RPC error: Internal error (Unknown tool)",
        "Failed: Unknown tool",
    ):
        assert not evaluate.has_evidence(mrtr, "shipping", {"errorMessage": message})
    multi = entries["input-required-result-multi-round"]
    no_result = {"errorMessage": "Expected InputRequiredResult with inputRequests and requestState"}
    assert evaluate.has_evidence(multi, "shipping", {**no_result, "details": {}})
    assert not evaluate.has_evidence(
        multi, "shipping", {**no_result, "details": {"result": {"content": []}}}
    )


def test_fixture_profile_input_request_evidence_needs_the_unknown_tool_result() -> None:
    entries = {e.scenario: e for e in evaluate.load_manifest(MANIFEST).entries}
    entry = entries["input-required-result-basic-elicitation"]
    message = {"errorMessage": "Expected InputRequiredResult but got a complete result."}

    def result(text: str) -> dict[str, Any]:
        return {"result": {"isError": True, "content": [{"text": text, "type": "text"}]}}

    own = result("Unknown tool: test_input_required_result_elicitation")
    assert evaluate.has_evidence(entry, "fixture", {**message, "details": own})
    for details in (
        result("Unknown tool: test_input_required_result_sampling"),  # another tool
        result("Internal error"),
        {"result": {"isError": False, "content": own["result"]["content"]}},
        None,
    ):
        assert not evaluate.has_evidence(entry, "fixture", {**message, "details": details})


def test_evidence_is_per_profile() -> None:
    """P9: the fixture profile serves the progress tool, so "Unknown tool" there is a regression."""
    entries = {e.scenario: e for e in evaluate.load_manifest(MANIFEST).entries}
    progress = entries["tools-call-with-progress"]
    unknown = {"errorMessage": "Failed: Unknown tool"}
    silent = {"errorMessage": "No progress notifications received"}
    assert evaluate.has_evidence(progress, "shipping", unknown)
    assert not evaluate.has_evidence(progress, "fixture", unknown)
    assert evaluate.has_evidence(progress, "fixture", silent)
    assert not evaluate.has_evidence(progress, "shipping", silent)
    assert not evaluate.has_evidence(progress, "shipping", {"errorMessage": None})


def test_both_profiles_must_report_the_same_check_ids(tmp_path: Path) -> None:
    """P1: a check dropped on one profile fails the run even when nothing is listed for it."""
    manifest = _manifest(tmp_path, BASE)
    results = _results(fixture__server_stateless=[_check("sep-2575-a")])
    assert _errors(manifest, results) == [
        "results: 'server-stateless' reports other check ids on the shipping and fixture "
        "profiles: ['sep-2575-b']"
    ]


def test_a_status_drift_fails_the_run(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """P1: a success turning into a skip, or a pinned status changing, fails the run."""
    manifest = _manifest(tmp_path, BASE)
    skipped = _results(
        fixture__server_stateless=[_check("sep-2575-a", "SKIPPED"), _check("sep-2575-b")]
    )
    assert _errors(manifest, skipped) == [
        "unexpected status: server-stateless/sep-2575-a is SKIPPED"
    ]
    now_passing = _results(shipping__server_stateless=[_check("sep-2575-a"), _check("sep-2575-b")])
    assert _errors(manifest, now_passing) == [
        "stale status pin: server-stateless/sep-2575-b now SUCCESS (update OTHER_STATUSES)"
    ]


def test_always_reported_checks_must_run(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        evaluate, "ALWAYS_REPORTED", {"server-stateless": frozenset({"sep-2575-a", "sep-2575-c"})}
    )
    errors = _errors(_manifest(tmp_path, BASE), _results())
    assert errors == [
        "missing: required check server-stateless/sep-2575-c did not run",
        "missing: required check server-stateless/sep-2575-c did not run",
    ]


def test_the_shipped_pins_cover_the_dns_rebinding_pair_and_both_profiles() -> None:
    pair = frozenset({"localhost-host-rebinding-rejected", "localhost-host-valid-accepted"})
    assert {"dns-rebinding-protection": pair} == SHIPPED_ALWAYS_REPORTED
    assert set(SHIPPED_OTHER_STATUSES) == set(evaluate.PROFILES)
    assert all(len(pins) == 11 for pins in SHIPPED_OTHER_STATUSES.values())


def test_a_citation_literal_must_be_long_enough_to_cite_anything(tmp_path: Path) -> None:
    with pytest.raises(evaluate.ManifestError, match="too short"):
        _manifest(tmp_path, BASE.replace("'`resources/list`'", "'`list`'"))


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


SHIPPED_EXCLUSIONS = {
    ("resources", "resources-list", "resources-list"),
    ("resources", "resources-read-text", "resources-read-text"),
    ("resources", "resources-read-binary", "resources-read-binary"),
    ("resources", "resources-templates-read", "resources-templates-read"),
    ("resources", "caching", "sep-2549-resources-list-caching-hints"),
    ("resources", "caching", "sep-2549-resources-templates-list-caching-hints"),
    ("prompts", "prompts-list", "prompts-list"),
    ("prompts", "prompts-get-simple", "prompts-get-simple"),
    ("prompts", "prompts-get-with-args", "prompts-get-with-args"),
    ("prompts", "prompts-get-embedded-resource", "prompts-get-embedded-resource"),
    ("prompts", "prompts-get-with-image", "prompts-get-with-image"),
    ("prompts", "caching", "sep-2549-prompts-list-caching-hints"),
    ("completions", "completion-complete", "completion-complete"),
    (
        "input_requests",
        "input-required-result-basic-elicitation",
        "sep-2322-elicitation-incomplete",
    ),
    ("input_requests", "input-required-result-basic-sampling", "sep-2322-sampling-incomplete"),
    ("input_requests", "input-required-result-basic-list-roots", "sep-2322-list-roots-incomplete"),
    ("input_requests", "input-required-result-request-state", "sep-2322-request-state-incomplete"),
    (
        "input_requests",
        "input-required-result-multiple-input-requests",
        "sep-2322-multiple-inputs-incomplete",
    ),
    ("input_requests", "input-required-result-multi-round", "sep-2322-multi-round-r1"),
    ("input_requests", "input-required-result-non-tool-request", "sep-2322-non-tool-incomplete"),
    ("input_requests", "input-required-result-result-type", "sep-2322-result-type-included"),
    ("input_requests", "input-required-result-tampered-state", "sep-2322-reject-tampered-state"),
    (
        "input_requests",
        "input-required-result-capability-check",
        "sep-2322-respect-client-capabilities",
    ),
    ("response_stream", "tools-call-with-progress", "tools-call-with-progress"),
}
SHIPPED_FIXTURE_COVERAGE = {
    ("test_simple_text", "tools-call-simple-text", "tools-call-simple-text"),
    ("test_image_content", "tools-call-image", "tools-call-image"),
    ("test_audio_content", "tools-call-audio", "tools-call-audio"),
    ("test_embedded_resource", "tools-call-embedded-resource", "tools-call-embedded-resource"),
    ("test_multiple_content_types", "tools-call-mixed-content", "tools-call-mixed-content"),
    ("test_error_handling", "tools-call-error", "tools-call-error"),
    (
        "test_missing_capability",
        "server-stateless",
        "sep-2575-server-rejects-undeclared-capability",
    ),
    ("test_missing_capability", "server-stateless", "sep-2575-missing-capability-http-400"),
    (
        "test_streaming_elicitation",
        "server-stateless",
        "sep-2575-http-server-no-independent-requests-on-stream",
    ),
    ("test_logging_tool", "server-stateless", "sep-2575-server-no-log-without-loglevel"),
}


def test_the_shipped_manifest_is_valid_and_covers_only_known_capabilities() -> None:
    """The exact (reason, scenario, check) sets: growing either is a reviewed change here."""
    manifest = evaluate.load_manifest(MANIFEST)

    assert set(manifest.capabilities) == set(evaluate.EXCLUDABLE)
    assert all(entry.justification and entry.source for entry in manifest.entries)
    by_kind: dict[str, set[tuple[str, str, str]]] = {"capability": set(), "fixture": set()}
    for entry in manifest.entries:
        by_kind[entry.kind].update((entry.reason, entry.scenario, c) for c in entry.checks)
    assert by_kind["capability"] == SHIPPED_EXCLUSIONS
    assert by_kind["fixture"] == SHIPPED_FIXTURE_COVERAGE
    assert all(
        entry.profiles == ("shipping", "fixture")
        for entry in manifest.entries
        if entry.kind == "capability"
    )


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


def test_main_still_writes_diagnostics_when_the_manifest_is_refused(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    results = _results()
    _suite_output(tmp_path, "shipping", results["shipping"])
    _suite_output(tmp_path, "fixture", results["fixture"])
    (tmp_path / "manifest.toml").write_text("[[waiver]]\n", encoding="utf-8")
    code = evaluate.main(
        [
            "--manifest",
            str(tmp_path / "manifest.toml"),
            "--requirements",
            str(_requirements_file(tmp_path)),
            "--package-source",
            str(tmp_path / "index.js"),
            "--shipping",
            str(tmp_path / "shipping" / "results"),
            "--fixture",
            str(tmp_path / "missing"),
            "--out",
            str(tmp_path / "out"),
        ]
    )

    assert code == 1
    shipping = json.loads((tmp_path / "out" / "shipping" / "checks.json").read_text("utf-8"))
    assert "server-stateless" in shipping
    fixture = json.loads((tmp_path / "out" / "fixture" / "checks.json").read_text("utf-8"))
    assert fixture == {"error": "FileNotFoundError"}
    summary = json.loads((tmp_path / "out" / "summary.json").read_text("utf-8"))
    assert summary == {"error": "FileNotFoundError", "errors": 1}
    assert "cannot evaluate" in capsys.readouterr().err


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
