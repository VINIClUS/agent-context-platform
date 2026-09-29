from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

import pytest
from argon2 import PasswordHasher

from agent_context_platform.ledger.auth import (
    Argon2Verifier,
    InsufficientScopeError,
    InvalidCredentialError,
    MissingCredentialError,
    ProducerAuthenticator,
    ProducerRegistration,
    parse_bearer,
)

pytestmark = pytest.mark.unit

NOW = datetime(2026, 9, 1, tzinfo=UTC)
PREFIX = "prod0001"
SECRET = "s" * 43
TOKEN = f"{PREFIX}.{SECRET}"
_HASHER = PasswordHasher(time_cost=1, memory_cost=8, parallelism=1)
VERIFIER = _HASHER.hash(TOKEN)


def _verifier(max_concurrency: int = 2, max_queue_depth: int = 8) -> Argon2Verifier:
    return Argon2Verifier(
        time_cost=1,
        memory_cost_kib=8,
        parallelism=1,
        max_concurrency=max_concurrency,
        max_queue_depth=max_queue_depth,
    )


def _registration(**overrides: object) -> ProducerRegistration:
    values: dict[str, object] = {
        "producer_id": "codex-1",
        "token_verifier": VERIFIER,
        "scope": "events:ingest",
        "expires_at": NOW + timedelta(days=1),
        "revoked_at": None,
    }
    values.update(overrides)
    return ProducerRegistration(**values)  # type: ignore[arg-type]


def _authenticator(registration: ProducerRegistration | None) -> ProducerAuthenticator:
    lookups: list[str] = []

    async def lookup(prefix: str) -> ProducerRegistration | None:
        lookups.append(prefix)
        return registration if prefix == PREFIX else None

    authenticator = ProducerAuthenticator(lookup, _verifier(), clock=lambda: NOW)
    authenticator.lookups = lookups  # type: ignore[attr-defined]
    return authenticator


@pytest.mark.parametrize(
    "header",
    [
        None,
        "",
        "Basic abc",
        "Bearer",
        "Bearer no-dot-here",
        f"Bearer {PREFIX}.short",
        f"Bearer short.{SECRET}",
        f"Bearer {PREFIX}.{'s' * 300}",
        f"Bearer {PREFIX}.{SECRET}!",
        f"Bearer {'x' * 600}",
    ],
)
def test_parse_bearer_rejects_malformed_headers(header: str | None) -> None:
    assert parse_bearer(header) is None


def test_parse_bearer_splits_prefix_and_keeps_secret_out_of_repr() -> None:
    bearer = parse_bearer(f"bearer  {TOKEN} ")

    assert bearer is not None
    assert bearer.prefix == PREFIX
    assert bearer.token == TOKEN
    assert SECRET not in repr(bearer)


def test_authenticate_binds_valid_token_to_its_producer() -> None:
    principal = asyncio.run(_authenticator(_registration()).authenticate(f"Bearer {TOKEN}"))

    assert principal.producer_id == "codex-1"


def test_authenticate_requires_a_credential() -> None:
    with pytest.raises(MissingCredentialError):
        asyncio.run(_authenticator(_registration()).authenticate(None))


def test_authenticate_rejects_malformed_credential_without_lookup() -> None:
    authenticator = _authenticator(_registration())

    with pytest.raises(InvalidCredentialError):
        asyncio.run(authenticator.authenticate("Bearer garbage"))

    assert authenticator.lookups == []  # type: ignore[attr-defined]


def test_authenticate_rejects_wrong_secret() -> None:
    wrong = f"{PREFIX}.{'w' * 43}"

    with pytest.raises(InvalidCredentialError):
        asyncio.run(_authenticator(_registration()).authenticate(f"Bearer {wrong}"))


@pytest.mark.parametrize(
    "overrides",
    [
        {"revoked_at": NOW - timedelta(seconds=1)},
        {"revoked_at": NOW},
        {"expires_at": NOW},
        {"expires_at": NOW - timedelta(days=1)},
    ],
)
def test_authenticate_rejects_revoked_and_expired_registrations(
    overrides: dict[str, object],
) -> None:
    with pytest.raises(InvalidCredentialError):
        asyncio.run(_authenticator(_registration(**overrides)).authenticate(f"Bearer {TOKEN}"))


def test_authenticate_accepts_a_registration_revoked_in_the_future() -> None:
    registration = _registration(revoked_at=NOW + timedelta(hours=1))

    principal = asyncio.run(_authenticator(registration).authenticate(f"Bearer {TOKEN}"))

    assert principal.producer_id == "codex-1"


def test_authenticate_rejects_a_credential_without_the_ingest_scope() -> None:
    with pytest.raises(InsufficientScopeError):
        asyncio.run(
            _authenticator(_registration(scope="memory:read")).authenticate(f"Bearer {TOKEN}")
        )


def test_scope_is_not_revealed_before_the_secret_verifies() -> None:
    wrong = f"{PREFIX}.{'w' * 43}"

    with pytest.raises(InvalidCredentialError):
        asyncio.run(
            _authenticator(_registration(scope="memory:read")).authenticate(f"Bearer {wrong}")
        )


def test_unknown_prefix_still_runs_an_argon2_verification(monkeypatch: pytest.MonkeyPatch) -> None:
    verifier = _verifier()
    calls: list[str | None] = []
    original = verifier.verify

    async def spy(stored: str | None, token: str) -> bool:
        calls.append(stored)
        return await original(stored, token)

    monkeypatch.setattr(verifier, "verify", spy)

    async def lookup(_prefix: str) -> ProducerRegistration | None:
        return None

    with pytest.raises(InvalidCredentialError):
        asyncio.run(ProducerAuthenticator(lookup, verifier).authenticate(f"Bearer {TOKEN}"))

    assert calls == [None]


def test_dummy_path_never_verifies_even_for_the_dummy_plaintext() -> None:
    verifier = _verifier()

    assert asyncio.run(verifier.verify(None, "agent-context-dummy-credential")) is False


def test_verify_treats_unparseable_verifier_as_a_mismatch() -> None:
    assert asyncio.run(_verifier().verify("not-a-hash", TOKEN)) is False


def test_verify_honours_the_cost_encoded_in_the_stored_verifier() -> None:
    assert asyncio.run(_verifier().verify(VERIFIER, TOKEN)) is True


def test_verifier_bounds_concurrent_worker_threads(monkeypatch: pytest.MonkeyPatch) -> None:
    import threading
    import time

    verifier = _verifier(max_concurrency=2)
    active = 0
    peak = 0
    guard = threading.Lock()

    def slow(_verifier: str, _token: str) -> bool:
        nonlocal active, peak
        with guard:
            active += 1
            peak = max(peak, active)
        time.sleep(0.05)
        with guard:
            active -= 1
        return True

    monkeypatch.setattr(verifier, "_verify_blocking", slow)

    async def run() -> None:
        await asyncio.gather(*(verifier.verify(VERIFIER, TOKEN) for _ in range(8)))

    asyncio.run(run())

    assert peak == 2


def test_verifier_sheds_load_beyond_the_queue_depth(monkeypatch: pytest.MonkeyPatch) -> None:
    import threading

    from agent_context_platform.ledger.auth import AuthOverloadedError

    verifier = _verifier(max_concurrency=1, max_queue_depth=2)
    release = threading.Event()

    def blocked(_verifier: str, _token: str) -> bool:
        release.wait(timeout=5)
        return True

    monkeypatch.setattr(verifier, "_verify_blocking", blocked)

    async def run() -> list[object]:
        tasks = [asyncio.ensure_future(verifier.verify(VERIFIER, TOKEN)) for _ in range(5)]
        await asyncio.sleep(0.1)
        release.set()
        return await asyncio.gather(*tasks, return_exceptions=True)

    results = asyncio.run(run())

    assert results.count(True) == 3  # one running, two queued
    assert sum(isinstance(result, AuthOverloadedError) for result in results) == 2
    assert verifier._pending == 0
    assert AuthOverloadedError.status_code == 503
