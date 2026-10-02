from __future__ import annotations

import pytest

from agent_context_platform.projection import registry
from agent_context_platform.projection.registry import (
    DuplicateProjectorNameError,
    DuplicateProjectorVersionError,
    ProjectorRegistryError,
    register_projectors,
    registered_projectors,
)
from agent_context_platform.projection.runtime import Projector

pytestmark = pytest.mark.unit


class Stub:
    def __init__(self, name: str, version: str = "1") -> None:
        self.name = name
        self.version = version

    def handles(self, event_type: str) -> bool:
        return False

    async def project(self, tx: object, event: object) -> None:
        return None


def test_registered_projectors_are_the_four_domain_projectors_in_dependency_order() -> None:
    projectors = registered_projectors()

    assert [(item.name, item.version) for item in projectors] == [
        ("portfolio", "1"),
        ("agent", "1"),
        ("git", "1"),
        ("code", "1"),
    ]
    assert all(isinstance(item, Projector) for item in projectors)
    assert registered_projectors() is registry.PROJECTORS


def test_registration_keeps_the_given_order() -> None:
    projectors = [Stub("b"), Stub("a"), Stub("c")]

    assert [item.name for item in register_projectors(projectors)] == ["b", "a", "c"]


def test_a_duplicate_name_and_version_is_rejected_with_a_typed_error() -> None:
    with pytest.raises(DuplicateProjectorVersionError) as raised:
        register_projectors([Stub("a"), Stub("a")])

    assert (raised.value.name, raised.value.version) == ("a", "1")
    assert isinstance(raised.value, ProjectorRegistryError)


def test_a_duplicate_name_under_another_version_is_rejected_with_its_own_error() -> None:
    with pytest.raises(DuplicateProjectorNameError) as raised:
        register_projectors([Stub("a", "1"), Stub("a", "2")])

    assert raised.value.name == "a"
    assert not isinstance(raised.value, DuplicateProjectorVersionError)
