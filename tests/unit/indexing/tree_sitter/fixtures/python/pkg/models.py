"""Domain models."""

import abc
import functools
from dataclasses import dataclass
from typing import Any

MAX_ITEMS = 10
default_name: str = "anon"
first, second = 1, 2

type Amount = int


class Base(abc.ABC):
    """Abstract base."""

    kind = "base"

    @abc.abstractmethod
    def describe(self) -> str: ...


@dataclass
class Account(Base):
    """An account."""

    balance: int = 0

    def describe(self) -> str:
        # a comment that must not matter
        return helper(self.name())

    @property
    def name(self) -> str:
        return "account"

    @name.setter
    def name(self, value: str) -> None:
        self._name = value

    @staticmethod
    def make(kind: str = "basic", *, limit: int = MAX_ITEMS) -> "Account":
        return Account(limit)

    @classmethod
    def build(cls, **options: Any) -> "Account":
        return cls.make(**options)

    async def refresh(self) -> None:
        await self.reload()
        self.describe()

    async def reload(self) -> None:
        return None

    class Meta:
        table = "accounts"

        def label(self) -> str:
            return "meta"


class Ledger(Account, Missing):
    def total(self) -> int:
        def inner(x: int) -> int:
            return x * 2

        return inner(len(fetch_all()))


@functools.lru_cache(maxsize=None)
def helper(value: str) -> str:
    return value.upper()


async def fetch_all() -> list[Account]:
    return [Account.build() for _ in range(MAX_ITEMS)]
