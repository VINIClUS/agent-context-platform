"""Relative imports, guarded bindings and the reference forms."""

from __future__ import annotations

import a.b as c
from .. import models
from ..models import Account
from . import sibling
from ...far import thing
from os.path import *
from typing import TYPE_CHECKING, Generic, TypeVar

T = TypeVar("T")

if TYPE_CHECKING:
    Alias = Account
else:
    Alias = object

head, *tail = [1, 2, 3]


class Box(Generic[T], models.Base):
    if T:
        flag = 1

    def open(self):
        return models.helper(self.flag), sibling.go(), c.run(), tail.count(1)
