"""Shape helpers."""

import math


class Circle:
    """A circle."""

    def __init__(self, radius: float) -> None:
        self.radius = radius

    def area(self) -> float:
        """Return the area."""
        return math.pi * self.radius**2


def describe(shape: Circle) -> float:
    """Describe a shape by its area."""
    return shape.area()
