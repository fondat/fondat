"""Module for type hint annotations."""


from fondat.validation import validate_arguments
from typing import Any


class Deprecated:
    """
    Type annotation to indicate a schema value is deprecated. Can be included in an
    annotation as an instance with a boolean value, or as the class itself, which is
    equivalent to Deprecated(True).
    """

    __slots__ = ("value",)

    @validate_arguments
    def __init__(self, value: bool):
        self.value = value

    def __repr__(self):
        return f"Deprecated({self.value!r})"

    def __str__(self):
        return str(self.value)


class Description:
    """Type annotation to provide a textual description."""

    __slots__ = ("value",)

    @validate_arguments
    def __init__(self, value: str):
        self.value = value

    def __repr__(self):
        return f"Description({self.value!r})"

    def __str__(self):
        return str(self.value)


class Example:
    """Type annotation to provide an example value."""

    __slots__ = ("value",)

    def __init__(self, value: Any):
        self.value = value

    def __repr__(self):
        return f"Example({self.value!r})"

    def __str__(self):
        return str(self.value)
