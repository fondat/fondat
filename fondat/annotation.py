"""Module for type hint annotations."""


from fondat.validation import validate_arguments
from typing import Any


class Annotation:
    """Base class for annotations."""

    __slots__ = {"value"}

    def __repr__(self):
        return f"{type(self).__name__}({self.value!r})"

    def __str__(self):
        return str(self.value)

    def __eq__(self, other: Any):
        return type(self) == type(other) and self.value == other.value

    def __hash__(self):
        try:
            return hash((self.__class__, self.value))
        except:
            return super().__hash__()


class Deprecated(Annotation):
    """Type annotation to indicate a value is deprecated."""

    @validate_arguments
    def __init__(self, value: bool):
        self.value = value


class Description(Annotation):
    """Type annotation to provide a textual description."""

    @validate_arguments
    def __init__(self, value: str):
        self.value = value


class Example(Annotation):
    """Type annotation to provide an example value."""

    def __init__(self, value: Any):
        self.value = value


class Format(Annotation):
    """Type annotation to express string format."""

    @validate_arguments
    def __init__(self, value: str):
        self.value = value


class ReadOnly(Annotation):
    """Type annotation to indicate a value is read-only."""

    @validate_arguments
    def __init__(self, value: bool):
        self.value = value


Password = Format("password")
