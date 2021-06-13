"""Module for type hint annotations."""


from typing import Any


class Description:
    """Type annotation to provide a textual description."""

    __slots__ = ("value",)

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
