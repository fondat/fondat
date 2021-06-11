"""Module for authentication and authorization of resource operations."""

from collections.abc import Callable, Coroutine, Iterable
from typing import Any


class Scheme:
    """Base class for authentication scheme."""

    def __init__(self, *, name: str, description: str = None):
        self.name = name
        self.description = description


class Policy:
    """
    A combination of authentication schemes and authorization rules. For a security policy to
    allow an operation to be performed, all authentication schemes must be satisfied and all
    authorization rules must pass.

    Parameters and attributes:
    • schemes: authentication schemes that must be satisfied
    • rules: authorization rules that must pass

    If schemes is None, then authenticaton is not applicable. If schemes is empty, then the
    policy allows access without authentication.

    An authorization rule is a coroutine function that raises a security exception if
    authorization to an operation should not granted:

    • UnauthorizedError: user could not be authenticated (misnomer)
    • ForbiddenError: user is authenticated and is denied access
    """

    __slots__ = ("schemes", "rules")

    def __init__(
        self,
        schemes: Iterable[Scheme] = None,
        rules: Iterable[Callable[[], Coroutine[Any, Any, Any]]] = None,
    ):
        self.schemes = schemes
        self.rules = rules or ()

    async def apply(self):
        """
        Apply the security policy by evaluating the authorization rules.

        When a security policy is applied, authorization rules are evaluated in the order
        specified. The first exception encountered is raised immediately, ceasing further
        evaluation.
        """
        for rule in self.rules:
            await rule()
