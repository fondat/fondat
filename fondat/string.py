"""Fondat string module."""

import re

from collections.abc import Callable, Coroutine
from typing import Any


class Template:
    """
    A string class for asynchronous substitution of template expressions.

    Parameters:
    • template: template string containing expressions to be substituted

    A substition expression in the template takes the form "${...}". For example, if a
    template contains "${foo}" substitution expression, then "foo" is the value that is passed
    to a resolver coroutine to be resolved. A subsitution expression cannot span lines
    within the template string.
    """

    _pattern = re.compile(r"\$\{(.*?)}")

    def __init__(self, template: str):
        self.template = template

    async def _resolve(
        self, template: str, resolver: Callable[[str], Coroutine[Any, Any, str | None]]
    ) -> str:
        segments = []
        for line in template.splitlines(keepends=True):
            index = 0
            while match := self._pattern.search(line[index:]):
                group = match.group(1)
                resolved = None
                if resolved := await resolver(group):
                    span = match.span()
                    segments.append(line[index : span[0]])
                    segments.append(await self._resolve(resolved, resolver))
                    index = span[1]
                    break
                if not resolved:
                    raise ValueError(f"could not resolve ${{{group}}} in template")
            segments.append(line[index:])
        return "".join(segments)

    async def resolve(self, resolver: Callable[[str], Coroutine[Any, Any, str | None]]):
        """
        Return a new string with resolved template substitutions.

        Parameters:
        • resolver: coroutine function to resolve substitution expressions

        A resolver coroutine function takes a string parameter and returns a string. If the
        resolver cannot resolve an expression, it should return None or raise a ValueError with
        a descriptive error message.

        It is valid for a resolver to return a value that itself contains one or more
        substitition expressions; they will then be resolved.
        """
        return await self._resolve(self.template, resolver)
