"""Resource error module."""

import http

from collections.abc import Iterator


class Error(Exception):
    """
    Base class for resource errors.

    All error classes must include the following attributes:
    • status: HTTP status code (int)
    • phrase: HTTP reason phrase
    """


class ClientError(Error):
    """
    Base class for client resource errors.
    """


class ServerError(Error):
    """
    Base class for server resource errors.
    """


class _Errors:
    """
    Encapsulates resource error exception classes. Errors are dynamically generated from
    errors in the http.HTTPStatus enum.

    Errors can be accessed by HTTP status or name.
    Example: fondat.error.errors[404] == fondat.error.errors.NotFoundError
    """

    def __init__(self):
        self._names = {}
        self._codes = {}
        for status in (s for s in http.HTTPStatus if 400 <= s.value <= 599):
            name = "".join(
                w.title() if w not in {"HTTP", "URI"} else w for w in status.name.split("_")
            )
            if not name.endswith("Error"):
                name += "Error"
            error = type(
                name,
                (ClientError if 400 <= status.value <= 499 else ServerError,),
                {
                    "status": status.value,
                    "phrase": status.phrase,
                    "__doc__": f"{status.description or status.phrase.capitalize()}.",
                },
            )
            self._names[name] = error
            self._codes[status.value] = error

    def get(self, code: int, default=None) -> Error:
        """Return error for code."""
        return self._codes.get(code, default)

    def __getitem__(self, code: int) -> Error:
        return self._codes[code]

    def __getattr__(self, name: str) -> Error:
        if error := self._names.get(name):
            return error
        raise AttributeError

    def __iter__(self) -> Iterator[Error]:
        return iter(self._codes.values())


errors = _Errors()


# commonly used errors
BadRequestError: ClientError = errors.BadRequestError
ConflictError: ClientError = errors.ConflictError
ForbiddenError: ClientError = errors.ForbiddenError
InternalServerError: ServerError = errors.InternalServerError
MethodNotAllowedError: ClientError = errors.MethodNotAllowedError
NotFoundError: ClientError = errors.NotFoundError
UnauthorizedError: ClientError = errors.UnauthorizedError
