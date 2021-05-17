"""
Module for resource errors.

Error classes are dynamically generated from all errors in the http.HTTPStatus enum.
"""

import http

from contextlib import contextmanager
from fondat.types import is_instance
from typing import Union


# will be appended to during generation
__all__ = ["Error", "errors", "error_for_status"]


class Error(Exception):
    """
    Base class for resource errors.

    Each error class must expose the following attributes:
    • status: HTTP status code (integer)
    • phrase: HTTP reason phrase
    """


_status_errors = {}


def _title(s):
    exclude = {"HTTP", "URI"}
    return s.title() if s not in exclude else s


for status in http.HTTPStatus:
    globalns = globals()
    if 400 <= status <= 599:
        name = "".join([_title(w) for w in status.name.split("_")])
        if not name.endswith("Error"):
            name += "Error"
        doc = status.description or status.phrase.capitalize()
        if not doc.endswith("."):
            doc += "."
        error = type(
            name,
            (Error,),
            {
                "status": status.value,
                "phrase": status.phrase,
                "__doc__": doc,
            },
        )
        globalns[name] = error
        _status_errors[status.value] = error
        __all__.append(name)


def error_for_status(status: Union[int, http.HTTPStatus], default=InternalServerError):
    """
    Return an error class matching the specified HTTP status.

    Parameters:
    • status: HTTP status object or integer code
    • default: default value to return if no matching error class
    """
    if is_instance(status, http.HTTPStatus):
        status = status.value
    return _status_errors.get(status, default)


@contextmanager
def reraise(exceptions, replacement):
    """
    Context manager that catches exception(s) and raises a replacement.

    Parameters:
    • exceptions: exception class or tuple of exception classes to be caught
    • replacement: exeption class to raise as the replacement
    """
    try:
        yield
    except exceptions as e:
        raise replacement from e
