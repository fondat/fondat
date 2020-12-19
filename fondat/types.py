"""Module to to manage various types."""

import dataclasses
import enum
import sys

from collections.abc import AsyncIterator, Iterable, Mapping
from typing import Union, get_type_hints


try:
    from enum import StrEnum
except ImportError:

    class StrEnum(str, enum.Enum):
        def __new__(cls, *values):
            value = str(*values)
            member = str.__new__(cls, value)
            member._value_ = value
            return member

        __str__ = str.__str__


def str_enum(typename: str, values: Union[str, Iterable[str]]):
    """
    Generate an enumeration for string values.

    • typename: the name of the enumeration class.
    • values: values with which to compose the enumeration.

    Values can be expressed as an Iterable of str values, or as a single
    str with values delimited by comma and/or space.
    """

    def _key(v):
        if not v.isidentifier():
            raise ValueError("str_enum only supports identifier values")
        return v.upper()

    if isinstance(values, str):
        values = values.replace(",", " ").split()

    e = StrEnum(typename, {_key(v): v for v in values})

    e.__module__ = sys._getframe(1).f_globals["__name__"]  # cough, hack

    return e


def affix_type_hints(obj, globalns=None, localns=None, attrs=True):
    """
    Affixes an object's type hints to the object.

    Parameters:
    • obj: Function, method, module or class object.
    • globalns: Global namespace to evaluate type hints.
    • localns: Local namespace to evaluate type hints.
    • attrs: Affix all of object's attribute type hints.

    Type hints are affixed by first being resolved through
    typing.get_type_hints, then by storing the result in the object's
    __annotations__ attribute.

    If the object is a class, this function will affix annotations from all
    superclasses into the object annotations.

    Affixation provides the following benefits:
    • time and scope of annotation evaluation is under the control of the caller
    • annotations are not re-evaluated for every call to typing.get_type_hints
    """

    if getattr(obj, "__annotations__", None):
        obj.__annotations__ = get_type_hints(
            obj, globalns, localns, include_extras=True
        )
    if attrs:
        for name in dir(obj):
            affix_type_hints(getattr(obj, name), globalns, localns, False)


def dataclass(cls, init=True, **kwargs):
    """
    Decorates a class to be a data class.

    This decorator utilizes the Python dataclass decorator, except any added
    __init__ method only accepts keyword arguments. This allows defaulted and
    required attributes to be declared in any order. Any missing values during
    initialization will be defaulted to None.
    """

    def __init__(self, **kwargs):
        hints = get_type_hints()
        for key in kwargs:
            if key not in hints:
                raise TypeError(f"__init__() got an unexpected keyword argument '{key}'")

        for key in get_type_hints(self):
            setattr(self, key, kwargs.get(key, getattr(cls, key, None)))

    c = dataclasses.dataclass(cls, init=False, **kwargs)

    if init:
        c.__init__ = __init__
        c.__init__.__globals__ = c.__globals__
        c.__init__.__module__ = c.__module__
        c.__init__.__qualname__ = f"{c.__qualname__}.__init__"

    return c


class Stream(AsyncIterator[bytes]):
    """
    Abstract base class for an asynchronous stream of bytes.

    Parameter and attribute:
    • content_type: The media type of the stream.
    """

    def __init__(self, content_type: str = None):
        self.content_type = content_type

    def __aiter__(self):
        return self


class BytesStream(Stream):
    """
    Expose a bytes object as an asynchronous byte stream.
    """

    def __init__(self, content: bytes, content_type: str = None):
        super().__init__(content_type)
        self._content = content

    async def __anext__(self) -> bytes:
        if self._content is None:
            raise StopAsyncIteration
        result = self._content
        self._content = None
        return result
