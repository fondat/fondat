"""Module to to manage various types."""

import dataclasses
import functools
import sys

from collections.abc import AsyncIterator, Iterable, Mapping
from typing import Union, get_type_hints


def affix_type_hints(obj=None, *, globalns=None, localns=None, attrs=True):
    """
    Affixes an object's type hints to the object.

    This can be applied as a decorator to a class or function.

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

    if obj is None:
        return functools.partial(
            affix_type_hints, globalns=globalns, localns=localns, attrs=attrs
        )

    if getattr(obj, "__annotations__", None):
        obj.__annotations__ = get_type_hints(
            obj, globalns, localns, include_extras=True
        )
    if attrs:
        for name in dir(obj):
            affix_type_hints(
                getattr(obj, name), globalns=globalns, localns=localns, attrs=False
            )

    return obj


def dataclass(cls, init=True, **kwargs):
    """
    Decorates a class to be a data class.

    This decorator utilizes the Python dataclass decorator, except the added
    __init__ method only accepts keyword arguments. This allows defaulted and
    required attributes to be declared in any order. Any missing values during
    initialization are defaulted to None.
    """

    def __init__(self, **kwargs):
        hints = get_type_hints(cls)
        for key in kwargs:
            if key not in hints:
                raise TypeError(
                    f"__init__() got an unexpected keyword argument '{key}'"
                )
        for key in hints:
            setattr(self, key, kwargs.get(key, getattr(cls, key, None)))

    c = dataclasses.dataclass(cls, init=False, **kwargs)

    if init:
        c.__init__ = __init__

    return c


class Stream(AsyncIterator[Union[bytes, bytearray]]):
    """
    Abstract base class to represent content as an asynchronous stream of bytes.

    Parameter and attribute:
    • content_type: The media type of the stream.
    • content_length: The length of the content, if known.
    """

    def __init__(
        self, content_type: str = "application/octet-stream", content_length=None
    ):
        self.content_type = content_type
        self.content_length = content_length

    def __aiter__(self):
        return self

    async def __anext__(self) -> Union[bytes, bytearray]:
        raise NotImplementedError


class BytesStream(Stream):
    """
    Expose a bytes object as an asynchronous byte stream.
    """

    def __init__(
        self,
        content: Union[bytes, bytearray],
        content_type: str = "application/octet-stream",
    ):
        super().__init__(content_type, len(content))
        self._content = content

    async def __anext__(self) -> bytes:
        if self._content is None:
            raise StopAsyncIteration
        result = self._content
        self._content = None
        return result
