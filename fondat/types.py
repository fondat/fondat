"""Module to to manage various types."""

import functools
import typing

from collections.abc import AsyncIterator
from typing import Annotated, Any, Union


NoneType = type(None)


def affix_type_hints(obj=None, *, globalns=None, localns=None, attrs: bool = True):
    """
        Affixes an object's type hints to the object by materializing evaluated string type hints
        into the type's __annotations__ attribute.

        This function exists due to PEP 563, in which annotations are stored as strings, are only
        evaluated when typing.get_type_hints is called; this will be the default behavior of
        annotations in Python 3.10. The work in PEP 649, if accepted, will likely eliminate the
        need to affix type hints.
    ow
        This function can be applied as a decorator to a class or function.

        Parameters:
        • obj: function, method, module or class object
        • globalns: global namespace to evaluate type hints
        • localns: local namespace to evaluate type hints
        • attrs: affix all of object's attribute type hints

        Type hints are affixed by first resolving through typing.get_type_hints, then by storing
        the result in the object's __annotations__ attribute.

        If the object is a class, this function will affix annotations from all superclasses into
        the object annotations.

        Affixation provides the following benefits (under PEP 563):
        • time and scope of annotation evaluation is under the control of the caller
        • annotations are not re-evaluated for every call to typing.get_type_hints
    """

    if obj is None:
        return functools.partial(
            affix_type_hints, globalns=globalns, localns=localns, attrs=attrs
        )

    if getattr(obj, "__annotations__", None):
        obj.__annotations__ = typing.get_type_hints(obj, globalns, localns, include_extras=True)
    if attrs:
        for name in dir(obj):
            affix_type_hints(
                getattr(obj, name), globalns=globalns, localns=localns, attrs=False
            )

    return obj


class Stream(AsyncIterator[Union[bytes, bytearray]]):
    """
    Abstract base class to represent content as an asynchronous stream of bytes.

    Parameter and attribute:
    • content_type: the media type of the stream
    • content_length: the length of the content, if known
    """

    def __init__(self, content_type: str = "application/octet-stream", content_length=None):
        self.content_type = content_type
        self.content_length = content_length

    def __aiter__(self):
        return self

    async def __anext__(self) -> Union[bytes, bytearray]:
        raise NotImplementedError


class BytesStream(Stream):
    """Expose a bytes object as an asynchronous byte stream."""

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


async def stream_bytes(stream: Stream) -> bytearray:
    """Read and return the content of a stream in a byte array."""

    if stream is None:
        return None
    result = bytearray()
    async for b in stream:
        result.extend(b)
    return result


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


def split_annotated(hint):
    """Return a tuple containing the python type and annotations."""
    if not typing.get_origin(hint) is typing.Annotated:
        return hint, ()
    args = typing.get_args(hint)
    return args[0], args[1:]


def is_optional(hint):
    """Return if the specified type is optional (contains Union[..., None])."""
    python_type, _ = split_annotated(hint)
    if not typing.get_origin(python_type) is Union:
        return python_type is NoneType
    for arg in typing.get_args(python_type):
        if is_optional(arg):
            return True
    return False


def strip_optional(hint):
    """Strip optionality from the type."""
    python_type, annotations = split_annotated(hint)
    if not typing.get_origin(python_type) is Union:
        return hint
    python_type = Union[
        tuple(
            strip_optional(arg) for arg in typing.get_args(python_type) if arg is not NoneType
        )
    ]
    if not annotations:
        return python_type
    return Annotated[tuple([python_type, *annotations])]


def is_subclass(cls, cls_or_tuple):
    """A more forgiving issubclass."""
    try:
        return issubclass(cls, cls_or_tuple)
    except:
        return False


def is_instance(obj, class_or_tuple):
    """A more forgiving isinstance."""
    try:
        return isinstance(obj, class_or_tuple)
    except:
        return False
