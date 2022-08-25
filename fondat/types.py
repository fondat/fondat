"""Module to manage types and type hints."""

import contextvars
import dataclasses
import functools
import types
import typing

from collections.abc import Iterable, Mapping
from contextlib import contextmanager
from types import NoneType
from typing import Any, TypeVar, get_args, get_origin


_typevars = contextvars.ContextVar("_fondat_typevars")


def affix_type_hints(
    obj: Any = None,
    *,
    globalns: Mapping[str, Any] | None = None,
    localns: Mapping[str, Any] | None = None,
    attrs: bool = True,
):
    """
    Affixes an object's type hints to the object by materializing evaluated string type hints
    into the type's __annotations__ attribute.

    This function exists due to PEP 563, in which annotations can be stored as strings, and are
    only evaluated when typing.get_type_hints is called; this will be the expected behavior of
    annotations in Python 3.11. The work in PEP 649, if accepted, will likely eliminate the
    need to affix type hints.

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

    obj, _ = split_annotated(obj)

    if getattr(obj, "__annotations__", None):
        obj.__annotations__ = typing.get_type_hints(obj, globalns, localns, include_extras=True)

    if dataclasses.is_dataclass(obj):
        for field in dataclasses.fields(obj):
            if type := obj.__annotations__.get(field.name):
                field.type = type

    if attrs:
        for name in dir(obj):
            if not name.startswith("__") and not name.endswith("__"):
                affix_type_hints(
                    getattr(obj, name), globalns=globalns, localns=localns, attrs=False
                )

    return obj


def split_annotated(type_hint: Any) -> tuple[Any, tuple[Any, ...]]:
    """Return a tuple separating the python type and annotations."""
    if not typing.get_origin(type_hint) is typing.Annotated:
        return type_hint, ()
    args = typing.get_args(type_hint)
    return args[0], args[1:]


def is_optional(type_hint: Any) -> bool:
    """
    Return if the specified type is optional.

    A type is optional if its type hint matches any of the following:
    • None
    • Optional[...]
    • Union[..., None]
    • ... | None
    """
    python_type, _ = split_annotated(type_hint)
    if not typing.get_origin(python_type) in {types.UnionType, typing.Union}:
        return python_type is NoneType
    for arg in typing.get_args(python_type):
        if is_optional(arg):
            return True
    return False


def strip_optional(type_hint):
    """Return a union type with optionality stripped."""
    python_type, annotations = split_annotated(type_hint)
    origin = typing.get_origin(python_type)
    if origin not in {types.UnionType, typing.Union}:
        return type_hint
    args = (strip_optional(arg) for arg in typing.get_args(python_type) if arg is not NoneType)
    python_type = union_type(args)
    if not annotations:
        return python_type
    return typing.Annotated[tuple([python_type, *annotations])]


def is_subclass(cls: Any, class_or_tuple: type | tuple[type, ...]) -> bool:
    """A more forgiving issubclass."""
    try:
        return issubclass(cls, class_or_tuple)
    except:
        return False


def is_instance(obj: Any, class_or_tuple: type | tuple[type, ...]) -> bool:
    """A more forgiving isinstance."""
    try:
        return isinstance(obj, class_or_tuple)
    except:
        return False


def literal_values(literal_type_hint) -> set[Any]:
    """Return a set of all values in a Literal type."""
    return set(typing.get_args(literal_type_hint))


def union_type(type_hints: Iterable[Any]) -> types.UnionType:
    """Construct a union type from an iterable of types."""
    types = iter(type_hints)
    try:
        result = types.__next__()
    except StopIteration:
        return NoneType
    while True:
        try:
            result |= types.__next__()
        except StopIteration:
            break
    return result


@contextmanager
def capture_typevars(alias: Any):
    """
    Return a context manager that captures type variable substitions in a provided generic
    alias, to be resolved through calls to `resolve_typevar` within the context. This allows
    dataclasses to contain generic types, and for those types to be resolved at runtime. If
    the passed value is not a generic alias, nothing is captured.
    """
    typevars = None
    if args := get_args(alias):
        if params := getattr(get_origin(alias), "__parameters__", None):
            typevars = {p: a for p, a in zip(params, args) if isinstance(p, TypeVar)}
    token = None
    if typevars:
        if outer := _typevars.get(None):  # nested generic alias
            typevars = outer | typevars
        token = _typevars.set(typevars)
    yield
    if token:
        _typevars.reset(token)


def resolve_typevar(typevar: TypeVar) -> Any:
    """
    Resolve a captured type variable substituion from a containing generic class instance.
    For more information, see `capture_typevars` function.
    """
    result = typevar
    while isinstance(result, TypeVar):
        result = _typevars.get({}).get(result) or Any
    return result
