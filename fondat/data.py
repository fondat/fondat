"""Dataclass module."""

import dataclasses
import typing

from collections.abc import Iterable
from fondat.types import is_optional
from typing import Union


class _MISSING:
    pass


def _datacls_init(dc):

    fields = {field.name: field for field in dataclasses.fields(dc) if field.init}

    def __init__(self, **kwargs):
        hints = {k: v for k, v in typing.get_type_hints(dc).items() if k in fields}
        for key in kwargs:
            if key not in fields:
                raise TypeError(f"__init__() got an unexpected keyword argument '{key}'")
        missing = [
            f"'{key}'"
            for key, hint in hints.items()
            if not is_optional(hint)
            and key not in kwargs
            and fields[key].default is dataclasses.MISSING
            and fields[key].default_factory is dataclasses.MISSING
        ]
        if missing:
            raise TypeError(
                f"__init__() missing {len(missing)} required keyword-only "
                + (
                    f"arguments: {', '.join(missing[0:-1])} and {missing[-1]}"
                    if len(missing) > 1
                    else f"argument: {missing[0]}"
                )
            )
        for key, field in fields.items():
            value = kwargs.get(key, _MISSING)
            if value is _MISSING:
                if field.default is not dataclasses.MISSING:
                    value = field.default
                elif field.default_factory is not dataclasses.MISSING:
                    value = field.default_factory()
                else:
                    value = None
            setattr(self, key, value)

    return __init__


def datacls(cls: type, init: bool = True, **kwargs) -> type:
    """
    Decorate a class to be a data class. This decorator wraps the dataclasses.dataclass
    decorator, with the following changes to the generated __init__ method:

    • fields (with default values or not) can be declared in any order
    • the __init__ method only accepts keyword arguments
    • Optional[...] fields default to None if no default value is specified
    """

    dc = dataclasses.dataclass(cls, init=False, **kwargs)
    if init:
        dc.__init__ = _datacls_init(dc)
    return dc


def make_datacls(
    cls_name: str,
    fields: Iterable[Union[str, tuple[str, type], tuple[str, type, dataclasses.Field]]],
    init: bool = True,
    **kwargs,
) -> type:
    """
    Return a new dynamically created dataclass. This decorator wraps the Python
    dataclasses.make_dataclass function, with the following changes to the generated __init__
    method:

    • fields (with default values or not) can be declared in any order
    • the __init__ method only accepts keyword arguments
    • Optional[...] fields default to None if no default value is specified
    """

    dc = dataclasses.make_dataclass(cls_name, fields, init=False, **kwargs)
    if init:
        dc.__init__ = _datacls_init(dc)
    return dc
