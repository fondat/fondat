"""Dataclass module."""

import dataclasses
import typing

from collections.abc import Iterable
from fondat.types import is_optional
from typing import Any, Optional, Union


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
    fields: Iterable[
        Union[str, tuple[str, type], tuple[str, type, dataclasses.Field], dataclasses.Field]
    ],
    init: bool = True,
    **kwargs,
) -> type:
    """
    Return a new dataclass. This decorator wraps the Python dataclasses.make_dataclass
    function, with the following changes to the generated __init__ method:

    • fields (with default values or not) can be declared in any order
    • fields also accepts the result of the dataclasses.fields() function
    • the __init__ method only accepts keyword arguments
    • Optional[...] fields default to None if no default value is specified
    """

    result = dataclasses.make_dataclass(
        cls_name=cls_name,
        fields=tuple(
            (f.name, f.type, f) if isinstance(f, dataclasses.Field) else f for f in fields
        ),
        init=False,
        **kwargs,
    )
    if init:
        result.__init__ = _datacls_init(result)
    return result


def flds(dataclass: Any, names: set[str] = None) -> tuple[dataclasses.Field]:
    """
    Return a tuple describing the fields of a dataclass.

    Parameters:
    • dataclass: dataclass or dataclass instance containing fields to describe
    • names: the names of the dataclass fields to describe
    """

    fields = {field.name: field for field in dataclasses.fields(dataclass)}
    if names is not None:
        fields = {name: fields[name] for name in names}
    return tuple(fields.values())


def subset_datacls(
    cls_name: str,
    dataclass: type,
    names: Iterable[str] = None,
    optional: Union[bool, Iterable[str]] = False,
    **kwargs,
) -> type:
    """
    Return a new dataclass, containing a subset of fields from another dataclass.

    • dataclass: source dataclass
    • names: the names of source dataclass fields to include
    • optional: fields to be made optional

    If names are not specified, then all fields from the source dataclass are included.

    If optional is a boolean value, it specifies if all fields should be made optional.
    """

    if names is None:
        names = dataclass.__annotations__.keys()
    fields = flds(dataclass, names)
    optional = set(names if optional is True else () if optional is False else optional)
    for field in fields:
        if field.name in optional:
            field.type = Optional[field.type]
    return make_datacls(cls_name, fields=fields, **kwargs)


def copy(source: Any, target: Any, fields: Iterable[str] = None):
    """
    Copy fields from one instance of a dataclass to another.

    Parameters:
    • source: instance to copy fields from
    • target: instance to copy fields to
    • fields: names of fields to be copied

    If fields are specified, then only those fields shall be copied; they must be present
    in both source and target dataclass instances. If fields are not specified, then only
    fields that source and target dataclasses have in common will be copied.

    This function makes no attempt to ensure type compatibility between dataclass fields.
    """

    if fields is None:
        fields = source.__annotations__.keys() & target.__annotations__.keys()

    for field in fields:
        setattr(target, field, getattr(source, field))
