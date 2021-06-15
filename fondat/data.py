"""Dataclass module."""

import dataclasses
import typing

from collections.abc import Iterable, Mapping
from fondat.types import is_optional, split_annotated
from typing import Any, Optional, TypedDict, Union


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
    fields: Iterable[Union[tuple[str, type], tuple[str, type, dataclasses.Field]]],
    init: bool = True,
    **kwargs,
) -> type:
    """
    Return a new dataclass. This function wraps the Python dataclasses.make_dataclass
    function, with the following changes to the generated __init__ method:

    • fields (with default values or not) can be declared in any order
    • the __init__ method only accepts keyword arguments
    • Optional[...] fields default to None if no default value is specified

    Keyword arguments are passed on to the dataclasses.make_dataclass function.
    """

    dataclass = dataclasses.make_dataclass(
        cls_name=cls_name,
        fields=fields,
        init=False,
        **kwargs,
    )
    if init:
        dataclass.__init__ = _datacls_init(dataclass)
    return dataclass


def derive_datacls(
    cls_name: str,
    dataclass: type,
    *,
    include: set[str] = None,
    exclude: set[str] = None,
    append: Iterable[Union[tuple[str, type], tuple[str, type, dataclasses.Field]]] = None,
    optional: Union[set[str], bool] = False,
    **kwargs,
) -> type:
    """
    Return a new dataclass with fields derived from another dataclass.

    Parameters:
    • cls_name: the name of the new dataclass
    • dataclass: dataclass to derive fields from
    • include: the names of dataclass fields to include  [all]
    • exclude: the names of dataclass fields to exclude  [none]
    • optional: derived fields to make optional
    • append: iterable of field tuples to append to the derived dataclass

    If include is not specified, then all fields from the source dataclass are included. If
    exclude is not specified, then no fields from the source dataclass are excluded.

    The optional parameter can be a boolean value or an iterable of field names. If boolean,
    it specifies whether all derived fields should be made optional.

    The append parameter takes the same form as the fields parameter in the make_datacls
    function. These fields are not affected by the optional parameter.

    Any additional keyword arguments are passed on to the make_datacls function.
    """

    def _type(f):
        if optional is True or (optional is not False and f.name in optional):
            return Optional[f.type]
        return f.type

    def _field(f):
        return dataclasses.field(
            default=f.default,
            default_factory=f.default_factory,
            init=f.init,
            repr=f.repr,
            hash=f.hash,
            compare=f.compare,
            metadata=f.metadata,
        )

    fields = [
        (f.name, _type(f), _field(f))
        for f in dataclasses.fields(dataclass)
        if (include is None or f.name in include) and (exclude is None or f.name not in exclude)
    ]

    for item in append or ():
        if len(item) == 2:
            item = (item[0], item[1], dataclasses.field())
        if len(item) != 3:
            raise TypeError(f"Invalid field: {item!r}")
        fields.append(item)

    return make_datacls(cls_name, fields=fields, **kwargs)


def copy_data(
    source: Any,
    target: Any,
    include: set[str] = None,
    exclude: set[str] = None,
):
    """
    Copy data from a dataclass instance to another.

    Parameters:
    • source: instance to copy data from
    • target: instance to copy data to
    • include: source dataclass fields to include  [all]
    • exclude: source dataclass fields to exclude  [none]

    If fields to include are not specified, then all fields from the source dataclass will be
    included. If fields to exclude are not specified, then no fields from the source dataclass
    will be excluded.
    """

    if include is None:
        include = source.__annotations__.keys()

    if exclude is None:
        exclude = set()

    for field in (f for f in include if f not in exclude):
        setattr(target, field, getattr(source, field))


def derive_typeddict(
    type_name: str,
    source: Any,
    *,
    include: set[str] = None,
    exclude: set[str] = None,
    total: bool = True,
) -> type:
    """
    Generate a derived TypedDict from a source TypedDict or dataclass.

    Parameters:
    • type_name: the name of the new TypedDict type
    • source: TypedDict or dataclass to derive from
    • include: the names of keys to include  [all]
    • exclude: the names of keys to exclude  [none]
    • total: must all keys be present in the TypedDict
    """

    source, _ = split_annotated(source)

    if include is None:
        include = source.__annotations__.keys()

    if exclude is None:
        exclude = set()

    return TypedDict(
        type_name,
        {
            key: type
            for key, type in source.__annotations__.items()
            if key in include and key not in exclude
        },
        total=total,
    )
