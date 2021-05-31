"""Dataclass module."""

import dataclasses
import typing

from collections.abc import Iterable, Mapping
from fondat.types import is_optional
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
    include: Iterable[str] = None,
    exclude: Iterable[str] = None,
    append: Iterable[Union[tuple[str, type], tuple[str, type, dataclasses.Field]]] = None,
    optional: Union[Iterable[str], bool] = False,
    **kwargs,
) -> type:
    """
    Return a new dataclass with fields derived from another dataclass.

    Parameters:
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
    fields: Union[Mapping[str, str], Iterable[str]] = None,
):
    """
    Copy data from one instance of a dataclass to another.

    Parameters:
    • source: instance to copy data from
    • target: instance to copy data to
    • fields: names of fields to be copied

    The source and target dataclass instances do not have to be of the same type. This
    function makes no attempt to ensure type compatibility between dataclass fields.

    The fields parameter can be provided as either an iterable of field names that are common
    between source and target dataclass, or a mapping of source-to-target field names.

    If fields are specified, then only those fields shall be copied; they must be present
    in both source and target dataclass instances. If fields are not specified, then only
    fields that source and target dataclasses have in common will be copied.
    """

    source_fields = dataclasses.fields(source)
    target_fields = dataclasses.fields(target)

    if fields is None:
        fields = set(f.name for f in source_fields) & set(f.name for f in target_fields)

    if not isinstance(fields, Mapping):
        fields = {field: field for field in fields}

    for s, t in fields.items():
        setattr(target, t, getattr(source, s))


def dataclass_typeddict(
    clsname: str,
    dataclass: type,
    include: set[str] = None,
    exclude: set[str] = None,
    total: bool = True,
) -> type:
    """
    Generate a TypedDict type from a dataclass.

    Parameters:
    • dataclass: dataclass to derive dictionary keys from
    • include: the names of dataclass fields to include  [all]
    • exclude: the names of dataclass fields to exclude  [none]
    • total: should all keys be present in the TypedDict
    """

    fields = dataclasses.fields(dataclass)

    if include is None:
        include = {field.name for field in fields}

    if exclude is None:
        exclude = set()

    return TypedDict(
        clsname,
        {f.name: f.type for f in fields if f.name in include and f.name not in exclude},
        total=total,
    )
