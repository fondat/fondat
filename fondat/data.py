"""Dataclass module."""

import dataclasses
import fondat.annotation
import functools
import typing

from fondat.annotation import Password
from collections.abc import Iterable, Mapping
from dataclasses import is_dataclass
from fondat.types import is_optional, is_subclass, split_annotated, strip_optional
from typing import Any, Optional, TypedDict, Union


class _MISSING:
    pass


def _datacls_init(dc: Any):

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

    • initialization method only processes keyword arguments
    • initialization method ignores unexpected keyword arguments
    • fields (with default values or not) can be declared in any order
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

    • initialization method only processes keyword arguments
    • initialization method ignores unexpected keyword arguments
    • fields (with default values or not) can be declared in any order
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


def copy_data(
    source: Any,
    target: Any,
    include: set[str] = None,
    exclude: set[str] = None,
) -> Any:
    """
    Creates a new instance of the target dataclass or TypedDict data type, populating it with
    data from the source dataclass or TypedDict instance.

    Parameters:
    • source: dataclass instance or mapping to copy data from
    • target: dataclass or TypedDict type or instance to copy data to
    • include: fields to include  [all]
    • exclude: fields to exclude  [none]

    If values to include are not specified, then all fields that source and target types have
    in common are copied. If fields to exclude are not specified, then no fields are excluded.

    This function makes no attempt to ensure that the types of data in source and target match.
    """

    if is_dataclass(source) and type(source) is not type:
        getter = lambda key: getattr(source, key, None)
    elif isinstance(source, Mapping):
        getter = lambda key: source.get(key, None)
    else:
        raise TypeError("source must be dataclass or mapping")

    if not (
        (is_dataclass(target) and type(target) is type)
        or (issubclass(target, dict) and hasattr(target, "__annotations__"))
    ):
        raise TypeError("target must be dataclass or TypedDict type")

    if include is None:
        include = source.__annotations__.keys() if is_dataclass(source) else source.keys()

    if exclude is None:
        exclude = set()

    keys = include & target.__annotations__.keys() - exclude
    kwargs = {key: getter(key) for key in keys}
    return target(**kwargs)


def redact_passwords(hint: Any, value: Any, redaction: str = "__REDACTED__"):
    """
    Redact password fields in dataclass or TypedDict value.
    """
    if is_dataclass(value):
        getter, setter = functools.partial(getattr, value), functools.partial(setattr, value)
    elif isinstance(value, Mapping):
        getter, setter = value.get, value.__setitem__
    else:
        raise TypeError("type must be dataclass or TypedDict")
    value_type, _ = split_annotated(strip_optional(hint))
    for field_name, field_hint in value_type.__annotations__.items():
        field_type, field_annotations = split_annotated(strip_optional(field_hint))
        field_value = getter(field_name)
        if hasattr(field_type, "__annotations__") and (
            is_dataclass(field_value) or isinstance(field_value, Mapping)
        ):
            redact_passwords(field_hint, field_value)
        elif (
            field_value is not None
            and is_subclass(field_type, str)
            and Password in field_annotations
        ):
            setter(field_name, redaction)
