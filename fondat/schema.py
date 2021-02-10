"""Module to generate JSON Schema structures for Python types."""

from __future__ import annotations

import dataclasses
import fondat.http
import fondat.types
import fondat.validation
import functools
import keyword
import typing

from collections.abc import Iterable, Mapping
from datetime import date, datetime
from decimal import Decimal
from fondat.types import Description, Example, NoneType
from fondat.types import dataclass, is_instance, is_optional, is_subclass
from typing import Any, Literal, Optional, Union
from uuid import UUID


providers = []


@dataclass
class Discriminator:
    propertyName: str
    mapping: Optional[Mapping[str, str]]


@dataclass
class ExternalDocumentation:
    description: Optional[str]
    url: str


@dataclass
class XML:
    name: Optional[str]
    namespace: Optional[str]
    prefix: Optional[str]
    attribute: Optional[bool]
    wrapped: Optional[bool]


@dataclass
class Schema:
    title: Optional[str]
    multipleOf: Optional[Union[int, float]]
    maximum: Optional[Union[int, float]]
    exclusiveMaximum: Optional[Union[int, float]]
    minimum: Optional[Union[int, float]]
    exclusiveMinimum: Optional[Union[int, float]]
    maxLength: Optional[int]
    minLength: Optional[int]
    pattern: Optional[str]
    maxItems: Optional[int]
    minItems: Optional[int]
    uniqueItems: Optional[bool]
    maxProperties: Optional[int]
    minProperties: Optional[int]
    required: Optional[Iterable[str]]
    enum: Optional[Iterable[Any]]
    type: Optional[str]
    allOf: Optional[Iterable[Schema]]
    oneOf: Optional[Iterable[Schema]]
    anyOf: Optional[Iterable[Schema]]
    not_: Optional[Schema]
    items: Optional[Schema]
    properties: Optional[Mapping[str, Schema]]
    additionalProperties: Optional[Union[bool, Schema]]
    description: Optional[str]
    format: Optional[str]
    default: Optional[Any]
    nullable: Optional[bool]
    discriminator: Optional[Discriminator]
    readOnly: Optional[bool]
    writeOnly: Optional[bool]
    xml: Optional[XML]
    externalDocs: Optional[ExternalDocumentation]
    example: Optional[Any]
    deprecated: Optional[bool]


for dc in (Discriminator, ExternalDocumentation, Schema, XML):
    fondat.types.affix_type_hints(dc)


def get_schema(type_hint, default=None):
    """
    Return a JSON schema for the specified Python type hint.

    Parameters:
    • type_hint: Python type optionally wrapped with Annotated arguments
    • default: default value to provide in schema

    A type hint contains not only the Python type, but also optional Annotated arguments to
    include validation, description, example values.
    """

    python_type, annotated = fondat.types.split_annotated(type_hint)
    origin = typing.get_origin(python_type)
    args = typing.get_args(python_type)

    for provider in providers:
        if (schema := provider(python_type, annotated, origin, args)) is not None:
            schema.default = default
            for annotation in annotated:
                if is_instance(annotation, Description):
                    schema.description = annotation.value
                elif is_instance(annotation, Example):
                    schema.example = annotation.value
            return schema

    raise TypeError(f"failed to determine JSON Schema for {python_type}")


def _provider(wrapped=None):
    if wrapped is None:
        return functools.partial(_provider)
    providers.append(wrapped)
    return wrapped


def _description(annotated):
    for annotation in annotated:
        if is_instance(annotation, str):
            return annotation.value


def _kwargs(annotated):
    kwargs = {}
    for annotation in annotated:
        if is_instance(annotation, str):
            kwargs["description"] = annotation
        elif is_instance(annotation, fondat.types.Description):
            kwargs["description"] = annotation.value
        elif is_instance(annotation, fondat.types.Example):
            kwargs["example"] = annotation.value
    return kwargs


# ----- simple -----


def _simple(python_type, schema_type, schema_format=None):
    @_provider
    def schema(pytype, annotated, origin, args):
        if pytype is python_type:
            return Schema(type=schema_type, format=schema_format, **_kwargs(annotated))


_simple(bool, "boolean")
_simple(Decimal, "string", "decimal")
_simple(datetime, "string", "date-time")
_simple(date, "string", "date")  # must come after datetime
_simple(UUID, "string", "uuid")


# ----- str -----


@_provider
def _str_schema(python_type, annotated, *_):
    if is_subclass(python_type, str):
        kwargs = {}
        for annotation in annotated:
            if is_instance(annotation, fondat.validation.MinLen):
                kwargs["minLength"] = annotation.value
            elif is_instance(annotation, fondat.validation.MaxLen):
                kwargs["maxLan ength"] = annotation.value
            elif is_instance(annotation, fondat.validation.Pattern):
                kwargs["pattern"] = annotation.value.pattern
        return Schema(type="string", **_kwargs(annotated), **kwargs)


# ----- bytes/bytearray -----


@_provider
def _bytes_schema(python_type, annotated, *_):
    if is_subclass(python_type, (bytes, bytearray)):
        kwargs = {}
        for annotation in annotated:
            if Is_instance(annotation, fondat.validation.MinLen):
                kwargs["minLength"] = annotation.value
            elif is_instance(annotation, fondat.validation.MaxLen):
                kwargs["maxLength"] = annotation.value
        return Schema(
            type="string",
            format="binary" if fondat.http.InBody in annotated else "byte",
            **_kwargs(annotated),
            **kwargs,
        )


# ----- int -----


@_provider
def _int_schema(python_type, annotated, *_):
    if is_subclass(python_type, int) and not is_subclass(python_type, bool):
        kwargs = {}
        for annotation in annotated:
            if is_instance(annotation, fondat.validation.MinValue):
                kwargs["minimum"] = annotation.value
            elif is_instance(annotation, fondat.validation.MaxValue):
                kwargs["maximum"] = annotation.value
        return Schema(type="integer", format="int64", **_kwargs(annotated), **kwargs)


# ----- float -----


@_provider
def _int_schema(python_type, annotated, *_):
    if is_subclass(python_type, float):
        kwargs = {}
        for annotation in annotated:
            if is_instance(annotation, fondat.validation.MinValue):
                kwargs["minimum"] = annotation.value
            elif is_instance(annotation, fondat.validation.MaxValue):
                kwargs["maximum"] = annotations.value
        return Schema(type="number", format="double", **_kwargs(annotated), **kwargs)


# ---- TypedDict ----


@_provider
def _typeddict_schema(python_type, annotated, origin, args):
    if is_subclass(python_type, dict) and hasattr(python_type, "__annotations__"):
        hints = typing.get_type_hints(python_type, include_extras=True)
        return Schema(
            type="object",
            properties={key: get_schema(pytype) for key, pytype in hints.items()},
            required=list(python_type.__required_keys__),
            additionalProperties=False,
            **_kwargs(annotated),
        )


# ----- Mapping -----


@_provider
def _mapping_schema(python_type, annotated, origin, args):
    if is_subclass(origin, Mapping) and len(args) == 2:
        return Schema(
            type="object",
            properties={},
            additionalProperties=get_schema(args[1]),
            **_kwargs(annotated),
        )


# ----- Iterable -----


@_provider
def _iterable_schema(python_type, annotated, origin, args):
    if is_subclass(origin, Iterable) and not is_subclass(origin, Mapping) and len(args) == 1:
        kwargs = {}
        is_set = is_subclass(origin, set)
        for annotation in annotated:
            if is_instance(annotation, fondat.validation.MinLen):
                kwargs["minItems"] = annotation.value
            elif is_instance(annotation, fondat.validation.MaxLen):
                kwargs["maxItems"] = annotation.value
            if is_set:
                kwargs["uniqueItems"] = True
        return Schema(
            type="array",
            items=get_schema(args[0]),
            **_kwargs(annotated),
            **kwargs,
        )


# ----- dataclass -----


# keywords have _ suffix in dataclass fields (e.g. "in_", "for_", ...)
_dc_kw = {k + "_": k for k in keyword.kwlist}


@_provider
def _dataclass_schema(python_type, annotated, origin, args):
    if dataclasses.is_dataclass(python_type):
        hints = typing.get_type_hints(python_type, include_extras=True)
        required = {
            f.name
            for f in dataclasses.fields(python_type)
            if f.default is dataclasses.MISSING
            and f.default_factory is dataclasses.MISSING
            and not is_optional(hints[f.name])
        }
        properties = {_dc_kw.get(key, key): get_schema(pytype) for key, pytype in hints.items()}
        for key, schema in properties.items():
            if key not in required:
                schema.nullable = None
        return Schema(
            type="object",
            properties=properties,
            required=required,
            additionalProperties=False,
            **_kwargs(annotated),
        )


# ----- Union -----


@_provider
def _union_schema(python_type, annotated, origin, args):
    if origin is Union:
        nullable = NoneType in args
        schemas = [get_schema(arg) for arg in args if arg is not NoneType]
        if len(schemas) == 1:  # Optional[...]
            schemas[0].nullable = True
            return schemas[0]
        return Schema(anyOf=schemas, nullable=nullable, **_kwargs(annotated))


# ----- Literal -----


@_provider
def _literal_schema(python_type, annotated, origin, args):
    if origin is Literal:
        nullable = None in args
        types = tuple({type(literal) for literal in args if literal is not None})
        enums = {t: [l for l in args if type(l) is t] for t in types}
        schemas = {t: get_schema(t) for t in types}
        for t, s in schemas.items():
            s.enum = enums[t]
        if len(types) == 1:  # homegeneous
            schema = schemas[types[0]]
            if nullable:
                schema.nullable = True
            return schema
        return Schema(  # heterogeneus
            anyOf=list(schemas.values()),
            nullable=nullable,
            **_kwargs(annotated),
        )


# ----- Any -----


@_provider
def _any_schema(python_type, annotated, origin, args):
    if python_type is Any:
        return Schema(**_kwargs(annotated))
