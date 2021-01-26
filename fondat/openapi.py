"""OpenAPI module for Fondat."""

# TODO: tags
# TODO: components for dataclasses


from __future__ import annotations

import dataclasses
import fondat.codec
import fondat.http
import fondat.types
import functools
import http
import inspect
import keyword
import typing

from collections.abc import Iterable, Mapping
from datetime import date, datetime
from decimal import Decimal
from fondat.resource import resource, operation
from fondat.types import dataclass, is_instance, is_optional, is_subclass
from fondat.types import NoneType
from typing import Any, Literal, Optional, TypedDict, Union
from uuid import UUID


_to_affix = []


def _affix(wrapped):
    _to_affix.append(wrapped)
    return wrapped


@_affix
@dataclass
class OpenAPI:
    openapi: str
    info: Info
    servers: Optional[Iterable[Server]]
    paths: Paths
    components: Optional[Components]
    security: Optional[Iterable[SecurityRequirement]]
    tags: Optional[Iterable[Tag]]
    externalDocs: Optional[ExternalDocumentation]


@_affix
@dataclass
class Info:
    title: str
    description: Optional[str]
    termsOfService: Optional[str]
    contact: Optional[Contact]
    license: Optional[License]
    version: str


@_affix
@dataclass
class Contact:
    name: Optional[str]
    url: Optional[str]
    email: Optional[str]


@_affix
@dataclass
class License:
    name: str
    url: Optional[str]


@_affix
@dataclass
class Server:
    url: str
    description: Optional[str]
    variables: Mapping[str, ServerVariable]


@_affix
@dataclass
class ServerVariable:
    enum: Optional[Iterable[str]]
    default: str = ""
    description: str = ""


@_affix
@dataclass
class Components:
    schemas: Optional[Mapping[str, Union[Schema, Reference]]]
    responses: Optional[Mapping[str, Union[Response, Reference]]]
    parameters: Optional[Mapping[str, Union[Parameter, Reference]]]
    examples: Optional[Mapping[str, Union[Example, Reference]]]
    requestBodies: Optional[Mapping[str, Union[RequestBody, Reference]]]
    headers: Optional[Mapping[str, Union[Header, Reference]]]
    securitySchemes: Optional[Mapping[str, Union[SecurityScheme, Reference]]]
    links: Optional[Mapping[str, Union[Link, Reference]]]
    callbacks: Optional[Mapping[str, Union[Callback, Reference]]]


@_affix
@dataclass
class PathItem:
    summary: Optional[str]
    description: Optional[str]
    get: Optional[Operation]
    put: Optional[Operation]
    post: Optional[Operation]
    delete: Optional[Operation]
    options: Optional[Operation]
    head: Optional[Operation]
    patch: Optional[Operation]
    trace: Optional[Operation]
    servers: Optional[Iterable[Server]]
    parameters: Optional[Iterable[Union[Parameter, Reference]]]


Reference = TypedDict("Reference", {"$ref": str})
_affix(Reference)


Paths = Mapping[str, Union[PathItem, Reference]]
_affix(Paths)


@_affix
@dataclass
class Operation:
    tags: Optional[Iterable[str]]
    summary: Optional[str]
    description: Optional[str]
    externalDocs: Optional[ExternalDocumentation]
    operationId: Optional[str]
    parameters: Optional[Iterable[Union[Parameter, Reference]]]
    requestBody: Optional[Union[RequestBody, Reference]]
    responses: Responses
    callbacks: Optional[Mapping[str, Union[Callback, Reference]]]
    deprecated: Optional[bool]
    security: Optional[Iterable[SecurityRequirement]]
    servers: Optional[Iterable[Server]]


@_affix
@dataclass
class ExternalDocumentation:
    description: Optional[str]
    url: str


@_affix
@dataclass
class Parameter:
    name: str
    in_: Literal["query", "header", "path", "cookie"]
    description: Optional[str]
    required: Optional[bool]
    deprecated: Optional[bool]
    allowEmptyValue: Optional[bool]
    style: Optional[str]
    explode: Optional[bool]
    allowReserved: Optional[bool]
    schema: Optional[Union[Schema, Reference]]
    example: Optional[Any]
    examples: Optional[Mapping[str, Union[Example, Reference]]]
    content: Optional[Mapping[str, MediaType]]


@_affix
@dataclass
class RequestBody:
    description: Optional[str]
    content: Mapping[str, MediaType]
    required: Optional[bool]


@_affix
@dataclass
class MediaType:
    schema: Optional[Union[Schema, Reference]]
    example: Optional[Any]
    examples: Optional[Mapping[str, Union[Example, Reference]]]
    encoding: Optional[Mapping[str, Encoding]]


@_affix
@dataclass
class Encoding:
    contentType: Optional[str]
    headers: Optional[Mapping[str, Union[Header, Reference]]]
    style: Optional[str]
    explode: Optional[bool]
    allowReserved: Optional[bool]


@_affix
@dataclass
class Response:
    description: str
    headers: Optional[Mapping[str, Union[Header, Reference]]]
    content: Optional[Mapping[str, MediaType]]
    links: Optional[Mapping[str, Union[Link, Reference]]]


Responses = Mapping[str, Union[Response, Reference]]
_affix(Responses)


Callback = Mapping[str, PathItem]
_affix(Callback)


@_affix
@dataclass
class Example:
    summary: Optional[str]
    description: Optional[str]
    value: Optional[Any]
    externalValue: Optional[str]


@_affix
@dataclass
class Link:
    operationRef: Optional[str]
    operationId: Optional[str]
    parameters: Optional[Mapping[str, Any]]
    requestBody: Optional[Any]
    description: Optional[str]
    server: Optional[Server]


@_affix
@dataclass
class Header:
    description: Optional[str]
    required: Optional[bool]
    deprecated: Optional[bool]
    allowEmptyValue: Optional[bool]
    style: Optional[str]
    explode: Optional[bool]
    allowReserved: Optional[bool]
    schema: Optional[Union[Schema, Reference]]
    example: Optional[Any]
    examples: Optional[Mapping[str, Union[Example, Reference]]]
    content: Optional[Mapping[str, MediaType]]


@_affix
@dataclass
class Tag:
    name: str
    description: Optional[str]
    externalDocs: Optional[ExternalDocumentation]


@_affix
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


@_affix
@dataclass
class Discriminator:
    propertyName: str
    mapping: Optional[Mapping[str, str]]


@_affix
@dataclass
class XML:
    name: Optional[str]
    namespace: Optional[str]
    prefix: Optional[str]
    attribute: Optional[bool]
    wrapped: Optional[bool]


@_affix
@dataclass
class SecurityScheme:
    type_: str
    description: Optional[str]
    name: Optional[str]
    in_: Optional[str]
    scheme: Optional[str]
    bearerFormat: Optional[str]
    flows: Optional[OAuthFlows]
    openIdConnectUrl: Optional[str]


@_affix
@dataclass
class OAuthFlows:
    implicit: Optional[OAuthFlow]
    password: Optional[OAuthFlow]
    clientCredentials: Optional[OAuthFlow]
    authorizationCode: Optional[OAuthFlow]


@_affix
@dataclass
class OAuthFlow:
    authorizationUrl: Optional[str]
    tokenUrl: Optional[str]
    refreshUrl: Optional[str]
    scopes: Mapping[str, str]


SecurityRequirement = Mapping[str, Iterable[str]]
_affix(SecurityRequirement)


# OpenAPI document graph complete; affix all type hints to avoid overhead
for dc in _to_affix:
    fondat.types.affix_type_hints(dc)


def _resource(attr):

    # property not yet bound as descriptor; use its underlying function
    if is_instance(attr, property):
        attr = attr.fget

    # instance of a resource class
    if hasattr(attr, "_fondat_resource") and type(attr) is not type:
        return type(attr)

    # callable that returns a resource
    if callable(attr) and hasattr(attr, "__annotations__"):
        try:
            hints = typing.get_type_hints(attr)
        except:
            return
        returns = hints.get("return", None)
        if hasattr(returns, "_fondat_resource"):
            return returns


_ops = {"get", "put", "post", "delete", "options", "head", "patch", "trace"}


def _description(annotated):
    for annotation in annotated:
        if is_instance(annotation, fondat.types.Description):
            return annotation.value


def _operation(method):

    fondat_op = getattr(method, "_fondat_operation", None)
    if not fondat_op or not fondat_op.publish:
        return

    op = Operation(parameters=[], responses={})

    if fondat_op.summary:
        op.summary = fondat_op.summary

    if fondat_op.description:
        op.description = fondat_op.description

    if fondat_op.deprecated:
        op.deprecated = True

    hints = typing.get_type_hints(method, include_extras=True)
    parameters = inspect.signature(method).parameters

    for name, hint in hints.items():

        python_type, annotated = fondat.types.split_annotated(hint)

        if name == "return":
            op.responses[str(http.HTTPStatus.OK.value)] = Response(
                description=_description(annotated) or "Response.",
                content={
                    fondat.codec.get_codec(fondat.codec.Binary, hint).content_type: MediaType(
                        schema=get_schema(hint)
                    )
                },
            )

        elif fondat.http.InBody in annotated:
            param = parameters[name]
            op.requestBody = RequestBody(
                description=_description(annotated),
                content={
                    fondat.codec.get_codec(fondat.codec.Binary, hint).content_type: MediaType(
                        schema=get_schema(hint)
                    )
                },
                required=param.default is param.empty,
            )

        else:
            param = parameters[name]
            op.parameters.append(
                Parameter(
                    name=name,
                    in_="query",
                    description=_description(annotated),
                    required=param.default is param.empty,
                    schema=get_schema(hint),
                )
            )

    if "return" not in hints:
        op.responses[str(http.HTTPStatus.NO_CONTENT.value)] = Response(
            description="No content.",
        )

    if not op.parameters:
        op.parameters = None

    return op


def _process(doc, resource, path, params={}):
    path_item = PathItem(
        parameters=[
            Parameter(
                name=key,
                in_="path",
                required=True,
                schema=get_schema(hint),
            )
            for key, hint in params.items()
        ] or None
    )
    for name in dir(resource):
        attr = getattr(resource, name)
        if res := _resource(attr):
            if name == "__getitem__":
                param_name, param_type = next(iter(get_type_hints(attr).items()))
                param_name = f"{param_type.__name__.casefold()}_{param_name}"
                _process(
                    doc,
                    res,
                    f"{path}/{{{param_name}}}",
                    {**params, param_name: param_type},
                )
            else:
                _process(doc, res, f"{path}/{name}", params)
        elif name in _ops and callable(attr):
            setattr(path_item, name, _operation(attr))
            doc.paths[path] = path_item


schema_providers = []


def _schema_provider(wrapped=None):
    if wrapped is None:
        return functools.partial(provider)
    schema_providers.append(wrapped)
    return wrapped


# ----- simple -----


def _simple(python_type, schema_type, schema_format=None):
    @_schema_provider
    def schema(pytype, *_):
        if pytype is python_type:
            return Schema(type=schema_type, format=schema_format)


_simple(bool, "boolean")
_simple(Decimal, "string", "decimal")
_simple(datetime, "string", "date-time")
_simple(date, "string", "date")  # must come after datetime
_simple(UUID, "string", "uuid")


# ----- str -----


@_schema_provider
def _str_schema(python_type, annotated, *_):
    if is_subclass(python_type, str):
        kwargs = {}
        for annotation in annotated:
            if is_instance(annotation, fondat.types.MinLen):
                kwargs["minLength"] = annotation.value
            elif is_instance(annotation, fondat.types.MaxLen):
                kwargs["maxLength"] = annotation.value
            elif is_instance(annotation, fondat.types.Pattern):
                kwargs["pattern"] = annotation.value.pattern
        return Schema(type="string", **kwargs)


# ----- bytes/bytearray -----


@_schema_provider
def _bytes_schema(python_type, annotated, *_):
    if is_subclass(python_type, (bytes, bytearray)):
        kwargs = {}
        for annotation in annotated:
            if Is_instance(annotation, fondat.types.MinLen):
                kwargs["minLength"] = annotation.value
            elif is_instance(annotation, fondat.types.MaxLen):
                kwargs["maxLength"] = annotation.value
        return Schema(
            type="string",
            format="binary" if fondat.http.InBody in annotated else "byte",
            **kwargs,
        )


# ----- int -----


@_schema_provider
def _int_schema(python_type, annotated, *_):
    if is_subclass(python_type, int) and not is_subclass(python_type, bool):
        kwargs = {}
        for annotation in annotated:
            if is_instance(annotation, fondat.types.MinValue):
                kwargs["minimum"] = annotation.value
            elif is_instance(annotation, fondat.types.MaxValue):
                kwargs["maximum"] = annotation.value
        return Schema(type="integer", format="int64", **kwargs)


# ----- float -----


@_schema_provider
def _int_schema(python_type, annotated, *_):
    if is_subclass(python_type, float):
        kwargs = {}
        for annotation in annotated:
            if is_instance(annotation, fondat.types.MinValue):
                kwargs["minimum"] = annotation.value
            elif is_instance(annotation, fondat.types.MaxValue):
                kwargs["maximum"] = annotations.value
        return Schema(type="number", format="double", **kwargs)


# ---- TypedDict ----


@_schema_provider
def _typeddict_schema(python_type, annotated, origin, args):
    if is_subclass(python_type, dict) and hasattr(python_type, "__annotations__"):
        hints = typing.get_type_hints(python_type, include_extras=True)
        return Schema(
            type="object",
            properties={key: get_schema(pytype) for key, pytype in hints.items()},
            required=list(python_type.__required_keys__),
            additionalProperties=False,
        )


# ----- Mapping -----


@_schema_provider
def _mapping_schema(python_type, annotated, origin, args):
    if is_subclass(origin, Mapping) and len(args) == 2:
        return Schema(
            type="object",
            properties={},
            additionalProperties=get_schema(args[1]),
        )


# ----- Iterable -----


@_schema_provider
def _iterable_schema(python_type, annotated, origin, args):
    if (
        is_subclass(origin, Iterable)
        and not is_subclass(origin, Mapping)
        and len(args) == 1
    ):
        kwargs = {}
        is_set = issubclass(origin, set)
        for annotation in annotated:
            if is_instance(annotation, typing.MinLen):
                kwargs["minItems"] = annotation.value
            elif is_instance(annotation, typing.MaxLen):
                kwargs["maxItems"] = annotation.value
            if is_set:
                kwargs["uniqueItems"] = True
        return Schema(type="array", items=get_schema(args[0]), **kwargs)


# ----- dataclass -----


# keywords have _ suffix in dataclass fields (e.g. "in_", "for_", ...)
_dc_kw = {k + "_": k for k in keyword.kwlist}


@_schema_provider
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
        properties = {
            _dc_kw.get(key, key): get_schema(pytype)
            for key, pytype in hints.items()
        }
        for key, schema in properties.items():
            if key not in required:
                schema.nullable = None
        return Schema(type="object", properties=properties, required=required, additionalProperties=False)


# ----- Union -----


@_schema_provider
def _union_schema(python_type, annotated, origin, args):
    if origin is Union:
        nullable = NoneType in args
        schemas = [get_schema(arg) for arg in args if arg is not NoneType]
        if len(schemas) == 1:  # Optional[...]
            schemas[0].nullable = True
            return schemas[0]
        return Schema(anyOf=schemas, nullable=nullable)


# ----- Literal -----


@_schema_provider
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
        return Schema(anyOf=list(schemas.values()), nullable=nullable)  # heterogeneous


def get_schema(type_hint):
    """Return a JSON schema for the specified Python type hint."""

    python_type, annotated = fondat.types.split_annotated(type_hint)
    origin = typing.get_origin(python_type)
    args = typing.get_args(python_type)

    for provider in schema_providers:
        if (schema := provider(python_type, annotated, origin, args)) is not None:
            return schema

    raise TypeError(f"failed to determine JSON Schema for {python_type}")


def generate_openapi(root: Any, info: Info) -> OpenAPI:
    """
    Generate an OpenAPI document.

    Parameters:
    • root: Root resource to generate OpenAPI document on.
    • info: Provides metadata about the API.
    """
    doc = OpenAPI(openapi="3.0.2", info=info, paths={})
    _process(doc, root, "")
    return doc


def openapi_resource(root: Any, info: Info):
    """
    Generate a resource that exposes an OpenAPI document.

    Parameters:
    • root: Root resource to generate OpenAPI document on.
    • info: Provides metadata about the API.
    """
    doc = generate_openapi(root, info)

    @resource
    class OpenAPIResource:
        @operation
        async def get(self) -> OpenAPI:
            return doc

    return OpenAPIResource()
