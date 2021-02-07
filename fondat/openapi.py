"""Module to generate OpenAPI documents and resources."""


# TODO: components for dataclasses
# TODO: deal with cyclic graphs?


from __future__ import annotations

import fondat.codec
import fondat.http
import fondat.resource
import fondat.types
import http
import inspect
import typing

from collections.abc import Iterable, Mapping
from fondat.schema import ExternalDocumentation, Schema, get_schema
from fondat.security import SecurityRequirement
from fondat.types import dataclass, is_instance
from typing import Any, Literal, Optional, TypedDict, Union


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
        if is_instance(annotation, str):
            return annotation
        elif is_instance(annotation, fondat.types.Description):
            return annotation.value


def _operation(tag, method):

    fondat_op = getattr(method, "_fondat_operation", None)
    if not fondat_op or not fondat_op.publish:
        return

    op = Operation(parameters=[], responses={})

    op.tags = [tag]

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


def _process(doc, resource, path, params={}, tag=None):
    tag = tag or resource._fondat_resource.tag
    path_item = PathItem(
        parameters=[
            Parameter(
                name=key,
                in_="path",
                required=True,
                schema=get_schema(hint),
            )
            for key, hint in params.items()
        ]
        or None
    )
    for name in (n for n in dir(resource) if not n.startswith("_")):
        attr = getattr(resource, name)
        if res := _resource(attr):
            _process(
                doc,
                res,
                f"{path}/{name}",
                params,
                tag if res._fondat_resource.tag == "__inner__" else None,
            )
        elif name in _ops and callable(attr):
            operation = _operation(tag, attr)
            if operation:
                setattr(path_item, name, operation)
                doc.paths[path] = path_item

    attr = getattr(resource, "__getitem__", None)
    if res := _resource(attr):
        param_name, param_type = next(iter(typing.get_type_hints(attr).items()))
        if param_name in params:
            param_name = f"{res.__name__.casefold()}_{param_name}"
        while param_name in params:
            param_name = f"{param_name}_"
        _process(
            doc,
            res,
            f"{path}/{{{param_name}}}",
            {**params, param_name: param_type},
        )


def generate_openapi_doc(*, resource: type, path: str = None, info: Info) -> OpenAPI:
    """
    Generate an OpenAPI document for a resource.

    Parameters:
    • resource: resource to generate OpenAPI document for
    • path: URI path to resource
    • info: metadata about the API
    """
    doc = OpenAPI(openapi="3.0.2", info=info, paths={})
    _process(doc, resource, path or "")
    return doc


def openapi_resource(
    *,
    resource: type,
    path: str = None,
    info: Info,
    security: Iterable[SecurityRequirement] = None,
):
    """
    Generate a resource that exposes an OpenAPI document for a given resource.

    Parameters:
    • resource: resource to generate OpenAPI document for
    • path: URI path to resource
    • info: provides metadata about the API
    • security: security requirements to apply to all operations
    """

    @fondat.resource.resource
    class OpenAPIResource:
        def __init__(self):
            self.doc = None

        @fondat.resource.operation(publish=False, security=security)
        async def get(self) -> OpenAPI:
            if not self.doc:
                self.doc = generate_openapi_doc(resource=resource, path=path, info=info)
            return self.doc

    return OpenAPIResource()
