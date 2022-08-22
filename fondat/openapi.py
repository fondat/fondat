"""Module to generate OpenAPI documents and resources."""

from __future__ import annotations

import dataclasses
import fondat.annotation
import fondat.codec
import fondat.http
import fondat.resource
import fondat.types
import functools
import http
import inspect
import keyword
import types
import typing

from collections.abc import Iterable, Mapping
from datetime import date, datetime
from decimal import Decimal
from fondat.data import datacls
from fondat.security import Policy
from fondat.types import is_instance, is_optional, is_subclass
from types import NoneType
from typing import Annotated, Any, Literal, TypedDict, TypeVar
from uuid import UUID


class Default:
    def __init__(self, value: Any):
        self.value = value


class ComponentSchema:
    """
    Annotation to request schema be stored in the OpenAPI component schema section.

    Parameter:
    • name: component schema name
    """

    name = None  # can use class if name not required

    def __init__(self, name=None):
        self.name = name


_to_affix = []


def _affix(wrapped):
    _to_affix.append(wrapped)
    return wrapped


def _datacls(wrapped):
    return _affix(Annotated[datacls(wrapped), ComponentSchema])


Reference = TypedDict("Reference", {"$ref": str})
_affix(Reference)


@_datacls
class OpenAPI:
    openapi: str
    info: Info
    servers: Iterable[Server] | None
    paths: Paths
    components: Components | None
    security: Iterable[SecurityRequirement] | None
    tags: Iterable[Tag] | None
    externalDocs: ExternalDocumentation | None


@_datacls
class Info:
    title: str
    description: str | None
    termsOfService: str | None
    contact: Contact | None
    license: License | None
    version: str


@_datacls
class Contact:
    name: str | None
    url: str | None
    email: str | None


@_datacls
class License:
    name: str
    url: str | None


@_datacls
class Server:
    url: str
    description: str | None
    variables: Mapping[str, ServerVariable]


@_datacls
class ServerVariable:
    enum: Iterable[str] | None
    default: str = ""
    description: str | None = ""


@_datacls
class Components:
    schemas: Mapping[str, Schema | Reference] | None
    responses: Mapping[str, Response | Reference] | None
    parameters: Mapping[str, Parameter | Reference] | None
    examples: Mapping[str, Example | Reference] | None
    requestBodies: Mapping[str, RequestBody | Reference] | None
    headers: Mapping[str, Header | Reference] | None
    securitySchemes: Mapping[str, SecurityScheme | Reference] | None
    links: Mapping[str, Link | Reference] | None
    callbacks: Mapping[str, Callback | Reference] | None


@_datacls
class PathItem:
    summary: str | None
    description: str | None
    get: Operation | None
    put: Operation | None
    post: Operation | None
    delete: Operation | None
    options: Operation | None
    head: Operation | None
    patch: Operation | None
    trace: Operation | None
    servers: Iterable[Server] | None
    parameters: Iterable[Parameter | Reference] | None


Paths = Mapping[str, PathItem | Reference]
_affix(Paths)


@_datacls
class Operation:
    tags: Iterable[str] | None
    summary: str | None
    description: str | None
    externalDocs: ExternalDocumentation | None
    operationId: str | None
    parameters: Iterable[Parameter | Reference] | None
    requestBody: RequestBody | Reference | None
    responses: Responses
    callbacks: Mapping[str, Callback | Reference] | None
    deprecated: bool | None
    security: Iterable[SecurityRequirement] | None
    servers: Iterable[Server] | None


@_datacls
class ExternalDocumentation:
    description: str | None
    url: str


@_datacls
class Parameter:
    name: str
    in_: Literal["query", "header", "path", "cookie"]
    description: str | None
    required: bool | None
    deprecated: bool | None
    allowEmptyValue: bool | None
    style: str | None
    explode: bool | None
    allowReserved: bool | None
    schema: Schema | Reference | None
    example: Any | None
    examples: Mapping[str, Example | Reference] | None
    content: Mapping[str, MediaType] | None


@_datacls
class RequestBody:
    description: str | None
    content: Mapping[str, MediaType]
    required: bool | None


@_datacls
class MediaType:
    schema: Schema | Reference | None
    example: Any | None
    examples: Mapping[str, Example | Reference] | None
    encoding: Mapping[str, Encoding] | None


@_datacls
class Encoding:
    contentType: str | None
    headers: Mapping[str, Header | Reference] | None
    style: str | None
    explode: bool | None
    allowReserved: bool | None


@_datacls
class Response:
    description: str
    headers: Mapping[str, Header | Reference] | None
    content: Mapping[str, MediaType] | None
    links: Mapping[str, Link | Reference] | None


Responses = Mapping[str, Response | Reference]
_affix(Responses)


Callback = Mapping[str, PathItem]
_affix(Callback)


@_datacls
class Example:
    summary: str | None
    description: str | None
    value: Any | None
    externalValue: str | None


@_datacls
class Link:
    operationRef: str | None
    operationId: str | None
    parameters: Mapping[str, Any] | None
    requestBody: Any | None
    description: str | None
    server: Server | None


@_datacls
class Header:
    description: str | None
    required: bool | None
    deprecated: bool | None
    allowEmptyValue: bool | None
    style: str | None
    explode: bool | None
    allowReserved: bool | None
    schema: Schema | Reference | None
    example: Any | None
    examples: Mapping[str, Example | Reference] | None
    content: Mapping[str, MediaType] | None


@_datacls
class Tag:
    name: str
    description: str | None
    externalDocs: ExternalDocumentation | None


@_datacls
class Schema:
    title: str | None
    multipleOf: int | float | None
    maximum: int | float | None
    exclusiveMaximum: int | float | None
    minimum: int | float | None
    exclusiveMinimum: int | float | None
    maxLength: int | None
    minLength: int | None
    pattern: str | None
    maxItems: int | None
    minItems: int | None
    uniqueItems: bool | None
    maxProperties: int | None
    minProperties: int | None
    required: Iterable[str] | None
    enum: Iterable[Any] | None
    type: str | None
    allOf: Iterable[Schema | Reference] | None
    oneOf: Iterable[Schema | Reference] | None
    anyOf: Iterable[Schema | Reference] | None
    not_: Schema | Reference | None
    items: Schema | Reference | None
    properties: Mapping[str, Schema | Reference] | None
    additionalProperties: bool | Schema | Reference | None
    description: str | None
    format: str | None
    default: Any | None
    nullable: bool | None
    discriminator: Discriminator | None
    readOnly: bool | None
    writeOnly: bool | None
    xml: XML | None
    externalDocs: ExternalDocumentation | None
    example: Any | None
    deprecated: bool | None


@_datacls
class Discriminator:
    propertyName: str
    mapping: Mapping[str, str] | None


@_datacls
class XML:
    name: str | None
    namespace: str | None
    prefix: str | None
    attribute: bool | None
    wrapped: bool | None


@_datacls
class SecurityScheme:
    type: str
    description: str | None
    name: str | None
    in_: str | None
    scheme: str | None
    bearerFormat: str | None
    flows: OAuthFlows | None
    openIdConnectUrl: str | None


@_datacls
class OAuthFlows:
    implicit: OAuthFlow | None
    password: OAuthFlow | None
    clientCredentials: OAuthFlow | None
    authorizationCode: OAuthFlow | None


@_datacls
class OAuthFlow:
    authorizationUrl: str | None
    tokenUrl: str | None
    refreshUrl: str | None
    scopes: Mapping[str, str]


SecurityRequirement = Mapping[str, Iterable[str]]
_affix(SecurityRequirement)


# OpenAPI document graph complete; affix all type hints to avoid overhead
for dc in _to_affix:
    fondat.types.affix_type_hints(dc)


def _kwargs(python_type, annotated):
    kwargs = {}
    for annotation in annotated:
        match annotation:
            case str():
                kwargs["description"] = annotation
            case fondat.annotation.Description():
                kwargs["description"] = annotation.value
            case fondat.annotation.Example():
                with fondat.validation.validation_error_path("example"):
                    fondat.validation.validate(annotation.value, python_type)
                kwargs["example"] = fondat.codec.get_codec(
                    fondat.codec.JSON, python_type
                ).encode(annotation.value)
            case Default():
                with fondat.validation.validation_error_path("default"):
                    fondat.validation.validate(annotation.value, python_type)
                kwargs["default"] = fondat.codec.get_codec(
                    fondat.codec.JSON, python_type
                ).encode(annotation.value)
            case fondat.annotation.Deprecated():
                kwargs["deprecated"] = annotation.value
            case fondat.annotation.Deprecated:
                kwargs["deprecated"] = True
            case fondat.annotation.ReadOnly():
                kwargs["readOnly"] = annotation.value
            case fondat.annotation.ReadOnly:
                kwargs["readOnly"] = True
    return kwargs


providers = []


def _provider(wrapped=None):
    if wrapped is None:
        return functools.partial(_provider)
    providers.append(wrapped)
    return wrapped


def _simple_schema(pytype, schema_type, schema_format=None):
    @_provider
    def simple(*, python_type, annotated, **_):
        if python_type is pytype:
            return Schema(
                type=schema_type, format=schema_format, **_kwargs(python_type, annotated)
            )


_simple_schema(bool, "boolean")
_simple_schema(Decimal, "string", "decimal")
_simple_schema(datetime, "string", "date-time")
_simple_schema(date, "string", "date")  # must be after datetime
_simple_schema(UUID, "string", "uuid")


@_provider
def _str_schema(*, python_type, annotated, **_):
    if is_subclass(python_type, str):
        kwargs = {}
        for annotation in annotated:
            match annotation:
                case fondat.annotation.Format():
                    kwargs["format"] = annotation.value
                case fondat.validation.MinLen():
                    kwargs["minLength"] = annotation.value
                case fondat.validation.MaxLen():
                    kwargs["maxLength"] = annotation.value
                case fondat.validation.Pattern():
                    kwargs["pattern"] = annotation.pattern.pattern
        return Schema(type="string", **_kwargs(python_type, annotated), **kwargs)


@_provider
def _bytes_schema(*, python_type, annotated, **_):
    if is_subclass(python_type, (bytes, bytearray)):
        kwargs = {}
        for annotation in annotated:
            match annotation:
                case fondat.validation.MinLen():
                    kwargs["minLength"] = annotation.value
                case fondat.validation.MaxLen():
                    kwargs["maxLength"] = annotation.value
        return Schema(
            type="string",
            format="binary" if fondat.http.InBody in annotated else "byte",
            **_kwargs(python_type, annotated),
            **kwargs,
        )


@_provider
def _int_schema(*, python_type, annotated, **_):
    if is_subclass(python_type, int) and not is_subclass(python_type, bool):
        kwargs = {}
        for annotation in annotated:
            match annotation:
                case fondat.validation.MinValue():
                    kwargs["minimum"] = annotation.value
                case fondat.validation.MaxValue():
                    kwargs["maximum"] = annotation.value
        return Schema(
            type="integer", format="int64", **_kwargs(python_type, annotated), **kwargs
        )


@_provider
def _float_schema(*, python_type, annotated, **_):
    if is_subclass(python_type, float):
        kwargs = {}
        for annotation in annotated:
            match annotation:
                case fondat.validation.MinValue():
                    kwargs["minimum"] = annotation.value
                case fondat.validation.MaxValue():
                    kwargs["maximum"] = annotations.value
        return Schema(
            type="number", format="double", **_kwargs(python_type, annotated), **kwargs
        )


def _get_component_schema(annotated):
    for annotation in annotated:
        if annotation is ComponentSchema or is_instance(annotation, ComponentSchema):
            return annotation


@_provider
def _typeddict_schema(*, python_type, annotated, origin, args, processor, **_):
    if is_subclass(python_type, dict) and hasattr(python_type, "__annotations__"):
        if ref := processor.references.get(python_type):
            return ref
        component_schema = _get_component_schema(annotated)
        if component_schema:
            name = component_schema.name or processor.component_schema_name(
                python_type.__name__
            )
            ref = {"$ref": f"#/components/schemas/{name}"}
            processor.references[python_type] = ref
        hints = typing.get_type_hints(python_type, include_extras=True)
        required = list(python_type.__required_keys__) or None
        schema = Schema(
            type="object",
            properties={key: processor.schema(pytype) for key, pytype in hints.items()},
            required=required,
            additionalProperties=False,
            **_kwargs(python_type, annotated),
        )
        if component_schema:
            processor.openapi.components.schemas[name] = schema
            return ref
        return schema


@_provider
def _mapping_schema(*, python_type, annotated, origin, args, processor, **_):
    if is_subclass(origin, Mapping) and len(args) == 2:
        if args[0] is not str:
            raise TypeError("Mapping[k, v] only supports str keys")
        return Schema(
            type="object",
            properties={},
            additionalProperties=processor.schema(args[1]),
            **_kwargs(python_type, annotated),
        )


@_provider
def _iterable_schema(*, python_type, annotated, origin, args, processor, **_):
    if is_subclass(origin, Iterable) and not is_subclass(origin, Mapping) and len(args) == 1:
        kwargs = {}
        if is_subclass(origin, set):
            kwargs["uniqueItems"] = True
        for annotation in annotated:
            match annotation:
                case fondat.validation.MinLen():
                    kwargs["minItems"] = annotation.value
                case fondat.validation.MaxLen():
                    kwargs["maxItems"] = annotation.value
        return Schema(
            type="array",
            items=processor.schema(args[0]),
            **_kwargs(python_type, annotated),
            **kwargs,
        )


# keywords have _ suffix in dataclass fields (e.g. "in_", "for_", ...)
_dc_kw = {k + "_": k for k in keyword.kwlist}


@_provider
def _typevar_schema(*, python_type, processor, **_):
    if isinstance(python_type, TypeVar):
        return processor.schema(fondat.types.resolve_typevar(python_type))


@_provider
def _dataclass_schema(*, python_type, annotated, origin, processor, **_):
    if dataclasses.is_dataclass(python_type) or (origin and dataclasses.is_dataclass(origin)):
        if ref := processor.references.get(python_type):
            return ref
        component_schema = _get_component_schema(annotated)
        dc_type = origin if origin else python_type
        if component_schema:
            name = component_schema.name or processor.component_schema_name(dc_type.__name__)
            ref = {"$ref": f"#/components/schemas/{name}"}
            processor.references[python_type] = ref
        hints = typing.get_type_hints(dc_type, include_extras=True)
        required = {
            f.name
            for f in dataclasses.fields(dc_type)
            if f.default is dataclasses.MISSING
            and f.default_factory is dataclasses.MISSING
            and not is_optional(hints[f.name])
        }
        with fondat.types.capture_typevars(python_type):
            properties = {
                _dc_kw.get(key, key): processor.schema(pytype) for key, pytype in hints.items()
            }
        for key, schema in properties.items():
            if key not in required and not fondat.validation.is_valid(schema, Reference):
                schema.nullable = None
        schema = Schema(
            type="object",
            properties=properties,
            required=required or None,
            additionalProperties=False,
            **_kwargs(python_type, annotated),
        )
        if component_schema:
            processor.openapi.components.schemas[name] = schema
            return ref
        return schema


@_provider
def _union_schema(*, python_type, annotated, origin, args, processor, **_):
    if origin in {types.UnionType, typing.Union}:
        nullable = NoneType in args
        schemas = [processor.schema(arg) for arg in args if arg is not NoneType]
        if len(schemas) == 1:  # optional
            schema = schemas[0]
            if not fondat.validation.is_valid(schema, Reference):
                schema.nullable = True
            return schema
        return Schema(anyOf=schemas, nullable=nullable, **_kwargs(python_type, annotated))


@_provider
def _literal_schema(*, python_type, annotated, origin, args, processor, **_):
    if origin is Literal:
        nullable = None in args
        types = tuple({type(literal) for literal in args if literal is not None})
        enums = {t: [l for l in args if type(l) is t] for t in types}
        schemas = {t: processor.schema(t) for t in types}
        for t, s in schemas.items():
            if fondat.validation.is_valid(s, Reference):
                raise TypeError(f"Cannot document literals containing complex type: {t}")
            s.enum = enums[t]
        if len(types) == 1:  # homegeneous
            schema = schemas[types[0]]
            if nullable and not fondat.validate.is_valid(schema, Reference):
                schema.nullable = True
        else:
            schema = Schema(  # heterogeneus
                anyOf=list(schemas.values()),
                nullable=nullable,
                **_kwargs(python_type, annotated),
            )
        return schema


@_provider
def _any_schema(*, python_type, annotated, origin, args, **_):
    if python_type is Any:
        return Schema(**_kwargs(python_type, annotated))


class Processor:
    """Processes resource and populates OpenAPI document."""

    def __init__(self, openapi: OpenAPI):
        self.openapi = openapi
        self.references = {}
        self.schemes = {}

    def process(self, resource, path, params={}):
        if path == "/":
            path = ""
        tag = resource._fondat_resource.tag
        path_item = PathItem(
            parameters=[
                Parameter(
                    name=key,
                    in_="path",
                    required=True,
                    schema=self.schema(hint),
                )
                for key, hint in params.items()
            ]
            or None
        )
        for name in (n for n in dir(resource) if not n.startswith("_")):
            attr = getattr(resource, name)
            if res := self.resource(attr):
                self.process(
                    res,
                    f"{path}/{name}",
                    params,
                )
            elif operation := self.operation(tag, attr):
                fondat_op = getattr(attr, "_fondat_operation")
                if name == fondat_op.method:
                    setattr(path_item, name, operation)
                    self.openapi.paths[path or "/"] = path_item
                else:
                    op_item = PathItem(parameters=path_item.parameters)
                    setattr(op_item, fondat_op.method, operation)
                    self.openapi.paths[f"{path or ''}/{name}"] = op_item
        attr = getattr(resource, "__getitem__", None)
        if res := self.resource(attr):
            param_name, param_type = next(iter(typing.get_type_hints(attr).items()))
            if param_name in params:
                param_name = f"{res.__name__.casefold()}_{param_name}"
            while param_name in params:
                param_name = f"{param_name}_"
            self.process(
                res,
                f"{path}/{{{param_name}}}",
                {**params, param_name: param_type},
            )
        if self.schemes:
            if not self.openapi.components:
                self.openapi.components = Components()
            if not self.openapi.components.securitySchemes:
                self.openapi.components.securitySchemes = {}
            for name, scheme in self.schemes.items():
                if name not in self.openapi.components.securitySchemes:
                    match scheme:
                        case fondat.http.BasicScheme():
                            security_scheme = SecurityScheme(
                                type="http",
                                description=scheme.description,
                                scheme="basic",
                            )
                        case fondat.http.BearerScheme():
                            security_scheme = SecurityScheme(
                                type="http",
                                description=scheme.description,
                                scheme="bearer",
                                bearerFormat=scheme.format,
                            )
                        case fondat.http.HeaderScheme():
                            security_scheme = SecurityScheme(
                                type="apiKey",
                                description=scheme.description,
                                name=scheme.header,
                                in_="header",
                            )
                        case fondat.http.CookieScheme():
                            security_scheme = SecurityScheme(
                                type="apiKey",
                                description=scheme.description,
                                name=scheme.cookie,
                                in_="cookie",
                            )
                        case _:
                            raise TypeError(f"unknown scheme type for: {name}")
                    self.openapi.components.securitySchemes[name] = security_scheme

    @staticmethod
    def resource(obj):
        if fondat.resource.is_resource(obj):  # resource class or instance
            return obj
        if is_instance(obj, property):  # unbound property
            obj = obj.fget
        if callable(obj):
            try:
                returns = typing.get_type_hints(obj)["return"]
            except:
                return None
            if fondat.resource.is_resource(returns):
                return returns
        return None

    def operation(self, tag, method):
        if not callable(method):
            return None
        fondat_op = getattr(method, "_fondat_operation", None)
        if not fondat_op or not fondat_op.publish:
            return None
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
                if python_type is NoneType:
                    op.responses[str(http.HTTPStatus.NO_CONTENT.value)] = Response(
                        description="No content.",
                    )
                else:
                    origin = typing.get_origin(python_type)
                    args = typing.get_args(python_type)
                    if origin in {types.UnionType, typing.Union} and NoneType in args:
                        op.responses[str(http.HTTPStatus.NO_CONTENT.value)] = Response(
                            description="No content.",
                        )
                        python_type = fondat.types.union_type(
                            arg for arg in args if arg is not None
                        )
                        hint = (
                            Annotated[tuple([python_type, *annotated])]
                            if annotated
                            else python_type
                        )
                    op.responses[str(http.HTTPStatus.OK.value)] = Response(
                        description=self.description(annotated) or "Response.",
                        content={
                            fondat.codec.get_codec(
                                fondat.codec.Binary, hint
                            ).content_type: MediaType(schema=self.schema(hint))
                        },
                    )
            else:
                param = parameters[name]
                if param.default is not param.empty:
                    hint = Annotated[hint, Default(param.default)]
                param_in = fondat.http.get_param_in(method, name, hint)
                if is_instance(param_in, fondat.http.InQuery):
                    name = param_in.name
                    in_ = "query"
                    style = "form"
                    explode = False
                else:
                    continue  # ignore AsBody, InBody
                op.parameters.append(
                    Parameter(
                        name=name,
                        in_=in_,
                        description=self.description(annotated),
                        required=param.default is param.empty,
                        schema=self.schema(hint),
                        style=style,
                        explode=explode,
                    )
                )
        body_type = fondat.http.get_body_type(method)
        if body_type:
            python_type, annotated = fondat.types.split_annotated(body_type)
            op.requestBody = RequestBody(
                description=self.description(annotated),
                content={
                    fondat.codec.get_codec(
                        fondat.codec.Binary, body_type
                    ).content_type: MediaType(schema=self.schema(body_type))
                },
                required=True,
            )
        if "return" not in hints:
            op.responses[str(http.HTTPStatus.NO_CONTENT.value)] = Response(
                description="No content."
            )
        op.responses = {key: op.responses[key] for key in sorted(op.responses.keys())}
        if not op.parameters:
            op.parameters = None
        op.security = self.security_requirements(fondat_op)
        return op

    def security_requirements(self, operation):
        result = []
        for policy in (p for p in operation.policies or () if p.schemes is not None):
            requirements = {}
            for scheme in policy.schemes:
                if self.schemes.get(scheme.name) not in (None, scheme):
                    raise TypeError(f"different schemes with same name: {scheme.name}")
                self.schemes[scheme.name] = scheme
                requirements[scheme.name] = []  # no scopes in currently supported schemes
            result.append(requirements)
        return result if result else None

    @staticmethod
    def description(annotated):
        for annotation in annotated:
            match annotation:
                case str():
                    return annotation
                case fondat.annotation.Description():
                    return annotation.value

    def schema(self, type_hint, default=None):
        python_type, annotated = fondat.types.split_annotated(type_hint)
        origin = typing.get_origin(python_type)
        args = typing.get_args(python_type)
        for provider in providers:
            if (
                schema := provider(
                    python_type=python_type,
                    annotated=annotated,
                    origin=origin,
                    args=args,
                    processor=self,
                )
            ) is not None:
                return schema
        raise TypeError(f"failed to generate JSON Schema for type: {python_type}")

    def component_schema_name(self, name):
        if self.openapi.components is None:
            self.openapi.components = Components()
        if self.openapi.components.schemas is None:
            self.openapi.components.schemas = {}
        while name in self.openapi.components.schemas:
            name = f"{name}_"
        self.openapi.components.schemas[name] = "__reserved__"
        return name


def generate_openapi(*, resource: type, path: str = "/", info: Info) -> OpenAPI:
    """
    Generate an OpenAPI document for a resource.

    Parameters:
    • resource: resource to generate OpenAPI document for
    • path: URI path to resource
    • info: metadata about the API
    """
    openapi = OpenAPI(openapi="3.0.3", info=info, paths={})
    Processor(openapi).process(resource=resource, path=path)
    if openapi.paths:
        openapi.paths = {
            k: openapi.paths[k] for k in sorted(openapi.paths.keys(), key=str.lower)
        }
    if openapi.components and openapi.components.schemas:
        openapi.components.schemas = {
            k: openapi.components.schemas[k]
            for k in sorted(openapi.components.schemas.keys(), key=str.lower)
        }
    return openapi


def openapi_resource(
    *,
    resource: type,
    path: str = "/",
    info: Info,
    policies: Iterable[Policy] | None = None,
    publish: bool = False,
) -> Any:
    """
    Generate a resource that exposes an OpenAPI document for a given resource.

    Parameters:
    • resource: resource to generate OpenAPI document for
    • path: URI path to resource
    • info: provides metadata about the API
    • policies: security policies to apply to all operations
    • publish: publish the resource in documentation
    """

    @fondat.resource.resource
    class OpenAPIResource:
        def __init__(self):
            self.openapi = None

        @fondat.resource.operation(publish=publish, policies=policies)
        async def get(self) -> OpenAPI:
            if not self.openapi:
                self.openapi = generate_openapi(resource=resource, path=path, info=info)
            return self.openapi

    return OpenAPIResource()
