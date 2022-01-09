"""Module to expose resources through HTTP."""

#  TODO: In docstring, add description of routing through resource(s) to an operation.

import asyncio
import base64
import fondat.error
import fondat.resource
import fondat.types
import functools
import http
import http.cookies
import inspect
import logging
import multidict
import typing

from collections import namedtuple
from collections.abc import Callable, Iterable, MutableSequence
from fondat.codec import Binary, JSON, String, get_codec, DecodeError
from fondat.data import datacls
from fondat.error import (
    BadRequestError,
    InternalServerError,
    MethodNotAllowedError,
    NotFoundError,
)
from fondat.security import Scheme
from fondat.stream import Stream, BytesStream, stream_bytes
from fondat.types import is_optional, is_subclass
from typing import Annotated, Any, Optional, TypedDict


_logger = logging.getLogger(__name__)


Headers = multidict.CIMultiDict
Cookies = http.cookies.SimpleCookie
Query = multidict.MultiDict


class Message:
    """
    Base class for HTTP request and response.

    Parameters and attributes:
    • headers: multi-value dictionary to store headers; excludes cookies
    • cookies: dictionary containing message cookies
    • body: stream message body, or None if no body
    """

    def __init__(
        self,
        *,
        headers: Optional[Headers] = None,
        cookies: Optional[Cookies] = None,
        body: Optional[Stream] = None,
    ):
        super().__init__()
        self.headers = headers or Headers()
        self.cookies = cookies or Cookies()
        self.body = body

    def __repr__(self):
        return f"Message(headers={self.headers}, cookies={self.cookies}, body={self.body})"


class Request(Message):
    """
    HTTP request.

    Parameters and attributes:
    • headers: multi-value dictionary to store headers; excludes cookies
    • cookies: simple cookie object to store request cookies
    • body: stream for request body, or None
    • method: the HTTP method name, in upper case
    • path: HTTP request target excluding query string
    • version: version of the incoming HTTP request
    • query: multi-value dictionary to store query string parameters
    """

    def __init__(
        self,
        *,
        headers: Optional[Headers] = None,
        cookies: Optional[Cookies] = None,
        body: Optional[Stream] = None,
        method: str = "GET",
        path: str = "/",
        version: str = "1.1",
        query: Optional[Query] = None,
    ):
        super().__init__(headers=headers, cookies=cookies, body=body)
        self.method = method
        self.path = path
        self.version = version
        self.query = query or Query()

    def __repr__(self):
        return (
            f"Request(headers={self.headers}, cookies={self.cookies}, body={self.body}, "
            f"method={self.method}, path={self.path}, version={self.version}, "
            f"query={self.query})"
        )


class Response(Message):
    """
    HTTP response.

    Parameters and attributes:
    • headers: multi-value dictionary to store headers; excludes cookies
    • cookies: dictionary containing response cookies
    • body: stream for response body, or None
    • status: HTTP status code
    """

    def __init__(
        self,
        *,
        headers: Optional[Headers] = None,
        cookies: Optional[Cookies] = None,
        body: Optional[Stream] = None,
        status: int = http.HTTPStatus.OK.value,
    ):
        super().__init__(headers=headers, cookies=cookies, body=body)
        self.status = status

    def __repr__(self):
        return (
            f"Response(headers={self.headers}, cookies={self.cookies}, body={self.body}, "
            f"status={self.status})"
        )


class Chain:
    """
    A chain of zero or more filters, terminated by a single handler.

    A handler is a coroutine function that receives a request and returns a response. A chain
    is itself a request handler.

    A filter is either a coroutine function or an asynchronous generator.

    A coroutine function filter can inspect and modify a request, and:
    • return no value to indicate that the filter passed; processing continues down the chain
    • return a response; the request is not processed by any subsequent filters or handler

    An asynchronous generator filter can inspect and modify a request, and:
    • yield no value; processing continues down the chain
    • yield a response; the request is not processed by any subsequent filters or handler

    The response from handler and downstream filters is sent to the asynchronous generator
    filter and becomes the result of the yield expression. The filter can then inspect and
    modify the response, and:
    • yield no value; the existing response is passed back up the chain
    • yield a new response; this response is passed back up the chain to the caller
    """

    def __init__(
        self, *, filters: Optional[MutableSequence[Callable]] = None, handler: Callable
    ):
        """Initialize a filter chain."""
        self.filters = filters  # concrete and mutable
        self.handler = handler

    async def __call__(self, request: Request) -> Response:
        """Handle a request."""
        unwind = []
        response = None
        exception = None
        for filter in (f(request) for f in self.filters):
            if inspect.isasyncgen(filter):
                try:
                    response = await filter.__anext__()
                    if not response:
                        unwind.append(filter)
                except StopAsyncIteration:
                    pass
                except Exception as e:
                    exception = e
            elif asyncio.iscoroutine(filter):
                try:
                    response = await filter
                except Exception as e:
                    exception = e
            if exception or response:
                break
        if not exception and not response:
            try:
                response = await self.handler(request)
            except Exception as e:
                exception = e
        for filter in reversed(unwind):
            try:
                _response = (
                    await filter.asend(response)
                    if not exception
                    else await filter.athrow(
                        type(exception), exception, exception.__traceback__
                    )
                )
                if _response:  # new response overrides previous response or exception
                    response = _response
                    exception = None
            except StopAsyncIteration:
                pass
            except Exception as e:  # new exception overrides previous response or exception
                exception = e
        if exception:
            raise exception
        return response


BasicCredentials = namedtuple("BasicCredentials", ("user_id", "password"))


class BasicScheme(Scheme):
    """
    HTTP basic authentication scheme.

    Parameters:
    • name: name of authentication scheme
    • description: a short description of authentication scheme
    """

    def extract(self, request: Request) -> Optional[BasicCredentials]:
        """Return basic credentials from request if provided, otherwise None."""
        try:
            scheme, credentials = request.headers["Authorization"].split(" ", 1)
            if scheme.lower() == "basic":
                return BasicCredentials(*base64.b64decode(credentials).decode().split(":", 1))
        except:
            return None


class BearerScheme(Scheme):
    """
    Bearer token authentication scheme.

    Parameters:
    • name: name of authentication scheme
    • description: a short description of authentication scheme
    • format: identifies how bearer token is formatted
    """

    def __init__(self, *, format: Optional[str] = None, **kwargs):
        super().__init__(**kwargs)
        self.format = format

    def extract(self, request: Request) -> Optional[str]:
        """Return bearer token value from request if provided, otherwise None."""
        try:
            name, token = request.headers["Authorization"].split(" ", 1)
            if name.lower() == "bearer":
                return token
        except:
            pass
        return None


class CookieScheme(Scheme):
    """
    Cookie authentication scheme.

    Parameters:
    • name: name of authentication scheme
    • description: a short description of authentication scheme
    • cookie: name of cookie to be used
    """

    def __init__(self, *, cookie: str, **kwargs):
        super().__init__(**kwargs)
        self.cookie = cookie

    def extract(self, request: Request) -> Optional[str]:
        """Return cookie value from request if provided, otherwise None."""
        try:
            return request.cookies[self.cookie].value
        except:
            return None


class HeaderScheme(Scheme):
    """
    Header authentication scheme.

    Parameters:
    • name: name of authentication scheme
    • description: a short description of authentication scheme
    • header: name of the header to be used
    """

    def __init__(self, *, header: str, **kwargs):
        super().__init__(**kwargs)
        self.header = header

    def extract(self, request: Request) -> Optional[str]:
        """Return header value from request if provided, otherwise None."""
        try:
            return request.headers[self.header]
        except:
            return None


async def _subordinate(resource, segment: str):

    # resource.attr
    if (attr := getattr(resource, segment, None)) is not None:
        if fondat.resource.is_resource(attr):
            return attr
        if not callable(attr):
            raise NotFoundError
        hints = typing.get_type_hints(attr)
        if not fondat.resource.is_resource(hints.get("return")):
            raise NotFoundError
        if asyncio.iscoroutinefunction(attr):
            return await attr()
        else:
            return attr()

    # resource[item]
    try:
        hints = typing.get_type_hints(resource.__getitem__)
        name = next(iter(hints))
        if name == "return":
            raise NotFoundError
        item = resource[get_codec(String, hints[name]).decode(segment)]
        if not fondat.resource.is_resource(item):
            raise NotFoundError
        return item
    except:
        raise NotFoundError


class InQuery:
    """
    Annotation to indicate an operation parameter is expected in a request query string
    parameter.  This is the default annotation for query operation parameters.

    If the InQuery class is used as the annotation instead of an InQuery(name=...) instance,
    then the name of the query string parameter will be the name of the operation parameter.

    Parameters:
    • name: name of the query string parameter
    """

    __slots__ = {"name"}

    def __init__(self, name):
        self.name = name

    def __str__(self):
        return f"request query string parameter: {self.name}"

    def __repr__(self):
        return f"InQuery({self.name})"


class InBody:
    """
    Annotation to indicate an operation parameter is expected in a body parameter. This is the
    default annotation for mutation operation parameters.

    If the InBody class is used as the annotation instead of an InBody(name=...) instance,
    then the name of the body parameter will be the name of the operation parameter.

    Parameters:
    • name: name of the body parameter
    """

    __slots__ = {"name"}

    def __init__(self, name):
        self.name = name

    def __str__(self):
        return f"request body parameter: {self.name}"

    def __repr__(self):
        return f"InBody({self.name})"


class AsBody:
    """Annotation to indicate an operation parameter is expected to be the request body."""

    def __str__(self):
        return f"request body"

    def __repr__(self):
        return "AsBody"


def get_param_in(method, param_name, type_hint):
    """
    Return an annotation expressing where a parameter is to be provided.

    If an annotations is a class, then it is instantated. If no annotation exists, then an
    appropriate InQuery annotation is provided.
    """
    stripped = fondat.types.strip_optional(type_hint)
    if typing.get_origin(stripped) is Annotated:
        args = typing.get_args(stripped)
        for annotation in args[1:]:
            if is_subclass(annotation, (InBody, InQuery)):
                return annotation(param_name)
            elif is_subclass(annotation, AsBody):
                return annotation()
            elif isinstance(annotation, (AsBody, InBody, InQuery)):
                return annotation
    if method._fondat_operation.type == "mutation":
        return InBody(param_name)
    return InQuery(param_name)


@functools.cache
def get_body_type(operation):
    """Return the type of the request body for the specified operation."""
    signature = inspect.signature(operation)
    type_hints = typing.get_type_hints(operation, include_extras=True)
    as_body_param = None
    in_body_params = {}
    required_keys = set()
    for name, hint in type_hints.items():
        if name == "return":
            continue
        param_in = get_param_in(operation, name, hint)
        if not isinstance(param_in, (AsBody, InBody)):
            continue
        is_required = signature.parameters[name].default is inspect.Parameter.empty
        if isinstance(param_in, InBody):
            in_body_params[param_in.name] = hint
            if is_required:
                required_keys.add(param_in.name)
        elif isinstance(param_in, AsBody):
            if as_body_param:
                raise TypeError("multiple AsBody annotated parameters")
            as_body_param = name
    if as_body_param and in_body_params:
        raise TypeError("mixed AsBody and InBody annotated parameters")
    if as_body_param:
        return type_hints[as_body_param]
    if not in_body_params:
        return None
    rb = TypedDict("RequestBody", in_body_params, total=False)
    rb.__required_keys__ = frozenset(required_keys)
    rb.__optional_keys__ = frozenset(k for k in in_body_params if k not in required_keys)
    return rb


async def simple_error_filter(request: Request):
    """Generates a simple JSON error response if an exception is raised."""
    try:
        try:
            yield
        except fondat.error.Error:
            raise
        except Exception as e:
            _logger.exception("unhandled exception")
            raise InternalServerError from e
    except fondat.error.Error as err:
        body = str(err)
        response = Response()
        response.status = err.status
        response.headers["content-type"] = "text/plain; charset=UTF-8"
        response.headers["content-length"] = str(len(body))
        response.body = BytesStream(body.encode())
        yield response


async def _decode_body(operation, request):
    body_type = get_body_type(operation)
    if not body_type:
        return None
    python_type, _ = fondat.types.split_annotated(body_type)
    if is_subclass(python_type, Stream):
        return request.body
    content = await stream_bytes(request.body)
    if len(content) == 0:
        return None  # empty body is no body
    try:
        with DecodeError.path_on_error("«body»"):
            result = get_codec(Binary, body_type).decode(content)
    except DecodeError as de:
        raise BadRequestError from de
    except Exception as e:
        raise InternalServerError from e
    return result


class Application:
    """
    An HTTP application, which handles ncoming HTTP requests by:
    • passing request through zero or more HTTP filters, and
    • dispatching it to a resource.

    Parameters and attributes:
    • root: resource to dispatch requests to
    • filters: filters to apply during HTTP request processing
    • path: URI path to root resource

    An HTTP application is a request handler; it's a coroutine callable that handles an HTTP
    request and returns an HTTP response. For a description of filters, see: Chain.
    """

    def __init__(
        self,
        root: type,
        *,
        filters: Iterable[Any] = (simple_error_filter,),
        path: str = "/",
    ):
        if not fondat.resource.is_resource(root):
            raise TypeError("root is not a resource")
        self.root = root
        self.path = path.rstrip("/") + "/"
        self.filters = list(filters or [])

    async def __call__(self, request: Request) -> Response:
        return await Chain(filters=self.filters, handler=self._handle)(request)

    async def _handle(self, request: Request) -> Response:
        if not request.path.startswith(self.path):
            raise NotFoundError
        path = request.path[len(self.path) :]
        response = Response()
        method = request.method.lower()
        segments = path.split("/") if path else ()
        resource = self.root
        operation = None
        for segment in segments:
            if operation:  # cannot have segments after operation name
                raise NotFoundError
            try:
                resource = await _subordinate(resource, segment)
            except NotFoundError:
                try:
                    operation = getattr(resource, segment)
                    if not fondat.resource.is_operation(operation):
                        raise NotFoundError
                except AttributeError:
                    raise NotFoundError
        if operation:  # operation name as segment (@query or @mutation)
            fondat_op = getattr(operation, "_fondat_operation", None)
            if not fondat_op or not fondat_op.method == method:
                raise MethodNotAllowedError
        else:  # no remaining segments; operation name as HTTP method
            operation = getattr(resource, method, None)
            if not fondat.resource.is_operation(operation):
                raise MethodNotAllowedError
        body = await _decode_body(operation, request)
        params = {}
        signature = inspect.signature(operation)
        hints = typing.get_type_hints(operation, include_extras=True)
        return_hint = hints.get("return", type(None))
        for name, hint in hints.items():
            if name == "return":
                continue
            required = signature.parameters[name].default is inspect.Parameter.empty
            param_in = get_param_in(operation, name, hint)
            if isinstance(param_in, AsBody) and body is not None:
                params[name] = body
            elif isinstance(param_in, InBody) and body is not None:
                if param_in.name in body:
                    params[name] = body[param_in.name]
            elif isinstance(param_in, InQuery):
                if param_in.name in request.query:
                    codec = get_codec(String, hint)
                    try:
                        with DecodeError.path_on_error(param_in.name):
                            params[name] = codec.decode(request.query[param_in.name])
                    except DecodeError as de:
                        raise BadRequestError from de
            if name not in params and required:
                if not is_optional(hint):
                    raise BadRequestError from DecodeError(
                        "required parameter", ["«params»", name]
                    )
                params[name] = None
        result = await operation(**params)
        if not is_subclass(return_hint, Stream):
            return_codec = get_codec(Binary, return_hint)
            try:
                result = BytesStream(return_codec.encode(result), return_codec.content_type)
            except Exception as e:
                raise InternalServerError from e
        response.body = result
        response.headers["Content-Type"] = response.body.content_type
        if response.body.content_length is not None:
            if response.body.content_length == 0:
                response.status = http.HTTPStatus.NO_CONTENT.value
            else:
                response.headers["Content-Length"] = str(response.body.content_length)
        return response
