"""???"""

import asyncio
import fondat.resource
import fondat.security
import http
import http.cookies
import inspect
import json
import logging
import multidict
import typing

from collections.abc import Callable, Iterable
from fondat.codec import get_codec
from fondat.error import (
    BadRequestError,
    InternalServerError,
    MethodNotAllowedError,
    NotFoundError,
)
from fondat.types import Stream, BytesStream, get_type_hints
from fondat.validate import validate
from typing import Annotated, Any


_logger = logging.getLogger(__name__)


Headers = multidict.CIMultiDict
Cookies = http.cookies.SimpleCookie
Query = multidict.MultiDict


class Message:
    """
    Base class for HTTP request and response.

    Attributes:
    • headers: Multi-value dictionary to store headers; excludes cookies.
    • cookies: Dictionary containing message cookies.
    • body: Stream message body, or None if no body.
    """

    def __init__(self):
        super().__init__()
        self.headers = Headers()
        self.cookies = Cookies()
        self.body = None


class Request(Message):
    """
    HTTP request.

    Attributes:
    • headers: Multi-value dictionary to store headers; excludes cookies.
    • cookies: Simple cookie object to store request cookies.
    • body: Stream for request body, or None.
    • method: The HTTP method name, in uppper case.
    • path: HTTP request target excluding query string.
    • query: Multi-value dictionary to store query string parameters.
    """

    def __init__(self):
        super().__init__()
        self.method = "GET"
        self.path = "/"
        self.query = Query()
        print(f"{self.query=}")


class Response(Message):
    """
    HTTP response.

    Attributes:
    • headers: Multi-value dictionary to store headers; excludes cookies.
    • cookies: Dictionary containing response cookies.
    • body: Stream for response body, or None.
    • status: HTTP status code.
    """

    def __init__(self):
        super().__init__()
        self.status: int = http.HTTPStatus.OK.value


class Chain:
    """
    A chain of zero or more filters, terminated by a single handler.

    A filter is a coroutine function or asynchronous generator that can
    inspect and/or modify a request and optionally inspect, modify and/or
    yield a new response.

    A handler is a coroutine function that inspects a request and returns a
    response. A chain is itself a request handler.
    """

    def __init__(self, *, filters=None, handler):
        """Initialize a filter chain."""
        self.filters = filters  # concrete and mutable
        self.handler = handler

    async def handle(self, request):
        """Handle a request."""
        rewind = []
        response = None
        for filter in (f(request) for f in self.filters):
            if inspect.isasyncgen(filter):
                try:
                    response = await filter.__anext__()
                    if response:  # yielded a response
                        break
                    rewind.append(filter)
                except StopAsyncIteration:
                    pass
            elif asyncio.iscoroutine(filter):
                response = await filter
                if response:  # yielded a response
                    break
        if not response:
            response = await self.handler(request)
        for filter in reversed(rewind):
            try:
                _response = await filter.asend(response)
                if _response:  # yielded a new response
                    response = _response
            except StopAsyncIteration:
                pass
        return response


class HTTPSecurityScheme(fondat.security.SecurityScheme):
    """
    Base class for HTTP authentication security scheme.

    Parameters:
    • name: Name of the security scheme.
    • scheme: Name of the HTTP authorization scheme.
    • description: A short description for the security scheme.
    """

    def __init__(self, name, scheme, **kwargs):
        super().__init__(name, "http", **kwargs)
        self.scheme = scheme

    @property
    def json(self):
        """JSON representation of the security scheme."""
        result = super().json
        result["scheme"] = self.scheme
        return result


class HTTPBasicSecurityScheme(HTTPSecurityScheme):
    """
    Base class for HTTP basic authentication security scheme. Subclass must
    implement the authenticate method.

    Parameters:
    • name: Name of the security scheme.
    • realm: Realm to include in the challenge.  [name]
    • description: A short description for the security scheme.
    """

    def __init__(self, name, realm=None, **kwargs):
        super().__init__(name, "basic", **kwargs)
        self.realm = realm or name

    async def filter(self, request):
        """
        Filters the incoming HTTP request. If the request contains credentials in the
        HTTP Basic authentication scheme, they are passed to the authenticate method.
        If authentication is successful, a context is added to the context stack.
        """
        auth = None
        header = request.headers.get("Authorization")
        if header and request.authorization[0].lower() == "basic":
            try:
                user_id, password = (
                    base64.b64decode(request.authorization[1]).decode().split(":", 1)
                )
            except (binascii.Error, UnicodeDecodeError):
                pass
            else:
                auth = await self.authenticate(user_id, password)
        with fondat.context.push(auth) if auth else contextlib.nullcontext():
            yield

    async def authenticate(user_id, password):
        """
        Perform authentication of credentials supplied in the HTTP request. If
        authentication is successful, a context is returned to be pushed on
        the context stack. If authentication fails, None is returned. This
        method should not raise an exception unless an unrecoverable error
        occurs.
        """
        raise NotImplementedError


class APIKeySecurityScheme(fondat.security.SecurityScheme):
    """
    Base class for API key authentication security scheme. Subclass must
    implement the authenticate method.

    Parameters:
    • name: Name of the security scheme.
    • key: Name of API key to be used.
    • location: Location of API key.  {"header", "cookie"}
    • description: A short description for the security scheme.
    """

    def __init__(self, name, key, location, **kwargs):
        super().__init__(name, "apiKey", **kwargs)
        self.key = key
        self.location = location

    @property
    def json(self):
        """JSON representation of the security scheme."""
        result = super().json
        result["name"] = self.key
        result["in"] = self.location
        return result

    async def authenticate(value):
        """
        Perform authentication of API key value supplied in the HTTP request.
        If authentication is successful, a context is returned to be pushed on
        the context stack. If authentication fails, None is returned. This
        method should not raise an exception unless an unrecoverable error
        occurs.
        """
        raise NotImplementedError


class HeaderSecurityScheme(APIKeySecurityScheme):
    """
    Base class for header authentication security scheme. Subclass must
    implement the authenticate method.

    Parameters:
    • name: Name of the security scheme.
    • header: Name of the header to be used.
    • description: A short description for the security scheme.
    """

    def __init__(self, name, header, **kwargs):
        super().__init__(name, location="header", key=header, **kwargs)

    async def filter(self, request):
        """
        Filters the incoming HTTP request. If the request contains the header,
        it is passed to the authenticate method. If authentication is
        successful, a context is added to the context stack.
        """
        header = request.headers.get(self.key)
        auth = await self.authenticate(header) if header is not None else None
        with fondat.context.push(auth) if auth else contextlib.nullcontext():
            yield

    async def authenticate(self, header):
        raise NotImplementedError


class CookieSecurityScheme(APIKeySecurityScheme):
    """
    Base class for cookie authentication security scheme. Subclass must
    implement the authenticate method.

    Parameters:
    • name: Name of the security scheme.
    • cookie: Name of cookie to be used.
    • description: A short description for the security scheme.
    """

    def __init__(self, name, cookie, **kwargs):
        super().__init__(name, location="cookie", key=cookie, **kwargs)

    async def filter(self, request):
        """
        Filters the incoming HTTP request. If the request contains the cookie,
        it is passed to the authenticate method. If authentication is
        successful, a context is added to the context stack.
        """
        cookie = request.cookies.get(self.key)
        auth = await self.authenticate(cookie) if cookie is not None else None
        with fondat.context.push(auth) if auth else contextlib.nullcontext():
            yield

    async def authenticate(self, cookie):
        raise NotImplementedError


async def _resource(attr):
    if fondat.resource.is_resource(attr):
        return attr
    if not callable(attr):
        return None
    hints = typing.get_type_hints(attr)
    if not fondat.resource.is_resource(hints.get("returns")):
        return None
    try:
        if is_coroutine_function(attr):
            return await attr()
        else:
            return attr()
    except:
        return None


class InParam:
    """Base class for parameter annotations."""


class InQuery(InParam):
    """Annotation to indicate a parameter is provided in request body."""

    def __init__(self, name: str = None):
        self.name = name

    async def get(self, codec, request):
        try:
            value = request.query.get(self.name)
            print(f"{value=}")
            return codec.str_decode(value)
        except (TypeError, ValueError) as e:
            raise BadRequestError(f"{e} in {self}")

    def __str__(self):
        return f"query parameter: {self.name}"


async def _ajoin(stream) -> bytearray:
    result = bytearray()
    if stream is not None:
        async for b in stream:
            result.append(b)
    return result


class InBody(InParam):
    """Annotation to indicate a parameter is provided in request body."""

    async def get(self, codec, request):
        try:
            value = await _ajoin(request.body)
            return codec.bytes_decode(value)
        except (TypeError, ValueError) as e:
            raise BadRequestError(f"{e} in {self}")

    def __str__(self):
        return "request body"


class InHeader(InParam):
    """Annotation to indicate a parameter is provided in request header."""

    def __init__(self, name: str):
        self.name = name

    async def get(self, codec, request):
        try:
            value = request.headers.get(self.name)
            return codec.str_decode(value)
        except (TypeError, ValueError) as e:
            raise BadRequestError(f"{e} in {self}")

    def __str__(self):
        return f"header: {self.name}"


class InCookie(InParam):
    """Annotation to indicate a parameter is provided in request cookie."""

    def __init__(self, name: str):
        self.name = name

    async def get(self, codec, request):
        try:
            value = request.cookies.get(self.name)
            return codec.str_decode(value)
        except (TypeError, ValueError) as e:
            raise BadRequestError(f"{e} in {self}")

    def __str__(self):
        return f"cookie: {self.name}"


async def handle_exception(error: fondat.error.Error):
    """Default exception handler."""

    response = Response()
    response.status = error.status
    print(f"{error=}")
    response.body = BytesStream(
        json.dumps(
            dict(
                error=error.status,
                detail=error.args[0] if error.args else error.__doc__,
            )
        ).encode()
    )
    return response


class Application:
    """
    An HTTP application.

    Parameters and attributes:
    • root: Resource to dispatch requests to.
    • url: URL to access the application.
    • title: Title of the application.
    • version: API implementation version.
    • description: Short description of the application.
    • filters: List of filters to apply during HTTP request processing.
    • exception_handler: Produces response for caught exception.
    """

    def __init__(
        self,
        root: Any,
        url: str,
        title: str,
        version: str,
        description: str,
        filters: Iterable[Any] = None,
        exception_handler: Callable = handle_exception,
    ):
        if not fondat.resource.is_resource(root):
            raise TypeError("root is not a resource")
        self.root = root
        self.url = url
        self.title = title
        self.version = version
        self.description = description
        self.filters = list(filters or [])
        self.exception_handler = exception_handler

    async def handle(self, request: Request):
        try:
            try:
                chain = Chain(filters=self.filters, handler=self._handle)
                return await chain.handle(request)
            except fondat.error.Error:
                raise
            except Exception as ex:
                raise InternalServerError from ex
        except fondat.error.Error as error:
            if isinstance(error, InternalServerError):
                _logger.error(
                    msg=error.__cause__.args[0], exc_info=error.__cause__, stacklevel=3
                )
            return await self.exception_handler(error)

    async def _handle(self, request: Request):
        response = Response()
        method = request.method.lower()
        segments = request.path.split("/")[1:]
        if segments == [""]:
            segments = []  # handle root "/" path
        resource = self.root
        operation = None
        print(f"{segments=}")
        for segment in segments:
            resource = await _resource(getattr(resource, segment, None))
            if not resource:
                raise NotFoundError
        print(f"found {resource=}")
        operation = getattr(resource, method, None)
        if not fondat.resource.is_operation(operation):
            raise MethodNotAllowedError
        signature = inspect.signature(operation)
        params = {}
        hints = get_type_hints(operation, include_extras=True)
        return_hint = hints.get("return", type(None))
        for name, hint in hints.items():
            if name == "return":
                continue
            param_codec = get_codec(hint)
            in_param = None
            if typing.get_origin(hint) is Annotated:
                args = typing.get_args(hint)
                hint = args[0]
                for ann in args[1:]:
                    if isinstance(ann, InParam):
                        in_param = ann
                        break
            if not in_param:
                in_param = InQuery(name)
            param = await in_param.get(param_codec, request)
            print(f"{param=}")
            if param is None:
                if signature.parameters[name].default is inspect.Parameter.empty:
                    raise BadRequestError(f"expecting value in {in_param}")
            else:
                try:
                    validate(param, hint)
                except (TypeError, ValueError) as tve:
                    raise BadRequestError(f"{tve} in {in_param}")
                params[name] = param
        response.body = await operation(**params)
        if not isinstance(response.body, Stream):
            return_codec = get_codec(return_hint)
            response.body = BytesStream(
                return_codec.bytes_encode(response.body), return_codec.content_type
            )
        response.headers["Content-Type"] = response.body.content_type
        if response.body.content_length is not None:
            if response.body.content_length == 0:
                response.status = http.HTTPStatus.NO_CONTENT.value
            else:
                response.headers["Content-Length"] = str(response.body.content_length)
        return response
