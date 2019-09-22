"""Module to expose resources through a WSGI interface."""

import base64
import binascii
import collections.abc
import contextlib
import copy
import logging
import mimetypes
import pathlib
import re
import roax.context
import roax.schema
import roax.static
import roax.security
import urllib.parse
import webob
import webob.exc


class _ErrorResponse(webob.Response):
    def __init__(self, code, message):
        super().__init__()
        self.status_code = code
        self.content_type = "application/json"
        self.json = {"error": code, "message": message}


class _App_Iter:
    def __init__(self, reader, chunk_size=4096):
        self.reader = reader
        self.chunk_size = chunk_size

    def __iter__(self):
        return self

    def __next__(self):
        buf = self.reader.read(self.chunk_size)
        if not buf:
            self.reader.close()
            raise StopIteration
        return buf


def _response(operation, result):
    returns = operation.returns
    response = webob.Response()
    if returns and result is not None:
        response.content_type = returns.content_type
        if isinstance(returns, roax.schema.reader):
            response.app_iter = _App_Iter(result)
        else:
            response.body = returns.bin_encode(result)
    else:
        response.status_code = webob.exc.HTTPNoContent.code
        response.content_type = None
    return response


def _params(request, operation):
    result = {}
    params = operation.params
    for name, param in params.props.items() if params else []:
        try:
            if name == "_body":
                if "_body" in params.required and not request.is_body_readable:
                    raise roax.schema.SchemaError("missing request entity-body")
                if isinstance(param, roax.schema.reader):
                    result["_body"] = request.body_file_raw
                else:
                    result["_body"] = param.bin_decode(request.body)
            else:
                result[name] = param.str_decode(request.params[name])
        except KeyError:
            pass
    return result


def _security_filters(requirements):
    """Produce set of filters associated with the security requirements."""
    result = set()
    for requirement in requirements or []:
        scheme = getattr(requirement, "scheme", None)
        if scheme:
            filter = getattr(scheme, "filter", None)
            if filter:
                result.add(filter)
    return result


_environ_keys = {
    "REQUEST_METHOD",
    "SCRIPT_NAME",
    "PATH_INFO",
    "QUERY_STRING",
    "CONTENT_TYPE",
    "CONTENT_LENGTH",
    "SERVER_NAME",
    "SERVER_PORT",
    "SERVER_PROTOCOL",
    "wsgi.version",
    "wsgi.multithread",
    "wsgi.multiprocess",
    "wsgi.run_once",
}


def _to_set(value):
    """Coerce an iterable or a single value into a set of values."""
    return set(
        filter(None, value if isinstance(value, collections.abc.Iterable) else [value])
    )


class App:
    """
    Roax WSGI application.

    Parameters:
    • url: URL to access the application.
    • title: Title of the application.
    • version: API implementation version.
    • description: Short description of the application.
    • filters: List of filters to apply during HTTP request processing.
    • authenticate: Default value of WWW-Authenticate header in Unauthorized responses.
    """

    def __init__(
        self, url, title, version, description=None, filters=None, authenticate=None
    ):
        self.url = url
        self.title = title
        self.version = version
        self.description = description
        self.filters = filters or []
        self.base = urllib.parse.urlparse(self.url).path.rstrip("/")
        self.operations = {}
        self.authenticate = authenticate

    def register_resource(self, path, resource, publish=True):
        """
        Register a resource to the application.

        Parameters:
        • path: Path of resource relative to application URL.
        • resource: Resource to be served when path is requested.
        • publish: Publish resource in online documentation.
        """
        res_path = self.base + "/" + path.lstrip("/")
        for op in resource.operations.values():
            if op.security is None:
                raise ValueError(
                    f"{op.name} operation must include security requirements"
                )
            if op.type == "create":
                op_method, op_path = "POST", res_path
            elif op.type == "read":
                op_method, op_path = "GET", res_path
            elif op.type == "update":
                op_method, op_path = "PUT", res_path
            elif op.type == "delete":
                op_method, op_path = "DELETE", res_path
            elif op.type == "action":
                op_method, op_path = "POST", res_path + "/" + op.name
            elif op.type == "query":
                op_method, op_path = "GET", res_path + "/" + op.name
            elif op.type == "patch":
                op_method, op_path = "PATCH", res_path
            else:
                raise ValueError(
                    f"operation {op.name} has unknown operation type: {op.type}"
                )
            if not publish:
                op = copy.copy(op)
                op.publish = False
            if (op_method, op_path) in self.operations:
                raise ValueError(f"operation already defined for {op_method} {op_path}")
            self.operations[(op_method, op_path)] = op

    def register_static(
        self, path, file_dir, security, index="index.html", publish=False
    ):
        """
        Register a file or directory as static resource(s). Each file will be
        registered as an individual resource in the application. If registering a
        directory, this method will not traverse or register any subdirectories.

        If registering a single file, the path parameter is the path that the
        file will be registered under. For example, "/" would resolve to the
        root of the application's path.

        If registering a directory, the path parameter is the path that will
        "contain" all files in the directory. If a file name matching the
        index argument is found in the directory, it will also be registered
        as the path itself.

        Every file is registered as a static resource; all content will be held
        in memory as byte arrays. This method is apprpriate for a limited
        number of files; for a large set of files, it would likely be more
        appropriate to serve them using a dedicated web server.

        Parameters:
        • path: Path of resource relative to application URL.
        • file_dir: Filesystem path of file or directory to register.
        • security: List of security requirements to apply to resource(s).
        • index: Name of file in directory to make path root resource.  ["index.html"]
        • publish: Publish resource(s) in online documentation.  [False]
        """

        def _static(fs_path):
            content = fs_path.read_bytes()
            content_type, encoding = mimetypes.guess_type(fs_path.name)
            if content_type is None or encoding is not None:
                content_type = "application/octet-stream"
            schema = roax.schema.bytes(format="binary", content_type=content_type)
            return roax.static.StaticResource(
                content, schema, fs_path.name, "Static resource.", security
            )

        path = path.rstrip("/")
        fs_path = pathlib.Path(file_dir).expanduser()
        if fs_path.is_file():
            self.register_resource(path, _static(fs_path))
        elif fs_path.is_dir():
            for child in fs_path.iterdir():
                if child.is_file():
                    resource = _static(child)
                    self.register_resource(f"{path}/{child.name}", resource, publish)
                    if child.name == index:
                        self.register_resource(f"{path}/", resource, publish)
        else:
            raise ValueError(f"invalid file or directory: {file_dir}")

    def __call__(self, environ, start_response):
        """Handle WSGI request."""
        request = webob.Request(environ)
        try:
            with roax.context.push(
                context="wsgi",
                environ={
                    k: v
                    for k, v in environ.items()
                    if k in _environ_keys or k.startswith("HTTP_")
                },
            ):
                operation = self._get_operation(request)

                def terminus(request):
                    try:
                        params = _params(request, operation)
                    except Exception:  # authorization trumps input validation
                        roax.resource.authorize(operation.security)
                        raise
                    return _response(operation, operation.call(**params))

                filters = self.filters.copy()
                filters.extend(_security_filters(operation.security))
                response = _Chain(filters, terminus).next(request)
        except webob.exc.HTTPException as he:
            response = _ErrorResponse(he.code, he.detail)
        except roax.resource.Unauthorized as u:
            response = _ErrorResponse(u.code, u.detail)
            if not response.www_authenticate:  # could have been set by filter
                response.www_authenticate = self.authenticate or "Authenticate"
        except roax.resource.ResourceError as re:
            response = _ErrorResponse(re.code, re.detail)
        except roax.schema.SchemaError as se:
            response = _ErrorResponse(webob.exc.HTTPBadRequest.code, str(se))
        except Exception as e:
            logging.exception(str(e))
            response = _ErrorResponse(webob.exc.HTTPInternalServerError.code, str(e))
        return response(environ, start_response)

    def _get_operation(self, request):
        operation = self.operations.get((request.method, request.path_info))
        if operation:
            return operation
        for _, op_path in self.operations:
            if op_path == request.path_info:  # path is defined, but not method
                raise webob.exc.MethodNotAllowed
        raise webob.exc.HTTPNotFound("resource or operation not found")


class _Chain:
    """A chain of request filters, terminated by a terminus request handler."""

    def __init__(self, filters=[], terminus=None):
        """Initialize a filter chain."""
        self.filters = list(filters)
        self.terminus = terminus

    def next(self, request):
        """Calls the next filter in the chain, or the terminus."""
        return (
            self.filters.pop(0)(request, self)
            if self.filters
            else self.terminus(request)
        )


class HTTPSecurityScheme(roax.security.SecurityScheme):
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

    def filter(self, request, chain):
        """
        Filters the incoming HTTP request. If the request contains credentials in the
        HTTP Basic authentication scheme, they are passed to the authenticate method.
        If authentication is successful, a context is added to the context stack.  
        """
        auth = None
        if request.authorization and request.authorization[0].lower() == "basic":
            try:
                user_id, password = (
                    base64.b64decode(request.authorization[1]).decode().split(":", 1)
                )
            except (binascii.Error, UnicodeDecodeError):
                pass
            else:
                auth = self.authenticate(user_id, password)
        with roax.context.push(auth) if auth else contextlib.nullcontext():
            return chain.next(request)

    def authenticate(user_id, password):
        """
        Perform authentication of credentials supplied in the HTTP request. If
        authentication is successful, a context is returned to be pushed on
        the context stack. If authentication fails, None is returned. This
        method should not raise an exception unless an unrecoverable error
        occurs.
        """
        raise NotImplementedError


class APIKeySecurityScheme(roax.security.SecurityScheme):
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

    def authenticate(value):
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
        super().__init__(name, key=header, location="header", **kwargs)

    def filter(self, request, chain):
        """
        Filters the incoming HTTP request. If the request contains the header,
        it is passed to the authenticate method. If authentication is
        successful, a context is added to the context stack.
        """
        header = request.headers.get(self.key)
        auth = self.authenticate(header) if header is not None else None
        with roax.context.push(auth) if auth else contextlib.nullcontext():
            return chain.next(request)


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
        super().__init__(name, key=cookie, location="cookie", **kwargs)

    def filter(self, request, chain):
        """
        Filters the incoming HTTP request. If the request contains the cookie,
        it is passed to the authenticate method. If authentication is
        successful, a context is added to the context stack.
        """
        cookie = request.cookies.get(self.key)
        auth = self.authenticate(cookie) if cookie is not None else None
        with roax.context.push(auth) if auth else contextlib.nullcontext():
            return chain.next(request)
