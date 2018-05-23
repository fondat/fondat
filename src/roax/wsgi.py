"""Module to expose resources through a WSGI interface."""

# Copyright © 2015–2018 Paul Bryan.
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this

import logging
import re
import roax.resource as resource
import roax.context as context
import roax.schema as s

from copy import copy
from mimetypes import guess_type
from os.path import isdir, isfile
from pathlib import Path
from roax.schema import SchemaError
from roax.static import StaticResource
from urllib.parse import urlparse
from webob import Request, Response, exc


class _ErrorResponse(Response):
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
    response = Response()
    if returns and result is not None:
        response.content_type = returns.content_type
        if isinstance(returns, s.reader):
            response.app_iter = _App_Iter(result)
        else:
            response.body = returns.bin_encode(result)
    else:
        response.status_code = exc.HTTPNoContent.code
        response.content_type = None
    return response

def _params(request, operation):
    result = {}
    params = operation.params
    for name, param in params.items() if params else []:
        try:
            if name == "_body":
                if param.required and not request.is_body_readable:
                    raise SchemaError("missing request entity-body")
                if isinstance(param, s.reader):
                    result["_body"] = request.body_file_raw
                else:
                    result["_body"] = param.bin_decode(request.body)
            else:
                result[name] = param.str_decode(request.params[name])
        except KeyError:
            pass
    return result

def _filters(requirements):
    """Produce set of filters associated with the security requirements."""
    result = set()
    for requirement in requirements or []:
        scheme = getattr(requirement, "scheme", None)
        if scheme:
            filter = getattr(scheme, "filter", None)
            if filter:
                result.add(filter)
    return result

_context_patterns = [re.compile(p) for p in (
    "^REQUEST_METHOD$", "^SCRIPT_NAME$", "^PATH_INFO$", "^QUERY_STRING$",
    "^CONTENT_TYPE$", "^CONTENT_LENGTH$", "^SERVER_NAME$", "^SERVER_PORT$",
    "^SERVER_PROTOCOL$", "^HTTP_.*", "^wsgi.version$", "^wsgi.multithread$",
    "^wsgi.multiprocess$", "^wsgi.run_once$")]

def _environ(environ):
    result = {}
    for key, value in environ.items():
        for pattern in _context_patterns:
            if pattern.match(key):
                result[key] = value
    return result


class App:
    """Roax WSGI application."""

    def __init__(self, url, title, version, description=None):
        """
        Initialize WSGI application.
        
        :param url: The URL to access the application.
        :param title: The title of the application.
        :param version: The API implementation version.
        :param description: A short description of the application.
        """
        self.url = url
        self.base = urlparse(self.url).path.rstrip("/")
        self.title = title
        self.description = description
        self.version = version
        self.operations = {}

    def register(self, path, resource, publish=True):
        """
        Register a resource to the application.

        :param path: Path of resource relative to application URL.
        :param resource: Resource to be served when path is requested.
        :param publish: Publish resource in online documentation.
        """
        resource_path = (self.base + "/" + path.lstrip("/")).rstrip("/")
        for op in resource.operations.values():
            if op.security is None:
                raise ValueError("operation {} must express security requirements".format(op.name))
            if op.type == "create":
                op_method, op_path = "POST", resource_path
            elif op.type == "read":
                op_method, op_path = "GET", resource_path
            elif op.type == "update":
                op_method, op_path = "PUT", resource_path
            elif op.type == "delete":
                op_method, op_path = "DELETE", resource_path
            elif op.type == "action":
                op_method, op_path = "POST", resource_path + "/" + op.name
            elif op.type == "query":
                op_method, op_path = "GET", resource_path + "/" + op.name
            elif op.type == "patch":
                op_method, op_path = "PATCH", resource_path
            else:
                raise ValueError("operation {} has unknown operation type: {}".format(op.name, op.type))
            if not publish:
                op = copy(op)
                op.publish = False
            if (op_method, op_path) in self.operations:
                raise ValueError("operation already defined for {} {}".format(op_method, op_path))
            self.operations[(op_method, op_path)] = op

    def register_static(self, path, file_dir, security, index="index.html", publish=False):
        """
        Register a file or directory as static resource(s). Each file will be
        registered as an individual resource in the application. If registering a
        directory, this method will not traverse or register any subdirectories.

        If registering a single file, the path parameter is the path that the file will
        be registered under. For example, "/" would resolve to the root of the
        application's path.

        If registering a directory, the path parameter is the path that will "contain"
        all files in the directory. If a file name matching the index argument is
        found in the directory, it will also be registered as the path itself.

        Every file is registered as a static resource; all content will be held in
        memory as byte arrays. This method is apprpriate for a handful of files; for a
        large set of files, it would likely be more appropriate to serve them using a
        dedicated web server.

        :param path: Path of resource relative to application URL.
        :param file_dir: Filesystem path of file or directory to register.
        :security: List of security requirements to apply to resource(s).
        :index: Name of file in directory to make path root resource. (default: index.html)
        :param publish: Publish resource(s) in online documentation.
        """
        def _static(fs_path):
            content = fs_path.read_bytes()
            content_type, encoding = guess_type(fs_path.name)
            if content_type is None or encoding is not None:
                content_type = "application/octet-stream"
            schema = s.bytes(format="binary", content_type=content_type)
            return StaticResource(content, schema, fs_path.name, "Static resource.", security)
        path = path.rstrip("/")
        fs_path = Path(file_dir).expanduser()
        if fs_path.is_file():
            self.register(path, _static(fs_path))
        elif fs_path.is_dir():
            for child in fs_path.iterdir():
                if child.is_file():
                    resource = _static(child)
                    self.register("{}/{}".format(path, child.name), resource, publish)
                    if child.name == index:
                        self.register(path, resource, publish)
        else:
            raise ValueError("invalid file or directory: {}".format(file_dir))

    def __call__(self, environ, start_response):
        """Handle WSGI request."""
        request = Request(environ)
        try:
            with context.push(context_type="http", http_environ=_environ(environ)):
                operation = self._get_operation(request)
                def handle(request):
                    try:
                        params = _params(request, operation)
                    except Exception:  # authorization trumps input validation
                        resource.authorize(operation.security)
                        raise
                    return _response(operation, operation.function(**params))
                response = Chain(_filters(operation.security), handle).next(request)
        except exc.HTTPException as he:
            response = _ErrorResponse(he.code, he.detail)
        except resource.ResourceError as re:
            response = _ErrorResponse(re.code, re.detail)
        except SchemaError as se:
            response = _ErrorResponse(exc.HTTPBadRequest.code, str(se))
        except Exception as e:
            logging.exception(str(e))
            response = _ErrorResponse(exc.HTTPInternalServerError.code, str(e))
        return response(environ, start_response)

    def _get_operation(self, request):
        operation = self.operations.get((request.method, request.path_info.rstrip("/")))
        if operation:
            return operation
        for _, op_path in self.operations:
            if op_path == request.path_info:  # path is defined, but not method
                raise exc.MethodNotAllowed
        raise exc.HTTPNotFound("resource or operation not found")


class Chain:
    """A chain of filters, terminated by a handler."""

    def __init__(self, filters=[], terminus=None):
        """Initialize a filter chain."""
        self.filters = list(filters)
        self.terminus = terminus

    def next(self, request):
        """Calls the next filter in the chain, or the terminus."""
        return self.filters.pop(0)(request, self) if self.filters else self.terminus(request)
