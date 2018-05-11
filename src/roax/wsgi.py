"""Module to expose resources through a WSGI interface."""

# Copyright © 2015–2018 Paul Bryan.
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this

import logging
import re
import roax.context as context
import roax.resource as resource

from copy import copy
from roax.schema import SchemaError
from urllib.parse import urlparse
from webob import Request, Response, exc


class _ErrorResponse(Response):

    def __init__(self, code, message):
        super().__init__()
        self.status_code = code
        self.content_type = "application/json"
        self.json = {"error": code, "message": message}


def _response(operation, result):
    returns = operation.returns
    response = Response()
    if returns and result is not None:
        response.content_type = returns.content_type
        if returns.json_type == "string" and returns.format == "binary":
            response.body = result
        elif returns.json_type == "string" and returns.format == "raw":
            response.text = result
        else:
            response.json = returns.json_encode(result)
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
                if param.json_type == "string" and param.format == "binary":
                    result["_body"] = param.bin_decode(request.body)
                elif param.json_type == "string" and param.format == "raw":
                    result["_body"] = param.str_decode(request.text)
                else:
                    try:
                        json = request.json
                    except:
                        raise SchemaError("invalid JSON in entity-body")
                    result["_body"] = param.json_decode(json)
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
        resource_path = self.base + "/" + path.lstrip("/").rstrip("/")
        for op in resource.operations.values():
            if op.security is None:
                raise ValueError("operation {} must define security requirements".format(op.name))
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
            else:
                raise ValueError("operation {} has unknown operation type: {}".format(op.name, op.type))
            if not publish:
                op = copy(op)
                op.publish = False
            if (op_method, op_path) in self.operations:
                raise ValueError("operation already defined for {} {}".format(op_method, op_path))
            self.operations[(op_method, op_path)] = op

    def __call__(self, environ, start_response):
        """Handle WSGI request."""
        request = Request(environ)
        try:
            with context.root(context_type="http", http_environ=_environ(environ)):
                operation = self._get_operation(request)
                filters = _filters(operation.security)
                try:
                    params = _params(request, operation)
                except Exception as e:  # authorization trumps input validation
                    resource.authorize(operation.security)
                def handle(request):
                    return _response(operation, operation.function(**params))
                response = Chain(filters, handle).next(request)
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
        operation = self.operations.get((request.method, request.path_info))
        if operation:
            return operation
        for _, op_path in self.operations:
            if op_path == request.path_info:  # path is defined, but not method
                raise exc.MethodNotAllowed()
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
