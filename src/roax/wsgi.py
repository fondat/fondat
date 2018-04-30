"""Module to expose resources through a WSGI interface."""

# Copyright © 2015–2018 Paul Bryan.
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import logging
import re

from abc import ABC, abstractmethod
from roax.context import context
from roax.resource import ResourceError
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
    returns = operation["returns"]
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
    params = operation["params"]
    for name, param in params.items() if params else []:
        try:
            if name == "_body":
                if param.required and not request.is_body_readable:
                    raise SchemaException("missing request entity-body")
                if param.json_type == "string" and param.format == "binary":
                    result["_body"] = param.bin_decode(request.body)
                elif param.json_type == "string" and param.format == "raw":
                    result["_body"] = param.str_decode(request.text)
                else:
                    try:
                        result["_body"] = request.json
                    except:
                        raise SchemaError("invalid entity-body JSON representation")
            else:
                result[name] = param.str_decode(request.params[name])
        except KeyError:
            pass
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

    def __init__(self, url=None, *, title=None, description=None, version=None, security=None):
        """TODO: Description."""
        self.url = url
        self.base = urlparse(self.url).path.rstrip("/")
        self.title = title
        self.description = description
        self.version = version
        self.security = security or []
        self.resources = []

    def register(self, path, resource, public=True):
        """Registers a resource to the application."""
        self.resources.append(dict(path=path, resource=resource, public=public))

    def __call__(self, environ, start_response):
        """TODO: Description."""
        request = Request(environ)
        try:
            operation = self._operation(request)
            # security = self.security + operation["security"]
            filters = []
            #for s in security:
            #    if instanceof(s, Filter):
            #        filters += s
            def handle(request):
                return _response(operation, operation["function"](**_params(request, operation)))
            with context(context_type="http", http_environ=_environ(environ)):
                response = Chain(filters, handle).next(request)
        except exc.HTTPException as he:
            response = ErrorResponse(he.code, he.detail)
        except ResourceError as re:
            response = ErrorResponse(re.code, re.detail)
        except SchemaError as se:
            response = ErrorResponse(exc.HTTPBadRequest.code, str(se))
        except Exception as e:
            logging.exception(str(e))
            response = ErrorResponse(exc.HTTPInternalServerError.code, str(e))
        return response(environ, start_response)

    def _operation(self, request):
        path = request.path_info
        base = self.base + "/"
        if path != base and not path.startswith(base):
            raise exc.HTTPNotFound()
        path = path[len(self.base):]
        for resource in self.resources:
            if path == resource["path"] or path.startswith(resource["path"] + "/"):
                split = path[1:].split("/", 1)
                if request.method == "GET":
                    op_type, op_name = ("query", split[1]) if len(split) > 1 else ("read", "read")
                elif request.method == "PUT":
                    op_type, op_name = ("update", "update")
                elif request.method == "POST":
                    op_type, op_name = ("action", split[1]) if len(split) > 1 else ("create", "create")
                elif request.method == "DELETE":
                    op_type, op_name = ("delete", "delete")
                else:
                    raise exc.HTTPMethodNotAllowed()
                operation = resource["resource"].operations.get(op_name)
                if not operation or operation["type"] != op_type:
                    if op_type in ["query", "action"]:
                        raise exc.HTTPNotFound()
                    else:
                        raise exc.HTTPMethodNotAllowed()
                return operation
        raise exc.HTTPNotFound()


class Chain:
    """A chain of filters, terminated by a handler."""

    def __init__(self, filters=[], handler=None):
        self.filters = filters
        self.handler = handle

    def next(self, request):
        """Calls the next filter in the chain, or the terminus."""
        if self.filters:
            return self.filters.pop(0)(request, self)
        else:
            return self.handler(request)
