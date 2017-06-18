"""TODO: Description."""

# Copyright © 2015–2017 Paul Bryan.
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import logging
import re
import wrapt

from webob import Request, Response, exc
from roax.resource import ResourceError
from roax.schema import SchemaError

def _params(request, _id, schema):
    """TODO: Description."""
    result = {}
    for k, v in schema.fields.items():
        try:
            if k == "_id":
                if _id is None:
                    _id = request.params["_id"]
                result["_id"] = v.decode_param(_id)
            elif k == "_rev":
                _rev = request.headers.get("If-Match")
                if _rev:
                    match = re.fullmatch('(W/)?"(.*)"', _rev)
                    if match:
                        _rev = match.group(2)
                if _rev is None:
                    _rev = request.params["_rev"]
                result["_rev"] = v.decode_param(_rev)
            elif k == "_doc":
                if request.content_type == "application/json" and request.is_body_readable:
                    try:
                        _doc = request.json
                    except Exception as e:
                        raise SchemaError("Expecting JSON entity-body") from e
                    result["_doc"] = v.decode_json(_doc)
            else:
                result[k] = v.decode_param(request.params[k])
        except KeyError:
            pass
    return result

def _dispatch(request, resource, _id):
    """TODO: Description."""
    if request.method == "GET":
        kind, name = ("query", request.params["_query"]) if "_query" in request.params else ("read", None)
    elif request.method == "PUT":
        kind, name = ("update", None)
    elif request.method == "POST":
        kind, name = ("action", request.params["_action"]) if "_action" in request.params else ("create", None)
    elif request.method == "DELETE":
        kind, name = ("delete", None)
    else:
        raise exc.HTTPMethodNotAllowed()
    try:
        params_schema, returns_schema = resource.methods[(kind, name)]
    except KeyError:
        raise exc.HTTPBadRequest()
    result = resource.call(kind, name, _params(request, _id, params_schema))
    response = Response()
    if result is not None:
        response.content_type = "application/json"
        response.json = json_result = returns_schema.encode_json(result)
        try:
            response.etag = str(json_result["_rev"])
        except KeyError:
            pass # _rev is optional
    else:
        response.status_code = exc.HTTPNoContent.code
    if kind == "create":
        response.status_code = exc.HTTPCreated.code
        response.headers["Location"] = str(result["_id"])
    return response

class ErrorResponse(Response):

    def __init__(self, code, message):
        super().__init__()
        self.status_code = code
        self.content_type = "application/json"
        self.json = {"error": code, "message": message}

@wrapt.decorator
def _error_wrapped(wrapped, instance, args, kwargs):
    """TODO: Description."""
    try:
        return wrapped(*args, **kwargs)
    except exc.HTTPException as he:
        return ErrorResponse(he.code, he.detail)
    except ResourceError as re:
        return ErrorResponse(re.code, re.detail)
    except SchemaError as se:
        return ErrorResponse(exc.HTTPBadRequest.code, str(se))
    except Exception as e:
        logging.exception(str(e))
        return ErrorResponse(exc.HTTPInternalServerError.code, str(e))

class App:
    """TODO: Description."""

    def __init__(self, base_path):
        """TODO: Description."""
        self.base_path = base_path
        self.resources = {}

    def resource(self, name, resource):
        """TODO: Description."""
        if name in self.resources:
            raise ValueError("resource already registered: {}".format(name))
        self.resources[name] = resource

    @_error_wrapped
    def _handle(self, request):
        """TODO: Description."""
        path = request.path_info
        if path.startswith(self.base_path):
            split = path[len(self.base_path):].split("/", 1)
            resource = self.resources.get(split[0])
            if resource is not None:
                return _dispatch(request, resource, split[1] if len(split) > 1 else None)
        raise exc.HTTPNotFound()

    def __call__(self, environ, start_response):
        """TODO: Description."""
        return self._handle(Request(environ))(environ, start_response)
