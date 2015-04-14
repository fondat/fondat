# Copyright © 2015 Paul Bryan.
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""TODO: Description."""

from webob import Request, Response
from webob.exc import HTTPException, HTTPBadRequest, HTTPNotFound
from roax.resource import Resource, ResourceError
import webob.exc

class App:
    """TODO: Description."""

    def __init__(self, base_path):
        """TODO: Description."""
        self.base_path = base_path
        self.resources = {}

    def resource(self, name, resource):
        """TODO: Description."""
        self.resources[name] = resource

    def _dispatch(self, resource, request, _id):
        """TODO: Description."""
        params = {}
        for (key, value) in request.params.items():
            params[key] = value
        if _id:
            params["_id"] = _id
        try:
            params["_body"] = request.json
        except:
            pass
        try:
            response = Response()
            if request.method == "GET":
                if "q" in params:
                    response.json = resource.call("query", name=params.pop("q"), params=params)
                else:
                    response.json =  resource.call("read", params=params)
            elif request.method == "PUT":
                response.json = resource.call("update", params=params)
            elif request.method == "POST":
                if "a" in params:
                    response.json = resource.call("action", name=params.pop("a"), params=params)
                else:
                    result = resource.call("create", params=params)
                    response.json = result
                    response.headers['Location'] = result["_id"]
                    response.status = 201 # FIX
            elif request.method == "DELETE":
                response.json = resource.call("delete", params=params)
            else:
                raise HTTPBadRequest()
        except ResourceError as re:
            raise webob.exc.status_map[re.code](detail=re.detail)
        return response

    def _handle(self, request):
        """TODO: Description."""
        response = Response()
        path = request.path_info
        if path.startswith(self.base_path):
            path = path[len(self.base_path):]
            split = path.split("/", 1)
            resource = self.resources.get(split[0])
            if resource is not None:
                _id = split[1] if len(split) > 1 else None
                return self._dispatch(resource, request, _id)
        raise HTTPNotFound()

    def __call__(self, environ, start_response):
        """TODO: Description."""
        try:
            return self._handle(Request(environ))(environ, start_response)
        except HTTPException as he:
            return he(environ, start_response)
