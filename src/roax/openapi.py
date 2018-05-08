"""Module to expose OpenAPI document describing Roax application."""

# Copyright © 2018 Paul Bryan.
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at

import roax.schema as s

from copy import copy
from roax.resource import Resource, operation


_schema = s.dict(
    properties = {},
    additional_properties = True,
)


def _body_schema(schema):
    return 


class OpenAPIResource(Resource):
    """
    A resource that serves an OpenAPI document for an application. The document is
    generated on the first read request, and is cached for further read requests.
    """

    def __init__(self, app, name=None, description=None, security=None):
        """
        Initialize the OpenAPI resource.

        :param name: The short name of the resource.
        :param description: A short description of the resource.
        :param security: The security requirements to read the resource.
        """
        super().__init__()
        self.app = app
        self.read = operation(params={}, returns=_schema, security=security)(self.read)
        self.content = None

    def read(self):
        """Read the OpenAPI document."""
        if not self.content:
            self.content = self._openapi()
        return self.content

    def _openapi(self):
        result = {}
        result["openapi"] = "3.0.1"
        result["info"] = self._info()
        result["paths"] = self._paths()
        result["security"] = []
        result["tags"] = self._tags()
        return result

    def _info(self):
        result = {}
        result["title"] = self.app.title or self.app.__class__.name
        if self.app.description is not None:
            result["description"] = self.app.description
        result["version"] = str(self.app.version)
        return result

    def _paths(self):
        result = {}
        for (op_method, op_path), operation in self.app.operations.items():
            if not operation.publish:
                continue
            path_item = result.get(op_path)
            if not path_item:
                path_item = {}
                result[op_path] = path_item
            obj = {}
            obj["tags"] = [operation.resource.name]
            if operation.summary:
                obj["summary"] = operation.summary
            if operation.description:
                obj["description"] = operation.description
            obj["operationId"] = operation.resource.name + "." + operation.name
            params = []
            for name, param in operation.params.items():
                if name == "_body":
                    continue
                p = {}
                p["name"] = name
                p["in"] = "query"
                if param.description:
                    p["description"] = param.description
                p["required"] = param.required
                p["deprecated"] = param.deprecated
                p["allowEmptyValue"] = True
                p["schema"] = param.json_schema
                params.append(p)
            obj["parameters"] = params
            _body = operation.params.get("_body")
            if _body:
                b = {}
                if _body.description:
                    b["description"] = _body.description
                    b["content"] = {_body.content_type: {"schema": _body.json_schema}}
                    b["required"] = _body.required
                    obj["requestBody"] = b
            if operation.returns:
                obj["responses"] = {
                    "200": {
                        "description": "OK",
                        "content": {operation.returns.content_type: {"schema": operation.returns.json_schema}
                    },
                }}
            else:
                obj["responses"] = {
                    "204": {
                        "description": "No content",
                    }
                }
            obj["security"] = []
            path_item[op_method.lower()] = obj
        return result

    def _tags(self):
        resources = {}
        for operation in self.app.operations.values():
            resources[operation.resource.name] = operation.resource
        result = []
        for name, resource in resources.items():
            result.append({"name": name, "description": resource.description})
        return result
