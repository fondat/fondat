"""Module to expose OpenAPI document describing a Roax application."""

import roax.resource
import roax.schema as s


_schema = s.dict(properties={}, additional=True)


def _body_schema(schema):
    return


def _security_requirements(operation):
    result = []
    for security in operation.security or []:
        if security:
            try:
                json = security.json
                if json:
                    result.append(json)
            except AttributeError:
                pass
    return result


class OpenAPIResource(roax.resource.Resource):
    """
    A resource that serves an OpenAPI document for an application. The document is
    generated on the first read request, and is cached for further read requests.

    Parameters:
    • name: The short name of the resource.
    • description: A short description of the resource.
    • security: The security requirements to read the resource.
    """

    def __init__(self, app, name=None, description=None, security=None):
        super().__init__()
        self.app = app
        self.read = roax.resource.operation(
            params={}, returns=_schema, security=security
        )(self.read)
        self.content = None

    def read(self):
        """Read the OpenAPI document."""
        if not self.content:
            self.content = self._openapi()
        return self.content

    def _openapi(self):
        result = {}
        result["openapi"] = "3.0.2"
        result["info"] = self._info()
        result["servers"] = self._servers()
        result["paths"] = self._paths()
        result["components"] = self._components()
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

    def _servers(self):
        return [{"url": self.app.url}]

    def _paths(self):
        result = {}
        for (op_method, op_path), operation in self.app.operations.items():
            if not operation.publish:
                continue
            base_len = len(self.app.base)
            op_path = op_path[base_len:]
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
            for name, param in operation.params.properties.items():
                if name != "_body":
                    p = {}
                    p["name"] = name
                    p["in"] = "query"
                    if param.description:
                        p["description"] = param.description
                    p["required"] = name in operation.params.required
                    p["deprecated"] = param.deprecated
                    p["allowEmptyValue"] = True
                    p["schema"] = param.json_schema
                    params.append(p)
            obj["parameters"] = params
            _body = operation.params.properties.get("_body")
            if _body:
                b = {}
                if _body.description:
                    b["description"] = _body.description
                b["content"] = {_body.content_type: {"schema": _body.json_schema}}
                p["required"] = "_body" in operation.params.required
                obj["requestBody"] = b
            if operation.returns:
                obj["responses"] = {
                    "200": {
                        "description": "OK",
                        "content": {
                            operation.returns.content_type: {
                                "schema": operation.returns.json_schema
                            }
                        },
                    }
                }
            else:
                obj["responses"] = {"204": {"description": "No content"}}
            obj["security"] = _security_requirements(operation)
            path_item[op_method.lower()] = obj
        return result

    def _components(self):
        result = {}
        result["securitySchemes"] = self._security_schemes()
        return result

    def _security_schemes(self):
        result = {}
        for operation in self.app.operations.values():
            for requirement in (req for req in operation.security or [] if req):
                try:
                    result[requirement.scheme.name] = requirement.scheme.json
                except AttributeError:  # no scheme, no problem
                    pass
        return result

    def _tags(self):
        resources = {}
        for operation in (o for o in self.app.operations.values() if o.publish):
            resources[operation.resource.name] = operation.resource
        result = []
        for name, resource in resources.items():
            result.append({"name": name, "description": resource.description})
        return result
