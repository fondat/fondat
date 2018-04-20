"""Module to implement resources."""

# Copyright © 2015–2018 Paul Bryan.
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import roax.schema as s
#import roax.security
import wrapt

from roax.context import context


class Resource:
    """Base class for a resource."""

    def _register_operation(self, **operation):
        """Register a resource operation."""
        type = operation["type"]
        name = operation.get("name")
        if self.operations.get((type, name)):
            raise ResourceError("operation already registered: {}".format((type, name)))
        self.operations[(type, name)] = operation

    def __init__(self, name=None, description=None):
        """
        Initialize the resource.

        name: The short name of the resource. Default: the class name in lower case.
        description: A short description of the resource. Default: the resource docstring.
        """
        super().__init__()
        self.name = name or getattr(self, "name", type(self).__name__.lower())
        self.description = description or getattr(self, "description", self.__doc__)
        self.operations = {}
        for function in (attr for attr in (getattr(self, nom) for nom in dir(self)) if callable(attr)):
            try:
                operation = function._roax_operation
            except:
                continue  # ignore undecorated functions
            self._register_operation(**{**operation, "function": function})

    def call(self, type, name=None, params={}):
        """Call a resource operation."""
        try:
            function = self.operations[(type, name)]["function"]
        except KeyError as e:
            raise ResourceError("resource does not support operation", 400)
        return function(**params)


def operation(
        *, type=None, name=None, summary=None, description=None, params=None,
        returns=None, security=[], deprecated=False):
    """
    Decorate a function to register it as a resource operation.

    type: The type of operation being registered ("create", "read", "update", "delete", "action", "query").
    name: The operation name. Required if the operation type is "query" or "action".
    summary: A short summary of what the operation does.
    description: A verbose description of the operation (default: function docstring).
    params: A mapping of operation's parameter names to their schemas.
    returns: The schema of operation's return value.
    security: Security schemes, one of which must be satisfied to perform the operation.
    deprecated: If True, declares the operation as deprecated.
    """
    def decorator(function):
        _type = type
        _name = name
        split = function.__name__.split("_", 1)
        if _type is None:
            _type = split[0]
            if len(split) > 1:
                _name = _name or split[1]
        elif _name is None:
            _name = function.__name__
        valid_types = ["create", "read", "update", "delete", "query", "action"]
        if _type not in valid_types:
            raise TypeError("operation type must be one of: {}".format(valid_types))
        if not _name and _type in ["query", "action"]:
            raise TypeError("{} operation must have a name".format(_type))
        def wrapper(wrapped, instance, args, kwargs):
            with context(context_type="operation", operation_resource=wrapped.__self__, operation_type=_type, operation_name=_name):
                #roax.security.apply(security)
                return wrapped(*args, **kwargs)
        decorated = s.validate(params, returns)(wrapt.decorator(wrapper)(function))
        operation = dict(function=decorated, type=_type, name=_name,
            summary=summary or [], description=description, params=params,
            returns=returns, security=security, deprecated=deprecated)
        try:
            getattr(function, "__self__")._register_operation(**operation)
        except AttributeError:  # not yet bound to an instance
            function._roax_operation = operation  # __init__ will register it instead
        return decorated
    return decorator


class ResourceError(Exception):
    """Base class for all resource errors."""
    def __init__(self, detail, code):
        """
        detail: textual description of the error.
        code: the HTTP status most closely associated with the error.
        """
        super().__init__(self, detail)
        self.detail = detail
        self.code = code


class BadRequest(ResourceError):
    """Raised if the request is malformed."""
    def __init__(self, detail=None):
        super().__init__(detail, 400)


class Unauthenticated(ResourceError):
    """Raised if the resource request requires authentication."""
    def __init__(self, realm, detail=None):
        super().__init__(detail, 401)
        self.realm = realm        


class Forbidden(ResourceError):
    """Raised if the resource request is refused."""
    def __init__(self, detail=None):
        super().__init__(detail, 403)

        
class NotFound(ResourceError):
    """Raised if the resource could not be found."""
    def __init__(self, detail=None):
        super().__init__(detail, 404)


class OperationNotAllowed(ResourceError):
    """Raised if the resource does not allow the requested operation."""
    def __init__(self, detail=None):
        super().__init__(detail, 405)


class Conflict(ResourceError):
    """Raised if there is a conflict with the current state of the resource."""
    def __init__(self, detail=None):
        super().__init__(detail, 409)


class PreconditionFailed(ResourceError):
    """Raised if the revision provided does not match the current resource."""
    def __init__(self, detail=None):
        super().__init__(detail, 412)


class InternalServerError(ResourceError):
    """Raised if the server encountered an unexpected condition."""
    def __init__(self, detail=None):
        super().__init__(detail, 500)
