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
        name = operation["name"]
        if self.operations.get(name):
            raise ValueError("operation already registered: {}".format(name))
        function = operation.get("function")
        self.operations[name] = operation

    def __init__(self, name=None, description=None):
        """
        Initialize the resource.

        name: The short name of the resource. Default: the class name in lower case.
        description: A short description of the resource. Default: the resource docstring.
        """
        super().__init__()
        self.name = name or getattr(self, "name", type(self).__name__.lower())
        self.description = description or getattr(self, "description", None) or self.__doc__ or self.__class__.__name__
        self.operations = {}
        for function in (attr for attr in (getattr(self, nom) for nom in dir(self)) if callable(attr)):
            try:
                operation = function._roax_operation
            except:
                continue  # ignore undecorated functions
            self._register_operation(**{**operation, "function": function})

    def call(self, name, params={}):
        """Call a resource operation."""
        try:
            return self.operations[name]["function"](**params)
        except KeyError as e:
            raise BadRequest("no such operation: {}".format(name))


def _summary(function):
    """
    Derive summary information from a function's docstring or name. The summary is
    the first sentence of the docstring, ending in a period, or if no dostring is
    present, the function's name capitalized.
    """
    if not function.__doc__:
        return "{}.".format(function.__name__.capitalize())
    result = []
    for word in function.__doc__.split():
        result.append(word)
        if word.endswith("."):
            break
    return " ".join(result)


def operation(
        *, name=None, type=None, summary=None, description=None, params=None,
        returns=None, security=[], documented=True, deprecated=False):
    """
    Decorate a function to register it as a resource operation.

    name: The operation name. Required if the operation type is "query" or "action".
    type: The type of operation being registered ("create", "read", "update", "delete", "action", "query").
    summary: A short summary of what the operation does.
    description: A verbose description of the operation (default: function docstring).
    params: A mapping of operation's parameter names to their schemas.
    returns: The schema of operation's return value.
    security: Security schemes, one of which must be satisfied to perform the operation.
    documented: Publishes the operation in documentation and help if True.
    deprecated: Declares the operation as deprecated if True.
    """
    def decorator(function):
        _name = name
        _type = type
        _description = description or function.__doc__ or function.__name__
        __summary = summary or _summary(function)
        if _name is None:
            _name = function.__name__
        if _type is None and _name in ["create", "read", "update", "delete"]:
            _type = _name
        valid_types = ["create", "read", "update", "delete", "query", "action"]
        if _type not in valid_types:
            raise TypeError("operation type must be one of: {}".format(valid_types))
        def wrapper(wrapped, instance, args, kwargs):
            with context(context_type="operation", operation_resource=wrapped.__self__, operation_name=_name):
                #roax.security.apply(security)
                return wrapped(*args, **kwargs)
        _params = s.function_params(function, params)
        decorated = s.validate(_params, returns)(wrapt.decorator(wrapper)(function))
        operation = dict(function=decorated, name=_name, type=_type,
            summary=__summary, description=_description, params=_params,
            returns=returns, security=security, documented=documented, deprecated=deprecated)
        try:
            getattr(function, "__self__")._register_operation(**operation)
        except AttributeError:  # not yet bound to an instance
            function._roax_operation = operation  # __init__ will register it
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
