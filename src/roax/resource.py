"""Module to implement resources."""

# Copyright © 2015–2018 Paul Bryan.
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import roax.schema as s
#import roax.security
import wrapt

from collections import namedtuple
from roax.context import context

_Operation = namedtuple("_Operation", "function, type, name, params, returns, security, deprecated")


class Resource:
    """Base class for a resource."""

    def _register_operation(self, function, type=None, name=None, params=None, returns=None, security=None, deprecated=False):
        """Register a resource operation."""
        if self.operations.get((type, name)):
            raise ResourceError("operation already registered: {}".format((type, name)))
        self.operations[(type, name)] = _Operation(function, type, name, params, returns, security, deprecated)

    def __init__(self):
        """Initialize the resource."""
        self.operations = {}
        for function in (attr for attr in (getattr(self, name) for name in dir(self)) if callable(attr)):
            try:
                op = function.roax_operation
            except:
                continue  # ignore undecorated functions
            self._register_operation(function, op.type, op.name, op.params, op.returns, op.security)

    def call(self, type, name=None, params={}):
        """Call a resource operation."""
        try:
            function = self.operations[(type, name)].function
        except KeyError as e:
            raise ResourceError("resource does not support operation", 400)
        return function(**params)


def operation(*, type=None, name=None, params=None, returns=None, security=None, deprecated=False):
    """
    Decorate a function to register it as a resource operation.

    type: The type of operation being registered.
    name: The name, if the operation is "query" or "action".
    summary: A short summary of what the operation does.
    params: The schema of operation parameters.
    returns: The schema of operation return value.
    security: TODO.
    deprecated: Declares the operator as deprecated.
    """
    def decorator(function):
        _type = type
        _name = name
        split = function.__name__.split("_", 1)
        if _type is None:
            _type = split[0]
        if len(split) > 1:
            _name = _name or split[1]
        def wrapper(wrapped, instance, args, kwargs):
            with context({"type": "operation", "op_type": _type, "name": _name}):
                #roax.security.apply(security)
                return wrapped(*args, **kwargs)
        decorated = s.validate(params, returns)(wrapt.decorator(wrapper)(function))
        try:
            getattr(function, "__self__")._register_operation(decorated, _type, _name, params, returns, security, deprecated)
        except AttributeError:  # not bound to an instance
            function.roax_operation = _Operation(None, _type, _name, params, returns, security, deprecated)  # __init__ will register
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


class Unauthorized(ResourceError):
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
