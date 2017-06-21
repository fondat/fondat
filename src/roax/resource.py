"""Module to implement resources."""

# Copyright © 2015–2017 Paul Bryan.
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import roax.schema

from collections import namedtuple

_Method = namedtuple("_Method", "function, kind, name, params, returns" )

class ResourceSet:
    """Base class for a set of like resources."""

    def _register_method(self, function, kind=None, name=None, params=None, returns=None):
        """Register a resource method."""
        if kind is None:
            splt = function.__name__.split("_", 1)
            kind = splt[0]
            if len(splt) > 1:
                name = splt[1]
        if self.methods.get((kind, name)):
            raise ResourceError("function already registered: {}".format((kind, name)))
        self.methods[(kind, name)] = _Method(function, kind, name, params, returns)

    def __init__(self):
        """TODO: Description.
        
        A class or instance variable named "schema" must be defined for the resources class.
        """
        self.methods = {}
        for function in [attr for attr in [getattr(self, name) for name in dir(self)] if callable(attr)]:
            try:
                method = function.roax_method
            except:
                continue # ignore undecorated methods
            self._register_method(function, method.kind, method.name, method.params, method.returns)

    def call(self, kind, name=None, params={}):
        """Call a resource method."""
        try:
            function = self.methods[(kind, name)].function
        except KeyError as e:
            raise ResourceError("resource does not provide method", 400)
        return function(**params)

def method(*, kind=None, name=None, params=None, returns=None):
    """Decorate a function to register it as a resource set method."""
    def decorator(function):
        decorated = roax.schema.validate(params, returns)(function) # validate params and returns
        try:
            getattr(function, "__self__")._register_method(decorated, kind, name, params, returns)
        except AttributeError: # not bound to an instance
            function.roax_method = _Method(None, kind, name, params, returns) # __init__ will register
        return decorated
    return decorator

class ResourceError(Exception):
    """Base class for all resource errors."""
    def __init__(self, detail, code):
        """
        detail -- textual description of the error.
        code -- the HTTP status most closely associated with the error.
        """
        super().__init__(self, detail)
        self.detail = detail
        self.code = code

class BadRequest(ResourceError):
    """Raised if the request contains malformed syntax."""
    def __init__(self, detail):
        super().__init__(detail, 400)

class NotFound(ResourceError):
    """Raised if the resource could not be found."""
    def __init__(self, detail):
        super().__init__(detail, 404)

class PreconditionFailed(ResourceError):
    """Raised if the revision provided does not match the current resource."""
    def __init__(self, detail):
        super().__init__(detail, 412)

class InternalServerError(ResourceError):
    """Raised if the server encountered an unexpected condition."""
    def __init__(self, detail):
        super().__init__(detail, 500)
