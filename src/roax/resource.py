"""Module to implement resources."""

# Copyright © 2015–2017 Paul Bryan.
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import roax.schema
import wrapt

from collections import namedtuple

class ResourceError(Exception):
    """TODO: Description."""

    def __init__(self, detail, code=400):
        """TODO: Description."""
        super().__init__(self, detail)
        self.detail = detail
        self.code = code

_Method = namedtuple("_Method", "function, kind, name, params, returns" )

class Resource:
    """Base class for resources."""

    def __init__(self):
        self.methods = {}
        for function in [attr for attr in [getattr(self, name) for name in dir(self)] if callable(attr)]:
            try:
                method = function.roax_method
            except:
                continue # ignore undecorated methods
            self.register_method(function, method.kind, method.name, method.params, method.returns)

    def call_method(self, kind, name=None, params={}):
        """Call a resource method."""
        try:
            function = self.methods[(kind, name)].function
        except KeyError as e:
            raise ResourceError("resource does not provide method", 400)
        return function(**params)

    def register_method(self, function, kind=None, name=None, params=None, returns=None):
        """Register a resource method."""
        if kind is None:
            kind = function.__name__
        if self.methods.get((kind, name)):
            raise ResourceError("function already registered: {}".format((kind, name)))
        self.methods[(kind, name)] = _Method(function, kind, name, params, returns)

def method(kind=None, name=None, params=None, returns=None):
    """Decorate a function to register it as a resource method."""
    def decorator(function):
        function.roax_method = _Method(None, kind if kind is not None else function.__name__, name, params, returns)
        def wrapper(wrapped, instance, args, kwargs):
            return roax.schema.call(wrapped, args, kwargs, params, returns)
        return wrapt.decorator(wrapper)(function)
    return decorator
