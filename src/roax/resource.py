# Copyright © 2015–2017 Paul Bryan.
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import roax.schema
import wrapt

from collections import namedtuple

"""TODO: Description."""

class ResourceError(Exception):
    """TODO: Description."""

    def __init__(self, detail, code=400):
        """TODO: Description."""
        super().__init__(self, detail)
        self.detail = detail
        self.code = code

_Method = namedtuple("_Method", "function, kind, name, params, returns" )

class Resource:
    """TODO: Description."""

    def __init__(self):
        """TODO: Description."""
        self.methods = {}
        for function in [attr for attr in [getattr(self, name) for name in dir(self)] if callable(attr)]:
            try:
                method = function.roax_method
            except:
                continue # ignore undecorated methods
            self.register(function, method.kind, method.name, method.params, method.returns)

    def call(self, kind, name=None, params={}):
        """TODO: Description."""
        try:
            function = self.methods[(kind, name)].function
        except KeyError as e:
            raise ResourceError("resource does not provide method", 400)
        return function(**params)

    def register(self, function, kind, name=None, params=None, returns=None):
        """TODO: Description."""
        if self.methods.get((kind, name)):
            raise ResourceError("function already registered: {}".format((kind, name)))
        self.methods[(kind, name)] = _Method(function, kind, name, params, returns)

def method(kind, name=None, params=None, returns=None):
    """TODO: Description."""
    def decorator(function):
        function.roax_method = _Method(None, kind, name, params, returns)
        def wrapper(wrapped, instance, args, kwargs):
            return roax.schema.call(wrapped, args, kwargs, params, returns)
        return wrapt.decorator(wrapper)(function)
    return decorator
