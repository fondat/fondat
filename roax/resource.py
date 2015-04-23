# Copyright © 2015 Paul Bryan.
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from collections import namedtuple
import inspect
import roax.schema as s
import wrapt

"""TODO: Description."""


class ResourceError(Exception):
    """TODO: Description."""

    def __init__(self, detail, code=400):
        """TODO: Description."""
        super().__init__(self, detail)
        self.detail = detail
        self.code = code


def _functions(object):
    """TODO: Description."""
    result = {}
    for function in [attr for attr in [getattr(object, name) for name in dir(object)] if callable(attr)]:
        try:
            result[function._roax_key] = function
        except:
            pass # Ignore undecorated methods.
    return result


class Resource:
    """TODO: Description."""

    def __init__(self):
        """TODO: Description."""
        self._functions = _functions(self)
        self.methods = {f._roax_key: f._roax_schemas for f in self._functions.values()}

    def call(self, kind, name=None, params={}):
        """TODO: Description."""
        try:
            function = self._functions[(kind, name)]
        except KeyError as e:
            raise ResourceError("resource does not support method", 400)
        return function(**params)


def _call(function, args, kwargs, params, returns):
    """TODO: Description."""
    build = {}
    sig = inspect.signature(function)
    if len(args) > len([p for p in sig.parameters.values() if p.kind in (p.POSITIONAL_ONLY, p.POSITIONAL_OR_KEYWORD)]):
        raise TypeError("too many positional arguments")
    for v, p in zip(args, sig.parameters.values()):
        build[p.name] = v
    for k, v in kwargs.items():
        if k in build:
            raise TypeError("multiple values for argument: {}".format(k))
        build[k] = v
    if params is not None:
        build = params.defaults(build)
        params.validate(build)
    args = []
    kwargs = {}
    for p in sig.parameters.values():
        try:
            v = build.pop(p.name)
        except KeyError:
            if p.default is not p.empty:
                v = p.default
            elif params is None:
                raise s.SchemaError("parameter required", p.name)
            else:
                v = None # Parameter is specified as optional in schema.
        if p.kind in (inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD):
            args.append(v)
        elif p.kind is inspect.Parameter.KEYWORD_ONLY:
            kwargs.append(v)
        elif p.kind is inspect.Parameter.VAR_KEYWORD:
            kwargs.append(v)
            kwargs.update(build)
            break
    result = function(*args, **kwargs)
    if returns is not None:
        returns.validate(result)
    return result


def method(kind, *, name=None, params=None, returns=None):
    """TODO: Description."""
    def decorator(function):
        for p in inspect.signature(function).parameters.values():
            if p.kind is inspect.Parameter.VAR_POSITIONAL:
                raise TypeError("resource methods do not support *args")
        function._roax_key = (kind, name)
        function._roax_schemas = (params, returns)
        def wrapper(wrapped, instance, args, kwargs):
            return _call(wrapped, args, kwargs, params, returns)
        return wrapt.decorator(wrapper)(function)
    return decorator
