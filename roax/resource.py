# Copyright © 2015 Paul Bryan.
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from collections import namedtuple
from inspect import signature, Parameter
import roax.schema as s
import wrapt

"""TODO: Description."""


Method = namedtuple("_Method", "kind, name, function, params, returns")


class ResourceError(Exception):
    """TODO: Description."""

    def __init__(self, detail, code=400):
        """TODO: Description."""
        super().__init__(self, detail)
        self.detail = detail
        self.code = code


class Resource:
    """TODO: Description."""

    @property
    def methods(self):
        """TODO: Description."""
        try:
            return self._roax_cached_methods
        except AttributeError:
            result = {}
            for function in [attr for attr in [getattr(self, name) for name in dir(self)] if callable(attr)]:
                try:
                    method = vars(function)["_roax_method"]
                except Exception:
                    pass # Ignore undecorated methods.
                result[(method.kind, method.name)] = method
            self._roax_cached_methods = result
            return result

    def call(self, kind, name=None, params={}):
        """TODO: Description."""
        try:
            function = self.methods[(kind, name)].function
        except KeyError as e:
            raise ResourceError("resource does not support method: {0}{1}".
                format(kind, "" if name is None else "." + name), 400)
        return function(**params)


def _call(function, args, kwargs, params, returns):
    build = {}
    sig = signature(function)
    if len(args) > len([p for p in sig.parameters.values() if p.kind in (p.POSITIONAL_ONLY, p.POSITIONAL_OR_KEYWORD)]):
        raise TypeError("too many positional arguments")
    for v, p in zip(args, sig.parameters.values()):
        build[p.name] = v
    for k,v in kwargs.items():
        if k in build:
            raise TypeError("multiple values for argument: {0}".format(k))
        build[k] = v
    if params is not None:
        build = params.defaults(build)
        try:
            params.validate(build)
        except s.SchemaError as se:
            raise ResourceError(str(se), 400)
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
        if p.kind in (Parameter.POSITIONAL_ONLY, Parameter.POSITIONAL_OR_KEYWORD):
            args.append(v)
        elif p.kind is Parameter.KEYWORD_ONLY:
            kwargs.append(v)
        elif p.kind is Parameter.VAR_KEYWORD:
            kwargs.append(v)
            kwargs.update(build)
            break
    result = function(*args, **kwargs)
    if (returns is not None):
        try:
            returns.validate(result)
        except s.SchemaError as se:
            raise ResourceError(str(se), 500)


def method(kind, *, name=None, params=None, returns=None):
    """TODO: Description."""
    def decorator(function):
        for p in signature(function).parameters.values():
            if p.kind is Parameter.VAR_POSITIONAL:
                raise TypeError("methods do not support *args")
        vars(function)["_roax_method"] = method = Method(kind, name, function, params, returns)
        def wrapper(wrapped, instance, args, kwargs):
            return _call(function, args, kwargs, params, returns)
        return wrapt.decorator(wrapper)(function)
    return decorator
