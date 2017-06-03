"""Module to define, encode, decode and validate JSON data structures."""

# Copyright © 2015–2017 Paul Bryan.
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import inspect
import wrapt

from abc import ABC, abstractmethod

_x_type = type
_x_dict = dict
_x_list = list
_x_str = str
_x_int = int
_x_float = float
_x_bool = bool

class SchemaError(Exception):
    """TODO: Description."""

    def __init__(self, msg, pointer=None):
        """TODO: Description."""
        self.msg = msg
        self.pointer = pointer

    def __str__(self):
        """TODO: Description."""
        result = []
        if self.pointer is not None:
            result.append(self.pointer)
        if self.msg is not None:
            result.append(self.msg)
        return ": ".join(result)

class type(ABC):
    """TODO: Description."""

    def __init__(self, *, jstype, format=None, required=True, default=None, enum=None, description=None, example=None):
        """TODO: Description."""
        self.jstype = jstype
        self.format = format
        self.required = required
        self.default = default
        self.enum = enum
        self.description = description
        self.example = example

    def validate(self, value):
        """TODO: Description."""
        if self.enum is not None and value not in self.enum:
            raise SchemaError("value must be one of: {}".format(", ".join([self.encode_param(v) for v in self.enum])))

    @abstractmethod
    def encode_json(self, value):
        """TODO: Description."""
        pass

    @abstractmethod
    def decode_json(self, value):
        """TODO: Description."""
        pass

    @abstractmethod
    def encode_param(self, value):
        """TODO: Description."""
        pass

    @abstractmethod
    def decode_param(self, value):
        """TODO: Description."""
        pass

from copy import deepcopy
from collections.abc import Mapping
class dict(type):
    """TODO: Description."""

    def __init__(self, fields, model=None, **kwargs):
        """TODO: Description."""
        super().__init__(jstype="object", **kwargs)
        self.fields = fields
        self.model = model

    def _fixup(self, se, key):
        se.pointer = _x_str(key) if se.pointer is None else "/".join([_x_str(key), se.pointer])

    def _process(self, method, value):
        """TODO: Description."""
        if not isinstance(value, Mapping):
            raise SchemaError("expecting a key-value mapping")
        result = {}
        for key, schema in self.fields.items():
            try:
                try:
                    result[key] = getattr(schema, method)(value[key])
                except KeyError:
                    pass
            except SchemaError as se:
                self._fixup(se, key)
                raise
        return result

    def defaults(self, value):
        """TODO: Description."""
        result = None
        for key, schema in self.fields.items():
            if key not in value and schema.default is not None:
                if result is None:
                    result = value.copy()
                result[key] = schema.default
        return result if result is not None else value

    def validate(self, value):
        """TODO: Description."""
        super().validate(value)
        self._process("validate", value)
        for key, schema in self.fields.items():
            try:
                try:
                    schema.validate(value[key])
                except KeyError:
                    if schema.required:
                        raise SchemaError("required")
            except SchemaError as se:
                self._fixup(se, key)
                raise
        return value

    def encode_json(self, value):
        """TODO: Description."""
        value = self.defaults(value)
        self.validate(value)
        return self._process("encode_json", value)

    def decode_json(self, value):
        """TODO: Description."""
        if not isinstance(value, _x_dict):
            raise SchemaError("expecting an object")
        result = self.defaults(self._process("decode_json", value))
        self.validate(result)
        return result

    def encode_param(self, value):
        """TODO: Description."""
        value = self.defaults(value)
        self.validate(value)
        return self._process("encode_param", value)

    def decode_param(self, value):
        """TODO: Description."""
        result = self.defaults(self._process("decode_param", value))
        self.validate(result)
        return result

import csv
from io import StringIO
class list(type):
    """TODO: Description."""

    def __init__(self, items, *, min_items=0, max_items=None, unique_items=False, **kwargs):
        """TODO: Description."""
        super().__init__(jstype="array", **kwargs)
        self.items = items
        self.min_items = min_items
        self.max_items = max_items
        self.unique_items = unique_items

    def _process(self, method, value):
        """TODO: Description."""
        if isinstance(value, _x_str): # Strings are iterable, but not what we want.
            raise SchemaError("expecting an iterable list or tuple")
        result = []
        try:
            for n, item in zip(range(len(value)), value):
                result.append(getattr(self.items, method)(item))
        except SchemaError as se:
            se.pointer = _x_str(n) if se.pointer is None else "/".join([_x_str(n), se.pointer])
            raise
        return result

    def validate(self, value):
        """TODO: Description."""
        self._process("validate", value)
        super().validate(value)
        if len(value) < self.min_items:
            raise SchemaError("expecting minimum number of {} items".format(self.min_items))
        if self.max_items is not None and len(value) > self.max_items:
            raise SchemaError("expecting maximum number of {} items".format(self.max_items))
        if self.unique_items and len(value) != len(set(value)):
            raise SchemaError("expecting items to be unique")

    def encode_json(self, value):
        """TODO: Description."""
        self.validate(value)
        return self._process("encode_json", value)

    def decode_json(self, value):
        """TODO: Description."""
        if not isinstance(value, _x_list):
            raise SchemaError("expecting a JSON array")
        result = self._process("decode_json", value)
        self.validate(result)
        return result

    def encode_param(self, value):
        """TODO: Description."""
        self.validate(value)
        sio = StringIO()
        csv.writer(sio).writerow(self._process("encode_param", value))
        return sio.getvalue().rstrip("\r\n")

    def decode_param(self, value):
        """TODO: Description."""
        result = self._process("decode_param", csv.reader([value]).__next__())
        self.validate(result)
        return result

import re
class str(type):
    """TODO: Description."""

    def __init__(self, *, min_len=0, max_len=None, pattern=None, **kwargs):
        """TODO: Description."""
        super().__init__(jstype="string", **kwargs)
        self.min_len = min_len
        self.max_len = max_len
        self.pattern = re.compile(pattern) if pattern is not None else None

    def validate(self, value):
        """TODO: Description."""
        if not isinstance(value, _x_str):
            raise SchemaError("expecting a str type")
        super().validate(value)
        if len(value) < self.min_len:
            raise SchemaError("expecting minimum length of {}".format(self.min_len))
        if self.max_len is not None and len(value) > self.max_len:
            raise SchemaError("expecting maximum length of {}".format(self.max_len))
        if self.pattern is not None and not self.pattern.match(value):
            raise SchemaError("expecting pattern: {}".format(self.pattern.pattern))

    def encode_json(self, value):
        """TODO: Description."""
        self.validate(value)
        return value

    def decode_json(self, value):
        """TODO: Description."""
        self.validate(value)
        return value

    def encode_param(self, value):
        """TODO: Description."""
        self.validate(value)
        return value

    def decode_param(self, value):
        """TODO: Description."""
        self.validate(value)
        return value

class _number(type):
    """TODO: Description."""

    def __init__(self, *, minimum=None, maximum=None, **kwargs):
        """TODO: Description."""
        super().__init__(**kwargs)
        self.minimum = minimum
        self.maximum = maximum

    def validate(self, value):
        """TODO: Description."""
        super().validate(value)
        if self.minimum is not None and value < self.minimum:
            raise SchemaError("expecting minimum value of {}".format(self.minimum))
        if self.maximum is not None and value > self.maximum:
            raise SchemaError("expecting maximum value of {}".format(self.maximum))

    def encode_json(self, value):
        """TODO: Description."""
        self.validate(value)
        return value

    def encode_param(self, value):
        self.validate(value)
        return _x_str(value)

class int(_number):
    """TODO: Description."""

    def __init__(self, **kwargs):
        """TODO: Description."""
        super().__init__(jstype="integer", format="int64", **kwargs)

    def validate(self, value):
        """TODO: Description."""
        if not isinstance(value, _x_int):
            raise SchemaError("expecting an int type")
        super().validate(value)

    def decode_json(self, value):
        """TODO: Description."""
        if not isinstance(value, _x_int):
            raise SchemaError("expecting an integer")
        self.validate(value)
        return value

    def decode_param(self, value):
        """TODO: Description."""
        if not isinstance(value, _x_str):
            raise SchemaError("expecting a string")
        try:
            result = _x_int(value)
        except ValueError:
            raise SchemaError("expecting an integer")
        self.validate(result)
        return result

class float(_number):
    """TODO: Description."""

    def __init__(self, **kwargs):
        """TODO: Description."""
        super().__init__(jstype="number", format="double", **kwargs)

    def validate(self, value):
        """TODO: Description."""
        if not isinstance(value, _x_float):
            raise SchemaError("expecting a float type")
        super().validate(value)
        return value

    def decode_json(self, value):
        """TODO: Description."""
        if isinstance(value, _x_float):
            result = value
        elif isinstance(value, _x_int):
            result = _x_float(value)
        else:
            raise SchemaError("expecting a number")
        self.validate(result)
        return result

    def decode_param(self, value):
        """TODO: Description."""
        if not isinstance(value, _x_str):
            raise SchemaError("expecting a string")
        try:
            result = _x_float(value)
        except ValueError:
            raise SchemaError("expecting a number")
        self.validate(result)
        return result

class bool(type):
    """TODO: Description."""

    def __init__(self, **kwargs):
        """TODO: Description."""
        super().__init__(jstype="boolean", **kwargs)

    def validate(self, value):
        if not isinstance(value, _x_bool):
            raise SchemaError("expecting a bool type")
        super().validate(value)

    def encode_json(self, value):
        """TODO: Description."""
        self.validate(value)
        return value

    def decode_json(self, value):
        """TODO: Description."""
        if not isinstance(value, _x_bool):
            raise SchemaError("expecting true or false")
        self.validate(value)
        return value

    def encode_param(self, value):
        """TODO: Description."""
        self.validate(value)
        if value is True:
            return "true"
        elif value is False:
            return "false"

    def decode_param(self, value):
        """TODO: Description."""
        if not isinstance(value, _x_str):
            raise SchemaError("expecting a string")
        elif value == "true":
            result = True
        elif value == "false":
            result = False
        else:
            raise SchemaError("expecting true or false")
        self.validate(result)
        return result

import isodate
from datetime import datetime as _x_datetime
class datetime(type):
    """TODO: Description."""

    _UTC = isodate.tzinfo.Utc()

    def __init__(self, **kwargs):
        """TODO: Description."""
        super().__init__(jstype="string", format="date-time", **kwargs)

    def _to_utc(self, value):
        """TODO: Description."""
        if value.tzinfo is None: # Naive value is assumed to be UTC.
            value = value.replace(tzinfo=isodate.tzinfo.Utc())
        return value.astimezone(datetime._UTC)

    def _to_string(self, value):
        """TODO: Description."""
        return isodate.datetime_isoformat(self._to_utc(value))

    def _parse(self, value):
        if not isinstance(value, _x_str):
            raise SchemaError("expecting a string")
        try:
            return self._to_utc(isodate.parse_datetime(value))
        except ValueError:
            raise SchemaError("expecting an ISO 8601 date-time value")

    def validate(self, value):
        """TODO: Description."""
        if not isinstance(value, _x_datetime):
            raise SchemaError("expecting a datetime type")
        super().validate(value)

    def encode_json(self, value):
        """TODO: Description."""
        self.validate(value)
        return self._to_string(value)

    def decode_json(self, value):
        """TODO: Description."""
        result = self._parse(value)
        self.validate(result)
        return result

    def encode_param(self, value):
        """TODO: Description."""
        self.validate(value)
        return self._to_string(value)

    def decode_param(self, value):
        """TODO: Description."""
        result = self._parse(value)
        self.validate(result)
        return result

from uuid import UUID
class uuid(type):
    """TODO: Description."""

    def __init__(self, **kwargs):
        """TODO: Description."""
        super().__init__(jstype="string", format="uuid", **kwargs)

    def _parse(self, value):
        """TODO: Description."""
        if not isinstance(value, _x_str):
            raise SchemaError("expecting a string")
        try:
            return UUID(value)
        except ValueError:
            raise SchemaError("expecting a UUID value")

    def validate(self, value):
        """TODO: Description."""
        if not isinstance(value, UUID):
            raise SchemaError("expecting a UUID type")
        super().validate(value)

    def encode_json(self, value):
        """TODO: Description."""
        self.validate(value)
        return _x_str(value)

    def decode_json(self, value):
        """TODO: Description."""
        result = self._parse(value)
        self.validate(result)
        return result

    def encode_param(self, value):
        """TODO: Description."""
        self.validate(value)
        return _x_str(value)

    def decode_param(self, value):
        """TODO: Description."""
        result = self._parse(value)
        self.validate(result)
        return result

def call(function, args, kwargs, params, returns):
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
        try:
            returns.validate(result)
        except s.SchemaError as se:
            se.pointer = "[return]/{}".format(se.pointer)
            raise
    return result

def validate(params=None, returns=None):
    """TODO: Description."""
    def decorator(function):
        def wrapper(wrapped, instance, args, kwargs):
            return call(wrapped, args, kwargs, params, returns)
        return wrapt.decorator(wrapper)(function)
    return decorator
