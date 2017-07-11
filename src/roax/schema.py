"""Module to define, encode, decode and validate JSON data structures."""

# Copyright © 2015–2017 Paul Bryan.
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import inspect
import wrapt

from abc import ABC, abstractmethod
from collections.abc import Sequence
from copy import copy

_type = type
_dict = dict
_list = list
_str = str
_int = int
_float = float
_bool = bool
_bytes = bytes

class _roax_schema_type(ABC):
    """TODO: Description."""

    def __init__(self, *, pytype=object, jstype=None, format=None, enum=None, required=True, default=None, description=None, examples=None):
        """
        pytype: the Python data type.
        jstype: the JSON schema data type.
        format: more finely defines the data type.
        enum: list of values that are valid.
        required: True if the value is mandatory.
        default: the default value, if the item value is not supplied.
        description: string providing information about the item.
        examples: an array of valid values.
        """
        self.pytype = pytype
        self.jstype = jstype
        self.format = format
        self.enum = enum
        self.required = required
        self.default = default
        self.description = description
        self.examples = examples

    def validate(self, value):
        """TODO: Description."""
        if not isinstance(value, self.pytype):
            raise SchemaError("expecting {} type".format(self.pytype.__name__))
        if self.enum is not None and value not in self.enum:
            raise SchemaError("value must be one of: {}".format(", ".join([self.str_encode(v) for v in self.enum])))

    @abstractmethod
    def json_encode(self, value):
        """TODO: Description."""

    @abstractmethod
    def json_decode(self, value):
        """TODO: Description."""

    def json_schema(self):
        """TODO: Description."""
        result = {}
        if self.jstype:
            result["type"] = self.jstype
        if self.format is not None:
            result["format"] = self.format
        if self.default is not None:
            result["default"] = self.json_encode(self.default)
        if self.enum:
            result["enum"] = [self.json_encode(v) for v in self.enum]
        if self.description:
            result["description"] = self.description
        if self.examples:
            result["examples"] = [self.json_encode(e) for e in self.examples]
        return result

    @abstractmethod
    def str_encode(self, value):
        """TODO: Description."""

    @abstractmethod
    def str_decode(self, value):
        """TODO: Description."""

from collections.abc import Mapping
class _roax_schema_dict(_roax_schema_type):
    """TODO: Description."""

    def __init__(self, properties, **kwargs):
        """
        properties: a mapping of name to schema. 
        required: True if the item is mandatory.
        default: The default value, if the item value is not supplied.
        description: string providing information about the item.
        examples: an array of valid values.
        """
        super().__init__(pytype=Mapping, jstype="object", **kwargs)
        self.properties = properties

    def _fixup(self, se, key):
        se.pointer = _str(key) if se.pointer is None else "/".join([_str(key), se.pointer])

    def _process(self, method, value):
        """TODO: Description."""
        result = {}
        for key, schema in self.properties.items():
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
        for key, schema in self.properties.items():
            if key not in value and not schema.required and schema.default is not None:
                if result is None:
                    result = _dict(value)
                result[key] = schema.default
        return result if result else value

    def validate(self, value):
        """TODO: Description."""
        super().validate(value)
        self._process("validate", value)
        for key, schema in self.properties.items():
            try:
                try:
                    schema.validate(value[key])
                except KeyError as ke:
                    if schema.required:
                        raise SchemaError("value required") from ke
            except SchemaError as se:
                self._fixup(se, key)
                raise
        for key in value:
            if key not in self.properties:
                raise SchemaError("unexpected property: {}".format(key))
        return value

    def json_encode(self, value):
        """TODO: Description."""
        value = self.defaults(value)
        self.validate(value)
        if not isinstance(value, dict):
            value = _dict(value) # make JSON encoder happy
        return self._process("json_encode", value)

    def json_decode(self, value):
        """TODO: Description."""
        result = self.defaults(self._process("json_decode", value))
        self.validate(result)
        return result

    def json_schema(self):
        result = super().json_schema()
        result["properties"] = {k: v.json_schema() for k, v in self.properties.items()}
        result["required"] = [k for k, v in self.properties.items() if v.required]
        return result

    def str_encode(self, value):
        raise RuntimeError("string encoding is not supported for dict type")

    def str_decode(self, value):
        raise RuntimeError("string decoding is not supported for dict type")

import csv
from io import StringIO
class _roax_schema_list(_roax_schema_type):
    """TODO: Description."""

    def __init__(self, items, *, min_items=0, max_items=None, unique_items=False, **kwargs):
        """
        items: the schema which all items should conform to.
        min_items: The minimum number of items required.
        max_items: The maximum number of items required.
        unique_items: True if all items must have unique values.
        required: True if the value is mandatory.
        default: The default value, if the item value is not supplied.
        description: string providing information about the item.
        examples: an array of valid values.
        """
        super().__init__(pytype=Sequence, jstype="array", **kwargs)
        self.items = items
        self.min_items = min_items
        self.max_items = max_items
        self.unique_items = unique_items

    def _process(self, method, value):
        """TODO: Description."""
        result = []
        try:
            for n, item in zip(range(len(value)), value):
                result.append(getattr(self.items, method)(item))
        except SchemaError as se:
            se.pointer = _str(n) if se.pointer is None else "/".join([_str(n), se.pointer])
            raise
        return result

    @staticmethod
    def _check_not_str(value):
        if isinstance(value, _str): # strings are iterable, but not what we want
            raise SchemaError("expecting a Sequence type")

    def validate(self, value):
        """TODO: Description."""
        self._check_not_str(value)
        super().validate(value)
        self._process("validate", value)
        if len(value) < self.min_items:
            raise SchemaError("expecting minimum number of {} items".format(self.min_items))
        if self.max_items is not None and len(value) > self.max_items:
            raise SchemaError("expecting maximum number of {} items".format(self.max_items))
        if self.unique_items and len(value) != len(set(value)):
            raise SchemaError("expecting items to be unique")

    def json_encode(self, value):
        """TODO: Description."""
        self._check_not_str(value)
        self.validate(value)
        if not isinstance(value, list):
            value = _list(value) # make JSON encoder happy
        return self._process("json_encode", value)

    def json_decode(self, value):
        """TODO: Description."""
        self._check_not_str(value)
        result = self._process("json_decode", value)
        self.validate(result)
        return result

    def json_schema(self):
        result = super().json_schema()
        result["items"] = self.items
        if self.min_items != 0:
            result["minItems"] = self.min_items
        if self.max_items is not None:
            result["maxItems"] = self.max_items
        if self.unique_items:
            result["uniqueItems"] = True
        return result

    def str_encode(self, value):
        """TODO: Description."""
        self.validate(value)
        sio = StringIO()
        csv.writer(sio).writerow(self._process("str_encode", value))
        return sio.getvalue().rstrip("\r\n")

    def str_decode(self, value):
        """TODO: Description."""
        result = self._process("str_decode", csv.reader([value]).__next__())
        self.validate(result)
        return result

import re
class _roax_schema_str(_roax_schema_type):
    """TODO: Description."""

    def __init__(self, *, min_len=0, max_len=None, pattern=None, **kwargs):
        """
        min_len: the minimum character length of the string.
        max_len: the maximum character length of the string.
        pattern: the regular expression that the string must match.
        format: more finely defines the data type.
        required: True if the value is mandatory.
        default: The default value, if the item value is not supplied.
        enum: list of values that are valid.
        description: string providing information about the item.
        examples: an array of valid values.
        """
        super().__init__(pytype=_str, jstype="string", **kwargs)
        self.min_len = min_len
        self.max_len = max_len
        self.pattern = re.compile(pattern) if pattern is not None else None

    def validate(self, value):
        """TODO: Description."""
        super().validate(value)
        if len(value) < self.min_len:
            raise SchemaError("expecting minimum length of {}".format(self.min_len))
        if self.max_len is not None and len(value) > self.max_len:
            raise SchemaError("expecting maximum length of {}".format(self.max_len))
        if self.pattern is not None and not self.pattern.match(value):
            raise SchemaError("expecting pattern: {}".format(self.pattern.pattern))

    def json_encode(self, value):
        """TODO: Description."""
        self.validate(value)
        return value

    def json_decode(self, value):
        """TODO: Description."""
        self.validate(value)
        return value

    def json_schema(self):
        result = super().json_schema()
        if self.min_len != 0:
             result["minLength"] = min_len
        if self.max_len is not None:
            result["maxLength"] = max_len
        if self.pattern:
            result["pattern"] = self.pattern.pattern
        return result
 
    def str_encode(self, value):
        """TODO: Description."""
        self.validate(value)
        return value

    def str_decode(self, value):
        """TODO: Description."""
        self.validate(value)
        return value

class _number(_roax_schema_type):
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

    def json_encode(self, value):
        """TODO: Description."""
        self.validate(value)
        return value

    def json_schema(self):
        result = super().json_schema()
        if self.minimum is not None:
            result["minimum"] = self.minimum
        if self.maximum is not None:
            result["maximum"] = self.maximum
        return result

    def str_encode(self, value):
        """TODO: Description."""
        self.validate(value)
        return _str(value)

class _roax_schema_int(_number):
    """TODO: Description."""

    def __init__(self, **kwargs):
        """
        minimum: the inclusive lower limit of the value.
        maximum: the inclusive upper limit of the value.
        required: True if the value is mandatory.
        default: The default value, if the item value is not supplied.
        enum: list of values that are valid.
        description: string providing information about the item.
        examples: an array of valid values.
        """
        super().__init__(pytype=_int, jstype="integer", format="int64", **kwargs)

    def validate(self, value):
        super().validate(value)
        if isinstance(value, _bool):
            raise SchemaError("expecting int type")

    def json_decode(self, value):
        """TODO: Description."""
        result = value
        if isinstance(result, _float):
            result = result.__int__()
            if result != value: # 1.0 == 1
                raise SchemaError("expecting integer value")
        self.validate(result)
        return result

    def str_decode(self, value):
        """TODO: Description."""
        try:
            result = _int(value)
        except ValueError as ve:
            raise SchemaError("expecting an integer value") from ve
        self.validate(result)
        return result

class _roax_schema_float(_number):
    """TODO: Description."""

    def __init__(self, **kwargs):
        """
        minimum: the inclusive lower limit of the value.
        maximum: the inclusive upper limit of the value.
        required: True if the value is mandatory.
        default: The default value, if the item value is not supplied.
        enum: list of values that are valid.
        description: string providing information about the item.
        examples: an array of valid values.
        """
        super().__init__(pytype=_float, jstype="number", format="double", **kwargs)

    def json_decode(self, value):
        """TODO: Description."""
        result = value.__float__() if isinstance(value, _int) else value
        self.validate(result)
        return result

    def str_decode(self, value):
        """TODO: Description."""
        try:
            result = _float(value)
        except ValueError as ve:
            raise SchemaError("expecting a number") from ve
        self.validate(result)
        return result

class _roax_schema_bool(_roax_schema_type):
    """TODO: Description."""

    def __init__(self, **kwargs):
        """
        required: True if the value is mandatory.
        default: The default value, if the item value is not supplied.
        description: string providing information about the item.
        examples: an array of valid values.
        """
        super().__init__(pytype=_bool, jstype="boolean", **kwargs)

    def validate(self, value):
        super().validate(value)

    def json_encode(self, value):
        """TODO: Description."""
        self.validate(value)
        return value

    def json_decode(self, value):
        """TODO: Description."""
        self.validate(value)
        return value

    def str_encode(self, value):
        """TODO: Description."""
        self.validate(value)
        return "true" if value else "false"

    def str_decode(self, value):
        """TODO: Description."""
        if value == "true":
            result = True
        elif value == "false":
            result = False
        else:   
            raise SchemaError("expecting true or false")
        self.validate(result)
        return result

import binascii
from base64 import b64decode, b64encode
class _roax_schema_bytes(_roax_schema_type):
    """TODO: Description."""

    def __init__(self, **kwargs):
        """
        required: True if the value is mandatory.
        default: The default value, if the item value is not supplied.
        description: string providing information about the item.
        examples: an array of valid values.
        """
        super().__init__(pytype=_bytes, jstype="string", format="byte", **kwargs)

    def validate(self, value):
        """TODO: Description."""
        super().validate(value)

    def json_encode(self, value):
        """TODO: Description."""
        return self.str_encode(value)

    def json_decode(self, value):
        """TODO: Description."""
        return self.str_decode(value)

    def str_encode(self, value):
        """TODO: Description."""
        self.validate(value)
        return b64encode(value).decode()

    def str_decode(self, value):
        """TODO: Description."""
        try:
            result = b64decode(value)
        except binascii.Error as be:
            raise SchemaError("expecting a Base64-encoded value") from be
        self.validate(result)
        return result

class _roax_schema_none(_roax_schema_type):
    """TODO: Description."""

    def __init__(self, **kwargs):
        """
        description: string providing information about the item.
        """
        super().__init__(pytype=_type(None), jstype="null", **kwargs)

    def validate(self, value):
        """TODO: Description."""
        super().validate(value)

    def json_encode(self, value):
        """TODO: Description."""
        self.validate(value)
        return value

    def json_decode(self, value):
        """TODO: Description."""
        self.validate(value)
        return value

    def str_encode(self, value):
        raise RuntimeError("string encoding is not supported for none type")

    def str_decode(self, value):
        raise RuntimeError("string decoding is not supported for none type")

import isodate
from datetime import datetime as _datetime
class _roax_schema_datetime(_roax_schema_type):
    """TODO: Description."""

    _UTC = isodate.tzinfo.Utc()

    def __init__(self, **kwargs):
        """
        required: True if the value is mandatory.
        default: The default value, if the item value is not supplied.
        enum: list of values that are valid.
        description: string providing information about the item.
        examples: an array of valid values.
        """
        super().__init__(pytype=_datetime, jstype="string", format="date-time", **kwargs)

    def _to_utc(self, value):
        """TODO: Description."""
        if value.tzinfo is None: # naive value interpreted as UTC
            value = value.replace(tzinfo=isodate.tzinfo.Utc())
        return value.astimezone(datetime._UTC)

    def validate(self, value):
        """TODO: Description."""
        super().validate(value)

    def json_encode(self, value):
        """TODO: Description."""
        return self.str_encode(value)

    def json_decode(self, value):
        """TODO: Description."""
        return self.str_decode(value)

    def str_encode(self, value):
        """TODO: Description."""
        self.validate(value)
        return isodate.datetime_isoformat(self._to_utc(value))

    def str_decode(self, value):
        """TODO: Description."""
        try:
            return self._to_utc(isodate.parse_datetime(value))
        except ValueError as ve:
            raise SchemaError("expecting an ISO 8601 date-time value") from ve
        result = self._parse(value)
        self.validate(result)
        return result

from uuid import UUID
class _roax_schema_uuid(_roax_schema_type):
    """TODO: Description."""

    def __init__(self, **kwargs):
        """
        required: True if the value is mandatory.
        default: The default value, if the item value is not supplied.
        enum: list of values that are valid.
        description: string providing information about the item.
        examples: an array of valid values.
        """
        super().__init__(pytype=UUID, jstype="string", format="uuid", **kwargs)

    def validate(self, value):
        """TODO: Description."""
        super().validate(value)

    def json_encode(self, value):
        """TODO: Description."""
        return self.str_encode(value)

    def json_decode(self, value):
        """TODO: Description."""
        return self.str_decode(value)

    def str_encode(self, value):
        """TODO: Description."""
        self.validate(value)
        return _str(value)

    def str_decode(self, value):
        """TODO: Description."""
        if not isinstance(value, _str):
            raise SchemaError("expecting a string")
        try:
            result = UUID(value)
        except ValueError as ve:
            raise SchemaError("expecting string to contain UUID value") from ve
        self.validate(result)
        return result

class _roax_schema_x_of(_roax_schema_type):

    def __init__(self, keyword, schemas, **kwargs):
        super().__init__(**kwargs)
        self.keyword = keyword
        self.schemas = schemas

    def _process(self, method, value):
        """TODO: Description."""
        results = []
        for schema in self.schemas:
            try:
                results.append(getattr(schema, method)(value))
            except SchemaError:
                pass
        return self._evaluate(results)

    @abstractmethod
    def _evaluate(self, method, value):
        """TODO: Description."""

    def validate(self, value):
        """TODO: Description."""
        self._process("validate", value)
        super().validate(value)

    def json_encode(self, value):
        """TODO: Description."""
        return self._process("json_encode", value)

    def json_decode(self, value):
        return self._process("json_decode", value)

    def json_schema(self, value):
        result = super().json_schema(value)
        result[jskeyword] = self.schemas

    def str_encode(self, value):
        """TODO: Description."""
        return self._process("str_encode", value)

    def str_decode(self, value):
        """TODO: Description."""
        return self._process("str_decode", value)

class _roax_schema_all(_roax_schema_x_of):
    """
    Valid if value validates successfully against all of the schemas.
    Values encode/decode using the first schema in the list.
    """

    def __init__(self, schemas, **kwargs):
        """
        schemas: list of schemas to match against.
        required: True if the value is mandatory.
        default: The default value, if the item value is not supplied.
        enum: list of values that are valid.
        description: string providing information about the item.
        examples: an array of valid values.
        """
        super().__init__("allOf", schemas, **kwargs)
        self.schemas = schemas

    def _evaluate(self, values):
        if len(values) != len(self.schemas):
            raise SchemaError("method does not match all schemas")
        return values[0]

class _roax_schema_any(_roax_schema_x_of):
    """
    Valid if value validates successfully against any of the schemas.
    Values encode/decode using the first matching schema in the list.
    """

    def __init__(self, schemas, **kwargs):
        """
        schemas: list of schemas to match against.
        required: True if the value is mandatory.
        default: The default value, if the item value is not supplied.
        enum: list of values that are valid.
        description: string providing information about the item.
        examples: an array of valid values.
        """
        super().__init__("anyOf", schemas, **kwargs)
        self.schemas = schemas

    def _evaluate(self, values):
        if len(values) == 0:
            raise SchemaError("value does not match any schema")
        return values[0] # return first schema-processed value

class _roax_schema_one(_roax_schema_x_of):
    """
    Valid if value validates successfully against exactly one schema.
    Values encode/decode using the matching schema.
    """

    def __init__(self, schemas, **kwargs):
        """
        schemas: list of schemas to match against.
        required: True if the value is mandatory.
        default: The default value, if the item value is not supplied.
        enum: list of values that are valid.
        description: string providing information about the item.
        examples: an array of valid values.
        """
        super().__init__("oneOf", schemas, **kwargs)
        self.schemas = schemas

    def _evaluate(self, values):
        if len(values) == 0:
            raise SchemaError("value does not match any schema")
        elif len(values) > 1:
            raise SchemaError("value matches more than one schema")
        return values[0] # return first matching value

def call(function, args, kwargs, params, returns):
    """Call a function, validating its input parameters and return value."""
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
        try:
            params.validate(build)
        except SchemaError as se:
            se.msg = "parameter: " + se.msg
            raise
    args = []
    kwargs = {}
    for p in sig.parameters.values():
        try:
            v = build.pop(p.name)
        except KeyError as ke:
            if p.default is not p.empty:
                v = p.default
            elif params is None:
                raise SchemaError("required parameter", p.name) from ke
            else:
                v = None # parameter is specified as optional in schema
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
        except SchemaError as se:
            raise ValueError("return value: {}".format(se.msg)) from se
    return result

def validate(params=None, returns=None):
    """Decorate a function to validate its input parameters and return value."""
    def decorator(function):
        _params = {}
        if params:
            sig = inspect.signature(function)
            for p in (p for p in sig.parameters.values() if p.name != "self"):
                schema = params.properties.get(p.name)
                if schema:
                    schema = copy(schema)
                    schema.required = p.default is p.empty
                    schema.default = p.default if p.default is not p.empty else None
                    _params[p.name] = schema
                elif p.default is p.empty:
                    raise TypeError("required parameter in function but not in validation decorator: {}".format(p.name)) 
        def wrapper(wrapped, instance, args, kwargs):
            return call(wrapped, args, kwargs, _roax_schema_dict(_params), returns)
        return wrapt.decorator(wrapper)(function)
    return decorator

class SchemaError(Exception):
    """Raised if a value does not conform to its schema."""

    def __init__(self, msg, pointer=None):
        """TODO: Description."""
        self.msg = msg
        self.pointer = pointer

    def __str__(self):
        result = []
        if self.pointer is not None:
            result.append(self.pointer)
        if self.msg is not None:
            result.append(self.msg)
        return ": ".join(result)

# export intuitive names
type = _roax_schema_type
dict = _roax_schema_dict
list = _roax_schema_list
str = _roax_schema_str
int = _roax_schema_int
float = _roax_schema_float
bool = _roax_schema_bool
bytes = _roax_schema_bytes
none = _roax_schema_none
datetime = _roax_schema_datetime
uuid = _roax_schema_uuid
all = _roax_schema_all
any = _roax_schema_any
one = _roax_schema_one
