"""Internal module to define, encode, decode and validate JSON data structures."""

# Copyright © 2015–2018 Paul Bryan.
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import inspect
import wrapt

from abc import ABC, abstractmethod
from collections.abc import Sequence
from copy import copy


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


class _type(ABC):
    """Base class for all schema types."""

    def __init__(
            self, *, python_type=object, json_type=None, format=None,
            content_type="application/json", enum=None, required=True,
            default=None, description=None, examples=None, nullable=False,
            deprecated=False):
        """
        python_type: the Python data type.
        json_type: the JSON schema data type.
        format: more finely defines the data type.
        content_type: the content type used when value is expressed in a body.
        enum: list of values that are valid.
        nullable: True if None is a valid value.
        required: True if the value is mandatory.
        default: the default value, if the item value is not supplied.
        description: string providing information about the item.
        examples: an array of valid values.
        deprecated: schema should be transitioned out of usage.
        """
        super().__init__()
        self.python_type = python_type
        self.json_type = json_type
        self.format = format
        self.content_type = content_type
        self.nullable = nullable
        self.enum = enum
        self.required = required
        self.default = default
        self.description = description
        self.examples = examples
        self.deprecated = deprecated

    def validate(self, value):
        """TODO: Description."""
        if value is None and not self.nullable:
            raise SchemaError("value cannot be None")
        if value is not None and not isinstance(value, self.python_type):
            raise SchemaError("expecting {} type".format(self.python_type.__name__))
        if value is not None and self.enum is not None and value not in self.enum:
            raise SchemaError("value must be one of: {}".format(", ".join([self.str_encode(v) for v in self.enum])))

    @abstractmethod
    def json_encode(self, value):
        """
        Encode the value into JSON object model representation. The method does not
        dump the value as JSON text; it represents the value such that the Python
        JSON module can dump as JSON text if required.
        """

    @abstractmethod
    def json_decode(self, value):
        """
        Decode the value from JSON object model representation. The method does not
        parse the value as JSON text; it takes a Python value as though the Python JSON
        module loaded the JSON text.
        """

    def json_schema(self):
        """TODO: Description."""
        result = {}
        if self.json_type:
            result["type"] = self.json_type
        if self.format is not None:
            result["format"] = self.format
        if self.nullable is not None:
            result["nullable"] = self.nullable
        if self.default is not None:
            result["default"] = self.json_encode(self.default)
        if self.enum:
            result["enum"] = [self.json_encode(v) for v in self.enum]
        if self.description:
            result["description"] = self.description
        if self.examples:
            result["examples"] = [self.json_encode(e) for e in self.examples]
        if self.deprecated is not None:
            result["deprecated"] = self.deprecated
        return result

    @abstractmethod
    def str_encode(self, value):
        """Encode the value into string representation."""

    @abstractmethod
    def str_decode(self, value):
        """Decode the value from string representation."""


from collections.abc import Mapping
class _dict(_type):
    """
    Schema type for dictionaries.
    """

    def __init__(self, properties, *, additional_properties=False, **kwargs):
        """
        properties: a mapping of name to schema. 
        additional_properties: True if additional unvalidated properties are allowed.
        nullable: allows expressing None as the value.
        required: True if the item is mandatory.
        default: The default value, if the item value is not supplied.
        description: string providing information about the item.
        examples: an array of valid values.
        """
        super().__init__(python_type=Mapping, json_type="object", **kwargs)
        self.properties = properties
        self.additional_properties = additional_properties

    def _fixup(self, se, key):
        se.pointer = str(key) if se.pointer is None else "/".join([str(key), se.pointer])

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
                    result = dict(value)
                result[key] = schema.default
        return result if result else value

    def validate(self, value):
        """TODO: Description."""
        super().validate(value)
        if value is not None:
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
            if not self.additional_properties:
                for key in value:
                    if key not in self.properties:
                        raise SchemaError("unexpected property: {}".format(key))
        return value

    def json_encode(self, value):
        """Encode the value into JSON object model representation."""
        value = self.defaults(value)
        self.validate(value)
        if not isinstance(value, dict):
            value = dict(value) # make JSON encoder happy
        return self._process("json_encode", value)

    def json_decode(self, value):
        """Decode the value from JSON object model representation."""
        result = self.defaults(self._process("json_decode", value))
        self.validate(result)
        return result

    def json_schema(self):
        result = super().json_schema()
        result["properties"] = {k: v.json_schema() for k, v in self.properties.items()}
        result["required"] = [k for k, v in self.properties.items() if v.required]
        return result

    def str_encode(self, value):
        """Encode the value into string representation."""
        result = []
        for key in value:
            result.append("{}={}".format(key,self.properties[key].str_encode(value[key])))
        return ",".join(result)

    def str_decode(self, value):
        """Decode the value from string representation."""
        raise RuntimeError("dict cannot be decoded from string representation")


import csv
from io import StringIO
class _list(_type):
    """
    Schema type for lists.
    """

    def __init__(self, items, *, min_items=0, max_items=None, unique_items=False, **kwargs):
        """
        items: the schema which all items should adhere to.
        min_items: The minimum number of items required.
        max_items: The maximum number of items required.
        unique_items: True if all items must have unique values.
        nullable: allows expressing None as the value.
        required: True if the value is mandatory.
        default: The default value, if the item value is not supplied.
        description: string providing information about the item.
        examples: an array of valid values.
        """
        super().__init__(python_type=Sequence, json_type="array", **kwargs)
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
            se.pointer = str(n) if se.pointer is None else "/".join([str(n), se.pointer])
            raise
        return result

    @staticmethod
    def _check_not_str(value):
        if isinstance(value, str): # strings are iterable, but not what we want
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
        """Encode the value into JSON object model representation."""
        self._check_not_str(value)
        self.validate(value)
        if not isinstance(value, list):
            value = list(value) # make JSON encoder happy
        return self._process("json_encode", value)

    def json_decode(self, value):
        """Decode the value from JSON object model representation."""
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
        """Encode the value into string representation."""
        self.validate(value)
        sio = StringIO()
        csv.writer(sio).writerow(self._process("str_encode", value))
        return sio.getvalue().rstrip("\r\n")

    def str_decode(self, value):
        """Decode the value from string representation."""
        result = self._process("str_decode", csv.reader([value]).__next__())
        self.validate(result)
        return result


import re
class _str(_type):
    """
    Schema type for Unicode character strings.
    """

    def __init__(self, *, min_len=0, max_len=None, pattern=None, **kwargs):
        """
        min_len: the minimum character length of the string.
        max_len: the maximum character length of the string.
        pattern: the regular expression that the string must match.
        format: more finely defines the data type.
        nullable: allows expressing None as the value.
        required: True if the value is mandatory.
        default: The default value, if the item value is not supplied.
        enum: list of values that are valid.
        description: string providing information about the item.
        examples: an array of valid values.
        """
        super().__init__(python_type=str, json_type="string", **kwargs)
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
        """Encode the value into JSON object model representation."""
        self.validate(value)
        return value

    def json_decode(self, value):
        """Decode the value from JSON object model representation."""
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
        """Encode the value into string representation."""
        self.validate(value)
        return value

    def str_decode(self, value):
        """Decode the value from string representation."""
        self.validate(value)
        return value


class _number(_type):
    """
    Base class for numeric types (int, float).
    """

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
        """Encode the value into JSON object model representation."""
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
        """Encode the value into string representation."""
        self.validate(value)
        return str(value)


class _int(_number):
    """
    Schema type for integers.
    """

    def __init__(self, **kwargs):
        """
        minimum: the inclusive lower limit of the value.
        maximum: the inclusive upper limit of the value.
        nullable: allows expressing None as the value.
        required: True if the value is mandatory.
        default: The default value, if the item value is not supplied.
        enum: list of values that are valid.
        description: string providing information about the item.
        examples: an array of valid values.
        """
        super().__init__(python_type=int, json_type="integer", format="int64", **kwargs)

    def validate(self, value):
        super().validate(value)
        if isinstance(value, bool):
            raise SchemaError("expecting int type")

    def json_decode(self, value):
        """Decode the value from JSON object model representation."""
        result = value
        if isinstance(result, float):
            result = result.__int__()
            if result != value: # 1.0 == 1
                raise SchemaError("expecting integer value")
        self.validate(result)
        return result

    def str_decode(self, value):
        """Decode the value from string representation."""
        try:
            result = int(value)
        except ValueError as ve:
            raise SchemaError("expecting an integer value") from ve
        self.validate(result)
        return result


class _float(_number):
    """
    Schema type for floating point numbers.
    """

    def __init__(self, **kwargs):
        """
        minimum: the inclusive lower limit of the value.
        maximum: the inclusive upper limit of the value.
        nullable: allows expressing None as the value.
        required: True if the value is mandatory.
        default: The default value, if the item value is not supplied.
        enum: list of values that are valid.
        description: string providing information about the item.
        examples: an array of valid values.
        """
        super().__init__(python_type=float, json_type="number", format="double", **kwargs)

    def json_decode(self, value):
        """Decode the value from JSON object model representation."""
        result = value.__float__() if isinstance(value, int) else value
        self.validate(result)
        return result

    def str_decode(self, value):
        """Decode the value from string representation."""
        try:
            result = float(value)
        except ValueError as ve:
            raise SchemaError("expecting a number") from ve
        self.validate(result)
        return result


class _bool(_type):
    """
    Schema type for boolean values.
    """

    def __init__(self, **kwargs):
        """
        nullable: allows expressing None as the value.
        required: True if the value is mandatory.
        default: The default value, if the item value is not supplied.
        description: string providing information about the item.
        examples: an array of valid values.
        """
        super().__init__(python_type=bool, json_type="boolean", **kwargs)

    def validate(self, value):
        super().validate(value)

    def json_encode(self, value):
        """Encode the value into JSON object model representation."""
        self.validate(value)
        return value

    def json_decode(self, value):
        """Decode the value from JSON object model representation."""
        self.validate(value)
        return value

    def str_encode(self, value):
        """Encode the value into string representation."""
        self.validate(value)
        return "true" if value else "false"

    def str_decode(self, value):
        """Decode the value from string representation."""
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
class _bytes(_type):
    """
    Schema type for byte sequences.
    
    Two formats are supported: "byte" and "binary".

    In "byte" format, a byte sequence is represented as a base64-encoded string.
    Example: "Um9heCBpcyBhIHBhcnQgb2YgYSBjb21wbGV0ZSBicmVha2Zhc3QuCg==".
    
    In "binary" format, a byte sequence is represented as a raw sequence of bytes.
    For this reason, it cannot be expressed in string or JSON representation. 
    """

    def __init__(self, format="byte", **kwargs):
        """
        format: "byte" (default) or "binary".
        nullable: allows expressing None as the value.
        required: True if the value is mandatory.
        default: the default value, if the item value is not supplied.
        description: string providing information about the item.
        examples: an array of valid values.
        """
        valid_formats = ["byte", "binary"]
        if format not in valid_formats:
            raise SchemaError("format must be one of {}".format(valid_formats))        
        super().__init__(python_type=bytes, json_type="string", format=format, **kwargs)

    def validate(self, value):
        """TODO: Description."""
        super().validate(value)

    def json_encode(self, value):
        """Encode the value into JSON object model representation."""
        if self.format == "binary":
            raise SchemaError("binary format cannot be encoded into JSON representation")
        return self.str_encode(value)

    def json_decode(self, value):
        """Decode the value from JSON object model representation."""
        if self.format == "binary":
            raise SchemaError("binary format cannot be decoded from JSON representation")
        return self.str_decode(value)

    def str_encode(self, value):
        """Encode the value into string representation."""
        if self.format == "binary":
            raise SchemaError("binary format cannot be encoded into string representation")
        self.validate(value)
        return b64encode(value).decode()

    def str_decode(self, value):
        """Decode the value from string representation."""
        if self.format == "binary":
            raise SchemaError("binary format cannot be decoded from string representation")
        try:
            result = b64decode(value)
        except binascii.Error as be:
            raise SchemaError("expecting a base64-encoded value") from be
        self.validate(result)
        return result

    def bin_encode(self, value):
        """Endode the value into binary representation."""
        return value

    def bin_decode(self, value):
        """Decode the value from binary representation."""
        return value


import isodate
class datetime(_type):
    """
    Schema type for datetime values.

    Datetime values are represented in string and JSON values as an ISO 8601 date
    and time in a string. Example: "2017-07-11T05:42:34Z".
    """

    _UTC = isodate.tzinfo.Utc()

    def __init__(self, **kwargs):
        """
        nullable: allows expressing None as the value.
        required: True if the value is mandatory.
        default: The default value, if the item value is not supplied.
        enum: list of values that are valid.
        description: string providing information about the item.
        examples: an array of valid values.
        """
        from datetime import datetime
        super().__init__(python_type=datetime, json_type="string", format="date-time", **kwargs)

    def _to_utc(self, value):
        """TODO: Description."""
        if value.tzinfo is None: # naive value interpreted as UTC
            value = value.replace(tzinfo=isodate.tzinfo.Utc())
        return value.astimezone(self._UTC)

    def validate(self, value):
        """TODO: Description."""
        super().validate(value)

    def json_encode(self, value):
        """Encode the value into JSON object model representation."""
        return self.str_encode(value)

    def json_decode(self, value):
        """Decode the value from JSON object model representation."""
        return self.str_decode(value)

    def str_encode(self, value):
        """Encode the value into string representation."""
        self.validate(value)
        return isodate.datetime_isoformat(self._to_utc(value))

    def str_decode(self, value):
        """Decode the value from string representation."""
        try:
            return self._to_utc(isodate.parse_datetime(value))
        except ValueError as ve:
            raise SchemaError("expecting an ISO 8601 date-time value") from ve
        result = self._parse(value)
        self.validate(result)
        return result


from uuid import UUID
class uuid(_type):
    """
    Schema type for universally unique identifiers.

    UUID values are represented in string and JSON values as a UUID string.
    Example: "035af02b-7ad7-4016-a101-96f8fc5ae6ec".
    """

    def __init__(self, **kwargs):
        """
        nullable: allows expressing None as the value.
        required: True if the value is mandatory.
        default: The default value, if the item value is not supplied.
        enum: list of values that are valid.
        description: string providing information about the item.
        examples: an array of valid values.
        """
        super().__init__(python_type=UUID, json_type="string", format="uuid", **kwargs)

    def validate(self, value):
        """TODO: Description."""
        super().validate(value)

    def json_encode(self, value):
        """Encode the value into JSON object model representation."""
        return self.str_encode(value)

    def json_decode(self, value):
        """Decode the value from JSON object model representation."""
        return self.str_decode(value)

    def str_encode(self, value):
        """Encode the value into string representation."""
        self.validate(value)
        return str(value)

    def str_decode(self, value):
        """Decode the value from string representation."""
        if not isinstance(value, str):
            raise SchemaError("expecting a string")
        try:
            result = UUID(value)
        except ValueError as ve:
            raise SchemaError("expecting string to contain UUID value") from ve
        self.validate(result)
        return result


#class all_of(_type):
#    """
#    Schema type that is valid if a value validates successfully against all
#    of the schemas. Values are encoded/decoded using the first schema in the
#    list.
#    """
#
#    def __init__(self, schemas, **kwargs):
#        """
#        schemas: list of schemas to match against.
#        required: True if the value is mandatory.
#        nullable: allows expressing None as the value.
#        default: The default value, if the item value is not supplied.
#        enum: list of values that are valid.
#        description: string providing information about the item.
#        examples: an array of valid values.
#        """
#        super().__init__(**kwargs)
#        self.schemas = schemas
#
#    def _evaluate(self, values):
#        if len(values) != len(self.schemas):
#            raise SchemaError("method does not match all schemas")
#        return values[0]


class _xof(_type):
    """TODO: Description."""

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
        """Encode the value into JSON object model representation."""
        return self._process("json_encode", value)

    def json_decode(self, value):
        """Decode the value from JSON object model representation."""
        return self._process("json_decode", value)

    def json_schema(self, value):
        result = super().json_schema(value)
        result[jskeyword] = self.schemas

    def str_encode(self, value):
        """Encode the value into string representation."""
        return self._process("str_encode", value)

    def str_decode(self, value):
        """Decode the value from string representation."""
        return self._process("str_decode", value)


class any_of(_xof):
    """
    Schema type that is valid if a value validates successfully against any
    of the schemas. Values are encoded/decoded using the first valid
    matching schema in the list.
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


class one_of(_xof):
    """
    Schema type that is valid if a value validates successfully against
    exactly one schema. Values are encoded/decoded using the sole matching
    schema.
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


def call(function, args, kwargs, params=None, returns=None):
    """
    Call a function, validating its input parameters and return value.
    Whether a parameter is required and any default value is defined by the
    function, not its schema specification. If a parameter is omitted from the
    params schema, its value is not validated. 
    """
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
    args = []
    kwargs = {}
    for p in sig.parameters.values():
        if p.kind is inspect.Parameter.VAR_POSITIONAL:
            raise TypeError("parameter validation does not support functions with *args")
        elif p.kind is inspect.Parameter.VAR_KEYWORD:
            raise TypeError("parameter validation does not support functions with **kwargs")
        if p.name in build:
            value = build[p.name]
            if params is not None and p.name in params:
                try:
                    params[p.name].validate(build[p.name])
                except SchemaError as se:
                    se.msg = "parameter: " + se.msg
                    raise
        elif p.default is not p.empty:
            value = p.default
        else:
            raise SchemaError("missing required parameter", p.name)
        if p.kind in (inspect.Parameter.POSITIONAL_ONLY, inspect.Parameter.POSITIONAL_OR_KEYWORD):
            args.append(value)
        elif p.kind is inspect.Parameter.KEYWORD_ONLY:
            kwargs[p.name] = value
        else:
            raise TypeError("unrecognized type for parameter {}".format(p.name))
    result = function(*args, **kwargs)
    if returns is not None:
        try:
            returns.validate(result)
        except SchemaError as se:
            raise ValueError("return value: {}".format(se.msg)) from se
    return result


def function_params(function, params):
    """
    Return a subset of the passed parameter schemas, based on the arguments of
    a defined function. The required and default properties are overridden,
    based function's arguments.
    """
    params = params or {}
    result = {}
    sig = inspect.signature(function)
    for p in (p for p in sig.parameters.values() if p.name != "self"):
        if p.kind is inspect.Parameter.VAR_POSITIONAL:
            raise TypeError("function with *args not supported")
        elif p.kind is inspect.Parameter.VAR_KEYWORD:
            raise TypeError("function with **kwargs not supported")
        schema = params.get(p.name)
        if schema:
            schema = copy(schema)
            schema.required = p.default is p.empty
            schema.default = p.default if p.default is not p.empty else None
            result[p.name] = schema
        elif p.default is p.empty:
            raise TypeError("required parameter in function but not in params: {}".format(p.name)) 
    return result


def validate(params=None, returns=None):
    """
    Decorate a function to validate its input parameters and return value.
    Whether a parameter is required and any default value is defined by the
    function, not its schema specification. If a parameter is omitted from the
    params schema, it must have a default value.     
    """
    def decorator(function):
        _params = function_params(function, params)
        def wrapper(wrapped, instance, args, kwargs):
            return call(wrapped, args, kwargs, _params, returns)
        return wrapt.decorator(wrapper)(function)
    return decorator
