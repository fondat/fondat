"""Internal module to define, encode, decode and validate JSON data structures."""

# Copyright © 2015–2018 Paul Bryan.
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import binascii
import csv
import inspect
import isodate
import json
import re
import wrapt

from base64 import b64decode, b64encode
from collections.abc import Mapping, Sequence
from datetime import datetime
from copy import copy
from io import IOBase, StringIO
from uuid import UUID


def _csv_encode(value):
    sio = StringIO()
    csv.writer(sio).writerow()
    return sio.getvalue().rstrip("\r\n")
    
def _csv_decode(value):
    return csv.reader([value]).__next__()


class SchemaError(Exception):
    """Raised if a value does not conform to its schema."""

    def __init__(self, msg, pointer=None):
        """Initialize the schema error."""
        self.msg = msg
        self.pointer = pointer

    def push(self, name):
        """Push a name in front of the pointer."""
        name = str(name)
        self.pointer = name if not self.pointer else "{}/{}".format(name, self.pointer)

    def __str__(self):
        result = []
        if self.pointer is not None:
            result.append(self.pointer)
        if self.msg is not None:
            result.append(self.msg)
        return ": ".join(result)


class _type:
    """Base class for all schema types."""

    def __init__(
            self, *, python_type=object, json_type=None, format=None,
            content_type="text/plain", enum=None, required=True,
            default=None, description=None, examples=None, nullable=False,
            deprecated=False):
        """
        Initialize the schema type.

        :param python_type: Python data type.
        :param json_type: JSON schema data type.
        :param format: More finely defines the data type.
        :param content_type: Content type used when value is expressed in a body. (default: text/plain)
        :param enum: A list of values that are valid.
        :param nullable: Allow None as a valid value.
        :param required: Value is mandatory.
        :param default: Default value if the item value is not supplied.
        :param description: A description of the schema.
        :param examples: A list of example valid values.
        :param deprecated: schema should be transitioned out of usage.
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
        """Validate value against the schema."""
        if value is None and not self.nullable:
            raise SchemaError("value cannot be None")
        if value is not None and not isinstance(value, self.python_type):
            raise SchemaError('value "{}" does not match expected type {}'.format(value, self.python_type.__name__))
        if value is not None and self.enum is not None and value not in self.enum:
            raise SchemaError('value "{}" is not one of: {}'.format(value, ", ".join([self.str_encode(v) for v in self.enum])))

    @property
    def json_schema(self):
        """JSON schema representation of the schema."""
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

    def json_encode(self, value):
        """
        Encode the value into JSON object model representation. The method does not
        dump the value as JSON text; it represents the value such that the Python
        JSON module can dump as JSON text if required.
        """
        raise NotImplementedError

    def json_decode(self, value):
        """
        Decode the value from JSON object model representation. The method does not
        parse the value as JSON text; it takes a Python value as though the Python JSON
        module loaded the JSON text.
        """
        raise NotImplementedError

    def str_encode(self, value):
        """Encode the value into string representation."""
        raise NotImplementedError

    def str_decode(self, value):
        """Decode the value from string representation."""
        raise NotImplementedError

    def bin_encode(self, value):
        """Encode the value into binary representation."""
        return None if value is None else self.str_encode(value).encode()

    def bin_decode(self, value):
        """Decode the value from binary representation."""
        try:
            return None if value is None else self.str_decode(value.decode())
        except ValueError as ve:
            raise SchemaError("binary decode failed") from ve


class _dict(_type):
    """
    Schema type for dictionaries.
    """

    def __init__(self, properties, *, content_type="application/json", additional_properties=False, **kwargs):
        """
        Initialize dictionary schema.

        :param properties: A mapping of name to schema.
        :param content_type: Content type used when value is expressed in a body. (default: application/json)
        :param additional_properties: Additional unvalidated properties are allowed.
        :param nullable: Allow None as a valid value.
        :param required: Value is mandatory.
        :param default: Default value if the item value is not supplied.
        :param description: A description of the schema.
        :param examples: A list of example valid values.
        """
        super().__init__(python_type=Mapping, json_type="object", content_type=content_type, **kwargs)
        self.properties = properties
        self.additional_properties = additional_properties

    def _process(self, method, value):
        if value is None:
            return None
        result = {}
        for k, v in value.items():
            try:
                if k in self.properties:
                    result[k] = getattr(self.properties[k], method)(v)
                else:
                    if not self.additional_properties:
                        raise SchemaError("unexpected property")
                    result[k] = v  # pass through                    
            except SchemaError as se:
                se.push(k)
                raise
        return result

    def defaults(self, value):
        """Populate missing dictionary properties with default values."""
        if value is None:
            return None
        result = None
        for key, schema in self.properties.items():
            if key not in value and not schema.required and schema.default is not None:
                if result is None:
                    result = dict(value)
                result[key] = schema.default
        return result if result else value

    def validate(self, value):
        """Validate value against the schema."""
        super().validate(value)
        if value is not None:
            self._process("validate", value)
            for key, schema in self.properties.items():
                if schema.required and key not in value:
                    raise SchemaError("value required", key)

    @property
    def json_schema(self):
        """JSON schema representation of the schema."""
        result = super().json_schema
        result["properties"] = {k: v.json_schema for k, v in self.properties.items()}
        result["required"] = [k for k, v in self.properties.items() if v.required]
        return result

    def json_encode(self, value):
        """Encode the value into JSON object model representation."""
        value = self.defaults(value)
        self.validate(value)
        if value is not None and not isinstance(value, dict):
            value = dict(value)  # make JSON encoder happy
        return self._process("json_encode", value)

    def json_decode(self, value):
        """Decode the value from JSON object model representation."""
        value = self.defaults(self._process("json_decode", value))
        self.validate(value)
        return value

    def str_encode(self, value):
        """Encode the value into string representation."""
        return json.dumps(self.json_encode(value))

    def str_decode(self, value):
        """Decode the value from string representation."""
        return self.json_decode(json.loads(value))


class _list(_type):
    """
    Schema type for lists.

    List values are represented in JSON as an array and string as comma-separated
    values.
    """

    def __init__(self, items, *, content_type="application/json", min_items=0, max_items=None, unique_items=False, **kwargs):
        """
        Initialize list schema.

        :params items: Schema which all items must adhere to.
        :param content_type: Content type used when value is expressed in a body. (default: application/json)
        :params min_items: The minimum number of items required.
        :params max_items: The maximum number of items required.
        :params unique_items: All items must have unique values.
        :param nullable: Allow None as a valid value.
        :param required: Value is mandatory.
        :param default: Default value if the item value is not supplied.
        :param description: A description of the schema.
        :param examples: A list of example valid values.
        """
        super().__init__(python_type=Sequence, json_type="array", content_type=content_type, **kwargs)
        self.items = items
        self.min_items = min_items
        self.max_items = max_items
        self.unique_items = unique_items

    def _process(self, method, value):
        if value is None:
            return None
        method = getattr(self.items, method)
        result = []
        try:
            for n, item in zip(range(len(value)), value):
                result.append(method(item))
        except SchemaError as se:
            se.push(n)
            raise
        return result

    @staticmethod
    def _check_not_str(value):
        if isinstance(value, str):  # strings are iterable, but not what we want
            raise SchemaError("expecting a Sequence type")

    def validate(self, value):
        """Validate value against the schema."""
        self._check_not_str(value)
        super().validate(value)
        if value is not None:
            self._process("validate", value)
            if len(value) < self.min_items:
                raise SchemaError("expecting minimum number of {} items".format(self.min_items))
            if self.max_items is not None and len(value) > self.max_items:
                raise SchemaError("expecting maximum number of {} items".format(self.max_items))
            if self.unique_items and len(value) != len(set(value)):
                raise SchemaError("expecting items to be unique")

    @property
    def json_schema(self):
        """JSON schema representation of the schema."""
        result = super().json_schema
        result["items"] = self.items.json_schema
        if self.min_items != 0:
            result["minItems"] = self.min_items
        if self.max_items is not None:
            result["maxItems"] = self.max_items
        if self.unique_items:
            result["uniqueItems"] = True
        return result

    def json_encode(self, value):
        """Encode the value into JSON object model representation."""
        self._check_not_str(value)
        self.validate(value)
        if value is not None and not isinstance(value, list):
            value = list(value)  # make JSON encoder happy
        return self._process("json_encode", value)

    def json_decode(self, value):
        """Decode the value from JSON object model representation."""
        self._check_not_str(value)
        result = self._process("json_decode", value)
        self.validate(result)
        return result

    def str_encode(self, value):
        """Encode the value into string representation."""
        self.validate(value)
        if value is None:
            return None
        return _csv_encode(self._process("str_encode", value))

    def str_decode(self, value):
        """Decode the value from string representation."""
        if value is not None:
            value = self._process("str_decode", _csv_decode(value))
        self.validate(value)
        return value


class _set(_type):
    """
    Schema type for sets.

    Set values are represented in JSON as an array and string as comma-separated
    values.
    """

    def __init__(self, items, *, content_type="application/json", **kwargs):
        """
        Initialize set schema.

        :params items: Schema which set items must adhere to.
        :param content_type: Content type used when value is expressed in a body. (default: application/json)
        :param nullable: Allow None as a valid value.
        :param required: Value is mandatory.
        :param default: Default value if the item value is not supplied.
        :param description: A description of the schema.
        :param examples: A list of example valid values.
        """
        super().__init__(python_type=set, json_type="array", content_type=content_type, **kwargs)
        self.items = items

    def _process(self, method, value):
        if value is None:
            return None
        method = getattr(self.items, method)
        result = set()
        for item in value:
            result.add(method(item))
        return result

    def validate(self, value):
        """Validate value against the schema."""
        super().validate(value)
        if value is not None:
            self._process("validate", value)

    @property
    def json_schema(self):
        """JSON schema representation of the schema."""
        result = super().json_schema
        result["items"] = self.items.json_schema
        result["uniqueItems"] = True
        return result

    def json_encode(self, value):
        """Encode the value into JSON object model representation."""
        self.validate(value)
        if value is not None:
            value = list(self._process("json_encode", value))
            value.sort()
        return value

    def json_decode(self, value):
        """Decode the value from JSON object model representation."""
        if value is not None:
            result = self._process("json_decode", value)
            if len(result) != len(value):
                raise SchemaError("expecting items to be unique")
            value = result
        self.validate(value)
        return value

    def str_encode(self, value):
        """Encode the value into string representation."""
        self.validate(value)
        if value is None:
            return None
        result = list(self._process("str_encode", value))
        result.sort()
        return _csv_encode()

    def str_decode(self, value):
        """Decode the value from string representation."""
        if value is not None:
            csv = _csv_decode(value)
            value = self._process("str_decode", csv)
            if len(value) != len(csv):
                raise SchemaError("expecting items to be unique")
        self.validate(value)
        return value


class _str(_type):
    """
    Schema type for Unicode character strings.
    """

    def __init__(self, *, min_length=0, max_length=None, pattern=None, **kwargs):
        """
        Initialize string schema.

        :param content_type: Content type used when value is expressed in a body. (default: text/plain)
        :param min_length: Minimum character length of the string.
        :param max_length: Maximum character length of the string.
        :param pattern: Regular expression that the string must match.
        :param format: More finely defines the data type.
        :param nullable: Allow None as a valid value.
        :param required: Value is mandatory.
        :param default: Default value if the item value is not supplied.
        :param enum: A list of values that are valid.
        :param description: A description of the schema.
        :param examples: A list of example valid values.
        """
        super().__init__(python_type=str, json_type="string", **kwargs)
        self.min_length = min_length
        self.max_length = max_length
        self.pattern = re.compile(pattern) if pattern is not None else None

    def validate(self, value):
        """Validate value against the schema."""
        super().validate(value)
        if value is not None:
            if len(value) < self.min_length:
                raise SchemaError("expecting minimum length of {}".format(self.min_length))
            if self.max_length is not None and len(value) > self.max_length:
                raise SchemaError("expecting maximum length of {}".format(self.max_length))
            if self.pattern is not None and not self.pattern.match(value):
                raise SchemaError("expecting pattern: {}".format(self.pattern.pattern))

    @property
    def json_schema(self):
        """JSON schema representation of the schema."""
        result = super().json_schema
        if self.min_length != 0:
             result["minLength"] = self.min_len
        if self.max_length is not None:
            result["maxLength"] = self.max_len
        if self.pattern:
            result["pattern"] = self.pattern.pattern
        return result
 
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
        """Initialize number schema."""
        super().__init__(**kwargs)
        self.minimum = minimum
        self.maximum = maximum

    def validate(self, value):
        """Validate value against the schema."""
        super().validate(value)
        if value is not None:
            if self.minimum is not None and value < self.minimum:
                raise SchemaError("expecting minimum value of {}".format(self.minimum))
            if self.maximum is not None and value > self.maximum:
                raise SchemaError("expecting maximum value of {}".format(self.maximum))

    @property
    def json_schema(self):
        """JSON schema representation of the schema."""
        result = super().json_schema
        if self.minimum is not None:
            result["minimum"] = self.minimum
        if self.maximum is not None:
            result["maximum"] = self.maximum
        return result

    def json_encode(self, value):
        """Encode the value into JSON object model representation."""
        self.validate(value)
        return value

    def str_encode(self, value):
        """Encode the value into string representation."""
        self.validate(value)
        return None if value is None else str(value)


class _int(_number):
    """
    Schema type for integers.
    """

    def __init__(self, **kwargs):
        """
        Initialize integer schema.
        
        :param content_type: Content type used when value is expressed in a body. (default: text/plain)
        :param minimum: Inclusive lower limit of the value.
        :param maximum: Inclusive upper limit of the value.
        :param nullable: Allow None as a valid value.
        :param required: Value is mandatory.
        :param default: Default value if the item value is not supplied.
        :param enum: A list of values that are valid.
        :param description: A description of the schema.
        :param examples: A list of example valid values.
        """
        super().__init__(python_type=int, json_type="integer", format="int64", **kwargs)

    def validate(self, value):
        """Validate value against the schema."""
        super().validate(value)
        if isinstance(value, bool):  # bool is a subclass of int
            raise SchemaError("expecting int type")

    def json_decode(self, value):
        """Decode the value from JSON object model representation."""
        result = value
        if isinstance(result, float):
            result = result.__int__()
            if result != value:  # 1.0 == 1
                raise SchemaError("expecting integer value")
        self.validate(result)
        return result

    def str_decode(self, value):
        """Decode the value from string representation."""
        if value is not None:
            try:
                value = int(value)
            except ValueError as ve:
                raise SchemaError("expecting an integer value") from ve
        self.validate(value)
        return value


class _float(_number):
    """
    Schema type for floating point numbers.
    """

    def __init__(self, **kwargs):
        """
        Initialize floating point schema.

        :param content_type: Content type used when value is expressed in a body. (default: text/plain)
        :param minimum: Inclusive lower limit of the value.
        :param maximum: Inclusive upper limit of the value.
        :param nullable: Allow None as a valid value.
        :param required: Value is mandatory.
        :param default: Default value if the item value is not supplied.
        :param enum: A list of values that are valid.
        :param description: A description of the schema.
        :param examples: A list of example valid values.
        """
        super().__init__(python_type=float, json_type="number", format="double", **kwargs)

    def json_decode(self, value):
        """Decode the value from JSON object model representation."""
        value = value.__float__() if isinstance(value, int) else value
        self.validate(value)
        return value

    def str_decode(self, value):
        """Decode the value from string representation."""
        if value is not None:
            try:
                value = float(value)
            except ValueError as ve:
                raise SchemaError("expecting a number") from ve
        self.validate(value)
        return value


class _bool(_type):
    """
    Schema type for boolean values.
    """

    def __init__(self, **kwargs):
        """
        Initialize boolean schema.

        :param content_type: Content type used when value is expressed in a body. (default: text/plain)
        :param nullable: Allow None as a valid value.
        :param required: Value is mandatory.
        :param default: Default value if the item value is not supplied.
        :param description: A description of the schema.
        :param examples: A list of example valid values.
        """
        super().__init__(python_type=bool, json_type="boolean", **kwargs)

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
        if value is None:
            return None
        return "true" if value else "false"

    def str_decode(self, value):
        """Decode the value from string representation."""
        try:
            value = {None: None, "true": True, "false": False}[value]
        except KeyError:
            raise SchemaError("expecting true or false")
        self.validate(value)
        return value


class _bytes(_type):
    """
    Schema type for byte sequences.
    
    Two formats are supported: "byte" and "binary".

    In "byte" format, a byte sequence is represented as a base64-encoded string.
    Example: "Um9heCBpcyBhIHBhcnQgb2YgYSBjb21wbGV0ZSBicmVha2Zhc3QuCg==".
    
    In "binary" format, a byte sequence is represented as a raw sequence of bytes.
    For this reason, it cannot be expressed in string or JSON representation. 
    """

    def __init__(self, *, format="byte", content_type="application/octet-stream", **kwargs):
        """
        Initialize byte sequence schema.

        :param content_type: Content type used when value is expressed in a body. (default: application/octet-stream)
        :param format: More finely defines the data type {byte,binary}.
        :param nullable: Allow None as a valid value.
        :param required: Value is mandatory.
        :param default: Default value if the item value is not supplied.
        :param description: A description of the schema.
        :param examples: A list of example valid values.
        """
        if format not in {"byte", "binary"}:
            raise SchemaError("format must be one of {}".format(valid_formats))
        super().__init__(python_type=bytes, json_type="string", format=format, content_type=content_type, **kwargs)

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
        return None if value is None else b64encode(value).decode()

    def str_decode(self, value):
        """Decode the value from string representation."""
        if self.format == "binary":
            raise SchemaError("binary format cannot be decoded from string representation")
        try:
            value = None if value is None else b64decode(value)
        except binascii.Error as be:
            raise SchemaError("expecting a base64-encoded value") from be
        self.validate(value)
        return value

    def bin_encode(self, value):
        """Encode the value into binary representation."""
        return value if self.format == "binary" else super().bin_encode(value)

    def bin_decode(self, value):
        """Decode the value from binary representation."""
        return value if self.format == "binary" else super().bin_decode(value)


class _datetime(_type):
    """
    Schema type for datetime values.

    Datetime values are represented in string and JSON values as an ISO 8601 date
    and time in a string. Example: "2017-07-11T05:42:34Z".
    """

    _UTC = isodate.tzinfo.Utc()

    def __init__(self, **kwargs):
        """
        Initialize byte sequence schema.

        :param content_type: Content type used when value is expressed in a body. (default: text/plain)
        :param nullable: Allow None as a valid value.
        :param required: Value is mandatory.
        :param default: Default value if the item value is not supplied.
        :param enum: A list of values that are valid.
        :param description: A description of the schema.
        :param examples: A list of example valid values.
        """
        super().__init__(python_type=datetime, json_type="string", format="date-time", **kwargs)

    def _to_utc(self, value):
        if value.tzinfo is None: # naive value interpreted as UTC
            value = value.replace(tzinfo=isodate.tzinfo.Utc())
        return value.astimezone(self._UTC)

    def json_encode(self, value):
        """Encode the value into JSON object model representation."""
        return self.str_encode(value)

    def json_decode(self, value):
        """Decode the value from JSON object model representation."""
        return self.str_decode(value)

    def str_encode(self, value):
        """Encode the value into string representation."""
        self.validate(value)
        if value is not None:
            value = isodate.datetime_isoformat(self._to_utc(value))
        return value 

    def str_decode(self, value):
        """Decode the value from string representation."""
        if value is not None:
            try:
                value = self._to_utc(isodate.parse_datetime(value))
            except ValueError as ve:
                raise SchemaError("expecting an ISO 8601 date-time value") from ve
        self.validate(value)
        return value


class uuid(_type):
    """
    Schema type for universally unique identifiers.

    UUID values are represented in string and JSON values as a UUID string.
    Example: "035af02b-7ad7-4016-a101-96f8fc5ae6ec".
    """

    def __init__(self, **kwargs):
        """
        Initialize UUID schema.

        :param content_type: Content type used when value is expressed in a body. (default: text/plain)
        :param nullable: Allow None as a valid value.
        :param required: Value is mandatory.
        :param default: Default value if the item value is not supplied.
        :param enum: A list of values that are valid.
        :param description: A description of the schema.
        :param examples: A list of example valid values.
        """
        super().__init__(python_type=UUID, json_type="string", format="uuid", **kwargs)

    def json_encode(self, value):
        """Encode the value into JSON object model representation."""
        return self.str_encode(value)

    def json_decode(self, value):
        """Decode the value from JSON object model representation."""
        return self.str_decode(value)

    def str_encode(self, value):
        """Encode the value into string representation."""
        self.validate(value)
        return None if value is None else str(value)

    def str_decode(self, value):
        """Decode the value from string representation."""
        if value is not None:
            if not isinstance(value, str):
                raise SchemaError("expecting a string")
            try:
                value = UUID(value)
            except ValueError as ve:
                raise SchemaError("expecting string to contain UUID value") from ve
        self.validate(value)
        return value


class all_of(_type):
    """
    Schema type that is valid if a value validates successfully against all of the
    schemas. This only makes sense where all schemas are dictionaries and where:
    each dictionary allows additional properties and no dictionary defines the same
    property as another.
    """

    def __init__(self, schemas, **kwargs):
        """
        Initialize all-of schema.

        :params schemas: List of schemas to match against.
        :param required: Value is mandatory.
        :param default: Default value if the item value is not supplied.
        :param enum: A list of values that are valid.
        :param description: A description of the schema.
        :param examples: A list of example valid values.
        """
        super().__init__(**kwargs)
        if not schemas:
            raise ValueException("schemas argument must be a list of schemas")
        for s1 in schemas:
            if not isinstance(s1, _dict):
                raise ValueException("all_of only supports dict schemas")
            if not s1.additional_properties:
                raise ValueException("all_of schemas must enable additional_properties")
            for s2 in schemas:
                if s1 is not s2 and set.intersection(set(s1.properties), set(s2.properties)):
                    raise ValueException("all_of schemas cannot share property names")
        self.schemas = schemas

    def validate(self, value):
        """Validate value against the schema."""
        super().validate(value)
        if value is not None:
            for schema in self.schemas:
                schema.validate(value)

    @property
    def json_schema(self):
        """JSON schema representation of the schema."""
        result = super().json_schema
        result["schemas"] = self.schemas
        return result

    def json_encode(self, value):
        """Encode the value into JSON object model representation."""
        self.validate(value)
        if value is not None:
            for schema in self.schemas:
                value = schema.json_encode(value)
        return value

    def json_decode(self, value):
        """Decode the value from JSON object model representation."""
        if value is not None:
            for schema in self.schemas:
                value = schema.json_decode(value)
        self.validate(value)
        return value

    def str_encode(self, value):
        """Encode the value into string representation."""
        return json.dumps(self.json_encode(value))

    def str_decode(self, value):
        """Decode the value from string representation."""
        return self.json_decode(json.loads(value))


class _xof(_type):
    """TODO: Description."""

    def __init__(self, keyword, schemas, **kwargs):
        super().__init__(**kwargs)
        self.keyword = keyword
        self.schemas = schemas

    def _process(self, method, value):
        if value is None:
            return None
        results = []
        for schema in self.schemas:
            try:
                results.append(getattr(schema, method)(value))
            except SchemaError:
                pass
        return self._evaluate(results)

    def _evaluate(self, method, value):
        raise NotImplementedError

    def validate(self, value):
        """Validate value against the schema."""
        super().validate(value)
        self._process("validate", value)

    @property
    def json_schema(self):
        """JSON schema representation of the schema."""
        result = super().json_schema
        result["schemas"] = self.schemas
        return result

    def json_encode(self, value):
        """Encode the value into JSON object model representation."""
        self.validate(value)
        return self._process("json_encode", value)

    def json_decode(self, value):
        """Decode the value from JSON object model representation."""
        value = self._process("json_decode", value)
        self.validate(value)
        return value

    def str_encode(self, value):
        """Encode the value into string representation."""
        self.validate(value)
        return self._process("str_encode", value)

    def str_decode(self, value):
        """Decode the value from string representation."""
        value = self._process("str_decode", value)
        self.validate(value)
        return value


class any_of(_xof):
    """
    Schema type that is valid if a value validates successfully against any
    of the schemas. Values are encoded/decoded using the first valid
    matching schema in the list.
    """

    def __init__(self, schemas, **kwargs):
        """
        Initialize any-of schema.

        :param schemas: List of schemas to match against.
        :param required: Value is mandatory.
        :param default: Default value if the item value is not supplied.
        :param enum: A list of values that are valid.
        :param description: A description of the schema.
        :param examples: A list of example valid values.
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
        Initialize one-of schema.

        :param schemas: List of schemas to match against.
        :param required: Value is mandatory.
        :param default: Default value if the item value is not supplied.
        :param enum: A list of values that are valid.
        :param description: A description of the schema.
        :param examples: A list of example valid values.
        """
        super().__init__("oneOf", schemas, **kwargs)
        self.schemas = schemas

    def _evaluate(self, values):
        if len(values) == 0:
            raise SchemaError("value does not match any schema")
        elif len(values) > 1:
            raise SchemaError("value matches more than one schema")
        return values[0] # return first matching value


class reader(_type):
    """
    Schema type for file-like object to read binary content. Allows large-payload
    values to be transmitted without allocating all in memory. In operations, this
    schema type can only used in _body parameter and return values.
    """    
    def __init__(self, *, content_type="application/octet-stream", **kwargs):
        """
        Initialize reader schema.

        :param content_type: Content type used when value is expressed in a body. (default: application/octet-stream)
        """
        super().__init__(python_type=IOBase, json_type="string", format="binary", content_type=content_type, **kwargs)

    def validate(self, value):
        super().validate(value)
        if not value.readable():
            raise SchemaError("expecting readable file-like object")


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
                    se.push(p.name)
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
            if isinstance(schema, reader) and p.name != "_body":
                raise TypeError("parameter cannot use reader schema type: {}".format(p.name))
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
