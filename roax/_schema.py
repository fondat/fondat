"""Internal module to define, encode, decode and validate JSON data structures."""

import base64
import binascii
import collections.abc
import copy
import csv
import datetime
import inspect
import io
import isodate
import json
import re
import uuid
import wrapt


def _csv_encode(value):
    sio = io.StringIO()
    csv.writer(sio).writerow(value)
    return sio.getvalue().rstrip("\r\n")


def _csv_decode(value):
    return csv.reader([value]).__next__()


class SchemaError(Exception):
    """Raised if a value does not conform to its schema."""

    def __init__(self, msg, pointer=None):
        """Initialize the schema error."""
        self.msg = str(msg)
        self.pointer = pointer

    def push(self, name):
        """Push a name in front of the pointer."""
        name = str(name)
        self.pointer = name if not self.pointer else f"{name}/{self.pointer}"

    def __str__(self):
        result = []
        if self.pointer is not None:
            result.append(self.pointer)
        if self.msg is not None:
            result.append(self.msg)
        return ": ".join(result)


class _type:
    """
    Base class for all schema types.

    Parameters and instance variables:
    • python_type: Python data type.
    • json_type: JSON schema data type.
    • format: More finely defines the data type.
    • content_type: Content type used when value is expressed in a body.  ["text/plain"]
    • enum: A list of values that are valid.
    • nullable: Allow None as a valid value.
    • default: Default value if the item value is not supplied.
    • description: A description of the schema.
    • example: An example of an instance for this schema.
    • deprecated: Schema should be transitioned out of usage.
    """

    def __init__(
        self,
        *,
        python_type=object,
        json_type=None,
        format=None,
        content_type="text/plain",
        enum=None,
        default=None,
        description=None,
        example=None,
        nullable=False,
        deprecated=False,
    ):
        super().__init__()
        self.python_type = python_type
        self.json_type = json_type
        self.format = format
        self.content_type = content_type
        self.nullable = nullable
        self.enum = enum
        self.default = default
        self.description = description
        self.example = example
        self.deprecated = deprecated

    def _nullable(self):
        """Return if value can be `None`. Can be overridden by subclasses."""
        return self.nullable

    def validate(self, value):
        """Validate value against the schema."""
        if value is None and not self._nullable():
            raise SchemaError("value is not nullable")
        if value is not None and not isinstance(value, self.python_type):
            raise SchemaError(
                f"value '{value}' does not match expected type {self.python_type.__name__}"
            )
        if value is not None and self.enum is not None and value not in self.enum:
            if len(self.enum) == 1:
                raise SchemaError(f"value must be {list(self.enum)[0]}")
            else:
                raise SchemaError(
                    f"value '{value}' must be one of: {', '.join([self.str_encode(v) for v in self.enum])}"
                )

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
        if self.example:
            result["example"] = self.json_encode(self.example)
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
            raise SchemaError(ve) from ve

    def copy(self):
        """Return a copy of the schema type."""
        return copy.deepcopy(self)


def _required(required):
    if isinstance(required, str):
        required = (item.lstrip().rstrip() for item in required.split(","))
        required = (item for item in required if item)
    return set(required)


class _dict(_type):
    """
    Schema type for dictionaries.

    Parameters and instance variables:
    • properties: A mapping of name to schema.
    • content_type: Content type used when value is expressed in a body.  ["application/json"]
    • additional: Additional unvalidated properties are allowed.
    • nullable: Allow None as a valid value.
    • required: Set of property names that are required.
    • default: Default value if the item value is not supplied.
    • description: A description of the schema.
    • example: An example of an instance for this schema.
    """

    def __init__(
        self,
        properties,
        required=set(),
        *,
        content_type="application/json",
        additional=False,
        **kwargs,
    ):
        super().__init__(
            python_type=collections.abc.Mapping,
            json_type="object",
            content_type=content_type,
            **kwargs,
        )
        self.properties = properties
        self.required = _required(required)
        self.additional = additional

    def __contains__(self, value):
        return value in self.properties

    def __getitem__(self, key):
        return self.properties[key]

    def __iter__(self):
        return self.properties.__iter__()

    def get(self, key, default=None):
        return self.properties.get(key, default)

    def _process(self, method, value):
        if value is None:
            return None
        result = {}
        for k, v in value.items():
            try:
                if k in self.properties:
                    result[k] = getattr(self.properties[k], method)(v)
                else:
                    if not self.additional:
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
            if (
                key not in value
                and key not in self.required
                and schema.default is not None
            ):
                if result is None:
                    result = dict(value)
                result[key] = schema.default
        return result if result else value

    def validate(self, value):
        """Validate value against the schema."""
        super().validate(value)
        if value is not None:
            for property in self.required:
                if property not in value:
                    raise SchemaError("value required", property)
            self._process("validate", value)

    @property
    def json_schema(self):
        """JSON schema representation of the schema."""
        result = super().json_schema
        result["properties"] = {k: v.json_schema for k, v in self.properties.items()}
        result["required"] = list(self.required)
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

    def copy(self, properties=None, required=None):
        """Make a copy of the schema, specifying a subset of properties."""
        result = super().copy()
        if properties is not None:
            result.properties = {k: v for k, v in result.properties if k in properties}
        if required is not None:
            result.required = _required(required)
        return result

    def strip(self, value):
        """Return a copy of the value with only properties specified in the schema."""
        return {k: v for k, v in value.items() if k in self.properties}


class _list(_type):
    """
    Schema type for lists.

    Parameters and instance variables:
    • items: Schema which all items must adhere to.
    • content_type: Content type used when value is expressed in a body.  ["application/json"]
    • min_items: The minimum number of items required.
    • max_items: The maximum number of items required.
    • unique_items: All items must have unique values.
    • nullable: Allow None as a valid value.
    • default: Default value if the item value is not supplied.
    • description: A description of the schema.
    • example: An example of an instance for this schema.

    List values are represented in JSON as an array and string as comma-separated
    values.
    """

    def __init__(
        self,
        items,
        *,
        content_type="application/json",
        min_items=0,
        max_items=None,
        unique_items=False,
        **kwargs,
    ):
        super().__init__(
            python_type=collections.abc.Sequence,
            json_type="array",
            content_type=content_type,
            **kwargs,
        )
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
                raise SchemaError(f"expecting minimum number of {self.min_items} items")
            if self.max_items is not None and len(value) > self.max_items:
                raise SchemaError(f"expecting maximum number of {self.max_items} items")
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

    def bin_encode(self, value):
        """Encode the value into binary representation."""
        if value is not None:
            try:
                return json.dumps(self.json_encode(value)).encode()
            except (TypeError, ValueError) as e:
                raise SchemaError(str(ve)) from e

    def bin_decode(self, value):
        """Decode the value from binary representation."""
        if value is not None:
            try:
                return self.json_decode(json.loads(value.decode()))
            except (TypeError, ValueError) as e:
                raise SchemaError(str(ve)) from e


class _set(_type):
    """
    Schema type for sets.

    Parameters and instance variables:
    • items: Schema which set items must adhere to.
    • content_type: Content type used when value is expressed in a body.
    • nullable: Allow None as a valid value.
    • required: Value is mandatory.
    • default: Default value if the item value is not supplied.
    • description: A description of the schema.
    • example: An example of an instance for this schema.

    Set values are represented in JSON as an array and string as comma-separated
    values.
    """

    def __init__(self, items, *, content_type="application/json", **kwargs):
        """
        Initialize set schema.

        """
        super().__init__(
            python_type=set, json_type="array", content_type=content_type, **kwargs
        )
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
        return _csv_encode(result)

    def str_decode(self, value):
        """Decode the value from string representation."""
        if value is not None:
            csv = _csv_decode(value)
            value = self._process("str_decode", csv)
            if len(value) != len(csv):
                raise SchemaError("expecting items to be unique")
        self.validate(value)
        return value

    def bin_encode(self, value):
        """Encode the value into binary representation."""
        if value is not None:
            try:
                return json.dumps(self.json_encode(value)).encode()
            except (TypeError, ValueError) as e:
                raise SchemaError(str(ve)) from e

    def bin_decode(self, value):
        """Decode the value from binary representation."""
        if value is not None:
            try:
                return self.json_decode(json.loads(value.decode()))
            except (TypeError, ValueError) as e:
                raise SchemaError(str(ve)) from e


class _str(_type):
    """
    Schema type for Unicode character strings.

    Parameters and instance variables:
    • content_type: Content type used when value is expressed in a body.  ["text/plain"]
    • min_length: Minimum character length of the string.
    • max_length: Maximum character length of the string.
    • pattern: Regular expression that the string must match.
    • format: More finely defines the data type.
    • nullable: Allow None as a valid value.
    • default: Default value if the item value is not supplied.
    • enum: A list of values that are valid.
    • description: A description of the schema.
    • example: An example of an instance for this schema.
    """

    def __init__(self, *, min_length=0, max_length=None, pattern=None, **kwargs):
        super().__init__(python_type=str, json_type="string", **kwargs)
        self.min_length = min_length
        self.max_length = max_length
        self.pattern = re.compile(pattern) if pattern is not None else None

    def validate(self, value):
        """Validate value against the schema."""
        super().validate(value)
        if value is not None:
            if len(value) < self.min_length:
                raise SchemaError(f"expecting minimum length of {self.min_length}")
            if self.max_length is not None and len(value) > self.max_length:
                raise SchemaError(f"expecting maximum length of {self.max_length}")
            if self.pattern is not None and not self.pattern.match(value):
                raise SchemaError(f"expecting pattern: {self.pattern.pattern}")

    @property
    def json_schema(self):
        """JSON schema representation of the schema."""
        result = super().json_schema
        if self.min_length != 0:
            result["minLength"] = self.min_length
        if self.max_length is not None:
            result["maxLength"] = self.max_length
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
                raise SchemaError(f"expecting minimum value of {self.minimum}")
            if self.maximum is not None and value > self.maximum:
                raise SchemaError(f"expecting maximum value of {self.maximum}")

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

    Parameters and instance variables:
    • content_type: Content type used when value is expressed in a body.  ["text/plain"]
    • minimum: Inclusive lower limit of the value.
    • maximum: Inclusive upper limit of the value.
    • nullable: Allow None as a valid value.
    • default: Default value if the item value is not supplied.
    • enum: A list of values that are valid.
    • description: A description of the schema.
    • example: An example of an instance for this schema.
    """

    def __init__(self, **kwargs):
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

    Parameters and instance variables:
    • content_type: Content type used when value is expressed in a body.  ["text/plain"]
    • minimum: Inclusive lower limit of the value.
    • maximum: Inclusive upper limit of the value.
    • nullable: Allow None as a valid value.
    • default: Default value if the item value is not supplied.
    • enum: A list of values that are valid.
    • description: A description of the schema.
    • example: An example of an instance for this schema.
    """

    def __init__(self, **kwargs):
        super().__init__(
            python_type=float, json_type="number", format="double", **kwargs
        )

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

    Parameters and instance variables:
    • content_type: Content type used when value is expressed in a body.  ["text/plain"]
    • nullable: Allow None as a valid value.
    • default: Default value if the item value is not supplied.
    • description: A description of the schema.
    • example: An example of an instance for this schema.
    """

    def __init__(self, **kwargs):
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
    
    Parameters and instance variables:
    • content_type: Content type used when value is expressed in a body.  ["application/octet-stream"]
    • format: More finely defines the data type.  {"byte", "hex", "binary"}.
    • nullable: Allow None as a valid value.
    • default: Default value if the item value is not supplied.
    • description: A description of the schema.
    • example: An example of an instance for this schema.

    In "byte" format, a byte sequence is represented as a base64-encoded string.
    Example: "Um9heCBpcyBhIHBhcnQgb2YgYSBjb21wbGV0ZSBicmVha2Zhc3QuCg==".

    In "hex" format, a byte sequence is represented as string of hexadecimal
    numbers. Example: "54776f2073636f6f7073206f662072616973696e732e".

    In "binary" format, a byte sequence is represented as a raw sequence of bytes.
    For this reason, it cannot be expressed in string or JSON representations. 
    """

    def __init__(
        self, *, format="byte", content_type="application/octet-stream", **kwargs
    ):
        valid_formats = {"byte", "hex", "binary"}
        if format not in valid_formats:
            raise ValueError(f"format must be one of {valid_formats}")
        super().__init__(
            python_type=bytes,
            json_type="string",
            format=format,
            content_type=content_type,
            **kwargs,
        )

    def json_encode(self, value):
        """Encode the value into JSON object model representation."""
        if self.format == "binary":
            raise SchemaError(
                "binary format cannot be encoded into JSON representation"
            )
        return self.str_encode(value)

    def json_decode(self, value):
        """Decode the value from JSON object model representation."""
        if self.format == "binary":
            raise SchemaError(
                "binary format cannot be decoded from JSON representation"
            )
        return self.str_decode(value)

    def str_encode(self, value):
        """Encode the value into string representation."""
        self.validate(value)
        if self.format == "binary":
            raise SchemaError(
                "binary format cannot be encoded into string representation"
            )
        elif value is None:
            return None
        elif self.format == "hex":
            return value.hex()
        elif self.format == "byte":
            return base64.b64encode(value).decode()

    def str_decode(self, value):
        """Decode the value from string representation."""
        if self.format == "binary":
            raise SchemaError(
                "binary format cannot be decoded from string representation"
            )
        elif value is None:
            return None
        elif self.format == "hex":
            try:
                result = bytes.fromhex(value)
            except ValueError as ve:
                raise SchemaError("expecting a hexadecimal encoded value") from ve
        elif self.format == "byte":
            try:
                result = None if value is None else base64.b64decode(value)
            except binascii.Error as be:
                raise SchemaError("expecting a base64-encoded value") from be
        self.validate(result)
        return result

    def bin_encode(self, value):
        """Encode the value into binary representation."""
        return value if self.format == "binary" else super().bin_encode(value)

    def bin_decode(self, value):
        """Decode the value from binary representation."""
        return value if self.format == "binary" else super().bin_decode(value)


class _date(_type):
    """
    Schema type for date values.

    Parameters and instance variables:
    • content_type: Content type used when value is expressed in a body.  ["text/plain"]
    • nullable: Allow None as a valid value.
    • default: Default value if the item value is not supplied.
    • enum: A list of values that are valid.
    • description: A description of the schema.
    • example: An example of an instance for this schema.

    Date values are represented in string and JSON values as an RFC 3339 date
    in a string. Example: "2018-06-16".
    """

    def __init__(self, **kwargs):
        super().__init__(
            python_type=datetime.date, json_type="string", format="date", **kwargs
        )

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
            value = isodate.date_isoformat(value)
        return value

    def str_decode(self, value):
        """Decode the value from string representation."""
        if value is not None:
            try:
                value = isodate.parse_date(value)
            except ValueError as ve:
                raise SchemaError("expecting an RFC 3339 date value") from ve
        self.validate(value)
        return value


class _datetime(_type):
    """
    Schema type for datetime values. It is highly recommended to always express
    datetime values in the UTC time zone.

    Datetime values are represented in string and JSON values as an RFC 3339 UTC
    date and time in a string. Example: "2018-06-16T12:34:56.789Z".

    Parameters and instance variables:
    • content_type: Content type used when value is expressed in a body.  ["text/plain"]
    • nullable: Allow None as a valid value.
    • default: Default value if the item value is not supplied.
    • enum: A list of values that are valid.
    • description: A description of the schema.
    • example: An example of an instance for this schema.
    • fractional: Include fractions of seconds.
    """

    _UTC = isodate.tzinfo.Utc()

    def __init__(self, fractional=False, **kwargs):
        super().__init__(
            python_type=datetime.datetime,
            json_type="string",
            format="date-time",
            **kwargs,
        )
        self.fractional = fractional
        self.isoformat = "%Y-%m-%dT%H:%M:%S.%fZ" if fractional else "%Y-%m-%dT%H:%M:%SZ"

    def _to_utc(self, value):
        if value.tzinfo is None:  # naive value interpreted as UTC
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
            value = isodate.datetime_isoformat(self._to_utc(value), self.isoformat)
        return value

    def str_decode(self, value):
        """Decode the value from string representation."""
        if value is not None:
            try:
                value = self._to_utc(isodate.parse_datetime(value))
            except ValueError as ve:
                raise SchemaError("expecting an RFC 3339 date-time value") from ve
            if value.microsecond and not self.fractional:  # truncate fractional value
                value -= datetime.timedelta(microseconds=value.microsecond)
        self.validate(value)
        return value


class _uuid(_type):
    """
    Schema type for universally unique identifiers.

    Parameters and instance variables:
    • content_type: Content type used when value is expressed in a body.  ["text/plain"]
    • nullable: Allow None as a valid value.
    • default: Default value if the item value is not supplied.
    • enum: A list of values that are valid.
    • description: A description of the schema.
    • example: An example of an instance for this schema.

    UUID values are represented in string and JSON values as a UUID string.
    Example: "035af02b-7ad7-4016-a101-96f8fc5ae6ec".
    """

    def __init__(self, **kwargs):
        super().__init__(
            python_type=uuid.UUID, json_type="string", format="uuid", **kwargs
        )

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
                value = uuid.UUID(value)
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

    Parameters and instance variables:
    • schemas: List of schemas to match against.
    • default: Default value if the item value is not supplied.
    • enum: A list of values that are valid.
    • description: A description of the schema.
    • example: An example of an instance for this schema.
    """

    def __init__(self, schemas, **kwargs):
        super().__init__(**kwargs)
        if not schemas:
            raise ValueError("schemas argument must be a list of schemas")
        for s1 in schemas:
            if not isinstance(s1, _dict):
                raise ValueError("all_of only supports dict schemas")
            if not s1.additional:
                raise ValueError("all_of schemas must enable additional properties")
            for s2 in schemas:
                if s1 is not s2 and set.intersection(
                    set(s1.properties), set(s2.properties)
                ):
                    raise ValueError("all_of schemas cannot share property names")
        self.schemas = schemas

    def _nullable(self):
        return super()._nullable() or [1 for n in self.schemas if n._nullable()]

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
    """Base class for `one_of` and `any_of` schema types."""

    def __init__(self, keyword, schemas, **kwargs):
        super().__init__(**kwargs)
        self.keyword = keyword
        self.schemas = schemas

    def _nullable(self):
        return super()._nullable() or [1 for n in self.schemas if n._nullable()]

    def _process(self, method, value):
        if value is None and self.nullable:  # don't introspect inner schemas
            return None
        results = []
        for schema in self.schemas:
            try:
                results.append(getattr(schema, method)(value))
            except SchemaError:
                pass
        return self._evaluate(results)

    def _evaluate(self, method, values):
        raise NotImplementedError

    def validate(self, value):
        """Validate value against the schema."""
        super().validate(value)
        self._process("validate", value)

    def match(self, value):
        """Return the first schema that matches the value, or `None`."""
        results = []
        for schema in self.schemas:
            try:
                schema.validate(value)
                results.append(schema)
            except SchemaError:
                pass
        try:
            return self._evaluate(results)
        except SchemaError:
            return None  # validation failure yields no match

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

    Parameters and instance variables:
    • schemas: List of schemas to match against.
    • default: Default value if the item value is not supplied.
    • enum: A list of values that are valid.
    • description: A description of the schema.
    • example: An example of an instance for this schema.
    """

    def __init__(self, schemas, **kwargs):
        super().__init__("anyOf", schemas, **kwargs)
        self.schemas = schemas

    def _evaluate(self, values):
        if len(values) == 0:
            raise SchemaError("value does not match any schema")
        return values[0]  # return first schema-processed value


class one_of(_xof):
    """
    Schema type that is valid if a value validates successfully against
    exactly one schema. Values are encoded/decoded using the sole matching
    schema.

    Parameters and instance variables:
    • schemas: List of schemas to match against.
    • default: Default value if the item value is not supplied.
    • enum: A list of values that are valid.
    • description: A description of the schema.
    • example: An example of an instance for this schema.
    """

    def __init__(self, schemas, **kwargs):
        super().__init__("oneOf", schemas, **kwargs)
        self.schemas = schemas

    def _evaluate(self, values):
        if len(values) == 0:
            raise SchemaError("value does not match any schema")
        elif len(values) > 1:
            raise SchemaError("value matches more than one schema")
        return values[0]  # return matching value


class reader(_type):
    """
    Schema type for file-like object to read binary content. Allows large-payload
    values to be transmitted without allocating all in memory. In operations, this
    schema type can only used in _body parameter and return values.

    Parameters and instance variables:
    • content_type: Content type used when value is expressed in a body.  ["application/octet-stream"]
    """

    def __init__(self, *, content_type="application/octet-stream", **kwargs):
        super().__init__(
            json_type="string", format="binary", content_type=content_type, **kwargs
        )

    def validate(self, value):
        """Validate value against the schema."""
        super().validate(value)
        if not callable(getattr(value, "read", None)):
            raise SchemaError("expecting readable file-like object")


def call(function, args, kwargs, params=None, returns=None):
    """
    Call a function, validating its input parameters and return value.

    Parameters:
    • function: Function to call.
    • args: Positional arguments to pass to function.
    • kwargs: Keyword arguments to pass to function.
    • params: Mapping of parameter names to associated schemas.
    • returns: Schema of expected function return value.

    Whether a parameter is required and any default value is defined by the
    function, not its schema specification. If a parameter is omitted from the
    params schema, its value is not validated.
    """
    sig = inspect.signature(function)
    if len(args) > len(
        [
            p
            for p in sig.parameters.values()
            if p.kind in (p.POSITIONAL_ONLY, p.POSITIONAL_OR_KEYWORD)
        ]
    ):
        raise TypeError("too many positional arguments")
    build = {p.name: v for p, v in zip(sig.parameters.values(), args)}
    for k, v in kwargs.items():
        if k in build:
            raise TypeError(f"multiple values for argument: {k}")
        build[k] = v
    args = []
    kwargs = {}
    for p in sig.parameters.values():
        if p.kind is inspect.Parameter.VAR_POSITIONAL:
            raise TypeError(
                "parameter validation does not support functions with *args"
            )
        elif p.kind is inspect.Parameter.VAR_KEYWORD:
            raise TypeError(
                "parameter validation does not support functions with **kwargs"
            )
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
        if p.kind in (
            inspect.Parameter.POSITIONAL_ONLY,
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
        ):
            args.append(value)
        elif p.kind is inspect.Parameter.KEYWORD_ONLY:
            kwargs[p.name] = value
        else:
            raise TypeError(f"unrecognized type for parameter {p.name}")
    result = function(*args, **kwargs)
    if returns is not None:
        try:
            returns.validate(result)
        except SchemaError as se:
            raise ValueError(f"return value: {se.msg}") from se
    return result


def function_params(function, params):
    """
    Return a subset of the passed parameter schemas, based on the arguments of
    a defined function. The default properties are overridden, based function's
    arguments.
    """
    params = params or {}
    properties = {}
    required = set()
    sig = inspect.signature(function)
    for p in (p for p in sig.parameters.values() if p.name != "self"):
        if p.kind is inspect.Parameter.VAR_POSITIONAL:
            raise TypeError("function with *args not supported")
        elif p.kind is inspect.Parameter.VAR_KEYWORD:
            raise TypeError("function with **kwargs not supported")
        schema = params.get(p.name)
        if schema:
            if isinstance(schema, reader) and p.name != "_body":
                raise TypeError(f"parameter cannot use reader schema type: {p.name}")
            if p.default is p.empty:
                required.add(p.name)
            schema = copy.copy(schema)
            schema.default = p.default if p.default is not p.empty else None
            properties[p.name] = schema
        elif p.default is p.empty:
            raise TypeError(
                f"required parameter in function but not in params: {p.name}"
            )
    return _dict(properties, required)


def validate(params=None, returns=None):
    """
    Decorate a function to validate its input parameters and return value.
    Parameter default values are defined by the function, not parameter schemas.
    If a parameter is omitted from the params schema, it must have a default
    value.     
    """

    def decorator(function):
        _params = function_params(function, params)

        def wrapper(wrapped, instance, args, kwargs):
            return call(wrapped, args, kwargs, _params, returns)

        return wrapt.decorator(wrapper)(function)

    return decorator
