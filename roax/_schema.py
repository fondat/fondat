"""Internal module to define, encode, decode and validate JSON data structures."""

import base64
import binascii
import collections.abc
import copy
import csv
import dataclasses
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


class SchemaError(ValueError):
    """
    Raised if a value does not conform to its schema.
    
    Parameters:
    • path: Python-style path to value with error.
    """

    def __init__(self, msg, path=None):
        self.msg = str(msg)
        self.path = path

    def __str__(self):
        result = []
        if self.path is not None:
            result.append(self.path)
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

    def __repr__(self):
        return self.python_type.__name__

    def _nullable(self):
        """Return if value can be None. Can be overridden by subclasses."""
        return self.nullable

    def validate(self, value):
        """Validate value against the schema."""
        if value is None and not self._nullable():
            raise SchemaError("value is not nullable")
        if value is not None and not isinstance(value, self.python_type):
            raise SchemaError(
                f"value {value} does not match expected type: {self.python_type.__name__}"
            )
        if value is not None and self.enum is not None and value not in self.enum:
            if len(self.enum) == 1:
                raise SchemaError(f"value must be {list(self.enum)[0]}")
            else:
                raise SchemaError(
                    f"value {value} is not one of: {', '.join([self.str_encode(v) for v in self.enum])}"
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
        required = required.replace(",", " ").split()
    return set(required)


class _dict(_type):
    """
    Schema type for dictionary.

    Parameters and instance variables:
    • props: A mapping of name to schema.
    • required: Set of property names that are required.
    • content_type: Content type used when value is expressed in a body.  ["application/json"]
    • additional: Additional unvalidated properties are allowed.
    • nullable: Allow None as a valid value.
    • default: Default value if the item value is not supplied.
    • description: A description of the schema.
    • example: An example of an instance for this schema.
    """

    def __init__(
        self,
        props,
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
        self.props = props
        self.required = _required(required)
        self.additional = additional

    def _process(self, method, value):
        if value is None:
            return None
        result = {}
        for k, v in value.items():
            try:
                if k in self.props:
                    result[k] = getattr(self.props[k], method)(v)
                else:
                    if not self.additional:
                        raise SchemaError("unexpected property")
                    result[k] = v  # pass through
            except SchemaError as se:
                se.path = f"/{k}{se.path}" if se.path else f"/{k}"
                raise
        return result

    def defaults(self, value):
        """Set default values."""
        if value is None:
            return None
        for prop, schema in self.props.items():
            if (
                prop not in value
                and prop not in self.required
                and schema.default is not None
            ):
                value[prop] = schema.default
            defaults = getattr(schema, "defaults", None)
            if defaults:
                defaults(value[prop])

    def validate(self, value):
        """Validate value against the schema."""
        super().validate(value)
        if value is not None:
            for prop in self.required:
                if prop not in value:
                    raise SchemaError("value required", prop)
            self._process("validate", value)

    @property
    def json_schema(self):
        """JSON schema representation of the schema."""
        result = super().json_schema
        result["properties"] = {k: v.json_schema for k, v in self.props.items()}
        result["required"] = list(self.required)
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
        return json.dumps(self.json_encode(value))

    def str_decode(self, value):
        """Decode the value from string representation."""
        return self.json_decode(json.loads(value))

    def copy(self, props=None, required=None):
        """Make a copy of the schema, specifying a subset of properties."""
        result = super().copy()
        if props is not None:
            result.props = {k: v for k, v in result.props if k in props}
        if required is not None:
            result.required = _required(required)
        return result

    def strip(self, value):
        """Return a copy of the value with only properties specified in the schema."""
        return {k: v for k, v in value.items() if k in self.props}


class _list(_type):
    """
    Schema type for list.

    Parameters and instance variables:
    • items: Schema which all items must adhere to.
    • content_type: Content type used when value is expressed in a body.  ["application/json"]
    • min: The minimum number of items required.
    • max: The maximum number of items required.
    • unique: All items must have unique values.
    • nullable: Allow None as a valid value.
    • default: Default value if the item value is not supplied.
    • description: A description of the schema.
    • example: An example of an instance for this schema.

    List values are represented in JSON as an array and string as
    comma-separated values.
    """

    def __init__(
        self,
        items,
        *,
        content_type="application/json",
        min=0,
        max=None,
        unique=False,
        **kwargs,
    ):
        super().__init__(
            python_type=collections.abc.Sequence,
            json_type="array",
            content_type=content_type,
            **kwargs,
        )
        self.items = items
        self.min = min
        self.max = max
        self.unique = unique

    def _process(self, method, value):
        if value is None:
            return None
        method = getattr(self.items, method)
        result = []
        try:
            for n, item in zip(range(len(value)), value):
                result.append(method(item))
        except SchemaError as se:
            se.path = f"/{n}{se.path}" if se.path else f"/{n}"
            raise
        return result

    def defaults(self, value):
        """Set default values."""
        defaults = getattr(self.items, "defaults", None)
        if defaults:
            for v in values:
                defaults(v)

    @staticmethod
    def _check_not_str(value):
        if isinstance(value, str):  # strings are iterable, but not what we want
            raise SchemaError("expecting a Sequence type")

    @staticmethod
    def _is_unique(value):
        try:
            computed = set(value)
        except:
            computed = []
            for v in value:
                if v not in computed:
                    computed.append(v)
        return len(value) == len(computed)

    def validate(self, value):
        """Validate value against the schema."""
        self._check_not_str(value)
        super().validate(value)
        if value is not None:
            self._process("validate", value)
            if len(value) < self.min:
                raise SchemaError(f"expecting minimum number of {self.min} items")
            if self.max is not None and len(value) > self.max:
                raise SchemaError(f"expecting maximum number of {self.max} items")
            if not self._is_unique(value):
                raise SchemaError("expecting list items to be unique")

    @property
    def json_schema(self):
        """JSON schema representation of the schema."""
        result = super().json_schema
        result["items"] = self.items.json_schema
        if self.min != 0:
            result["minItems"] = self.min
        if self.max is not None:
            result["maxItems"] = self.max
        if self.unique:
            result["uniqueItems"] = True
        return result

    def json_encode(self, value):
        """Encode the value into JSON object model representation."""
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
                raise SchemaError(str(e)) from e

    def bin_decode(self, value):
        """Decode the value from binary representation."""
        if value is not None:
            try:
                return self.json_decode(json.loads(value.decode()))
            except (TypeError, ValueError) as e:
                raise SchemaError(str(e)) from e


class _set(_type):
    """
    Schema type for set.

    Parameters and instance variables:
    • items: Schema which all items must adhere to.
    • content_type: Content type used when value is expressed in a body.  ["application/json"]
    • nullable: Allow None as a valid value.
    • default: Default value if the item value is not supplied.
    • description: A description of the schema.
    • example: An example of an instance for this schema.

    Set values are represented in JSON as an array and string as
    comma-separated values.
    """

    def __init__(self, items, *, content_type="application/json", **kwargs):
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

    def defaults(self, value):
        """Set default values."""
        defaults = getattr(self.items, "defaults", None)
        if defaults:
            for v in values:
                defaults(v)

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

    @staticmethod
    def _sort(value):
        result = value
        if result is not None:
            result = list(result)
            result.sort()
        return result

    def json_encode(self, value):
        """Encode the value into JSON object model representation."""
        self.validate(value)
        return self._sort(self._process("json_encode", value))

    def json_decode(self, value):
        """Decode the value from JSON object model representation."""
        if value is not None:
            result = self._process("json_decode", value)
            if len(result) != len(value):
                raise SchemaError("expecting set items to be unique")
            self.validate(result)
            value = result
        return value

    def str_encode(self, value):
        """Encode the value into string representation."""
        self.validate(value)
        if value is not None:
            value = _csv_encode(self._sort(self._process("str_encode", value)))
        return value

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
                raise SchemaError(str(e)) from e

    def bin_decode(self, value):
        """Decode the value from binary representation."""
        if value is not None:
            try:
                return self.json_decode(json.loads(value.decode()))
            except (TypeError, ValueError) as e:
                raise SchemaError(str(e)) from e


class _str(_type):
    """
    Schema type for Unicode character string.

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
    Schema type for integer.

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
    Schema type for floating point number.

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
    Schema type for boolean value.

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
    Schema type for byte sequence.
    
    Parameters and instance variables:
    • content_type: Content type used when value is expressed in a body.
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
    Schema type for date value.

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
    Schema type for datetime value. It is highly recommended to always express
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
    Schema type for universally unique identifier.

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
                if s1 is not s2 and set.intersection(set(s1.props), set(s2.props)):
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
    """Base class for one_of and any_of schema types."""

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
        """Return the first schema that matches the value, or None."""
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


class _dataclass(_type):
    """
    Schema type for data class.

    Parameters and instance variables:
    • cls: Data class.
    • required: Names of attributes that are required.
    • content_type: Content type used when value is expressed in a body.  ["application/json"]
    • nullable: Allow None as a valid value.
    • description: A description of the schema.
    • example: An example of an instance for this schema.
    """

    class _attrs:
        def __init__(self, cls):
            self.__dict__ = cls.__annotations__

    def __init__(
        self, cls, required=set(), *, content_type="application/json", **kwargs
    ):
        super().__init__(
            python_type=object, json_type="object", content_type=content_type, **kwargs
        )
        self.cls = cls
        self.required = _required(required)
        self.attrs = _dataclass._attrs(cls)

    def _process(self, method, value):
        if value is None:
            return None
        result = {}
        for name, schema in self.cls.__annotations__.items():
            try:
                v = getattr(value, name)
                if v is not None or name in self.required:
                    result[name] = getattr(schema, method)(v)
            except SchemaError as se:
                se.path = f"/{name}{se.path}" if se.path else f"/{name}"
                raise
        return result

    def defaults(self, value):
        """Set default values."""
        for name, schema in self.cls.__annotations__.items():
            attr = getattr(value, name)
            if name not in self.required and attr is None:
                setattr(value, name, schema.default)
            defaults = getattr(schema, "defaults", None)
            if defaults:
                defaults(attr)

    def validate(self, value):
        """Validate value against the schema."""
        super().validate(value)
        if value is not None:
            if not dataclasses.is_dataclass(value):
                raise SchemaError("expected dataclass")
            for name in self.required:
                if (
                    getattr(value, name) is None
                    and not self.cls.__annotations__[name].nullable
                ):
                    raise SchemaError("value required", name)
            self._process("validate", value)

    @property
    def json_schema(self):
        """JSON schema representation of the schema."""
        result = super().json_schema
        result["properties"] = {k: v.json_schema for k, v in vars(self.attrs).items()}
        result["required"] = list(self.required)
        return result

    def json_encode(self, value):
        """Encode the value into JSON object model representation."""
        self.validate(value)
        result = self._process("json_encode", value)
        if result is not None:
            for name in result:
                if result[name] is None and name not in self.required:
                    del result[name]
        return result

    def json_decode(self, value):
        """Decode the value from JSON object model representation."""
        if value is not None:
            result = {}
            for name, schema in self.cls.__annotations__.items():
                v = value.get(name)
                if v is not None or name in self.required:
                    try:
                        v = schema.json_decode(v)
                    except SchemaError as se:
                        se.path = f"/{name}{se.path}" if se.path else f"/{name}"
                        raise
                result[name] = v
            value = self.cls(**result)
        self.validate(value)
        return value

    def str_encode(self, value):
        """Encode the value into string representation."""
        return json.dumps(self.json_encode(value))

    def str_decode(self, value):
        """Decode the value from string representation."""
        return self.json_decode(json.loads(value))


def call(function, args, kwargs):
    """
    Call a function, validating its input parameters and return value.

    Parameters:
    • function: Function to call.
    • args: Positional arguments to pass to function.
    • kwargs: Keyword arguments to pass to function.
    """
    sig = inspect.signature(function)
    if len(args) > len(
        [
            p
            for p in sig.parameters.values()
            if p.kind in (p.POSITIONAL_ONLY, p.POSITIONAL_OR_KEYWORD)
        ]
    ):
        raise TypeError("too many positional parameters")
    params = {p.name: v for p, v in zip(sig.parameters.values(), args)}
    for k, v in kwargs.items():
        if k in params:
            raise TypeError(f"multiple values for parameter {k}")
        params[k] = v
    args = []
    kwargs = {}
    for p in sig.parameters.values():
        if p.kind is p.VAR_POSITIONAL:
            raise TypeError("function with *args unsupported in validate")
        elif p.kind is p.VAR_KEYWORD:
            raise TypeError("function with **kwargs unsupported in validate")
        if p.name in params:
            value = params[p.name]
            if p.name in function.__annotations__:
                schema = function.__annotations__[p.name]
                if isinstance(schema, reader) and p.name != "_body":
                    raise TypeError(f"cannot use reader schema for parameter {p.name}")
                elif isinstance(schema, _type):
                    try:
                        schema.validate(params[p.name])
                    except SchemaError as se:
                        se.path = f"{p.name}:{se.path}" if se.path else p.name
                        raise
        elif p.default is not p.empty:
            value = p.default
        else:
            raise SchemaError("missing required parameter", p.name)
        if p.kind in (p.POSITIONAL_ONLY, p.POSITIONAL_OR_KEYWORD):
            args.append(value)
        elif p.kind is inspect.Parameter.KEYWORD_ONLY:
            kwargs[p.name] = value
        else:
            raise TypeError(f"unrecognized kind for parameter {p.name}")
    result = function(*args, **kwargs)
    returns = function.__annotations__.get("return")
    if returns is not None and isinstance(returns, _type):
        returns.validate(result)
    return result


@wrapt.decorator
def validate(wrapped, instance, args, kwargs):
    """
    Decorate a function to validate its parameters and return value.

    Example:

    import roax.schema as schema

    @schema.validate
    def fn(a: schema.str(), b: schema.int()) -> schema.str():
        ...
    """
    return call(wrapped, args, kwargs)
