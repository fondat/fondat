"""Module to encode and decode values."""

from __future__ import annotations

import base64
import binascii
import collections.abc
import csv
import dataclasses
import datetime
import decimal
import enum
import functools
import io
import json
import typing
import uuid
import wrapt

from collections.abc import Iterable
from fondat.types import affix_type_hints
from fondat.validate import validate_arguments
from typing import Any, Union


NoneType = type(None)


class Codec:
    """Base class for codecs."""

    pass


class StrCodec(Codec):
    """Codec for Unicode character strings."""

    python_type = str
    json_type = str

    @validate_arguments
    def json_encode(self, value: str) -> str:
        return value

    @validate_arguments
    def json_decode(self, value: str) -> str:
        return value

    @validate_arguments
    def str_encode(self, value: str) -> str:
        return value

    @validate_arguments
    def str_decode(self, value: str) -> str:
        return value

    @validate_arguments
    def bytes_encode(self, value: str) -> bytes:
        return value.encode()

    @validate_arguments
    def bytes_decode(self, value: bytes) -> str:
        return value.decode()


class BytesCodec(Codec):
    """
    Codec for byte sequences.

    A byte sequence is represented in JSON and string values as a
    base64-encoded string. Example: "SGVsbG8gRm9uZGF0".
    """

    python_type = bytes
    json_type = str

    @validate_arguments
    def json_encode(self, value: bytes) -> str:
        return self.str_encode(value)

    @validate_arguments
    def json_decode(self, value: str) -> bytes:
        return self.str_decode(value)

    @validate_arguments
    def str_encode(self, value: bytes) -> str:
        return base64.b64encode(value).decode()

    @validate_arguments
    def str_decode(self, value: str) -> bytes:
        try:
            return base64.b64decode(value)
        except binascii.Error:
            raise ValueError("expecting a base64-encoded value")

    @validate_arguments
    def bytes_encode(self, value: bytes) -> bytes:
        return value

    @validate_arguments
    def bytes_decode(self, value: bytes) -> bytes:
        return value


class IntCodec(Codec):
    """Codec for integers."""

    python_type = int
    json_type = Union[int, float]

    @validate_arguments
    def json_encode(self, value: int) -> int:
        return value

    @validate_arguments
    def json_decode(self, value: IntCodec.json_type) -> int:
        result = value
        if isinstance(result, float):
            result = int(result)
            if result != value:  # 1.0 == 1
                raise TypeError("expecting int")
        return result

    @validate_arguments
    def str_encode(self, value: int) -> str:
        return str(value)

    @validate_arguments
    def str_decode(self, value: str) -> int:
        return int(value)

    @validate_arguments
    def bytes_encode(self, value: int) -> bytes:
        return self.str_encode(value).encode()

    @validate_arguments
    def bytes_decode(self, value: bytes) -> int:
        return self.str_decode(value.decode())


class FloatCodec(Codec):
    """Codec for floating point numbers."""

    python_type = float
    json_type = Union[int, float]

    @validate_arguments
    def json_encode(self, value: float) -> float:
        return value

    @validate_arguments
    def json_decode(self, value: FloatCodec.json_type) -> float:
        return float(value) if isinstance(value, int) else value

    @validate_arguments
    def str_encode(self, value: float) -> str:
        return str(value)

    @validate_arguments
    def str_decode(self, value: str) -> float:
        return float(value)

    @validate_arguments
    def bytes_encode(self, value: float) -> bytes:
        return self.str_encode(value).encode()

    @validate_arguments
    def bytes_decode(self, value: bytes) -> float:
        return self.str_decode(value.decode())


class BoolCodec(Codec):
    """Codec for boolean values."""

    python_type = bool
    json_type = bool

    @validate_arguments
    def json_encode(self, value: bool) -> bool:
        return value

    @validate_arguments
    def json_decode(self, value: bool) -> bool:
        return value

    @validate_arguments
    def str_encode(self, value: bool) -> str:
        return "true" if value else "false"

    @validate_arguments
    def str_decode(self, value: str) -> bool:
        try:
            return {"true": True, "false": False}[value]
        except KeyError:
            raise ValueError("expecting true or false")

    @validate_arguments
    def bytes_encode(self, value: bool) -> bytes:
        return self.str_encode(value).encode()

    @validate_arguments
    def bytes_decode(self, value: bytes) -> bool:
        return self.str_decode(value.decode())


class NoneCodec(Codec):
    """Codec for None."""

    python_type = NoneType
    json_type = NoneType

    @validate_arguments
    def json_encode(self, value: NoneType) -> NoneType:
        return None

    @validate_arguments
    def json_decode(self, value: NoneType) -> NoneType:
        return None

    @validate_arguments
    def str_encode(self, value: NoneType) -> str:
        return ""

    @validate_arguments
    def str_decode(self, value: str) -> NoneType:
        if value:
            raise ValueError("expected empty string")
        return None

    @validate_arguments
    def bytes_encode(self, value: NoneType) -> bytes:
        return self.str_encode(value).encode()

    @validate_arguments
    def bytes_decode(self, value: bytes) -> NoneType:
        return self.str_decode(value.decode())


class DecimalCodec(Codec):
    """
    Codec for decimal numbers.

    Decimal numbers are represented in JSON as strings, due to the imprecision
    of floating point numbers.
    """

    python_type = decimal.Decimal
    json_type = str

    @validate_arguments
    def json_encode(self, value: decimal.Decimal) -> str:
        return self.str_encode(value)

    @validate_arguments
    def json_decode(self, value: str) -> decimal.Decimal:
        return self.str_decode(value)

    @validate_arguments
    def str_encode(self, value: decimal.Decimal) -> str:
        return str(value)

    @validate_arguments
    def str_decode(self, value: str) -> decimal.Decimal:
        try:
            return decimal.Decimal(value)
        except decimal.InvalidOperation:
            raise ValueError("expecting a string containing decimal number")

    @validate_arguments
    def bytes_encode(self, value: decimal.Decimal) -> bytes:
        return self.str_encode(value).encode()

    @validate_arguments
    def bytes_decode(self, value: bytes) -> decimal.Decimal:
        return self.str_decode(value.decode())


class DateCodec(Codec):
    """
    Codec for date values.

    Date values are represented in string and JSON values as an RFC 3339 date
    in a string. Example: "2018-06-16".
    """

    python_type = datetime.date
    json_type = str

    @validate_arguments
    def json_encode(self, value: datetime.date) -> str:
        return self.str_encode(value)

    @validate_arguments
    def json_decode(self, value: str) -> datetime.date:
        return self.str_decode(value)

    @validate_arguments
    def str_encode(self, value: datetime.date) -> str:
        return value.isoformat()

    @validate_arguments
    def str_decode(self, value: str) -> datetime.date:
        return datetime.date.fromisoformat(value)

    @validate_arguments
    def bytes_encode(self, value: datetime.date) -> bytes:
        return self.str_encode(value).encode()

    @validate_arguments
    def bytes_decode(self, value: bytes) -> datetime.date:
        return self.str_decode(value.decode())


def _to_utc(value):
    if value.tzinfo is None:  # naive value interpreted as UTC
        value = value.replace(tzinfo=datetime.timezone.utc)
    return value.astimezone(datetime.timezone.utc)


class DatetimeCodec(Codec):
    """
    Codec for datetime values.

    Datetime values are represented in string and JSON values as an RFC 3339
    UTC date and time in a string. Example: "2018-06-16T12:34:56.789012Z".
    """

    python_type = datetime.datetime
    json_type = str

    @validate_arguments
    def json_encode(self, value: datetime.datetime) -> str:
        return self.str_encode(value)

    @validate_arguments
    def json_decode(self, value: str) -> datetime.datetime:
        return self.str_decode(value)

    @validate_arguments
    def str_encode(self, value: datetime.datetime) -> str:
        result = _to_utc(value).isoformat()
        if result.endswith("+00:00"):
            result = result[0:-6]
        if "+" not in result and not result.endswith("Z"):
            result = f"{result}Z"
        return result

    @validate_arguments
    def str_decode(self, value: str) -> datetime.datetime:
        if value.endswith("Z"):
            value = value[0:-1]
        return _to_utc(datetime.datetime.fromisoformat(value))

    @validate_arguments
    def bytes_encode(self, value: datetime.datetime) -> bytes:
        return self.str_encode(value).encode()

    @validate_arguments
    def bytes_decode(self, value: bytes) -> datetime.datetime:
        return self.str_decode(value.decode())


class UUIDCodec(Codec):
    """
    Codec for universally unique identifiers.

    UUID values are represented in string and JSON values as a UUID string.
    Example: "035af02b-7ad7-4016-a101-96f8fc5ae6ec".
    """

    python_type = uuid.UUID
    json_type = str

    @validate_arguments
    def json_encode(self, value: uuid.UUID) -> str:
        return self.str_encode(value)

    @validate_arguments
    def json_decode(self, value: str) -> uuid.UUID:
        return self.str_decode(value)

    @validate_arguments
    def str_encode(self, value: uuid.UUID) -> str:
        return str(value)

    @validate_arguments
    def str_decode(self, value: str) -> uuid.UUID:
        return uuid.UUID(value)

    @validate_arguments
    def bytes_encode(self, value: uuid.UUID) -> bytes:
        return self.str_encode(value).encode()

    @validate_arguments
    def bytes_decode(self, value: bytes) -> uuid.UUID:
        return self.str_decode(value.decode())


def _csv_encode(value):
    sio = io.StringIO()
    csv.writer(sio).writerow(value)
    return sio.getvalue().rstrip("\r\n")


def _csv_decode(value):
    return csv.reader([value]).__next__()


def _typed_dict_codec(pytype):

    codecs = {k: get_codec(v) for k, v in typing.get_type_hints(pytype).items()}

    def _process(value, method):
        result = {}
        for key, codec in codecs.items():
            try:
                result[key] = getattr(codec, method)(value[key])
            except KeyError:
                continue
        return result

    class TypedDictCodec:

        python_type = pytype
        json_type = dict[str, typing.Union[tuple(c.json_type for c in codecs.values())]]

        @validate_arguments
        def json_encode(self, value: python_type) -> json_type:
            return _process(value, "json_encode")

        @validate_arguments
        def json_decode(self, value: json_type) -> python_type:
            return _process(value, "json_decode")

        @validate_arguments
        def str_encode(self, value: python_type) -> str:
            return json.dumps(self.json_encode(value))

        @validate_arguments
        def str_decode(self, value: str) -> python_type:
            return self.json_decode(json.loads(value))

        @validate_arguments
        def bytes_encode(self, value: python_type) -> bytes:
            return self.str_encode(value).encode()

        @validate_arguments
        def bytes_decode(self, value: bytes) -> python_type:
            return self.str_decode(value.decode())

    affix_type_hints(TypedDictCodec, localns=TypedDictCodec.__dict__)
    return TypedDictCodec()


def _mapping_codec(pytype):

    key_codec, value_codec = tuple(get_codec(t) for t in typing.get_args(pytype))

    class MappingCodec:

        python_type = pytype
        json_type = dict[str, value_codec.json_type]

        @validate_arguments
        def json_encode(self, value: python_type) -> json_type:
            return {
                key_codec.str_encode(k): value_codec.json_encode(v)
                for k, v in value.items()
            }

        @validate_arguments
        def json_decode(self, value: json_type) -> python_type:
            return pytype(
                {
                    key_codec.str_decode(k): value_codec.json_decode(v)
                    for k, v in value.items()
                }
            )

        @validate_arguments
        def str_encode(self, value: python_type) -> str:
            return json.dumps(self.json_encode(value))

        @validate_arguments
        def str_decode(self, value: str) -> python_type:
            return _type(self.json_decode(json.loads(value)))

        @validate_arguments
        def bytes_encode(self, value: python_type) -> bytes:
            return self.str_encode(value).encode()

        @validate_arguments
        def bytes_decode(self, value: bytes) -> python_type:
            return self.str_decode(value.decode())

    affix_type_hints(MappingCodec, localns=MappingCodec.__dict__)
    return MappingCodec()


def _iterable_codec(pytype):

    (item_codec,) = tuple(get_codec(t) for t in typing.get_args(pytype))
    is_set = issubclass(typing.get_origin(pytype), collections.abc.Set)

    class IterableCodec:

        python_type = pytype
        json_type = list[item_codec.json_type]

        @validate_arguments
        def json_encode(self, value: python_type) -> json_type:
            if is_set:
                value = sorted(value)
            return [item_codec.json_encode(item) for item in value]

        @validate_arguments
        def json_decode(self, value: json_type) -> python_type:
            return pytype((item_codec.json_decode(item) for item in value))

        @validate_arguments
        def str_encode(self, value: python_type) -> str:
            if is_set:
                value = sorted(value)
            return _csv_encode((item_codec.str_encode(item) for item in value))

        @validate_arguments
        def str_decode(self, value: str) -> python_type:
            return pytype((item_codec.str_decode(item) for item in _csv_decode(value)))

        @validate_arguments
        def bytes_encode(self, value: python_type) -> bytes:
            return json.dumps(self.json_encode(value)).encode()

        @validate_arguments
        def bytes_decode(self, value: bytes) -> python_type:
            return pytype(self.json_decode(json.loads(value.decode())))

    affix_type_hints(IterableCodec, localns=IterableCodec.__dict__)
    return IterableCodec()


def _dataclass_codec(pytype):

    codecs = {k: get_codec(v) for k, v in typing.get_type_hints(pytype).items()}

    class DataclassCodec:

        python_type = pytype
        json_type = dict[str, typing.Union[tuple(c.json_type for c in codecs.values())]]

        @validate_arguments
        def json_encode(self, value: python_type) -> json_type:
            return {
                key: codec.json_encode(getattr(value, key))
                for key, codec in codecs.items()
            }

        @validate_arguments
        def json_decode(self, value: json_type) -> python_type:
            kwargs = {}
            for key, codec in codecs.items():
                try:
                    kwargs[key] = codec.json_encode(value[key])
                except KeyError:
                    continue
            return pytype(**kwargs)

        @validate_arguments
        def str_encode(self, value: python_type) -> str:
            return json.dumps(self.json_encode(value))

        @validate_arguments
        def str_decode(self, value: str) -> python_type:
            return self.json_decode(json.loads(value))

        @validate_arguments
        def bytes_encode(self, value: python_type) -> bytes:
            return self.str_encode(value).encode()

        @validate_arguments
        def bytes_decode(self, value: bytes) -> python_type:
            return self.str_decode(value.decode())

    affix_type_hints(DataclassCodec, localns=DataclassCodec.__dict__)
    return DataclassCodec()


def _union_codec(pytype):

    types = typing.get_args(pytype)
    codecs = tuple(get_codec(t) for t in types)

    class UnionCodec:

        python_type = pytype
        json_type = typing.Union[tuple(codec.json_type for codec in codecs)]

        def _process(self, method, value):
            for codec in codecs:
                try:
                    return getattr(codec, method)(value)
                except (TypeError, ValueError) as e:
                    continue
            raise ValueError

        @validate_arguments
        def json_encode(self, value: python_type) -> json_type:
            return self._process("json_encode", value)

        @validate_arguments
        def json_decode(self, value: json_type) -> python_type:
            return self._process("json_decode", value)

        @validate_arguments
        def str_encode(self, value: python_type) -> str:
            return self._process("str_encode", value)

        @validate_arguments
        def str_decode(self, value: str) -> python_type:
            return self._process("str_decode", value)

        @validate_arguments
        def bytes_encode(self, value: python_type) -> bytes:
            return self._process("bytes_encode", value)

        @validate_arguments
        def bytes_decode(self, value: bytes) -> python_type:
            return self._process("bytes_decode", value)

    affix_type_hints(UnionCodec, localns=UnionCodec.__dict__)
    return UnionCodec()


def _enum_codec(pytype):

    codecs = {member: get_codec(type(member.value)) for member in pytype}
    codec_set = set(codecs.values())

    class EnumCodec:

        python_type = pytype
        json_type = typing.Union[tuple(codec.json_type for codec in codecs.values())]

        def _decode(self, method, value):
            for codec in codec_set:
                try:
                    return getattr(codec, method)(value)
                except:
                    continue
            raise ValueError

        @validate_arguments
        def json_encode(self, value: python_type) -> json_type:
            return codecs[value].json_encode(value.value)

        @validate_arguments
        def json_decode(self, value: json_type) -> python_type:
            return pytype(self._decode("json_decode", value))

        @validate_arguments
        def str_encode(self, value: python_type) -> str:
            return codecs[value].str_encode(value.value)

        @validate_arguments
        def str_decode(self, value: str) -> python_type:
            return pytype(self._decode("str_decode", value))

        @validate_arguments
        def bytes_encode(self, value: python_type) -> bytes:
            return codecs[value].bytes_encode(value.value)

        @validate_arguments
        def bytes_decode(self, value: bytes) -> python_type:
            return pytype(self._decode("bytes_decode", value))

    affix_type_hints(EnumCodec, localns=EnumCodec.__dict__)
    return EnumCodec()


_builtins = {}
for codec in (
    StrCodec,
    BytesCodec,
    IntCodec,
    FloatCodec,
    BoolCodec,
    NoneCodec,
    DecimalCodec,
    DateCodec,
    DatetimeCodec,
    UUIDCodec,
):
    affix_type_hints(codec)
    _builtins[codec.python_type] = codec()


def _issubclass(cls, cls_or_tuple):
    try:
        return issubclass(cls, cls_or_tuple)
    except TypeError:
        return False


@functools.cache
def get_codec(pytype):
    """Return a codec compatible with the specified type."""

    if typing.get_origin(pytype) is typing.Annotated:
        args = typing.get_args(pytype)
        for annotation in args[1:]:
            if isinstance(annotation, Codec):
                return annotation
        pytype = args[0]  # strip annotation

    if result := _builtins.get(pytype):
        return result
    if origin := typing.get_origin(pytype):
        pytype = origin[typing.get_args(pytype)]
    if origin is typing.Union:
        return _union_codec(pytype)
    if _issubclass(pytype, dict) and getattr(pytype, "__annotations__", None) is not None:
        return _typed_dict_codec(pytype)
    if _issubclass(origin, collections.abc.Mapping):
        return _mapping_codec(pytype)
    if _issubclass(origin, collections.abc.Iterable):
        return _iterable_codec(pytype)
    if _issubclass(pytype, enum.Enum):
        return _enum_codec(pytype)
    if dataclasses.is_dataclass(pytype):
        return _dataclass_codec(pytype)

    raise TypeError(f"invalid type: {pytype}")
