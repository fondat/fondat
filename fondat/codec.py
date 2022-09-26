"""Module to support encoding and decoding of values."""

# from __future__ import annotations

import base64
import csv
import dataclasses
import fondat.types
import functools
import io
import iso8601
import json
import keyword
import logging

from collections.abc import Iterable, Mapping
from contextlib import contextmanager
from datetime import date, datetime, timezone
from decimal import Decimal
from fondat.types import (
    capture_typevars,
    is_optional,
    is_subclass,
    resolve_typevar,
    split_annotated,
)
from types import NoneType, UnionType
from typing import (
    Any,
    Generic,
    Literal,
    TypedDict,
    TypeVar,
    Union,
    get_args,
    get_origin,
    get_type_hints,
    is_typeddict,
)
from uuid import UUID


_logger = logging.getLogger(__name__)


providers = []


_TEXT_PLAIN = "text/plain; charset=UTF-8"


# tracks types being built to deal with recursion (cyclic graphs)
_building = {}


def _provider(wrapped=None):
    if wrapped is None:
        return functools.partial(_provider)
    providers.append(wrapped)
    return wrapped


class CodecError(ValueError):
    """
    Error raised in the event that a value cannot be decoded.
    """

    __slots__ = {"message", "path"}

    def __init__(self, message: str | None = None, path: list[str | int] | None = None):
        self.message = message
        self.path = path

    def __repr__(self):
        return f"{self.__class__.__name__}({self.message!r}, {self.path!r})"

    def __str__(self):
        return " ".join(str(s) for s in (self.message, self.path) if s is not None)

    @staticmethod
    @contextmanager
    def path_on_error(path: list[str | int] | str | int) -> None:
        """Context manager to add to error path in the event that a DecodeError is raised."""
        try:
            yield
        except CodecError as ce:
            if ce.path is None:
                ce.path = []
            match path:
                case str() | int():
                    ce.path.insert(0, path)
                case list():
                    ce.path = path + ce.path
            raise


class EncodeError(CodecError):
    pass


class DecodeError(CodecError):
    pass


# ----- base -----


F = TypeVar("F")  # from
T = TypeVar("T")  # to


class Codec(Generic[F, T]):
    """Base class for all things encode and decode."""

    def encode(self, value: F) -> T:
        """Encode value from F type to T type."""
        raise NotImplementedError

    def decode(self, value: T) -> F:
        """Decode value from T type to F type."""
        raise NotImplementedError


class String(Codec[F, str]):
    """Encodes Python types to/from Unicode string representations."""


class Binary(Codec[F, bytes | bytearray]):
    """
    Encodes Python types to/from binary representations.

    Attribute:
    • content_type: string containing the media type of the binary representation
    """


class JSON(Codec[F, Any]):
    """
    Encodes Python types to/from the JSON representations.

    The JSON object model is strictly composed of the following types: dict, list, str, int,
    float, bool, NoneType.

    Attribute:
    • json_type: the JSON object model type encoded/decoded by the codec
    """


def _b2s(b):
    if not isinstance(b, (bytes, bytearray)):
        raise DecodeError
    try:
        return b.decode()
    except Exception as e:
        raise DecodeError from e


def _s2j(s):
    if not isinstance(s, str):
        raise DecodeError
    try:
        return json.loads(s)
    except Exception as e:
        raise DecodeError from e


@contextmanager
def _wrap(exception):
    try:
        yield
    except Exception as e:
        raise exception from e


# ----- str -----


class _Str_Binary(Binary[str]):
    """Bytes codec for Unicode character strings."""

    content_type = _TEXT_PLAIN

    def encode(self, value: str) -> bytes:
        if not isinstance(value, str):
            raise EncodeError
        with _wrap(EncodeError):
            return value.encode()

    def decode(self, value: bytes | bytearray) -> str:
        return _b2s(value)


_str_binarycodec = _Str_Binary()


class _Str_String(String[str]):
    """String codec for Unicode character strings."""

    def encode(self, value: str) -> str:
        if not isinstance(value, str):
            raise EncodeError
        return value

    def decode(self, value: str) -> str:
        if not isinstance(value, str):
            raise DecodeError
        return value


_str_stringcodec = _Str_String()


class _Str_JSON(JSON[str]):
    """JSON codec for Unicode character strings."""

    json_type = str

    def encode(self, value: str) -> str:
        return _str_stringcodec.encode(value)

    def decode(self, value: str) -> str:
        return _str_stringcodec.decode(value)


_str_jsoncodec = _Str_JSON()


@_provider
def _str(codec_type, python_type):
    python_type, _ = split_annotated(python_type)
    if is_subclass(python_type, str):
        if codec_type is Binary:
            return _str_binarycodec
        if codec_type is String:
            return _str_stringcodec
        if codec_type is JSON:
            return _str_jsoncodec


# ----- bytes/bytearray -----


class _Bytes_Binary(Binary[bytes | bytearray]):
    """Binary codec for byte sequences."""

    content_type = "application/octet-stream"

    def encode(self, value: bytes | bytearray) -> bytes | bytearray:
        if not isinstance(value, (bytes, bytearray)):
            raise EncodeError
        return value

    def decode(self, value: bytes | bytearray) -> bytes | bytearray:
        if not isinstance(value, (bytes, bytearray)):
            raise DecodeError
        return value


_bytes_binarycodec = _Bytes_Binary()


class _Bytes_String(String[bytes | bytearray]):
    """
    String codec for byte sequences. A byte sequence is represented in string values as a
    base64-encoded string. Example: "SGVsbG8gRm9uZGF0".
    """

    def encode(self, value: bytes | bytearray) -> str:
        if not isinstance(value, (bytes, bytearray)):
            raise EncodeError
        with _wrap(EncodeError):
            return base64.b64encode(value).decode()

    def decode(self, value: str) -> bytes:
        if not isinstance(value, str):
            raise DecodeError
        with _wrap(DecodeError):
            return base64.b64decode(value)


_bytes_stringcodec = _Bytes_String()


class _Bytes_JSON(JSON[bytes | bytearray]):
    """
    JSON codec for byte sequences. A byte sequence is represented in JSON values as a
    base64-encoded string. Example: "SGVsbG8gRm9uZGF0".
    """

    json_type = str

    def encode(self, value: bytes | bytearray) -> str:
        return _bytes_stringcodec.encode(value)

    def decode(self, value: str) -> bytes:
        return _bytes_stringcodec.decode(value)


_bytes_jsoncodec = _Bytes_JSON()


@_provider
def _bytes(codec_type, python_type):
    python_type, _ = split_annotated(python_type)
    if is_subclass(python_type, (bytes, bytearray)):
        if codec_type is Binary:
            return _bytes_binarycodec
        if codec_type is String:
            return _bytes_stringcodec
        if codec_type is JSON:
            return _bytes_jsoncodec


# ----- int -----


class _Int_String(String[int]):
    """String codec for integers."""

    def encode(self, value: int) -> str:
        if not isinstance(value, int) or isinstance(value, bool):
            raise EncodeError
        return str(value)

    def decode(self, value: str) -> int:
        if not isinstance(value, str):
            raise DecodeError
        with _wrap(DecodeError):
            return int(value)


_int_stringcodec = _Int_String()


class _Int_Binary(Binary[int]):
    """Binary codec for integers."""

    content_type = _TEXT_PLAIN

    def encode(self, value: int) -> bytes:
        return _int_stringcodec.encode(value).encode()

    def decode(self, value: bytes | bytearray) -> int:
        return _int_stringcodec.decode(_b2s(value))


_int_binarycodec = _Int_Binary()


class _Int_JSON(JSON[int]):
    """JSON codec for integers."""

    json_type = int | float

    def encode(self, value: int) -> int:
        if not isinstance(value, int) or isinstance(value, bool):
            raise EncodeError
        return value

    def decode(self, value: int | float) -> int:
        if not isinstance(value, (int, float)) or isinstance(value, bool):
            raise DecodeError
        result = value
        if isinstance(result, float):
            result = int(result)
            if result != value:  # 1.0 == 1
                raise DecodeError
        return result


_int_jsoncodec = _Int_JSON()


@_provider
def _int(codec_type, python_type):
    python_type, _ = split_annotated(python_type)
    if is_subclass(python_type, int) and not is_subclass(python_type, bool):
        if codec_type is Binary:
            return _int_binarycodec
        if codec_type is String:
            return _int_stringcodec
        if codec_type is JSON:
            return _int_jsoncodec


# ----- float -----


class _Float_String(String[float]):
    """String codec for floating point numbers."""

    def encode(self, value: float) -> str:
        if not isinstance(value, float):
            raise EncodeError
        return str(value)

    def decode(self, value: str) -> float:
        if not isinstance(value, str):
            raise DecodeError
        with _wrap(DecodeError):
            return float(value)


_float_stringcodec = _Float_String()


class _Float_Binary(Binary[float]):
    """Binary codec for floating point numbers."""

    content_type = _TEXT_PLAIN

    def encode(self, value: float) -> bytes:
        return _float_stringcodec.encode(value).encode()

    def decode(self, value: bytes | bytearray) -> float:
        return _float_stringcodec.decode(_b2s(value))


_float_binarycodec = _Float_Binary()


class _Float_JSON(JSON[float]):
    """JSON codec for floating point numbers."""

    json_type = int | float

    def encode(self, value: float) -> float:
        if not isinstance(value, float):
            raise EncodeError
        return value

    def decode(self, value: int | float) -> float:
        if not isinstance(value, (int, float)) or isinstance(value, bool):
            raise DecodeError
        return float(value)


_float_jsoncodec = _Float_JSON()


@_provider
def _float(codec_type, python_type):
    python_type, _ = split_annotated(python_type)
    if is_subclass(python_type, float):
        if codec_type is Binary:
            return _float_binarycodec
        if codec_type is String:
            return _float_stringcodec
        if codec_type is JSON:
            return _float_jsoncodec


# ----- bool -----


class _Bool_String(String[bool]):
    """String codec for boolean values."""

    def encode(self, value: bool) -> str:
        if not isinstance(value, bool):
            raise EncodeError
        return "true" if value else "false"

    def decode(self, value: str) -> bool:
        if not isinstance(value, str):
            raise DecodeError
        try:
            return {"true": True, "false": False}[value]
        except KeyError:
            raise DecodeError


_bool_stringcodec = _Bool_String()


class _Bool_Binary(Binary[bool]):
    """Binary codec for boolean values."""

    content_type = _TEXT_PLAIN

    def encode(self, value: bool) -> bytes:
        return _bool_stringcodec.encode(value).encode()

    def decode(self, value: bytes | bytearray) -> bool:
        return _bool_stringcodec.decode(_b2s(value))


_bool_binarycodec = _Bool_Binary()


class _Bool_JSON(JSON[bool]):
    """JSON codec for boolean values."""

    json_type = bool

    def encode(self, value: bool) -> bool:
        if not isinstance(value, bool):
            raise EncodeError
        return value

    def decode(self, value: bool) -> bool:
        if not isinstance(value, bool):
            raise DecodeError
        return value


_bool_jsoncodec = _Bool_JSON()


@_provider
def _bool(codec_type, python_type):
    python_type, _ = split_annotated(python_type)
    if is_subclass(python_type, bool):
        if codec_type is Binary:
            return _bool_binarycodec
        if codec_type is String:
            return _bool_stringcodec
        if codec_type is JSON:
            return _bool_jsoncodec


# ----- NoneType -----


class _NoneType_String(String[NoneType]):
    """String codec for None value."""

    def encode(self, value: NoneType) -> str:
        if value is not None:
            raise EncodeError
        return ""

    def decode(self, value: str) -> NoneType:
        if str != "":
            raise DecodeError
        return None


_nonetype_stringcodec = _NoneType_String()


class _NoneType_Binary(Binary[NoneType]):
    """Binary codec for None value."""

    content_type = _TEXT_PLAIN

    def encode(self, value: NoneType) -> bytes:
        if value is not None:
            raise EncodeError
        return b""

    def decode(self, value: bytes | bytearray) -> NoneType:
        if value != b"":
            raise DecodeError
        return None


_nonetype_binarycodec = _NoneType_Binary()


class _NoneType_JSON(JSON[NoneType]):
    """JSON codec for None value."""

    json_type = NoneType

    def encode(self, value: NoneType) -> NoneType:
        if value is not None:
            raise EncodeError
        return None

    def decode(self, value: NoneType) -> NoneType:
        if value is not None:
            raise DecodeError
        return None


_nonetype_jsoncodec = _NoneType_JSON()


@_provider
def _NoneType(codec_type, python_type):
    python_type, _ = split_annotated(python_type)
    if python_type is NoneType:
        if codec_type is Binary:
            return _nonetype_binarycodec
        if codec_type is String:
            return _nonetype_stringcodec
        if codec_type is JSON:
            return _nonetype_jsoncodec


# ----- Decimal -----


class _Decimal_String(String[Decimal]):
    """String codec for Decimal numbers."""

    def encode(self, value: Decimal) -> str:
        if not isinstance(value, Decimal):
            raise EncodeError
        return str(value)

    def decode(self, value: str) -> Decimal:
        if not isinstance(value, str):
            raise DecodeError
        with _wrap(DecodeError):
            return Decimal(value)


_decimal_string = _Decimal_String()


class _Decimal_Binary(Binary[Decimal]):
    """Binary codec for Decimal numbers."""

    content_type = _TEXT_PLAIN

    def encode(self, value: Decimal) -> bytes:
        return _decimal_string.encode(value).encode()

    def decode(self, value: bytes | bytearray) -> Decimal:
        return _decimal_string.decode(_b2s(value))


_decimal_binary = _Decimal_Binary()


class _Decimal_JSON(JSON[Decimal]):
    """
    JSON codec for Decimal numbers. Decimal numbers are represented in JSON as strings, due to
    the imprecision of floating point numbers.
    """

    json_type = str

    def encode(self, value: Decimal) -> str:
        return _decimal_string.encode(value)

    def decode(self, value: str) -> Decimal:
        return _decimal_string.decode(value)


_decimal_json = _Decimal_JSON()


@_provider
def _Decimal(codec_type, python_type):
    python_type, _ = split_annotated(python_type)
    if is_subclass(python_type, Decimal):
        if codec_type is Binary:
            return _decimal_binary
        if codec_type is String:
            return _decimal_string
        if codec_type is JSON:
            return _decimal_json


# ----- date -----


class _Date_String(String[date]):
    """
    String codec for dates. A date is represented in a string in RFC 3339 format.
    Example: "2018-06-16".
    """

    def encode(self, value: date) -> str:
        if not isinstance(value, date):
            raise EncodeError
        return value.isoformat()

    def decode(self, value: str) -> date:
        if not isinstance(value, str):
            raise DecodeError
        with _wrap(DecodeError):
            return date.fromisoformat(value)


_date_stringcodec = _Date_String()


class _Date_Binary(Binary[date]):
    """
    Binary codec for dates. A date is represented in a binary format as an RFC 3339 UTF-8 or
    ASCII encoded string. Example: "2018-06-16".
    """

    content_type = _TEXT_PLAIN

    def encode(self, value: date) -> bytes:
        return _date_stringcodec.encode(value).encode()

    def decode(self, value: bytes | bytearray) -> date:
        return _date_stringcodec.decode(_b2s(value))


_date_binarycodec = _Date_Binary()


class _Date_JSON(JSON[date]):
    """
    JSON codec for dates. A date is represented in JSON as an RFC 3339 formatted string.
    Example: "2018-06-16".
    """

    json_type = str

    def encode(self, value: date) -> str:
        return _date_stringcodec.encode(value)

    def decode(self, value: str) -> date:
        return _date_stringcodec.decode(value)


_date_jsoncodec = _Date_JSON()


@_provider
def _date(codec_type, python_type):
    python_type, _ = split_annotated(python_type)
    if is_subclass(python_type, date) and not is_subclass(python_type, datetime):
        if codec_type is Binary:
            return _date_binarycodec
        if codec_type is String:
            return _date_stringcodec
        if codec_type is JSON:
            return _date_jsoncodec


# ----- datetime -----


def _to_utc(value):
    if value.tzinfo is None:  # naive value interpreted as UTC
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


class _Datetime_String(String[datetime]):
    """
    String codec for datetime.

    It will decode a datetime represented in an ISO 8601 formatted string. It will encode a
    datetime to an RFC 3339 (subset of ISO 8601) formatted string.

    Datetimes always encode and decode to UTC timezone offset.

    Example: "2020-04-07T12:34:56.789012Z".
    """

    def encode(self, value: datetime) -> str:
        if not isinstance(value, datetime):
            raise EncodeError
        result = _to_utc(value).isoformat()
        if result.endswith("+00:00"):
            result = result[0:-6]
        if "+" not in result and not result.endswith("Z"):
            result = f"{result}Z"
        return result

    def decode(self, value: str) -> datetime:
        if not isinstance(value, str):
            raise DecodeError
        with _wrap(DecodeError):
            return _to_utc(iso8601.parse_date(value))


_datetime_stringcodec = _Datetime_String()


class _Datetime_Binary(Binary[datetime]):
    """
    Binary codec for datetime.

    It will decode a datetime represented in an ISO 8601 formatted UTF-8 or ASCII encoded byte
    string. It will encode a datetime to an RFC 3339 (subset of ISO 8601) formatted UTF-8
    encoded byte string.

    Datetimes always encode and decode to UTC timezone offset.

    Example: b"2020-04-07T12:34:56.789012Z".
    """

    content_type = _TEXT_PLAIN

    def encode(self, value: datetime) -> bytes:
        return _datetime_stringcodec.encode(value).encode()

    def decode(self, value: bytes | bytearray) -> datetime:
        return _datetime_stringcodec.decode(_b2s(value))


_datetime_binarycodec = _Datetime_Binary()


class _Datetime_JSON(JSON[datetime]):
    """
    String codec for datetime.

    It will decode a datetime represented in an ISO 8601 formatted string. It will encode a
    datetime to an RFC 3339 (subset of ISO 8601) formatted string.

    Datetimes always encode and decode to UTC timezone offset.

    Example: "2020-04-07T12:34:56.789012Z".
    """

    json_type = str

    def encode(self, value: datetime) -> str:
        return _datetime_stringcodec.encode(value)

    def decode(self, value: str) -> datetime:
        return _datetime_stringcodec.decode(value)


_datetime_jsoncodec = _Datetime_JSON()


@_provider
def _datetime(codec_type, python_type):
    python_type, _ = split_annotated(python_type)
    if is_subclass(python_type, datetime):
        if codec_type is Binary:
            return _datetime_binarycodec
        if codec_type is String:
            return _datetime_stringcodec
        if codec_type is JSON:
            return _datetime_jsoncodec


# ----- UUID -----


class _UUID_String(String[UUID]):
    """String codec for UUID."""

    def encode(self, value: UUID) -> str:
        if not isinstance(value, UUID):
            raise EncodeError
        return str(value)

    def decode(self, value: str) -> UUID:
        if not isinstance(value, str):
            raise DecodeError
        with _wrap(DecodeError):
            return UUID(value)


_uuid_stringcodec = _UUID_String()


class _UUID_Binary(Binary[UUID]):
    """Binary codec for UUID."""

    content_type = _TEXT_PLAIN

    def encode(self, value: UUID) -> bytes:
        return _uuid_stringcodec.encode(value).encode()

    def decode(self, value: bytes | bytearray) -> UUID:
        return _uuid_stringcodec.decode(_b2s(value))


_uuid_binarycodec = _UUID_Binary()


class _UUID_JSON(JSON[UUID]):
    """JSON codec for UUID."""

    json_type = str

    def encode(self, value: UUID) -> str:
        return _uuid_stringcodec.encode(value)

    def decode(self, value: str) -> UUID:
        return _uuid_stringcodec.decode(value)


_uuid_jsoncodec = _UUID_JSON()


@_provider
def _uuid(codec_type, python_type):
    python_type, _ = split_annotated(python_type)
    if is_subclass(python_type, UUID):
        if codec_type is Binary:
            return _uuid_binarycodec
        if codec_type is String:
            return _uuid_stringcodec
        if codec_type is JSON:
            return _uuid_jsoncodec


# ----- TypedDict -----


@_provider
def _typeddict(codec_type, python_type):

    python_type, _ = split_annotated(python_type)

    if not is_typeddict(python_type):
        return

    if codec_type is JSON:

        if c := _building.get((codec_type, python_type)):
            return c  # return the (incomplete) outer one still being built

        hints = get_type_hints(python_type, include_extras=True)

        for key in hints:
            if type(key) is not str:
                raise TypeError("codec only supports TypedDict with str keys")

        class _TypedDict_JSON(JSON[python_type]):

            json_type = dict[str, Any]  # will be replaced below

            def _process(self, value, method):
                result = {}
                for key in hints:
                    codec = get_codec(JSON, hints[key])
                    try:
                        with CodecError.path_on_error(key):
                            result[key] = getattr(codec, method)(value[key])
                    except KeyError:
                        continue
                return result

            def encode(self, value: python_type) -> Any:
                if not isinstance(value, dict):
                    raise EncodeError
                return self._process(value, "encode")

            def decode(self, value: Any) -> python_type:
                return self._process(value, "decode")

        result = _TypedDict_JSON()
        _building[(codec_type, python_type)] = result

        try:
            json_type = TypedDict(
                "_TypedDict",
                {key: get_codec(JSON, hints[key]).json_type for key in hints},
                total=python_type.__total__,
            )
            json_type.__required_keys__ = python_type.__required_keys__
            json_type.__optional_keys__ = python_type.__optional_keys__
            result.json_type = result.__class__.json_type = json_type

        finally:
            del _building[(codec_type, python_type)]

        return result

    if codec_type is String:
        json_codec = get_codec(JSON, python_type)

        class _TypedDict_String(String[python_type]):
            def encode(self, value: python_type) -> str:
                if not isinstance(value, dict):
                    raise EncodeError
                return json.dumps(json_codec.encode(value))

            def decode(self, value: str) -> python_type:
                return json_codec.decode(_s2j(value))

        return _TypedDict_String()

    if codec_type is Binary:
        string_codec = get_codec(String, python_type)

        class _TypedDict_Binary(Binary[python_type]):
            content_type = "application/json"

            def encode(self, value: python_type) -> bytes:
                return string_codec.encode(value).encode()

            def decode(self, value: bytes | bytearray) -> python_type:
                return string_codec.decode(_b2s(value))

        return _TypedDict_Binary()


# ----- tuple -----


@_provider
def _tuple(codec_type, python_type):

    pytype, _ = split_annotated(python_type)

    if pytype is tuple:
        origin = tuple
        args = (Any, ...)

    else:
        origin = get_origin(pytype)
        args = get_args(pytype)

    if origin is not tuple:
        return

    if len(args) != 2 and Ellipsis in args or args[0] is Ellipsis:
        raise TypeError("unexpected ellipsis")

    varg = args[0] if len(args) == 2 and args[1] is Ellipsis else None
    args = () if varg else args

    if codec_type is JSON:

        codecs = [get_codec(JSON, arg) for arg in args]
        vcodec = get_codec(JSON, varg) if varg else None

        class _Tuple_JSON(JSON[python_type]):

            json_type = list[Any]

            def encode(self, value: python_type) -> list[Any]:
                if not isinstance(value, tuple) or (args and len(value) != len(args)):
                    raise EncodeError
                # TODO: path
                return (
                    [vcodec.encode(item) for item in value]
                    if vcodec
                    else [codecs[n].encode(value[n]) for n in range(len(codecs))]
                )

            def decode(self, value: list[Any]) -> python_type:
                if not isinstance(value, list) or (args and len(value) != len(args)):
                    raise DecodeError
                # TODO: path
                return tuple(
                    [vcodec.decode(item) for item in value]
                    if vcodec
                    else [codecs[n].decode(value[n]) for n in range(len(codecs))]
                )

        return _Tuple_JSON()

    if codec_type is String:

        codecs = [get_codec(String, arg) for arg in args]
        vcodec = get_codec(String, varg) if varg else None

        class _Tuple_String(String[python_type]):
            def encode(self, value: python_type) -> str:
                if not isinstance(value, tuple) or (args and len(value) != len(args)):
                    raise EncodeError
                # TODO: path
                return _csv_encode(
                    [vcodec.encode(item) for item in value]
                    if vcodec
                    else [codecs[n].encode(value[n]) for n in range(len(codecs))]
                )

            def decode(self, value: str) -> python_type:
                decoded = _csv_decode(value)
                if args and len(decoded) != len(args):
                    raise DecodeError
                # TODO: path
                return tuple(
                    [vcodec.decode(item) for item in decoded]
                    if vcodec
                    else [codecs[n].decode(decoded[n]) for n in range(len(codecs))]
                )

        return _Tuple_String()

    if codec_type is Binary:

        json_codec = get_codec(JSON, python_type)

        class _Tuple_Binary(Binary[python_type]):

            content_type = "application/json"

            def encode(self, value: python_type) -> bytes:
                return json.dumps(json_codec.encode(value)).encode()

            def decode(self, value: bytes | bytearray) -> python_type:
                return json_codec.decode(_s2j(_b2s(value)))

        return _Tuple_Binary()


# ----- Mapping -----


@_provider
def _mapping(codec_type, python_type):

    python_type, _ = split_annotated(python_type)

    if is_subclass(python_type, Mapping):
        origin = Mapping
        args = [Any, Any]

    else:
        origin = get_origin(python_type)
        if not is_subclass(origin, Mapping) or getattr(python_type, "__annotations__", None):
            return  # not a Mapping
        args = get_args(python_type)
        if len(args) != 2:
            raise TypeError("expecting Mapping[KT, VT]")

    if codec_type is JSON:
        key_codec = get_codec(String, args[0])
        value_codec = get_codec(JSON, args[1])
        _json_type = dict[str, value_codec.json_type]

        class _Mapping_JSON(JSON[python_type]):
            json_type = _json_type

            def encode(self, value: python_type) -> _json_type:
                if not isinstance(value, Mapping):
                    raise EncodeError
                result = {}
                for k, v in value.items():
                    key = key_codec.encode(k)
                    with CodecError.path_on_error(key):
                        result[key] = value_codec.encode(v)
                return result

            def decode(self, value: _json_type) -> python_type:
                if not isinstance(value, Mapping):
                    raise DecodeError
                result = {}
                for k, v in value.items():
                    key = key_codec.decode(k)
                    with CodecError.path_on_error(key):
                        result[key] = value_codec.decode(v)
                return result

        return _Mapping_JSON()

    if codec_type is String:
        json_codec = get_codec(JSON, python_type)

        class _Mapping_String(String[python_type]):
            def encode(self, value: python_type) -> str:
                if not isinstance(value, Mapping):
                    raise EncodeError
                return json.dumps(json_codec.encode(value))

            def decode(self, value: str) -> python_type:
                return json_codec.decode(_s2j(value))

        return _Mapping_String()

    if codec_type is Binary:

        string_codec = get_codec(String, python_type)

        class _Mapping_Binary(Binary[python_type]):

            content_type = "application/json"

            def encode(self, value: python_type) -> bytes:
                if not isinstance(value, Mapping):
                    raise EncodeError
                return string_codec.encode(value).encode()

            def decode(self, value: bytes | bytearray) -> python_type:
                return string_codec.decode(_b2s(value))

        return _Mapping_Binary()


# ----- Iterable -----


def _csv_encode(value):
    sio = io.StringIO()
    csv.writer(sio).writerow(value)
    return sio.getvalue().rstrip("\r\n")


def _csv_decode(value):
    if not isinstance(value, str):
        raise DecodeError
    return csv.reader([value]).__next__()


@_provider
def _iterable(codec_type, python_type):

    decode_type = list if get_origin(python_type) is Iterable else python_type

    python_type, _ = split_annotated(python_type)

    if is_subclass(python_type, Iterable) and not is_subclass(
        python_type, (str, bytes, bytearray)
    ):
        origin = python_type
        args = (Any,)

    else:
        origin = get_origin(python_type)
        if not is_subclass(origin, Iterable) or is_subclass(origin, Mapping):
            return
        args = get_args(python_type)

    if len(args) != 1:
        raise TypeError("expecting Iterable[T]")

    item_type = args[0]
    is_set = is_subclass(origin, set)

    if codec_type is JSON:

        item_codec = get_codec(JSON, item_type)
        _json_type = list[item_codec.json_type]

        class _Iterable_JSON(JSON[python_type]):

            json_type = _json_type

            def encode(self, value: python_type) -> _json_type:
                if not isinstance(value, Iterable) or isinstance(value, str):
                    raise EncodeError
                if is_set:
                    value = sorted(value)
                # TODO: path
                return [item_codec.encode(item) for item in value]

            def decode(self, value: _json_type) -> python_type:
                if not isinstance(value, list):
                    raise DecodeError
                # TODO: path
                return decode_type((item_codec.decode(item) for item in value))

        return _Iterable_JSON()

    if codec_type is String:

        item_codec = get_codec(String, item_type)

        class _Iterable_String(String[python_type]):
            def encode(self, value: python_type) -> str:
                if not isinstance(value, Iterable) or isinstance(value, str):
                    raise EncodeError
                if is_set:
                    value = sorted(value)
                return _csv_encode((item_codec.encode(item) for item in value))

            def decode(self, value: str) -> python_type:
                # TODO: path
                return decode_type((item_codec.decode(item) for item in _csv_decode(value)))

        return _Iterable_String()

    if codec_type is Binary:

        json_codec = get_codec(JSON, python_type)

        class _Iterable_Binary(Binary[python_type]):

            content_type = "application/json"

            def encode(self, value: python_type) -> bytes:
                if not isinstance(value, Iterable) or isinstance(value, str):
                    raise EncodeError
                return json.dumps(json_codec.encode(value)).encode()

            def decode(self, value: bytes | bytearray) -> python_type:
                return json_codec.decode(_s2j(_b2s(value)))

        return _Iterable_Binary()


# ----- dataclass -----


# keywords have _ suffix in dataclass fields (e.g. "in_", "for_", ...)
_dc_kw = {k + "_": k for k in keyword.kwlist}


@_provider
def typevar_codec(codec_type, python_type):
    if not isinstance(python_type, TypeVar):
        return
    return get_codec(codec_type, resolve_typevar(python_type))


@_provider
def dataclass_codec(codec_type, python_type):

    dc_type, _ = split_annotated(python_type)
    origin = get_origin(dc_type)

    if not dataclasses.is_dataclass(dc_type) and (
        not origin or not dataclasses.is_dataclass(origin)
    ):
        return

    with capture_typevars(dc_type):

        if origin:
            dc_type = origin

        fields = dataclasses.fields(dc_type)

        if codec_type is JSON:

            if c := _building.get((codec_type, python_type)):
                return c  # return the (incomplete) outer one still being built

            hints = get_type_hints(dc_type, include_extras=True)

            class _Dataclass_JSON(JSON[python_type]):

                json_type = dict[str, Any]  # will be replaced below

                def encode(self, value: python_type) -> Any:
                    if not isinstance(value, dc_type):
                        raise EncodeError
                    result = {}
                    for field in fields:
                        v = getattr(value, field.name, None)
                        if v is not None:
                            with CodecError.path_on_error(field.name):
                                result[_dc_kw.get(field.name, field.name)] = get_codec(
                                    JSON, field.type
                                ).encode(v)
                    return result

                def decode(self, value: Any) -> python_type:
                    if not isinstance(value, dict):
                        raise DecodeError
                    kwargs = {}
                    for field in fields:
                        codec = get_codec(JSON, field.type)
                        try:
                            with CodecError.path_on_error(field.name):
                                kwargs[field.name] = codec.decode(
                                    value[_dc_kw.get(field.name, field.name)]
                                )
                        except KeyError:
                            if (
                                is_optional(field.type)
                                and field.default is dataclasses.MISSING
                                and field.default_factory is dataclasses.MISSING
                            ):
                                kwargs[field.name] = None
                    try:
                        return python_type(**kwargs)
                    except Exception as e:
                        raise DecodeError from e

            result = _Dataclass_JSON()
            _building[(codec_type, python_type)] = result

            try:
                json_type = TypedDict(
                    "_TypedDict",
                    {key: get_codec(JSON, hints[key]).json_type for key in hints},
                    total=False,
                )

                # workaround for https://bugs.python.org/issue42059
                json_type.__required_keys__ = frozenset()
                json_type.__optional_keys__ = frozenset(hints)

                result.json_type = result.__class__.json_type = json_type

            finally:
                del _building[(codec_type, python_type)]

            return result

        if codec_type is String:

            json_codec = get_codec(JSON, python_type)

            class _Dataclass_String(String[python_type]):
                def encode(self, value: python_type) -> str:
                    return json.dumps(json_codec.encode(value))

                def decode(self, value: str) -> python_type:
                    return json_codec.decode(_s2j(value))

            return _Dataclass_String()

        if codec_type is Binary:

            string_codec = get_codec(String, python_type)

            class _Dataclass_Binary(Binary[python_type]):

                content_type = "application/json"

                def encode(self, value: python_type) -> bytes:
                    return string_codec.encode(value).encode()

                def decode(self, value: bytes | bytearray) -> python_type:
                    return string_codec.decode(_b2s(value))

            return _Dataclass_Binary()


# ----- UnionType/Union -----


@_provider
def _union(codec_type, python_type):

    python_type, _ = split_annotated(python_type)

    if get_origin(python_type) not in {UnionType, Union}:
        return

    types = get_args(python_type)
    codecs = [get_codec(codec_type, type) for type in types]

    def _encode(value):
        for codec in codecs:
            try:
                return codec.encode(value)
            except EncodeError:
                continue
        raise EncodeError(f"{python_type} as {codec_type} for value: {value}")

    def _decode(value):
        for codec in codecs:
            try:
                return codec.decode(value)
            except DecodeError:
                continue
        raise DecodeError(f"{python_type} as {codec_type} for value: {value}")

    if codec_type is String:

        class _Union_String(String[python_type]):
            def encode(self, value: python_type) -> str:
                if value is None and NoneType in types:
                    return ""
                return _encode(value)

            def decode(self, value: str) -> python_type:
                return _decode(value)

        return _Union_String()

    if codec_type is Binary:

        class _Union_Binary(Binary[python_type]):

            content_type = "application/octet-stream"

            def encode(self, value: python_type) -> bytes:
                if value is None and NoneType in types:
                    return b""
                return _encode(value)

            def decode(self, value: bytes | bytearray) -> python_type:
                if not isinstance(value, (bytes, bytearray)):
                    raise DecodeError
                return _decode(value)

        return _Union_Binary()

    if codec_type is JSON:

        _json_type = fondat.types.union_type(codec.json_type for codec in codecs)

        class _Union_JSON(JSON[python_type]):

            json_type = _json_type

            def encode(self, value: python_type) -> _json_type:
                if value is None and NoneType in types:
                    return None
                return _encode(value)

            def decode(self, value: _json_type) -> python_type:
                return _decode(value)

        return _Union_JSON()


# ----- Literal -----


@_provider
def _literal(codec_type, python_type):

    python_type, _ = split_annotated(python_type)
    origin = get_origin(python_type)

    if origin is not Literal:
        return

    literals = {(type(l), l) for l in get_args(python_type)}

    def decode(codecs, value):
        for codec in codecs:
            try:
                v = getattr(codec, "decode")(value)
                if (type(v), v) in literals:
                    return v
            except:
                continue
        raise DecodeError(f"expecting one of: {get_args(python_type)}; received: {value}")

    if codec_type is String:

        codecs = tuple(get_codec(String, literal[0]) for literal in literals)

        class _Literal_String(String[python_type]):
            def encode(self, value: python_type) -> str:
                return get_codec(String, type(value)).encode(value)

            def decode(self, value: str) -> python_type:
                return decode(codecs, value)

        return _Literal_String()

    if codec_type is Binary:

        codecs = tuple(get_codec(Binary, literal[0]) for literal in literals)

        class _Literal_Binary(Binary[python_type]):

            content_type = "application/octet-stream"

            def encode(self, value: python_type) -> bytes:
                return get_codec(Binary, type(value)).encode(value)

            def decode(self, value: bytes | bytearray) -> python_type:
                if not isinstance(value, (bytes, bytearray)):
                    raise DecodeError
                return decode(codecs, value)

        return _Literal_Binary()

    if codec_type is JSON:

        codecs = tuple(get_codec(JSON, literal[0]) for literal in literals)
        _json_type = fondat.types.union_type(codec.json_type for codec in codecs)

        class _Literal_JSON(JSON[python_type]):

            json_type = _json_type

            def encode(self, value: python_type) -> _json_type:
                return get_codec(JSON, type(value)).encode(value)

            def decode(self, value: _json_type) -> python_type:
                return decode(codecs, value)

        return _Literal_JSON()


# ----- Any -----


class _Any_String(String[Any]):
    """String codec for Any."""

    def encode(self, value: Any) -> str:
        return get_codec(String, type(value)).encode(value)

    def decode(self, value: str) -> str:
        return value


_any_stringcodec = _Any_String()


class _Any_Binary(Binary[Any]):
    """Binary codec for Any."""

    content_type = "application/octet-stream"

    def encode(self, value: Any) -> bytes:
        return get_codec(Binary, type(value)).encode(value)

    def decode(self, value: bytes | bytearray) -> bytes | bytearray:
        if not isinstance(value, (bytes, bytearray)):
            raise DecodeError
        return value


_any_binarycodec = _Any_Binary()


class _Any_JSON(JSON[Any]):
    """JSON codec for Any."""

    json_type = Any

    def encode(self, value: Any) -> Any:
        return get_codec(JSON, type(value)).encode(value)

    def decode(self, value: Any) -> Any:
        return value


_any_jsoncodec = _Any_JSON()


@_provider
def _any(codec_type, python_type):
    python_type, _ = split_annotated(python_type)
    if python_type is Any:
        if codec_type is Binary:
            return _any_binarycodec
        if codec_type is String:
            return _any_stringcodec
        if codec_type is JSON:
            return _any_jsoncodec


_cache = {}


def _get_codec(codec_type, python_type, annotations):

    _, annotations = split_annotated(python_type)

    for annotation in annotations:
        if isinstance(annotation, codec_type):
            return annotation

    for provider in providers:
        if (codec := provider(codec_type, python_type)) is not None:
            return codec

    raise TypeError(f"failed to provide {codec_type} for {python_type}")


def get_codec(codec_type, python_type, annotations=None):
    """Return a codec compatible with the specified Python type."""

    cache_key = tuple((type(arg), arg) for arg in (codec_type, python_type, annotations))

    try:
        return _cache[cache_key]
    except:
        pass

    result = _get_codec(codec_type, python_type, annotations)

    try:
        _cache[cache_key] = result
    except:  # cache on best-effort basis
        pass

    return result
