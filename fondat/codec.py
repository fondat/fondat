"""Module to support encoding and decoding of values."""

import base64
import csv
import dataclasses
import fondat.types
import io
import iso8601
import json
import keyword
import logging
import typing

from collections import namedtuple
from collections.abc import Iterable, Mapping, Set
from contextlib import contextmanager, suppress
from datetime import date, datetime, timezone
from decimal import Decimal
from fondat.types import is_optional, is_subclass, strip_annotations
from types import NoneType, UnionType
from typing import Any, Generic, Literal, TypeVar, Union, get_args, get_origin
from uuid import UUID


_logger = logging.getLogger(__name__)


# ----- content types -----


APPLICATION_JSON = "application/json"
APPLICATION_OCTET_STREAM = "application/octet-stream"
TEXT_PLAIN = "text/plain; charset=UTF-8"


# ----- type aliases -----


BinaryType = bytes | bytearray
JSONType = Any
StringType = str


# ----- utilities -----


@contextmanager
def _wrap(exception):
    try:
        yield
    except Exception as e:
        if isinstance(e, exception):
            raise
        raise exception from e


def _csv_encode(value):
    sio = io.StringIO()
    csv.writer(sio).writerow(value)
    return sio.getvalue().rstrip("\r\n")


def _csv_decode(value):
    if not isinstance(value, str):
        raise DecodeError
    return csv.reader([value]).__next__()


def _b2s(b):
    if not isinstance(b, bytes | bytearray):
        raise DecodeError
    with _wrap(DecodeError):
        return b.decode()


def _s2j(s):
    if not isinstance(s, str):
        raise DecodeError
    with _wrap(DecodeError):
        return json.loads(s)


# ----- errors -----


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
    """..."""


class DecodeError(CodecError):
    """..."""


# ----- base -----


PT = TypeVar("PT")  # Python type hint
TT = TypeVar("TT")  # target type hint


class Codec(Generic[PT, TT]):
    """
    Base class for all things encode and decode.
    """

    def __init__(self, python_type: Any):
        self.python_type = python_type

    @staticmethod
    def handles(python_type: Any) -> bool:
        """Return True if the codec handles the specified Python type."""
        raise NotImplementedError

    @classmethod
    def get(cls, python_type: Any) -> "Codec[PT, TT]":
        """
        Return a codec that handles the specified Python type.

        If the subclass contains a `_cache` attribute, and does not

        """
        if cls is Codec:
            raise NotImplementedError
        with suppress(AttributeError, KeyError):
            return cls._cache[python_type]
        for codec_class in cls.__subclasses__():
            if codec_class.handles(python_type):
                codec = codec_class(python_type)
                with suppress(AttributeError):
                    cache = getattr(codec, "_cache", False)
                    if isinstance(cache, Mapping):
                        cache[python_type] = codec
                return codec
        raise TypeError(f"no codec for {python_type}")

    def encode(self, value: PT) -> TT:
        """Encode value from F type to T type."""
        raise NotImplementedError

    def decode(self, value: TT) -> PT:
        """Decode value from T type to F type."""
        raise NotImplementedError


class StringCodec(Codec[PT, StringType]):
    """Encodes Python types to/from Unicode string representations."""

    _cache = {}

    def encode(self, value: PT) -> StringType:
        """Encode value from Python type to string type."""
        raise NotImplementedError

    def decode(self, value: StringType) -> PT:
        """Decode value from string type to Python type."""
        raise NotImplementedError


class BinaryCodec(Codec[PT, BinaryType]):
    """Encodes Python types to/from binary representations.

    Attribute:
    • content_type: string containing the media type of the binary representation
    """

    content_type = APPLICATION_OCTET_STREAM

    _cache = {}

    def encode(self, value: PT) -> BinaryType:
        """Encode value from Python type to binary type."""
        raise NotImplementedError

    def decode(self, value: BinaryType) -> PT:
        """Decode value from binary type to Python type."""
        raise NotImplementedError


class JSONCodec(Codec[PT, JSONType]):
    """Encodes Python types to/from the JSON representations."""

    _cache = {}

    def encode(self, value: PT) -> JSONType:
        """Encode value from Python type to binary type."""
        raise NotImplementedError

    def decode(self, value: JSONType) -> PT:
        """Decode value from binary type to Python type."""
        raise NotImplementedError


# ----- str -----


class StrBinaryCodec(BinaryCodec[str]):
    """Bytes codec for Unicode character strings."""

    content_type = TEXT_PLAIN

    @staticmethod
    def handles(python_type: Any) -> bool:
        python_type = strip_annotations(python_type)
        return is_subclass(python_type, str)

    def encode(self, value: str) -> BinaryType:
        if not isinstance(value, str):
            raise EncodeError
        with _wrap(EncodeError):
            return value.encode()

    def decode(self, value: BinaryType) -> str:
        return _b2s(value)


class StrStringCodec(StringCodec[str]):
    """String codec for Unicode character strings."""

    @staticmethod
    def handles(python_type: Any) -> bool:
        python_type = strip_annotations(python_type)
        return is_subclass(python_type, str)

    def encode(self, value: str) -> StringType:
        if not isinstance(value, str):
            raise EncodeError
        return value

    def decode(self, value: StringType) -> str:
        if not isinstance(value, str):
            raise DecodeError
        return value


class StrJSONCodec(JSONCodec[str]):
    """JSON codec for Unicode character strings."""

    def __init__(self, python_type: Any):
        super().__init__(python_type)
        self.codec = StrStringCodec(python_type)

    @staticmethod
    def handles(python_type: Any) -> bool:
        return StrStringCodec.handles(python_type)

    def encode(self, value: str) -> JSONType:
        return self.codec.encode(value)

    def decode(self, value: JSONType) -> str:
        if not isinstance(value, str):
            raise DecodeError
        return self.codec.decode(value)


# ----- bytes/bytearray -----


class BytesBinaryCodec(BinaryCodec[bytes | bytearray]):
    """Binary codec for byte sequences."""

    content_type = APPLICATION_OCTET_STREAM

    @staticmethod
    def handles(python_type: Any) -> bool:
        python_type = strip_annotations(python_type)
        return is_subclass(python_type, bytes | bytearray)

    def encode(self, value: bytes | bytearray) -> BinaryType:
        if not isinstance(value, bytes | bytearray):
            raise EncodeError
        return value

    def decode(self, value: BinaryType) -> bytes | bytearray:
        if not isinstance(value, BinaryType):
            raise DecodeError
        return value


class BytesStringCodec(StringCodec[bytes | bytearray]):
    """
    String codec for byte sequences. A byte sequence is represented in string values as a
    base64-encoded string. Example: "SGVsbG8gRm9uZGF0".
    """

    @staticmethod
    def handles(python_type: Any) -> bool:
        python_type = strip_annotations(python_type)
        return is_subclass(python_type, BinaryType)

    def encode(self, value: bytes | bytearray) -> StringType:
        with _wrap(EncodeError):
            return base64.b64encode(value).decode()

    def decode(self, value: StringType) -> bytes | bytearray:
        with _wrap(DecodeError):
            return base64.b64decode(value)


class BytesJSONCodec(JSONCodec[bytes | bytearray]):
    """
    JSON codec for byte sequences. A byte sequence is represented in JSON values as a
    base64-encoded string. Example: "SGVsbG8gRm9uZGF0".
    """

    def __init__(self, python_type: Any):
        super().__init__(python_type)
        self.codec = BytesStringCodec(python_type)

    @staticmethod
    def handles(python_type: Any) -> bool:
        return BytesStringCodec.handles(python_type)

    def encode(self, value: JSONType) -> bytes | bytearray:
        return self.codec.encode(value)

    def decode(self, value: bytes | bytearray) -> JSONType:
        return self.codec.decode(value)


# ----- int -----


class IntStringCodec(StringCodec[int]):
    """String codec for integers."""

    @staticmethod
    def handles(python_type: Any) -> bool:
        python_type = strip_annotations(python_type)
        return is_subclass(python_type, int) and not is_subclass(python_type, bool)

    def encode(self, value: int) -> StringType:
        if not isinstance(value, int) or isinstance(value, bool):
            raise EncodeError
        return str(value)

    def decode(self, value: StringType) -> int:
        if not isinstance(value, str):
            raise DecodeError
        with _wrap(DecodeError):
            return int(value)


class IntBinaryCodec(BinaryCodec[int]):
    """Binary codec for integers."""

    content_type = TEXT_PLAIN

    @staticmethod
    def handles(python_type: Any) -> bool:
        return IntStringCodec.handles(python_type)

    def __init__(self, python_type: Any):
        super().__init__(python_type)
        self.codec = IntStringCodec(python_type)

    def encode(self, value: int) -> BinaryType:
        return self.codec.encode(value).encode()

    def decode(self, value: BinaryType) -> int:
        return self.codec.decode(_b2s(value))


class IntJSONCodec(JSONCodec[int]):
    """JSON codec for integers."""

    @staticmethod
    def handles(python_type: Any) -> bool:
        python_type = strip_annotations(python_type)
        return is_subclass(python_type, int) and not is_subclass(python_type, bool)

    def encode(self, value: int) -> JSONType:
        if not isinstance(value, int) or isinstance(value, bool):
            raise EncodeError
        return value

    def decode(self, value: JSONType) -> int:
        if not isinstance(value, int | float) or isinstance(value, bool):
            raise DecodeError
        result = value
        if isinstance(result, float):
            result = int(result)
            if result != value:  # 1.0 == 1
                raise DecodeError
        return result


# ----- float -----


class FloatStringCodec(StringCodec[float]):
    """String codec for floating point numbers."""

    @staticmethod
    def handles(python_type: Any) -> bool:
        python_type = strip_annotations(python_type)
        return is_subclass(python_type, float)

    def encode(self, value: float) -> StringType:
        if not isinstance(value, float):
            raise EncodeError
        return str(value)

    def decode(self, value: StringType) -> float:
        if not isinstance(value, str):
            raise DecodeError
        with _wrap(DecodeError):
            return float(value)


class FloatBinaryCodec(BinaryCodec[float]):
    """Binary codec for floating point numbers."""

    content_type = TEXT_PLAIN

    @staticmethod
    def handles(python_type: Any) -> bool:
        return FloatStringCodec.handles(python_type)

    def __init__(self, python_type: Any):
        super().__init__(python_type)
        self.codec = FloatStringCodec(python_type)

    def encode(self, value: float) -> BinaryType:
        with _wrap(EncodeError):
            return self.codec.encode(value).encode()

    def decode(self, value: BinaryType) -> float:
        return self.codec.decode(_b2s(value))


class FloatJSONCodec(JSONCodec[float]):
    """JSON codec for floating point numbers."""

    @staticmethod
    def handles(python_type: Any) -> bool:
        python_type = strip_annotations(python_type)
        return is_subclass(python_type, float)

    def encode(self, value: float) -> JSONType:
        if not isinstance(value, float):
            raise EncodeError
        return value

    def decode(self, value: JSONType) -> float:
        if not isinstance(value, int | float) or isinstance(value, bool):
            raise DecodeError
        return float(value)


# ----- bool -----


class BoolStringCodec(StringCodec[bool]):
    """String codec for boolean values."""

    @staticmethod
    def handles(python_type: Any) -> bool:
        python_type = strip_annotations(python_type)
        return is_subclass(python_type, bool)

    def encode(self, value: bool) -> StringType:
        if not isinstance(value, bool):
            raise EncodeError
        return "true" if value else "false"

    def decode(self, value: StringType) -> bool:
        if not isinstance(value, str):
            raise DecodeError
        try:
            return {"true": True, "false": False}[value]
        except KeyError:
            raise DecodeError


class BoolBinaryCodec(BinaryCodec[bool]):
    """Binary codec for boolean values."""

    content_type = TEXT_PLAIN

    @staticmethod
    def handles(python_type: Any) -> bool:
        return BoolStringCodec.handles(python_type)

    def __init__(self, python_type: Any):
        super().__init__(python_type)
        self.codec = BoolStringCodec(python_type)

    def encode(self, value: bool) -> BinaryType:
        with _wrap(EncodeError):
            return self.codec.encode(value).encode()

    def decode(self, value: BinaryType) -> bool:
        return self.codec.decode(_b2s(value))


class BoolJSONCodec(JSONCodec[bool]):
    """JSON codec for boolean values."""

    @staticmethod
    def handles(python_type: Any) -> bool:
        python_type = strip_annotations(python_type)
        return is_subclass(python_type, bool)

    def encode(self, value: bool) -> JSONType:
        if not isinstance(value, bool):
            raise EncodeError
        return value

    def decode(self, value: JSONType) -> bool:
        if not isinstance(value, bool):
            raise DecodeError
        return value


# ----- NoneType -----


class NoneTypeStringCodec(StringCodec[NoneType]):
    """String codec for None value."""

    @staticmethod
    def handles(python_type: Any) -> bool:
        python_type = strip_annotations(python_type)
        return python_type is NoneType

    def encode(self, value: NoneType) -> StringType:
        if value is not None:
            raise EncodeError
        return ""

    def decode(self, value: StringType) -> NoneType:
        if value != "":
            raise DecodeError
        return None


class NoneTypeBinaryCodec(BinaryCodec[NoneType]):
    """Binary codec for None value."""

    content_type = TEXT_PLAIN

    @staticmethod
    def handles(python_type: Any) -> bool:
        python_type = strip_annotations(python_type)
        return python_type is NoneType

    def encode(self, value: NoneType) -> BinaryType:
        if value is not None:
            raise EncodeError
        return b""

    def decode(self, value: BinaryType) -> NoneType:
        if value != b"":
            raise DecodeError
        return None


class NoneTypeJSONCodec(JSONCodec[NoneType]):
    """JSON codec for None value."""

    @staticmethod
    def handles(python_type: Any) -> bool:
        python_type = strip_annotations(python_type)
        return python_type is NoneType

    def encode(self, value: NoneType) -> JSONType:
        if value is not None:
            raise EncodeError
        return None

    def decode(self, value: JSONType) -> NoneType:
        if value is not None:
            raise DecodeError
        return None


# ----- Decimal -----


class DecimalStringCodec(StringCodec[Decimal]):
    """String codec for Decimal numbers."""

    @staticmethod
    def handles(python_type: Any) -> bool:
        python_type = strip_annotations(python_type)
        return is_subclass(python_type, Decimal)

    def encode(self, value: Decimal) -> StringType:
        if not isinstance(value, Decimal):
            raise EncodeError
        return str(value)

    def decode(self, value: StringType) -> Decimal:
        if not isinstance(value, str):
            raise DecodeError
        with _wrap(DecodeError):
            return Decimal(value)


class DecimalBinaryCodec(BinaryCodec[Decimal]):
    """Binary codec for Decimal numbers."""

    content_type = TEXT_PLAIN

    @staticmethod
    def handles(python_type: Any) -> bool:
        return DecimalStringCodec.handles(python_type)

    def __init__(self, python_type: Any):
        super().__init__(python_type)
        self.codec = DecimalStringCodec(python_type)

    def encode(self, value: Decimal) -> BinaryType:
        return self.codec.encode(value).encode()

    def decode(self, value: BinaryType) -> Decimal:
        return self.codec.decode(_b2s(value))


class DecimalJSONCodec(JSONCodec[Decimal]):
    """
    JSON codec for Decimal numbers. Decimal numbers are represented in JSON as strings, due to
    the imprecision of floating point numbers.
    """

    @staticmethod
    def handles(python_type: Any) -> bool:
        return DecimalStringCodec.handles(python_type)

    def __init__(self, python_type: Any):
        super().__init__(python_type)
        self.codec = DecimalStringCodec(python_type)

    def encode(self, value: Decimal) -> JSONType:
        return self.codec.encode(value)

    def decode(self, value: JSONType) -> Decimal:
        return self.codec.decode(value)


# ----- date -----


class DateStringCodec(StringCodec[date]):
    """
    String codec for dates. A date is represented in a string in RFC 3339 format.
    Example: "2018-06-16".
    """

    @staticmethod
    def handles(python_type: Any) -> bool:
        python_type = strip_annotations(python_type)
        return is_subclass(python_type, date) and not is_subclass(python_type, datetime)

    def encode(self, value: date) -> StringType:
        if not isinstance(value, date):
            raise EncodeError
        return value.isoformat()

    def decode(self, value: StringType) -> date:
        if not isinstance(value, str):
            raise DecodeError
        with _wrap(DecodeError):
            return date.fromisoformat(value)


class DateBinaryCodec(BinaryCodec[date]):
    """
    Binary codec for dates. A date is represented in a binary format as an RFC 3339 UTF-8 or
    ASCII encoded string. Example: "2018-06-16".
    """

    content_type = TEXT_PLAIN

    @staticmethod
    def handles(python_type: Any) -> bool:
        return DateStringCodec.handles(python_type)

    def __init__(self, python_type: Any):
        super().__init__(python_type)
        self.codec = DateStringCodec(python_type)

    def encode(self, value: date) -> BinaryType:
        return self.codec.encode(value).encode()

    def decode(self, value: BinaryType) -> date:
        return self.codec.decode(_b2s(value))


class DateJSONCodec(JSONCodec[date]):
    """
    JSON codec for dates. A date is represented in JSON as an RFC 3339 formatted string.
    Example: "2018-06-16".
    """

    @staticmethod
    def handles(python_type: Any) -> bool:
        return DateStringCodec.handles(python_type)

    def __init__(self, python_type: Any):
        super().__init__(python_type)
        self.codec = DateStringCodec(python_type)

    def encode(self, value: date) -> JSONType:
        return self.codec.encode(value)

    def decode(self, value: JSONType) -> date:
        return self.codec.decode(value)


# ----- datetime -----


def _to_utc(value):
    if value.tzinfo is None:  # naive value interpreted as UTC
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


class DatetimeStringCodec(StringCodec[datetime]):
    """
    String codec for datetime.

    It will decode a datetime represented in an ISO 8601 formatted string. It will encode a
    datetime to an RFC 3339 (subset of ISO 8601) formatted string.

    Datetimes always encode and decode to UTC timezone offset.

    Example: "2020-04-07T12:34:56.789012Z".
    """

    @staticmethod
    def handles(python_type: Any) -> bool:
        python_type = strip_annotations(python_type)
        return is_subclass(python_type, datetime)

    def encode(self, value: datetime) -> StringType:
        if not isinstance(value, datetime):
            raise EncodeError
        result = _to_utc(value).isoformat()
        if result.endswith("+00:00"):
            result = result[0:-6]
        if "+" not in result and not result.endswith("Z"):
            result = f"{result}Z"
        return result

    def decode(self, value: StringType) -> datetime:
        if not isinstance(value, str):
            raise DecodeError
        with _wrap(DecodeError):
            return _to_utc(iso8601.parse_date(value))


class DatetimeBinaryCodec(BinaryCodec[datetime]):
    """
    Binary codec for datetime.

    It will decode a datetime represented in an ISO 8601 formatted UTF-8 or ASCII encoded byte
    string. It will encode a datetime to an RFC 3339 (subset of ISO 8601) formatted UTF-8
    encoded byte string.

    Datetimes always encode and decode to UTC timezone offset.

    Example: b"2020-04-07T12:34:56.789012Z".
    """

    content_type = TEXT_PLAIN

    @staticmethod
    def handles(python_type: Any) -> bool:
        return DatetimeStringCodec.handles(python_type)

    def __init__(self, python_type: Any):
        super().__init__(python_type)
        self.codec = DatetimeStringCodec(python_type)

    def encode(self, value: datetime) -> BinaryType:
        return self.codec.encode(value).encode()

    def decode(self, value: BinaryType) -> datetime:
        return self.codec.decode(_b2s(value))


class DatetimeJSONCodec(JSONCodec[datetime]):
    """
    String codec for datetime.

    It will decode a datetime represented in an ISO 8601 formatted string. It will encode a
    datetime to an RFC 3339 (subset of ISO 8601) formatted string.

    Datetimes always encode and decode to UTC timezone offset.

    Example: "2020-04-07T12:34:56.789012Z".
    """

    @staticmethod
    def handles(python_type: Any) -> bool:
        return DatetimeStringCodec.handles(python_type)

    def __init__(self, python_type: Any):
        super().__init__(python_type)
        self.codec = DatetimeStringCodec(python_type)

    def encode(self, value: datetime) -> JSONType:
        return self.codec.encode(value)

    def decode(self, value: JSONType) -> datetime:
        return self.codec.decode(value)


# ----- UUID -----


class UUIDStringCodec(StringCodec[UUID]):
    """String codec for UUID."""

    @staticmethod
    def handles(python_type: Any) -> bool:
        python_type = strip_annotations(python_type)
        return is_subclass(python_type, UUID)

    def encode(self, value: UUID) -> StringType:
        if not isinstance(value, UUID):
            raise EncodeError
        return str(value)

    def decode(self, value: StringType) -> UUID:
        if not isinstance(value, str):
            raise DecodeError
        with _wrap(DecodeError):
            return UUID(value)


class UUIDBinaryCodec(BinaryCodec[UUID]):
    """Binary codec for UUID."""

    content_type = TEXT_PLAIN

    @staticmethod
    def handles(python_type: Any) -> bool:
        return UUIDStringCodec.handles(python_type)

    def __init__(self, python_type: Any):
        super().__init__(python_type)
        self.codec = UUIDStringCodec(python_type)

    def encode(self, value: UUID) -> BinaryType:
        return self.codec.encode(value).encode()

    def decode(self, value: BinaryType) -> UUID:
        return self.codec.decode(_b2s(value))


class UUIDJSONCodec(JSONCodec[UUID]):
    """JSON codec for UUID."""

    @staticmethod
    def handles(python_type: Any) -> bool:
        return UUIDStringCodec.handles(python_type)

    def __init__(self, python_type: Any):
        super().__init__(python_type)
        self.codec = UUIDStringCodec(python_type)

    def encode(self, value: UUID) -> JSONType:
        return self.codec.encode(value)

    def decode(self, value: JSONType) -> UUID:
        return self.codec.decode(value)


# ----- TypedDict -----


class TypedDictJSONCodec(JSONCodec[PT]):
    """..."""

    @staticmethod
    def handles(python_type: Any) -> bool:
        python_type = strip_annotations(python_type)
        return typing.is_typeddict(python_type)

    def __init__(self, python_type: Any):
        super().__init__(python_type)
        python_type = strip_annotations(python_type)
        self.hints = typing.get_type_hints(python_type, include_extras=True)
        if {type(k) for k in self.hints.keys()} != {str}:
            raise TypeError("codec only supports TypedDict with str keys")

    def _process(self, value: dict[str, Any], method) -> dict[str, Any]:
        result = {}
        for key in self.hints:
            codec = JSONCodec.get(self.hints[key])
            with suppress(KeyError):
                with CodecError.path_on_error(key):
                    result[key] = getattr(codec, method)(value[key])
        return result

    def encode(self, value: PT) -> JSONType:
        if not isinstance(value, dict):
            raise EncodeError
        return self._process(value, "encode")

    def decode(self, value: JSONType) -> PT:
        return self._process(value, "decode")


class TypedDictStringCodec(StringCodec[PT]):
    """..."""

    @staticmethod
    def handles(python_type: Any) -> bool:
        return TypedDictJSONCodec.handles(python_type)

    def __init__(self, python_type: Any):
        super().__init__(python_type)
        self.codec = TypedDictJSONCodec(python_type)

    def encode(self, value: PT) -> StringType:
        if not isinstance(value, dict):
            raise EncodeError
        return json.dumps(self.codec.encode(value))

    def decode(self, value: StringType) -> PT:
        return self.codec.decode(_s2j(value))


class TypedDictBinaryCodec(BinaryCodec[PT]):
    """..."""

    content_type = APPLICATION_JSON

    @staticmethod
    def handles(python_type: Any) -> bool:
        return TypedDictStringCodec.handles(python_type)

    def __init__(self, python_type: Any):
        super().__init__(python_type)
        self.codec = TypedDictStringCodec(python_type)

    def encode(self, value: PT) -> BinaryType:
        return self.codec.encode(value).encode()

    def decode(self, value: BinaryType) -> PT:
        return self.codec.decode(_b2s(value))


# ----- tuple -----


class _TupleCodec(Codec[PT, TT]):
    @classmethod
    def handles(cls, python_type: Any) -> bool:
        python_type = strip_annotations(python_type)
        return is_subclass(python_type, tuple) or is_subclass(get_origin(python_type), tuple)

    def __init__(self, python_type: Any, base_codec_type: type[Codec[PT, TT]]):
        python_type = strip_annotations(python_type)
        args = get_args(python_type) or (Any, ...)
        if len(args) != 2 and Ellipsis in args or args[0] is Ellipsis:
            raise TypeError(f"unexpected ellipsis in tuple[{', '.join(args)}]")
        self.varg = args[0] if len(args) == 2 and args[1] is Ellipsis else None
        self.args = () if self.varg else args
        self.codecs = [base_codec_type.get(arg) for arg in self.args]
        self.vcodec = base_codec_type.get(self.varg) if self.varg else None

    def _encode(self, value: PT) -> list[Any]:
        if not isinstance(value, tuple) or (self.args and len(value) != len(self.args)):
            raise EncodeError
        # TODO: path
        return list(
            (self.vcodec.encode(item) for item in value)
            if self.vcodec
            else (self.codecs[n].encode(value[n]) for n in range(len(self.codecs)))
        )

    def _decode(self, value: list[Any]) -> PT:
        if not isinstance(value, list) or (self.args and len(value) != len(self.args)):
            raise DecodeError
        # TODO: path
        return tuple(
            (self.vcodec.decode(item) for item in value)
            if self.vcodec
            else (self.codecs[n].decode(value[n]) for n in range(len(self.codecs)))
        )


class TupleJSONCodec(_TupleCodec[PT, JSONType], JSONCodec[PT]):
    """..."""

    def __init__(self, python_type: Any):
        _TupleCodec.__init__(self, python_type, JSONCodec)
        JSONCodec.__init__(self, python_type)

    def encode(self, value: PT) -> JSONType:
        return self._encode(value)

    def decode(self, value: JSONType) -> PT:
        return self._decode(value)


class TupleStringCodec(_TupleCodec[PT, StringType], StringCodec[PT]):
    """..."""

    def __init__(self, python_type: Any):
        _TupleCodec.__init__(self, python_type, StringCodec)
        StringCodec.__init__(self, python_type)

    def encode(self, value: PT) -> StringType:
        return _csv_encode(self._encode(value))

    def decode(self, value: StringType) -> PT:
        return self._decode(_csv_decode(value))


class TupleBinaryCodec(BinaryCodec[PT]):
    """..."""

    content_type = APPLICATION_JSON

    @staticmethod
    def handles(python_type: Any) -> bool:
        return TupleJSONCodec.handles(python_type)

    def __init__(self, python_type: Any):
        super().__init__(python_type)
        self.codec = TupleJSONCodec(python_type)

    def encode(self, value: PT) -> BinaryType:
        return json.dumps(self.codec.encode(value)).encode()

    def decode(self, value: BinaryType) -> PT:
        return self.codec.decode(_s2j(_b2s(value)))


# ----- Mapping -----


class MappingJSONCodec(JSONCodec[PT]):
    """..."""

    @staticmethod
    def handles(python_type: Any) -> bool:
        python_type = strip_annotations(python_type)
        origin = get_origin(python_type) or python_type
        return is_subclass(origin, Mapping) and not getattr(origin, "__annotations__", None)

    def __init__(self, python_type: Any):
        super().__init__(python_type)
        python_type = strip_annotations(python_type)
        args = get_args(python_type) or (Any, Any)
        if len(args) != 2:
            raise TypeError("expecting Mapping[KT, VT]")
        self.key_codec = StringCodec.get(args[0])
        self.value_codec = JSONCodec.get(args[1])

    def encode(self, value: PT) -> JSONType:
        if not isinstance(value, Mapping):
            raise EncodeError
        result = {}
        for k, v in value.items():
            key = self.key_codec.encode(k)
            with CodecError.path_on_error(key):
                result[key] = self.value_codec.encode(v)
        return result

    def decode(self, value: JSONType) -> PT:
        if not isinstance(value, Mapping):
            raise DecodeError
        result = {}
        for k, v in value.items():
            key = self.key_codec.decode(k)
            with CodecError.path_on_error(key):
                result[key] = self.value_codec.decode(v)
        return result


class MappingStringCodec(StringCodec[PT]):
    """..."""

    @staticmethod
    def handles(python_type: Any) -> bool:
        return MappingJSONCodec.handles(python_type)

    def __init__(self, python_type: Any):
        super().__init__(python_type)
        self.codec = MappingJSONCodec(python_type)

    def encode(self, value: PT) -> StringType:
        return json.dumps(self.codec.encode(value))

    def decode(self, value: StringType) -> PT:
        return self.codec.decode(_s2j(value))


class MappingBinaryCodec(BinaryCodec[PT]):
    """..."""

    content_type = APPLICATION_JSON

    @staticmethod
    def handles(python_type: Any) -> bool:
        return MappingStringCodec.handles(python_type)

    def __init__(self, python_type: Any):
        super().__init__(python_type)
        self.codec = MappingStringCodec(python_type)

    def encode(self, value: PT) -> BinaryType:
        if not isinstance(value, Mapping):
            raise EncodeError
        return self.codec.encode(value).encode()

    def decode(self, value: BinaryType) -> PT:
        return self.codec.decode(_b2s(value))


# ----- Iterable -----


class _IterableCodec(Codec[PT, TT]):
    """..."""

    _AVOID = str | bytes | bytearray | Mapping | tuple

    @classmethod
    def handles(cls, python_type: Any) -> bool:
        python_type = strip_annotations(python_type)
        origin = get_origin(python_type) or python_type
        return is_subclass(origin, Iterable) and not is_subclass(origin, _IterableCodec._AVOID)

    def __init__(self, python_type: Any, base_codec_type: type[Codec[PT, TT]]):
        python_type = strip_annotations(python_type)
        origin = get_origin(python_type) or python_type
        args = get_args(python_type) or (Any,)
        if len(args) != 1:
            raise TypeError("expecting Iterable[T]")
        self.decode_type = list if origin is Iterable else python_type
        self.codec = base_codec_type.get(args[0])
        self.is_set = is_subclass(origin, Set)

    def _encode(self, value: PT) -> list[Any]:
        if not isinstance(value, Iterable) or isinstance(value, _IterableCodec._AVOID):
            raise EncodeError
        if self.is_set:
            value = sorted(value)
        # TODO: path
        return [self.codec.encode(item) for item in value]

    def _decode(self, value: list[Any]) -> PT:
        if not isinstance(value, list):
            raise DecodeError
        # TODO: path
        return self.decode_type((self.codec.decode(item) for item in value))


class IterableJSONCodec(_IterableCodec[PT, JSONType], JSONCodec[PT]):
    """..."""

    def __init__(self, python_type: Any):
        JSONCodec.__init__(self, python_type)
        _IterableCodec.__init__(self, python_type, JSONCodec)

    def encode(self, value: PT) -> JSONType:
        return self._encode(value)

    def decode(self, value: JSONType) -> PT:
        return self._decode(value)


class IterableStringCodec(_IterableCodec[PT, StringType], StringCodec[PT]):
    """..."""

    def __init__(self, python_type: Any):
        JSONCodec.__init__(self, python_type)
        _IterableCodec.__init__(self, python_type, StringCodec)

    def encode(self, value: PT) -> StringType:
        return _csv_encode(self._encode(value))

    def decode(self, value: StringType) -> PT:
        return self._decode(_csv_decode(value))


class IterableBinaryCodec(BinaryCodec[PT]):
    """..."""

    content_type = APPLICATION_JSON

    @staticmethod
    def handles(python_type: Any) -> bool:
        return IterableJSONCodec.handles(python_type)

    def __init__(self, python_type: Any):
        super().__init__(python_type)
        self.codec = IterableJSONCodec(python_type)

    def encode(self, value: PT) -> BinaryType:
        return json.dumps(self.codec.encode(value)).encode()

    def decode(self, value: BinaryType) -> PT:
        return self.codec.decode(_s2j(_b2s(value)))


# ----- Generic -----


class _GenericCodec(Generic[PT, TT]):
    """..."""

    @classmethod
    def handles(cls, python_type: Any) -> bool:
        python_type = strip_annotations(python_type)
        with suppress(AttributeError):
            return Generic in get_origin(python_type).__bases__
        return False

    def __init__(self, python_type: Any, base_codec_type: type[Codec[PT, TT]]):
        self.raw_type = strip_annotations(python_type)
        self.base_codec_type = base_codec_type

    @property
    def _codec(self) -> Codec[PT, TT]:
        return self.base_codec_type.get(get_origin(self.raw_type))

    def encode(self, value: PT) -> TT:
        with fondat.types.capture_typevars(self.raw_type):
            return self._codec.encode(value)

    def decode(self, value: PT) -> TT:
        with fondat.types.capture_typevars(self.raw_type):
            return self._codec.decode(value)


class GenericJSONCodec(_GenericCodec[PT, JSONType], JSONCodec[PT]):
    def __init__(self, python_type: Any):
        JSONCodec.__init__(self, python_type)
        _GenericCodec.__init__(self, python_type, JSONCodec)


class GenericStringCodec(_GenericCodec[PT, StringType], StringCodec[PT]):
    def __init__(self, python_type: Any):
        StringCodec.__init__(self, python_type)
        _GenericCodec.__init__(self, python_type, StringCodec)


class GenericBinaryCodec(_GenericCodec[PT, BinaryType], BinaryCodec[PT]):
    def __init__(self, python_type: Any):
        BinaryCodec.__init__(self, python_type)
        _GenericCodec.__init__(self, python_type, BinaryCodec)


# ----- TypeVar -----


class _TypeVarCodec(Generic[PT, TT]):
    """..."""

    _cache = False  # TypeVars can be reused

    @classmethod
    def handles(cls, python_type: Any) -> bool:
        python_type = strip_annotations(python_type)
        return isinstance(python_type, TypeVar)

    def __init__(self, python_type: Any, base_codec_type: type[Codec[PT, TT]]):
        self.raw_type = strip_annotations(python_type)
        self.base_codec_type = base_codec_type

    @property
    def _codec(self) -> Codec[PT, TT]:
        resolved = fondat.types.resolve_typevar(self.raw_type)
        return self.base_codec_type.get(resolved)

    def encode(self, value: PT) -> TT:
        return self._codec.encode(value)

    def decode(self, value: TT) -> PT:
        return self._codec.decode(value)


class TypeVarJSONCodec(_TypeVarCodec[PT, JSONType], JSONCodec[PT]):
    """..."""

    def __init__(self, python_type: Any):
        JSONCodec.__init__(self, python_type)
        _TypeVarCodec.__init__(self, python_type, JSONCodec)


class TypeVarStringCodec(_TypeVarCodec[PT, StringType], StringCodec[PT]):
    """..."""

    def __init__(self, python_type: Any):
        StringCodec.__init__(self, python_type)
        _TypeVarCodec.__init__(self, python_type, StringCodec)


class TypeVarBinaryCodec(_TypeVarCodec[PT, BinaryType], BinaryCodec[PT]):
    """..."""

    def __init__(self, python_type: Any):
        BinaryCodec.__init__(self, python_type)
        _TypeVarCodec.__init__(self, python_type, BinaryCodec)
        self.content_type = self._codec.content_type


# ----- dataclass -----


class DataclassJSONCodec(JSONCodec[PT]):

    # keywords have _ suffix in dataclass fields (e.g. "in_", "for_", ...)
    _dc_kw = {k + "_": k for k in keyword.kwlist}

    @staticmethod
    def handles(python_type: Any) -> bool:
        python_type = strip_annotations(python_type)
        return dataclasses.is_dataclass(python_type)

    def __init__(self, python_type: Any):
        super().__init__(python_type)
        self.raw_type = strip_annotations(python_type)
        self.hints = typing.get_type_hints(self.raw_type, include_extras=True)

    @property
    def _codecs(self) -> Iterable[JSONCodec[Any]]:
        return {key: JSONCodec.get(hint) for key, hint in self.hints.items()}

    def encode(self, value: PT) -> JSONType:
        if not isinstance(value, self.raw_type):
            raise EncodeError
        result = {}
        for field in dataclasses.fields(self.raw_type):
            v = getattr(value, field.name, None)
            if v is not None:
                with CodecError.path_on_error(field.name):
                    result[
                        DataclassJSONCodec._dc_kw.get(field.name, field.name)
                    ] = self._codecs[field.name].encode(v)
        return result

    def decode(self, value: JSONType) -> PT:
        if not isinstance(value, dict):
            raise DecodeError
        kwargs = {}
        for field in dataclasses.fields(self.raw_type):
            try:
                with CodecError.path_on_error(field.name):
                    kwargs[field.name] = self._codecs[field.name].decode(
                        value[DataclassJSONCodec._dc_kw.get(field.name, field.name)]
                    )
            except KeyError:
                if (
                    is_optional(field.type)
                    and field.default is dataclasses.MISSING
                    and field.default_factory is dataclasses.MISSING
                ):
                    kwargs[field.name] = None
        with _wrap(DecodeError):
            return self.raw_type(**kwargs)


class DataclassStringCodec(StringCodec[PT]):
    """..."""

    @staticmethod
    def handles(python_type: Any) -> bool:
        return DataclassJSONCodec.handles(python_type)

    def __init__(self, python_type: Any):
        super().__init__(python_type)
        self.codec = DataclassJSONCodec(python_type)

    def encode(self, value: PT) -> StringType:
        return json.dumps(self.codec.encode(value))

    def decode(self, value: StringType) -> PT:
        return self.codec.decode(_s2j(value))


class DataclassBinaryCodec(BinaryCodec[PT]):
    """..."""

    content_type = APPLICATION_JSON

    @staticmethod
    def handles(python_type: Any) -> bool:
        return DataclassStringCodec.handles(python_type)

    def __init__(self, python_type: Any):
        super().__init__(python_type)
        self.codec = DataclassStringCodec(python_type)

    def encode(self, value: PT) -> BinaryType:
        return self.codec.encode(value).encode()

    def decode(self, value: BinaryType) -> PT:
        return self.codec.decode(_b2s(value))


# ----- UnionType/Union -----


class _UnionCodec(Codec[PT, TT]):
    """..."""

    @classmethod
    def handles(cls, python_type: Any) -> bool:
        python_type = strip_annotations(python_type)
        return get_origin(python_type) in {UnionType, Union}

    def __init__(self, python_type: Any, base_codec_type: type[Codec[PT, TT]]):
        python_type = strip_annotations(python_type)
        self.codecs = tuple(base_codec_type.get(type) for type in set(get_args(python_type)))

    def encode(self, value: PT) -> TT:
        for codec in self.codecs:
            with suppress(EncodeError):
                return codec.encode(value)
        raise EncodeError

    def decode(self, value: TT) -> PT:
        for codec in self.codecs:
            with suppress(DecodeError):
                return codec.decode(value)
        raise DecodeError


class UnionJSONCodec(_UnionCodec[PT, Any], JSONCodec[PT]):
    """..."""

    def __init__(self, python_type: Any):
        JSONCodec.__init__(self, python_type)
        _UnionCodec.__init__(self, python_type, JSONCodec)


class UnionStringCodec(_UnionCodec[PT, str], StringCodec[PT]):
    """..."""

    def __init__(self, python_type: Any):
        StringCodec.__init__(self, python_type)
        _UnionCodec.__init__(self, python_type, StringCodec)


class UnionBinaryCodec(_UnionCodec[PT, bytes | bytearray], BinaryCodec[PT]):
    """..."""

    content_type = APPLICATION_OCTET_STREAM

    @staticmethod
    def handles(python_type: Any) -> bool:
        return _UnionCodec.handles(python_type)

    def __init__(self, python_type: Any):
        BinaryCodec.__init__(self, python_type)
        _UnionCodec.__init__(self, python_type, BinaryCodec)


# ----- Literal -----


_VT = namedtuple("VT", "value,type")


class _LiteralCodec(Codec[PT, TT]):
    """..."""

    @classmethod
    def handles(cls, python_type: Any) -> bool:
        python_type = strip_annotations(python_type)
        return get_origin(python_type) is Literal

    def __init__(self, python_type: Any, base_codec_type: type[Codec[PT, TT]]):
        python_type = strip_annotations(python_type)
        self.vts = {_VT(v, type(v)) for v in fondat.types.literal_values(python_type)}
        self.codecs = {vt: base_codec_type.get(vt.type) for vt in self.vts}

    def encode(self, value: PT) -> TT:
        with _wrap(EncodeError):
            return self.codecs[_VT(value, type(value))].encode(value)
        raise EncodeError

    def decode(self, value: TT) -> PT:
        for codec in self.codecs.values():
            with suppress(DecodeError):
                decoded = codec.decode(value)
                if _VT(decoded, type(decoded)) in self.vts:
                    return decoded
        raise DecodeError


class LiteralStringCodec(_LiteralCodec[PT, StringType], StringCodec[PT]):
    """..."""

    def __init__(self, python_type: Any):
        StringCodec.__init__(self, python_type)
        _LiteralCodec.__init__(self, python_type, StringCodec)


class LiteralBinaryCodec(_LiteralCodec[PT, BinaryType], BinaryCodec[PT]):
    """..."""

    content_type = APPLICATION_OCTET_STREAM

    def __init__(self, python_type: Any):
        StringCodec.__init__(self, python_type)
        _LiteralCodec.__init__(self, python_type, BinaryCodec)


class LiteralJSONCodec(_LiteralCodec[PT, JSONType], JSONCodec[PT]):
    """..."""

    def __init__(self, python_type: Any):
        JSONCodec.__init__(self, python_type)
        _LiteralCodec.__init__(self, python_type, JSONCodec)


# ----- Any -----


class AnyStringCodec(StringCodec[Any]):
    """String codec for Any."""

    @staticmethod
    def handles(python_type: Any) -> bool:
        python_type = strip_annotations(python_type)
        return python_type is Any

    def encode(self, value: Any) -> StringType:
        return StringCodec.get(type(value)).encode(value)

    def decode(self, value: StringType) -> Any:
        return value


class AnyBinaryCodec(BinaryCodec[Any]):
    """Binary codec for Any."""

    content_type = APPLICATION_OCTET_STREAM

    @staticmethod
    def handles(python_type: Any) -> bool:
        python_type = strip_annotations(python_type)
        return python_type is Any

    def encode(self, value: Any) -> BinaryType:
        return BinaryCodec.get(type(value)).encode(value)

    def decode(self, value: BinaryType) -> Any:
        if not isinstance(value, (bytes, bytearray)):
            raise DecodeError
        return value


class AnyJSONCodec(JSONCodec[Any]):
    """JSON codec for Any."""

    @staticmethod
    def handles(python_type: Any) -> bool:
        python_type = strip_annotations(python_type)
        return python_type is Any

    def encode(self, value: Any) -> Any:
        return JSONCodec.get(type(value)).encode(value)

    def decode(self, value: Any) -> Any:
        return value
