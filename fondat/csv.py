"""Comma separated values encoding module."""

import csv
import dataclasses
import io

from collections.abc import AsyncIterator, Iterable, Mapping
from fondat.codec import Codec, DecodeError, StringCodec
from fondat.data import derive_typeddict
from fondat.stream import Reader, Stream
from fondat.types import is_optional, strip_annotations
from numbers import Number
from typing import Any, TypeVar, get_type_hints, is_typeddict


# type variables
N = TypeVar("N", bound=Number)
T = TypeVar("T")


# type aliases
Row = Iterable[str]


def _round(value: Number, precision: int | None) -> str:
    if precision is None:  # floating point
        value = str(value)
        if "." in value:
            value = value.rstrip("0").rstrip(".")
        return value
    if precision == 0:
        return str(round(value))
    return f"{{:.{precision}f}}".format(value)


class CurrencyCodec(Codec[N, str]):
    """
    String codec that encodes/decodes a number as a currency value; optionally encodes with
    fixed-point precision.

    Parameters:
    • python_type: type of the value to be encoded/decoded
    • prefix: currency prefix (e.g. "$")
    • suffix: currency suffix
    • precision: round encoded value to number of digits  [floating point]
    """

    def __init__(
        self,
        python_type: Any,
        prefix: str = "",
        suffix: str = "",
        precision: int | None = None,
    ):
        self.prefix = prefix
        self.suffix = suffix
        self.precision = precision
        self.codec = StringCodec.get(python_type)

    def encode(self, value: N) -> str:
        return (
            f"{self.prefix}{_round(value, self.precision)}{self.suffix}"
            if value is not None
            else ""
        )

    def decode(self, value: str) -> N:
        result = self.codec.decode(value.lstrip(self.prefix).rstrip(self.suffix))
        if self.precision is not None:
            result = round(result, self.precision)
        return result


class PercentCodec(Codec[N, str]):
    """
    String codec that encodes/decodes a fractional number as a percentage string with
    fixed-point precision.

    Parameters:
    • python_type: type of the value to be encoded/decoded
    • precision: round encoded value to number of digits
    """

    def __init__(self, python_type: Any, precision: int):
        self.precision = precision
        self.codec = StringCodec.get(python_type)

    def encode(self, value: N) -> str:
        return f"{_round(value * 100, self.precision)}%"

    def decode(self, value: str) -> N:
        result = self.codec.decode(value.rstrip("%")) / 100
        if self.precision is not None:
            result = round(result, self.precision + 2)
        return result


class FixedCodec(Codec[N, str]):
    """
    String codec encodes/decodes a number with fixed-point precision.

    Parameter:
    • python_type: type of the value to be encoded/decoded
    • precision: round encoded value to number of digits
    """

    def __init__(self, python_type: Any, precision: int):
        self.precision = precision
        self.codec = StringCodec.get(python_type)

    def encode(self, value: N) -> str:
        return _round(value, self.precision) if value is not None else ""

    def decode(self, value: str) -> N:
        result = self.codec.decode(value)
        if self.precision is not None:
            result = round(result, self.precision)
        return result


class TypedDictCodec(Codec[T, Row]):
    """
    Codec that encodes/decodes a typed dictionary to/from a CSV row.

    Parameters:
    • typeddict: TypedDict type to encode/decode
    • columns: ordered column names
    • keys: mapping between columns and dictionary keys
    • codecs: mapping between columns and codecs

    Attribute:
    • columns: ordered column names

    The columns parameter and attribute specifies the names of CSV columns, and the order they
    are encoded in a row. If the columns parameter is omitted, then columns will be all
    TypedDict keys, in the order they are defined in the TypedDict.

    The keys mapping specifies the mapping between columns and dictionary keys. If no key
    for a given column is specified, then the column will map the to dictionary key of the
    same name.

    The codecs mapping specifies which codecs are used to encode columns. If no codec for a
    given column is specified, then the default string codec for its associated type is used.
    """

    def __init__(
        self,
        typeddict: Any,
        columns: Iterable[str] | None = None,
        keys: Mapping[str, str] | None = None,
        codecs: Mapping[str, Any] | None = None,
    ):
        typeddict = strip_annotations(typeddict)
        if not is_typeddict(typeddict):
            raise TypeError("typeddict parameter must be a TypedDict")

        hints = get_type_hints(typeddict, include_extras=True)

        self.columns = columns or tuple(k for k in hints.keys())

        if keys is None:
            keys = {column: column for column in self.columns}

        self._keys = {column: key for column, key in keys.items() if key in hints}

        if codecs is None:
            codecs = {}

        self._codecs = {
            column: codecs.get(column, StringCodec.get(hints[self._keys[column]]))
            for column in self.columns
            if column in self._keys
        }

        self._optional = {key for key, hint in hints.items() if is_optional(hint)}

    def encode(self, value: T) -> Row:
        """
        Encode from TypedDict value to CSV row. If a field value is None, it will be
        represented in a column as an empty string.
        """
        return [
            self._codecs[c].encode(value.get(self._keys[c], None)) if c in self._codecs else ""
            for c in self.columns
        ]

    def decode(self, values: Row) -> T:
        """
        Decode from CSV row to TypedDict value. If a column to decode contains an empty
        string value, it will be represented as None if the associated TypedDict value is
        optional.
        """
        result = {}
        for n in range(len(self.columns)):
            column = self.columns[n]
            key = self._keys.get(column, None)
            if key is not None:
                try:
                    value = values[n]
                except IndexError:
                    value = ""
                if value == "" and key in self._optional:
                    result[key] = None
                else:
                    with DecodeError.path_on_error(column):
                        result[key] = self._codecs[column].decode(value)
        return result


class DataclassCodec(Codec[T, Row]):
    """
    Codec that encodes/decodes a dataclass to/from a CSV row.

    Parameters:
    • dataclass: dataclass type to encode/decode
    • columns: ordered column names
    • fields: mapping between row columns and dataclass fields
    • codecs: mapping between columns and codecs

    Attribute:
    • columns: ordered column names

    The columns parameter specifies the names of CSV columns, and the order they are encoded
    in a row. If the columns parameter is omitted, then columns will be all dataclass
    fields, in the order they are defined in the dataclass.

    The fields mapping specifies the mapping between column names and dictionary keys. If no
    mapping for a given column is specified, then the column will map to the field name of
    the same name.

    The codecs mapping specifies which codecs are used to encode columns. If no mapping for a
    given column is provided, then the default codec for its associated field is used.
    """

    def __init__(
        self,
        dataclass: Any,
        columns: Iterable[str] = None,
        fields: Mapping[str, str] = None,
        codecs: Mapping[str, Any] = None,
    ):
        dataclass = strip_annotations(dataclass)
        if not dataclasses.is_dataclass(dataclass):
            raise TypeError("dataclass parameter must be a dataclass")

        self.dataclass = dataclass
        self.codec = TypedDictCodec(
            typeddict=derive_typeddict("TD", dataclass),
            columns=columns,
            keys=fields,
            codecs=codecs,
        )

    @property
    def columns(self) -> Iterable[str]:
        return self.codec.columns

    def encode(self, value: T) -> Row:
        """
        Encode from dataclass value to CSV row. If a field value is None, it will be
        represented in a column as an empty string.
        """
        return self.codec.encode(dataclasses.asdict(value))

    def decode(self, values: Row) -> T:
        """
        Decode from CSV row to dataclass value. If a column to decode contains an empty
        string value, it will be represented as None if the associated field is optional.
        """
        return self.dataclass(**self.codec.decode(values))


class CSVStream(Stream):
    """
    Streams binary data from a CSV data source.

    Parameters:
    • source: asynchronous iterator over CSV rows
    • dialect: CSV dialect to write rows

    The CSV source can be any asynchronous iterator of rows.
    """

    def __init__(self, source: AsyncIterator[Row], dialect: csv.Dialect = csv.excel):
        self.source = source
        self.dialect = dialect

    async def __anext__(self) -> bytes:
        if not self.source:
            raise StopAsyncIteration
        sio = io.StringIO()
        csv.writer(sio, self.dialect).writerow(await anext(self.source))
        return sio.getvalue().encode()

    async def close(self):
        self.source = None


class CSVReader(AsyncIterator[Row]):
    """
    Reads CSV data from a stream.

    Parameters:
    • stream: stream from which to read binary data
    • dialect: CSV dialect to read rows
    """

    def __init__(self, stream: Stream, dialect: csv.Dialect = csv.excel):
        self.dialect = dialect
        self._reader = Reader(stream)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        await self.close()

    async def __anext__(self) -> Row:
        if not self._reader:
            raise StopAsyncIteration
        row = await self._reader.read_until(b"\n")
        if not row:
            raise StopAsyncIteration
        return next(csv.reader([row.decode()], self.dialect))

    async def close(self):
        if self._reader:
            await self._reader.close()
            self._reader = None
