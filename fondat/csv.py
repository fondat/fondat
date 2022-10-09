"""Comma separated values encoding module."""

import dataclasses

from collections.abc import Iterable, Mapping
from fondat.codec import DecodeError, StringCodec
from fondat.data import derive_typeddict
from fondat.types import is_optional, strip_annotations
from typing import Any, Generic, TypeVar, get_type_hints, is_typeddict


def _round(value: Any, precision: int | None) -> str:
    if precision is None:  # floating point
        value = str(value)
        if "." in value:
            value = value.rstrip("0").rstrip(".")
        return value
    if precision == 0:
        return str(round(value))
    return f"{{:.{precision}f}}".format(value)


T = TypeVar("T")


class CurrencyCodec(Generic[T]):
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

    def encode(self, value: T) -> str:
        return (
            f"{self.prefix}{_round(value, self.precision)}{self.suffix}"
            if value is not None
            else ""
        )

    def decode(self, value: str) -> T:
        result = self.codec.decode(value.lstrip(self.prefix).rstrip(self.suffix))
        if self.precision is not None:
            result = round(result, self.precision)
        return result


class PercentCodec(Generic[T]):
    """
    String codec that encodes/decodes a fractional value as a percentage string with
    fixed-point precision.

    Parameters:
    • python_type: type of the value to be encoded/decoded
    • precision: round encoded value to number of digits
    """

    def __init__(self, python_type: Any, precision: int):
        self.precision = precision
        self.codec = StringCodec.get(python_type)

    def encode(self, value: T) -> str:
        return f"{_round(value * 100, self.precision)}%"

    def decode(self, value: str) -> T:
        result = self.codec.decode(value.rstrip("%")) / 100
        if self.precision is not None:
            result = round(result, self.precision + 2)
        return result


class FixedCodec(Generic[T]):
    """
    String codec encodes/decodes a number with fixed-point precision.

    Parameter:
    • python_type: type of the value to be encoded/decoded
    • precision: round encoded value to number of digits
    """

    def __init__(self, python_type: Any, precision: int):
        self.precision = precision
        self.codec = StringCodec.get(python_type)

    def encode(self, value: T) -> str:
        return _round(value, self.precision) if value is not None else ""

    def decode(self, value: str) -> T:
        result = self.codec.decode(value)
        if self.precision is not None:
            result = round(result, self.precision)
        return result


class TypedDictCodec(Generic[T]):
    """
    Codec that encodes/decodes a typed dictionary to/from a CSV row. A CSV row is a list of
    strings.

    Parameters:
    • typeddict: TypedDict type to encode/decode
    • columns: sequence of column names
    • keys: mapping between columns and dictionary keys
    • codecs: mapping between columns and codecs

    The columns parameter specifies the names of CSV columns, and the order they are encoded
    in a row. If the columns parameter is omitted, then columns will be all dictionary keys,
    in the order they are defined in the TypedDict.

    The keys mapping specifies the mapping between columns and dictionary keys. If no mapping
    for a given column is specified, then the column will map the to dictionary key of the
    same name.

    The codecs mapping specifies which codecs are used to encode columns. If no mapping for a
    given column is provided, then the default codec for its associated field is used.
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

        self.hints = get_type_hints(typeddict, include_extras=True)

        if keys is None:
            keys = {key: key for key in self.hints}

        self.columns = columns or tuple(key for key in self.hints.keys())

        self.keys = {column: key for column, key in keys.items() if column in self.columns}

        if codecs is None:
            codecs = {}

        self.codecs = {
            column: codecs.get(column, StringCodec.get(self.hints[keys[column]]))
            for column in self.columns
            if column in keys
        }

    def encode(self, value: T) -> list[str]:
        """
        Encode from TypedDict value to CSV row. If a field value is None, it will be
        represented in a column as an empty string.
        """
        return [self.codecs[c].encode(value.get(self.keys[c])) for c in self.columns]

    def decode(self, values: list[str]) -> T:
        """
        Decode from CSV row to TypedDict value. If a column to decode contains an empty
        string value, it will be represented as None if the associated field is optional.
        """
        result = {}
        for column, value in zip(self.columns, values):
            key = self.keys.get(column)
            if not key:  # ignore unmapped column
                continue
            if value == "" and is_optional(self.hints[key]):
                result[key] = None
            else:
                with DecodeError.path_on_error(column):
                    result[key] = self.codecs[column].decode(value)
        return result


class DataclassCodec(Generic[T]):
    """
    Codec that encodes/decodes a dataclass to/from a CSV row. A CSV row is a list of strings.

    Parameters:
    • dataclass: dataclass type to encode/decode
    • columns: ordered column names
    • fields: mapping between row columns and dataclass fields
    • codecs: mapping between columns and codecs

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

    def encode(self, value: T) -> list[str]:
        """
        Encode from dataclass value to CSV row. If a field value is None, it will be
        represented in a column as an empty string.
        """
        return self.codec.encode(dataclasses.asdict(value))

    def decode(self, values: list[str]) -> T:
        """
        Decode from CSV row to dataclass value. If a column to decode contains an empty
        string value, it will be represented as None if the associated field is optional.
        """
        return self.dataclass(**self.codec.decode(values))
