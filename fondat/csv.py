"""Comma separated values encoding module."""

import dataclasses

from collections.abc import Mapping, Sequence
from fondat.codec import Codec, String, get_codec, DecodeError
from fondat.data import derive_typeddict
from fondat.types import is_optional, is_subclass
from typing import Any, Optional, get_type_hints


def _round(value: Any, precision: Optional[int]) -> str:
    if precision is None:  # floating point
        value = str(value)
        if "." in value:
            value = value.rstrip("0").rstrip(".")
        return value
    if precision == 0:
        return str(round(value))
    return f"{{:.{precision}f}}".format(value)


def currency_codec(
    python_type: Any, prefix: str = "", suffix: str = "", precision: Optional[int] = None
):
    """
    Return a codec that encodes/decodes a number as a currency value; optionally encodes with
    fixed-point precision.

    Parameters:
    • python_type: type of the value to be encoded/decoded
    • prefix: currency prefix (e.g. "$")
    • suffix: currency suffix
    • precision: round encoded value to number of digits  [floating point]
    """

    codec = get_codec(String, python_type)

    class CurrencyCodec(String[python_type]):
        def encode(self, value: python_type) -> str:
            return f"{prefix}{_round(value, precision)}{suffix}" if value is not None else ""

        def decode(self, value: str) -> python_type:
            result = codec.decode(value.lstrip(prefix).rstrip(suffix))
            if precision is not None:
                result = round(result, precision)
            return result

    return CurrencyCodec()


def percent_codec(python_type: Any, precision: int):
    """
    Return a codec that encodes/decodes a fractional value as a percentage string with
    fixed-point precision.

    Parameter:
    • python_type: type of the value to be encoded/decoded
    • precision: round encoded value to number of digits
    """

    codec = get_codec(String, python_type)

    class PercentCodec(String[python_type]):
        def encode(self, value: python_type) -> str:
            return f"{_round(value * 100, precision)}%"

        def decode(self, value: str) -> python_type:
            result = codec.decode(value.rstrip("%")) / 100
            if precision is not None:
                result = round(result, precision + 2)
            return result

    return PercentCodec()


def fixed_codec(python_type: Any, precision: int):
    """
    Return a codec encodes/decodes a number with fixed-point precision.

    Parameter:
    • python_type: type of the value to be encoded/decoded
    • precision: round encoded value to number of digits
    """

    codec = get_codec(String, python_type)

    class FixedCodec(String[python_type]):
        def encode(self, value: Any) -> str:
            return _round(value, precision) if value is not None else ""

        def decode(self, value: str) -> python_type:
            result = codec.decode(value)
            if precision is not None:
                result = round(result, precision)
            return result

    return FixedCodec()


def typeddict_codec(
    typeddict: Any,
    columns: Optional[Sequence[str]] = None,
    keys: Optional[Mapping[str, str]] = None,
    codecs: Optional[Mapping[str, Any]] = None,
):
    """
    Return a codec that encodes/decodes a typed dictionary to/from a CSV row. A CSV row is
    represented as a list of strings.

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

    if not is_subclass(typeddict, dict) or getattr(typeddict, "__annotations__", None) is None:
        raise TypeError("typeddict parameter must be a TypedDict")

    hints = get_type_hints(typeddict, include_extras=True)

    if columns is None:
        columns = tuple(key for key in hints.keys())

    if keys is None:
        keys = {key: key for key in hints}

    keys = {column: key for column, key in keys.items() if column in columns}

    if codecs is None:
        codecs = {}

    codecs = {
        column: codecs.get(column, get_codec(String, hints[keys[column]]))
        for column in columns
        if column in keys
    }

    optional_fields = {key for key in keys if is_optional(hints[key])}

    class TypedDictRowCodec(Codec[typeddict, list[str]]):
        """Encodes/decodes a dataclass to/from a CSV row."""

        def __init__(self, columns: Sequence[str]):
            self.columns = columns

        def encode(self, value: typeddict) -> list[str]:
            """
            Encode from TypedDict value to CSV row. If a field value is None, it will be
            represented in a column as an empty string.
            """
            return [codecs[column].encode(value.get(keys[column])) for column in self.columns]

        def decode(self, values: list[str]) -> typeddict:
            """
            Decode from CSV row to TypedDict value. If a column to decode contains an empty
            string value, it will be represented as None if the associated field is optional.
            """
            items = {}
            for column, value in zip(self.columns, values):
                key = keys.get(column)
                if not key:  # ignore unmapped column
                    continue
                if value == "" and key in optional_fields:
                    items[key] = None
                else:
                    with DecodeError.path_on_error(column):
                        items[key] = codecs[column].decode(value)
            return typeddict(items)

    return TypedDictRowCodec(columns=columns)


def dataclass_codec(
    dataclass: Any,
    columns: Sequence[str] = None,
    fields: Mapping[str, str] = None,
    codecs: Mapping[str, Any] = None,
):
    """
    Return a codec that encodes/decodes a dataclass to/from a CSV row. A CSV row is
    represented as a list of strings.

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

    td_codec = typeddict_codec(
        derive_typeddict("TD", dataclass), columns=columns, keys=fields, codecs=codecs
    )

    class DataclassRowCodec(Codec[dataclass, list[str]]):
        """Encodes/decodes a dataclass value to/from a CSV row."""

        def __init__(self, columns: Sequence[str]):
            self.columns = columns

        def encode(self, value: dataclass) -> list[str]:
            """
            Encode from dataclass value to CSV row. If a field value is None, it will be
            represented in a column as an empty string.
            """
            return td_codec.encode(dataclasses.asdict(value))

        def decode(self, values: list[str]) -> dataclass:
            """
            Decode from CSV row to dataclass value. If a column to decode contains an empty
            string value, it will be represented as None if the associated field is optional.
            """
            return dataclass(**td_codec.decode(values))

    return DataclassRowCodec(td_codec.columns)
