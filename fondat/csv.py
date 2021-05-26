"""
Comma separated values encoding module.

This module contains encoders that generate values that can be interpreted by most
spreadsheets: currency, percent, number. These encoders can also be configured to round values
to a specific decimal precision.
"""

import csv
import dataclasses
import io

from collections.abc import Iterable, Mapping
from fondat.codec import Codec, String, get_codec
from fondat.types import is_optional
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


def currency_codec(python_type: Any, prefix: str = "", suffix: str = "", precision: int = None):
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


def dataclass_row_codec(
    dataclass: Any,
    columns: Iterable[str] = None,
    fields: Mapping[str, str] = None,
    codecs: Mapping[str, Any] = None,
):
    """
    Return a codec that encodes/decodes a dataclass to/from a CSV row.

    Parameters:
    • dataclass: dataclass type to encode/decode
    • columns: ordered column names
    • fields: column-to-field mappings
    • codecs: column-to-codec mappings

    A row is list of strings.

    The columns parameter specifies the names of CSV columns, and the order they are encoded
    in a row. If the columns parameter is omitted, then columns will be all dataclass
    fields, in the order they are defined in the dataclass.

    The fields mapping specifies which database fields map to which columns. If no mapping for
    a given column is specified, then the column name will match the field name.

    The codecs mapping specifies which codecs are used to encode columns. If no mapping for a
    given column is provided, then the default codec for its associated field is used.
    """

    if not dataclasses.is_dataclass(dataclass):
        raise TypeError("dataclass parameter must be a dataclass")

    hints = get_type_hints(dataclass, include_extras=True)

    if columns is None:
        columns = tuple(key for key in hints.keys())

    if fields is None:
        fields = {field: field for field in hints}

    fields = {column: field for column, field in fields.items() if column in columns}

    if codecs is None:
        codecs = {}

    codecs = {
        column: codecs.get(column, get_codec(String, hints[fields[column]]))
        for column in columns
        if column in fields
    }

    optional_fields = {field for field in fields if is_optional(hints[field])}

    class DataclassRowCodec(Codec[dataclass, list[str]]):
        """Encodes/decodes a dataclass to/from a CSV row."""

        def encode(self, value: dataclass) -> list[str]:
            """
            Encode from dataclass to row.

            If a field value is None, it will be represented in a column as an empty string.
            """
            return [codecs[column].encode(getattr(value, fields[column])) for column in columns]

        def decode(self, values: list[str]) -> dataclass:
            """
            Decode from row to dataclass.

            If a column to decode contains an empty string value, it will be represented as
            None if the associated field is optional.
            """
            kwargs = {}
            for column, value in zip(columns, values):
                field = fields.get(column)
                if not field:  # ignore unmapped column
                    continue
                hint = hints[field]
                if value == "" and field in optional_fields:
                    kwargs[field] = None
                else:
                    kwargs[field] = codecs[column].decode(value)
            return dataclass(**kwargs)

    return DataclassRowCodec()
