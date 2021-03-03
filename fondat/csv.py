"""
Comma separated values encoding module.

This module contains encoders that generate values that can be interpreted by most
spreadsheets: currency, percent, number. These encoders can also be configured to round values
to a specific decimal precision.
"""

import csv
import io

from collections.abc import Callable, Iterable
from dataclasses import dataclass
from decimal import Decimal
from fondat.codec import get_codec, String
from typing import Any, Optional


def _round(value: Any, precision: Optional[int]) -> str:
    if precision is None:  # floating point
        value = str(value)
        if "." in value:
            value = value.rstrip("0").rstrip(".")
        return value
    if precision == 0:
        return str(int(round(value)))
    return f"{{:.{precision}f}}".format(value)


def currency_encoder(
    prefix: str = "", suffix: str = "", precision: int = None
) -> Callable[(Any,), str]:
    """
    Return a function that encodes a number as a currency value.

    Parameters:
    • prefix: currency prefix (e.g. "$")
    • suffix: currency suffix
    • precision: round values to precise precision  [floating point]
    """

    def encode(value: Any) -> str:
        return f"{prefix}{_round(value, precision)}{suffix}" if value is not None else ""

    return encode


def percent_encoder(precision: int = None) -> Callable[(Any,), str]:
    """
    Return a function that encodes fractional value as a percentage.

    Parameter:
    • precision: round values to precise precision  [floating point]
    """

    def encode(value: Any) -> str:
        if value is None:
            return ""
        value = str(_round(Decimal(str(value)) * 100, precision))
        return f"{value}%"

    return encode


def number_encoder(precision: int = None) -> Callable[(Any,), str]:
    """
    Return a function that encodes a numeric value.

    Parameter:
    • precision: round values to precise precision  [floating point]
    """

    def encode(value: Any) -> str:
        return _round(value, precision) if value is not None else ""

    return encode


class DataclassWriter:
    """
    Writes dataclass instances as CSV rows.

    Each dataclass attribute becomes a column in a row. An attribute is encoded by either a
    supplied encoder, or with an appropriate string codec.

    Parameters:
    • fileobj: writeable file-like object to write CSV into
    • dataclass: dataclass defining columns in CSV row
    • encoders: mapping of attribute names to encoder to encode it in CSV
    • dialect: string, type or instance for dialect of CSV file being written
    """

    __slots__ = ("_writer", "_dataclass", "_encoders")

    def __init__(
        self,
        fileobj: Any,
        dataclass: type,
        encoders: dict[str, Callable] = None,
        dialect: Any = "excel",
    ):
        self._writer = csv.writer(fileobj, dialect)
        self._dataclass = dataclass
        self._encoders = {
            name: encoders.get(name, get_codec(String, dataclass.__annotations__[name]).encode)
            for name in dataclass.__annotations__
        }

    def write_header(self) -> None:
        self._writer.writerow(name for name in self._dataclass.__annotations__)

    def write_row(self, row: Any) -> None:
        columns = (
            self._encoders[name](getattr(row, name)) for name in self._dataclass.__annotations__
        )
        self._writer.writerow(columns)

    def write_rows(self, rows: Iterable[Any]) -> None:
        for row in rows:
            self.write_row(row)
