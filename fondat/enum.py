"""Module for enumeration data type."""

import collections.abc
import enum
import re

from collections.abc import Iterable, Mapping
from typing import Union

try:
    from enum import StrEnum
except ImportError:

    class StrEnum(str, enum.Enum):
        def __new__(cls, *values):
            value = str(*values)
            member = str.__new__(cls, value)
            member._value_ = value
            return member

        __str__ = str.__str__


def _key(v):
    if not v.isidentifier():
        raise ValueError("str_enum only supports identifier values")
    return v.upper()


def str_enum(typename: str, values: Union[str, Iterable[str]]):
    """
    Generate an enumeration for string values.

    • typename: the name of the enumeration class.
    • values: values with which to compose the enumeration.
    """
    if isinstance(values, str):
        values = values.replace(",", " ").split()
    return StrEnum(typename, {_key(v): v for v in values})
