import asyncio
import dataclasses
import json
import pytest
import re

from base64 import b64encode
from dataclasses import make_dataclass, field
from decimal import Decimal
from fondat.validation import MinLen, MaxLen, Pattern, MinValue, MaxValue
from fondat.validation import validate, validate_return_value
from io import BytesIO
from datetime import date, datetime, timezone
from typing import Annotated, Literal, Optional, T, TypedDict, Union
from uuid import UUID


# ----- str -----


def test_str_type_success():
    validate("foo", str)


def test_str_type_error():
    with pytest.raises(TypeError):
        validate(123, str)


def test_str_min_length_success():
    validate("12345", Annotated[str, MinLen(3)])


def test_str_min_length_error():
    with pytest.raises(ValueError):
        validate("123", Annotated[str, MinLen(4)])


def test_str_max_length_success():
    validate("12345", Annotated[str, MaxLen(5)])


def test_str_max_length_error():
    with pytest.raises(ValueError):
        validate("1234567", Annotated[str, MaxLen(6)])


def test_str_pattern_success():
    validate("abc", Annotated[str, Pattern(r"^abc$")])


def test_str_pattern_error():
    with pytest.raises(ValueError):
        validate("ghi", Annotated[str, Pattern(r"^def$")])


# ----- int -----


def test_int_type_success():
    validate(123, int)


def test_int_type_error():
    with pytest.raises(TypeError):
        validate(123.45, int)


def test_int_min_success():
    validate(2, Annotated[int, MinValue(1)])


def test_int_minerror():
    with pytest.raises(ValueError):
        validate(1, Annotated[int, MinValue(2)])


def test_int_max_success():
    validate(2, Annotated[int, MaxValue(3)])


def test_int_max_error():
    with pytest.raises(ValueError):
        validate(5, Annotated[int, MaxValue(4)])


def test_int_reject_bool():
    with pytest.raises(TypeError):
        validate(True, int)


# ----- float -----


def test_float_type_success():
    validate(123.45, float)


def test_float_type_error():
    with pytest.raises(TypeError):
        validate("123.45", float)


def test_float_min_success():
    validate(1.1, Annotated[float, MinValue(1.0)])


def test_float_min_error():
    with pytest.raises(ValueError):
        validate(1.9, Annotated[float, MinValue(2.0)])


def test_float_max_success():
    validate(2.9, Annotated[float, MaxValue(3.0)])


def test_float_max_error():
    with pytest.raises(ValueError):
        validate(4.1, Annotated[float, MaxValue(4.0)])


# ----- decimal -----


def test_decimal_type_success():
    validate(Decimal("123.45"), Decimal)


def test_decimal_type_error():
    with pytest.raises(TypeError):
        validate("123.45", Decimal)


def test_decimal_min_success():
    validate(Decimal("1.1"), Annotated[Decimal, MinValue(Decimal("1.0"))])


def test_decimal_min_error():
    with pytest.raises(ValueError):
        validate(Decimal("1.9"), Annotated[Decimal, MinValue(Decimal("2.0"))])


def test_decimal_max_success():
    validate(Decimal("2.9"), Annotated[Decimal, MaxValue(Decimal("3.0"))])


def test_decimal_max_error():
    with pytest.raises(ValueError):
        validate(Decimal("4.1"), Annotated[Decimal, MaxValue(Decimal("4.0"))])


# ----- bool -----


def test_bool_type_true():
    validate(True, bool)


def test_bool_type_false():
    validate(False, bool)


def test_bool_type_error():
    with pytest.raises(TypeError):
        validate("foo", bool)


# ----- date -----


def test_date_type_success():
    validate(date(2015, 6, 7), date)


def test_date_type_error():
    with pytest.raises(TypeError):
        validate("not_a_date", date)


# ----- datetime -----


def test_datetime_type_success():
    validate(datetime(2015, 6, 7, 8, 9, 10, 0, timezone.utc), datetime)


def test_datetime_type_error():
    with pytest.raises(TypeError):
        validate("not_a_datetime", datetime)


# ----- uuid -----


def test_uuid_type_success():
    validate(UUID("af327a12-c469-11e4-8e4f-af4f7c44473b"), UUID)


def test_uuid_type_error():
    with pytest.raises(TypeError):
        validate("not_a_uuid", UUID)


# ----- bytes -----


def test_bytes_type_success():
    validate(bytes([1, 2, 3]), bytes)


def test_bytes_type_error():
    with pytest.raises(TypeError):
        validate("not_a_byte_string", bytes)


# ----- dict -----


def test_dict_success():
    validate(dict(a=1, b=2), dict[str, int])


def test_dict_type_error():
    with pytest.raises(TypeError):
        validate("should_not_validate", dict[str, int])


def test_dict_error_key_type():
    with pytest.raises(TypeError):
        validate({1: 2}, dict[str, int])


def test_dict_error_value_type():
    with pytest.raises(TypeError):
        validate(dict(this="should_not_validate"), dict[str, int])


# ----- list -----


def test_list_type_str_success():
    validate(["a", "b", "c"], list[str])


def test_list_type_int_success():
    validate([1, 2, 3], list[int])


def test_list_type_str_error():
    with pytest.raises(TypeError):
        validate([4, 5, 6], list[str])


def test_list_type_int_error():
    with pytest.raises(TypeError):
        validate(["d", "e", "f"], list[int])


def test_list_type_error():
    with pytest.raises(TypeError):
        validate("this_is_not_a_list", list[bool])


def test_list_min_success():
    validate([1, 2, 3], Annotated[list[int], MinLen(2)])


def test_list_min_error():
    with pytest.raises(ValueError):
        validate([1, 2], Annotated[list[int], MinLen(3)])


def test_list_max_success():
    validate([1, 2, 3, 4], Annotated[list[int], MaxLen(5)])


def test_list_max_error():
    with pytest.raises(ValueError):
        validate([1, 2, 3, 4, 5, 6, 7], Annotated[list[int], MaxLen(6)])


# ----- set -----


def test_set_type_str_success():
    validate({"a", "b", "c"}, set[str])


def test_set_type_int_success():
    validate({1, 2, 3}, set[int])


def test_set_type_str_error():
    with pytest.raises(TypeError):
        validate({4, 5, 6}, set[str])


def test_set_type_int_error():
    with pytest.raises(TypeError):
        validate({"d", "e", "f"}, set[int])


def test_set_type_error():
    with pytest.raises(TypeError):
        validate("not_a_set", set[bool])


# ----- union -----


def test_union_none_match():
    with pytest.raises(TypeError):
        validate(123.45, Union[str, int])


def test_union_match():
    u = Union[str, int]
    validate("one", u)
    validate(1, u)


def test_optional():
    o = Optional[int]
    validate(1, o)
    validate(None, o)
    with pytest.raises(TypeError):
        validate("1", o)


def test_optional_generic():
    o = list[Optional[int]]
    validate([1, 2, 3, None, 4, 5], o)


def test_generic_range():
    Range = Annotated[list[Optional[T]], MinLen(2), MaxLen(2)]
    IntRange = Range[int]
    validate([1, 2], IntRange)
    validate([None, 2], IntRange)
    validate([1, None], IntRange)
    with pytest.raises(TypeError):
        validate(["1", 2], IntRange)
    with pytest.raises(ValueError):
        validate([1, 2, 3], IntRange)


# -- literal -----


def test_literal_match():
    l = Literal["a", "b", "c"]
    validate("a", l)
    validate("b", l)
    validate("c", l)


def test_literal_not_match():
    l = Literal["a", "b", "c"]
    with pytest.raises(ValueError):
        validate("d", l)


def test_literal_bool_int():
    l = Literal[2, 3, True]
    validate(2, l)
    validate(3, l)
    validate(True, l)
    with pytest.raises(ValueError):
        validate(1, l)


def test_literal_int_bool():
    l = Literal[1, 2, 3]
    with pytest.raises(ValueError):
        validate(True, l)


def test_literal_int_float():
    l = Literal[1, 2, 3]
    with pytest.raises(ValueError):
        validate(1.0, l)


# -- dataclass -----


def test_dataclass_success():
    DC = make_dataclass("DC", [("a", str)])
    validate(DC(a="b"), DC)


def test_dataclass_error():
    DC = make_dataclass("DC", [("c", int)])
    with pytest.raises(TypeError):
        validate(DC(c="str"), DC)


# -- typeddict -----


def test_typeddict_success():
    TD = TypedDict("TD", a=str)
    validate(dict(a="b"), TD)


def test_typeddict_error():
    TD = TypedDict("TD", c=int)
    with pytest.raises(TypeError):
        validate(dict(c="str"), TD)


def test_typeddict_required_success():
    TD = TypedDict("TD", e=float)
    validate(dict(e=1.2), TD)


def test_typeddict_required_error():
    TD = TypedDict("TD", f=str)
    with pytest.raises(ValueError):
        validate(dict(), TD)


def test_typeddict_optional_success():
    class TD(TypedDict, total=False):
        l: str

    validate(dict(), TD)


# -- decorators -----


def test_sync_decorator_success():
    @validate_return_value
    def fn() -> str:
        return "str_ftw"

    fn()


def test_sync_decorator_error():
    @validate_return_value
    def fn() -> str:
        return 1

    with pytest.raises(TypeError):
        fn()


def test_async_decorator_success():
    @validate_return_value
    async def coro() -> str:
        return "str_ftw"

    assert asyncio.run(coro()) == "str_ftw"


def test_async_decorator_error():
    @validate_return_value
    def coro() -> str:
        return 1

    with pytest.raises(TypeError):
        asyncio.run(coro())
