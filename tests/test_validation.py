import pytest

from dataclasses import dataclass, make_dataclass
from datetime import date, datetime, timezone
from decimal import Decimal
from fondat.validation import (
    MaxLen,
    MaxValue,
    MinLen,
    MinValue,
    Pattern,
    ValidationError,
    ValidationErrors,
    validate_arguments,
    validate_return_value,
    validate_value,
)
from typing import Annotated, Generic, Literal, Optional, T, TypedDict, TypeVar, Union
from uuid import UUID


# ----- str -----


def test_str_type_success():
    validate_value("foo", str)


def test_str_type_error():
    with pytest.raises(ValidationError):
        validate_value(123, str)


def test_str_min_length_success():
    validate_value("12345", Annotated[str, MinLen(3)])


def test_str_min_length_error():
    with pytest.raises(ValidationError):
        validate_value("123", Annotated[str, MinLen(4)])


def test_str_max_length_success():
    validate_value("12345", Annotated[str, MaxLen(5)])


def test_str_max_length_error():
    with pytest.raises(ValidationError):
        validate_value("1234567", Annotated[str, MaxLen(6)])


def test_str_pattern_success():
    validate_value("abc", Annotated[str, Pattern(r"^abc$")])


def test_str_pattern_error():
    with pytest.raises(ValidationError):
        validate_value("ghi", Annotated[str, Pattern(r"^def$")])


# ----- int -----


def test_int_type_success():
    validate_value(123, int)


def test_int_type_error():
    with pytest.raises(ValidationError):
        validate_value(123.45, int)


def test_int_min_success():
    validate_value(2, Annotated[int, MinValue(1)])


def test_int_minerror():
    with pytest.raises(ValidationError):
        validate_value(1, Annotated[int, MinValue(2)])


def test_int_max_success():
    validate_value(2, Annotated[int, MaxValue(3)])


def test_int_max_error():
    with pytest.raises(ValidationError):
        validate_value(5, Annotated[int, MaxValue(4)])


def test_int_reject_bool():
    with pytest.raises(ValidationError):
        validate_value(True, int)


# ----- float -----


def test_float_type_success():
    validate_value(123.45, float)


def test_float_type_error():
    with pytest.raises(ValidationError):
        validate_value("123.45", float)


def test_float_min_success():
    validate_value(1.1, Annotated[float, MinValue(1.0)])


def test_float_min_error():
    with pytest.raises(ValidationError):
        validate_value(1.9, Annotated[float, MinValue(2.0)])


def test_float_max_success():
    validate_value(2.9, Annotated[float, MaxValue(3.0)])


def test_float_max_error():
    with pytest.raises(ValidationError):
        validate_value(4.1, Annotated[float, MaxValue(4.0)])


# ----- decimal -----


def test_decimal_type_success():
    validate_value(Decimal("123.45"), Decimal)


def test_decimal_type_error():
    with pytest.raises(ValidationError):
        validate_value("123.45", Decimal)


def test_decimal_min_success():
    validate_value(Decimal("1.1"), Annotated[Decimal, MinValue(Decimal("1.0"))])


def test_decimal_min_error():
    with pytest.raises(ValidationError):
        validate_value(Decimal("1.9"), Annotated[Decimal, MinValue(Decimal("2.0"))])


def test_decimal_max_success():
    validate_value(Decimal("2.9"), Annotated[Decimal, MaxValue(Decimal("3.0"))])


def test_decimal_max_error():
    with pytest.raises(ValidationError):
        validate_value(Decimal("4.1"), Annotated[Decimal, MaxValue(Decimal("4.0"))])


# ----- bool -----


def test_bool_type_true():
    validate_value(True, bool)


def test_bool_type_false():
    validate_value(False, bool)


def test_bool_type_error():
    with pytest.raises(ValidationError):
        validate_value("foo", bool)


# ----- date -----


def test_date_type_success():
    validate_value(date(2015, 6, 7), date)


def test_date_type_error():
    with pytest.raises(ValidationError):
        validate_value("not_a_date", date)


# ----- datetime -----


def test_datetime_type_success():
    validate_value(datetime(2015, 6, 7, 8, 9, 10, 0, timezone.utc), datetime)


def test_datetime_type_error():
    with pytest.raises(ValidationError):
        validate_value("not_a_datetime", datetime)


# ----- uuid -----


def test_uuid_type_success():
    validate_value(UUID("af327a12-c469-11e4-8e4f-af4f7c44473b"), UUID)


def test_uuid_type_error():
    with pytest.raises(ValidationError):
        validate_value("not_a_uuid", UUID)


# ----- bytes -----


def test_bytes_type_success():
    validate_value(bytes([1, 2, 3]), bytes)


def test_bytes_type_error():
    with pytest.raises(ValidationError):
        validate_value("not_a_byte_string", bytes)


# ----- dict -----


def test_dict_success():
    validate_value(dict(a=1, b=2), dict[str, int])


def test_dict_type_error():
    with pytest.raises(ValidationError):
        validate_value("should_not_validate", dict[str, int])


def test_dict_error_key_type():
    with pytest.raises(ValidationError):
        validate_value({1: 2}, dict[str, int])


def test_dict_error_value_type():
    with pytest.raises(ValidationError):
        validate_value(dict(this="should_not_validate"), dict[str, int])


# ----- tuple -----


def test_tuple_success():
    validate_value(("a", 1, 2.3), tuple[str, int, float])


def test_tuple_value_error():
    with pytest.raises(ValidationError):
        validate_value(("a", "b", "c"), tuple[str, int, float])


def test_tuple_ellipsis_success():
    validate_value(("a", "b", "c"), tuple[str, ...])


def test_tuple_ellipsis_value_error():
    with pytest.raises(ValidationError):
        validate_value(("a", "b", 1), tuple[str, ...])


# ----- list -----


def test_list_type_str_success():
    validate_value(["a", "b", "c"], list[str])


def test_list_type_int_success():
    validate_value([1, 2, 3], list[int])


def test_list_type_str_error():
    with pytest.raises(ValidationError):
        validate_value([4, 5, 6], list[str])


def test_list_type_int_error():
    with pytest.raises(ValidationError):
        validate_value(["d", "e", "f"], list[int])


def test_list_type_error():
    with pytest.raises(ValidationError):
        validate_value("this_is_not_a_list", list[bool])


def test_list_min_success():
    validate_value([1, 2, 3], Annotated[list[int], MinLen(2)])


def test_list_min_error():
    with pytest.raises(ValidationError):
        validate_value([1, 2], Annotated[list[int], MinLen(3)])


def test_list_max_success():
    validate_value([1, 2, 3, 4], Annotated[list[int], MaxLen(5)])


def test_list_max_error():
    with pytest.raises(ValidationError):
        validate_value([1, 2, 3, 4, 5, 6, 7], Annotated[list[int], MaxLen(6)])


# ----- set -----


def test_set_type_str_success():
    validate_value({"a", "b", "c"}, set[str])


def test_set_type_int_success():
    validate_value({1, 2, 3}, set[int])


def test_set_type_str_error():
    with pytest.raises(ValidationError):
        validate_value({4, 5, 6}, set[str])


def test_set_type_int_error():
    with pytest.raises(ValidationError):
        validate_value({"d", "e", "f"}, set[int])


def test_set_type_error():
    with pytest.raises(ValidationError):
        validate_value("not_a_set", set[bool])


# ----- union -----


def test_union_none_match():
    with pytest.raises(ValidationError):
        validate_value(123.45, Union[str, int])


def test_union_match():
    u = Union[str, int]
    validate_value("one", u)
    validate_value(1, u)


def test_optional():
    o = Optional[int]
    validate_value(1, o)
    validate_value(None, o)
    with pytest.raises(ValidationError):
        validate_value("1", o)


def test_optional_generic():
    o = list[Optional[int]]
    validate_value([1, 2, 3, None, 4, 5], o)


def test_generic_range():
    Range = Annotated[list[Optional[T]], MinLen(2), MaxLen(2)]
    IntRange = Range[int]
    validate_value([1, 2], IntRange)
    validate_value([None, 2], IntRange)
    validate_value([1, None], IntRange)
    with pytest.raises(ValidationError):
        validate_value(["1", 2], IntRange)
    with pytest.raises(ValidationError):
        validate_value([1, 2, 3], IntRange)


# -- literal -----


def test_literal_match():
    l = Literal["a", "b", "c"]
    validate_value("a", l)
    validate_value("b", l)
    validate_value("c", l)


def test_literal_not_match():
    l = Literal["a", "b", "c"]
    with pytest.raises(ValidationError):
        validate_value("d", l)


def test_literal_bool_int():
    l = Literal[2, 3, True]
    validate_value(2, l)
    validate_value(3, l)
    validate_value(True, l)
    with pytest.raises(ValidationError):
        validate_value(1, l)


def test_literal_int_bool():
    l = Literal[1, 2, 3]
    with pytest.raises(ValidationError):
        validate_value(True, l)


def test_literal_int_float():
    l = Literal[1, 2, 3]
    with pytest.raises(ValidationError):
        validate_value(1.0, l)


# -- dataclass -----


def test_dataclass_success():
    DC = make_dataclass("DC", [("a", str)])
    validate_value(DC(a="b"), DC)


def test_dataclass_error():
    DC = make_dataclass("DC", [("c", int)])
    with pytest.raises(ValidationError):
        validate_value(DC(c="str"), DC)


# -- typeddict -----


def test_typeddict_success():
    TD = TypedDict("TD", {"a": str})
    validate_value(dict(a="b"), TD)


def test_typeddict_error():
    TD = TypedDict("TD", {"c": int})
    with pytest.raises(ValidationError):
        validate_value(dict(c="str"), TD)


def test_typeddict_total_success():
    TD = TypedDict("TD", {"e": float})
    validate_value(dict(e=1.2), TD)


def test_typeddict_total_error():
    TD = TypedDict("TD", {"f": str})
    with pytest.raises(ValidationError):
        validate_value(dict(), TD)


def test_typeddict_total_false():
    class TD(TypedDict, total=False):
        l: str

    validate_value(dict(), TD)


def test_typeddict_optional():
    TD = TypedDict("TD", {"a": Optional[str]})
    validate_value(dict(a=None), TD)


# -- decorators -----


def test_sync_decorator_arguments_success():
    @validate_arguments
    def fn(s: Annotated[str, MinLen(2), MaxLen(2)]):
        pass

    fn("12")


def test_sync_decorator_arguments_error():
    @validate_arguments
    def fn(s: Annotated[str, MinLen(2), MaxLen(2)]):
        pass

    with pytest.raises(ValidationError):
        fn("1")


async def test_async_decorator_arguments_success():
    @validate_arguments
    async def fn(s: Annotated[str, MinLen(2), MaxLen(2)]):
        pass

    await fn("12")


async def test_async_decorator_arguments_error():
    @validate_arguments
    async def fn(s: Annotated[str, MinLen(2), MaxLen(2)]):
        pass

    with pytest.raises(ValidationError):
        await fn("1")


def test_sync_decorator_return_success():
    @validate_return_value
    def fn() -> str:
        return "str_ftw"

    fn()


def test_sync_decorator_return_error():
    @validate_return_value
    def fn() -> str:
        return 1

    with pytest.raises(ValidationError):
        fn()


async def test_async_decorator_return_success():
    @validate_return_value
    async def coro() -> str:
        return "str_ftw"

    assert await coro() == "str_ftw"


async def test_async_decorator_return_error():
    @validate_return_value
    def coro() -> str:
        return 1

    with pytest.raises(ValidationError):
        await coro()


def test_generic_dataclass():
    T = TypeVar("T")
    S = TypeVar("S")

    @dataclass
    class A(Generic[T]):
        a: list[T]

    @dataclass
    class B(Generic[S]):
        b: A[S]

    BB = B[bytes]

    validate_value(BB(b=A(a=[b"a", b"b"])), BB)

    with pytest.raises(ValidationError):
        validate_value(BB(b=1), BB)

    with pytest.raises(ValidationError):
        validate_value(BB(b=A(a=1)), BB)


# ---- collections -----


def test_validation_errors():
    errors = ValidationErrors()
    errors.add(ValidationError("foo"))
    try:
        raise errors
    except ValidationErrors as ve:
        assert ve is errors
        assert len(ve.errors) == 1


def test_validation_errors_context_errors():
    with pytest.raises(ValidationErrors):
        with ValidationErrors.collect() as errors:
            errors.add(ValidationError("foo"))
            errors.add(ValidationError("bar"))


def test_validation_errors_context_no_errors():
    with ValidationErrors.collect():
        pass
    # exiting context should not raise exception


def test_validation_errors_catch():
    errors = ValidationErrors()
    with errors.catch():
        raise ValidationError("foo")
    assert len(errors) == 1
    with errors.catch():
        raise ValidationError("bar")
    assert len(errors) == 2


def test_validation_errors_iter():
    errors = ValidationErrors(ValidationError("foo"), ValidationError("bar)"))
    count = 0
    for error in errors:
        assert isinstance(error, ValidationError)
        count += 1
    assert count == 2


def test_validation_error_path():
    DC1 = make_dataclass("DC1", [("s", str), ("i", int)])
    DC2 = make_dataclass("DC2", [("dc1", DC1)])
    dc2 = DC2(dc1=DC1(s=1, i=1))
    try:
        validate_value(dc2, DC2)
    except ValidationError as ve:
        assert ve.path == ["dc1", "s"]
