import datetime
import decimal
import json
import pytest

from base64 import b64encode
from collections.abc import Iterable
from dataclasses import dataclass, field, make_dataclass
from fondat.codec import BinaryCodec, DecodeError, EncodeError, JSONCodec, StringCodec
from fondat.data import make_datacls
from fondat.types import affix_type_hints
from types import NoneType
from typing import Any, Generic, Literal, Optional, TypedDict, TypeVar, Union
from uuid import UUID


def _test_encoding(python_type, value):
    for codec_type in (StringCodec, BinaryCodec, JSONCodec):
        codec = codec_type.get(python_type)
        encoded = codec.encode(value)
        if codec_type is StringCodec:
            assert isinstance(encoded, str)
        elif codec_type is BinaryCodec:
            assert isinstance(encoded, bytes | bytearray)
        elif codec_type is JSONCodec:
            assert isinstance(encoded, str | int | float | bool | NoneType | dict | list)
        assert codec.decode(encoded) == value


# ----- dict -----


def test_dict_json_encode_success():
    T = dict[str, int]
    value = dict(a=1, b=2, c=3)
    assert JSONCodec.get(T).encode(value) == value


def test_dict_json_encode_error():
    T = dict[str, int]
    value = dict(a="not int")
    with pytest.raises(EncodeError):
        JSONCodec.get(T).encode(value)


def test_dict_json_decode_success():
    T = dict[str, bool]
    value = dict(a=True, b=False)
    assert JSONCodec.get(T).decode(value) == value


def test_dict_json_decode_error():
    T = dict[str, str]
    value = dict(a=False)
    with pytest.raises(DecodeError):
        JSONCodec.get(T).decode(value)


def test_raw_dict():
    value = {"a": 1}
    codec = StringCodec.get(type(value))
    assert codec.decode(codec.encode(value)) == value


# ----- TypedDict -----


def test_typeddict_json_encode_success():
    TD = TypedDict("TD", dict(eja=str, ejb=int))
    value = dict(eja="foo", ejb=123)
    assert JSONCodec.get(TD).encode(value) == value


def test_typeddict_json_encode_optional_success():
    class TD(TypedDict, total=False):  # https://bugs.python.org/issue42059
        ejc: float
        ejd: bool

    value = dict(ejc=123.45)
    assert JSONCodec.get(TD).encode(value) == value


def test_typeddict_json_encode_optional_absent():
    class TD(TypedDict, total=False):  # https://bugs.python.org/issue42059
        eje: bool

    value = dict()
    assert JSONCodec.get(TD).encode(value) == value


def test_typeddict_json_encode_error():
    TD = TypedDict("TD", dict(ejh=int))
    with pytest.raises(EncodeError):
        JSONCodec.get(TD).encode(dict(ejh="not an int"))


def test_typeddict_json_decode_success():
    TD = TypedDict("TD", dict(dja=float, djb=bool))
    value = dict(dja=802.11, djb=True)
    assert JSONCodec.get(TD).decode(value) == value


def test_typeddict_json_decode_optional_success():
    class TD(TypedDict, total=False):  # https://bugs.python.org/issue42059
        djc: int
        djd: str

    value = dict(djc=12345)
    assert JSONCodec.get(TD).decode(value) == value


def test_typeddict_json_decode_error():
    TD = TypedDict("TD", dict(djx=str))
    value = dict(djx=False)
    with pytest.raises(DecodeError):
        JSONCodec.get(TD).decode(value)


# ----- tuple -----


def test_tuple_encodings():
    _test_encoding(tuple[int, str, float], (1, "foo", 2.3))


def test_tuple_json_encode_item_type_error():
    with pytest.raises(EncodeError):
        JSONCodec.get(tuple[str, str, str]).encode((1, 2, 3))


def test_tuple_json_decode_success():
    assert JSONCodec.get(tuple[float, float, float]).decode([1.2, 3.4, 5.6]) == (
        1.2,
        3.4,
        5.6,
    )


def test_tuple_json_decode_error():
    with pytest.raises(DecodeError):
        JSONCodec.get(tuple[str, str, str]).decode("not_a_tuple")


def test_tuple_str_encode_success():
    assert StringCodec.get(tuple[str, str, str]).encode(("a", "b", "c")) == "a,b,c"


def test_tuple_str_decode_success():
    assert StringCodec.get(tuple[str, str, str]).decode("a,b,c") == ("a", "b", "c")


def test_tuple_bytes_encode_success():
    assert BinaryCodec.get(tuple[str, str, str]).encode(("a", "b", "c")) == b'["a", "b", "c"]'


def test_tuple_bytes_decode_success():
    assert BinaryCodec.get(tuple[str, str, str]).decode(b'["a", "b", "c"]') == (
        "a",
        "b",
        "c",
    )


def test_tuple_str_decode_int_success():
    assert StringCodec.get(tuple[int, str, float]).decode("12,foo,3.4") == (12, "foo", 3.4)


def test_tuple_str_decode_float_success():
    assert StringCodec.get(tuple[float, float]).decode("12.34,56.78") == (
        12.34,
        56.78,
    )


def test_tuple_str_decode_crazy_csv_scenario():
    assert StringCodec.get(tuple[str, str, str, str]).decode('a,"b,c",d,"""e"""') == (
        "a",
        "b,c",
        "d",
        '"e"',
    )


def test_tuple_str_decode_int_error():
    with pytest.raises(DecodeError):
        StringCodec.get(tuple[int, int, int, int]).decode("12,a,34,56")


def test_tuple_ellipsis_encodings():
    _test_encoding(tuple[int, ...], (1, 2, 3))


def test_tuple_ellipsis_json_encode_item_type_error():
    with pytest.raises(EncodeError):
        JSONCodec.get(tuple[str, ...]).encode((1, 2, 3))


def test_tuple_ellipsis_json_decode_success():
    assert JSONCodec.get(tuple[float, ...]).decode([1.2, 3.4, 5.6]) == (1.2, 3.4, 5.6)


def test_tuple_ellipsis_json_decode_error():
    with pytest.raises(DecodeError):
        JSONCodec.get(tuple[str, ...]).decode("not_a_tuple")


def test_tuple_ellipsis_str_encode_success():
    assert StringCodec.get(tuple[str, ...]).encode(("a", "b", "c")) == "a,b,c"


def test_tuple_ellipsis_str_decode_success():
    assert StringCodec.get(tuple[str, ...]).decode("a,b,c") == ("a", "b", "c")


def test_tuple_ellipsis_bytes_encode_success():
    assert BinaryCodec.get(tuple[str, ...]).encode(("a", "b", "c")) == b'["a", "b", "c"]'


def test_tuple_ellipsis_bytes_decode_success():
    assert BinaryCodec.get(tuple[str, ...]).decode(b'["a", "b", "c"]') == (
        "a",
        "b",
        "c",
    )


def test_tuple_ellipsis_str_decode_int_success():
    assert StringCodec.get(tuple[int, ...]).decode("12,34,56") == (12, 34, 56)


def test_tuple_ellipsis_str_decode_float_success():
    assert StringCodec.get(tuple[float, ...]).decode("12.34,56.78") == (
        12.34,
        56.78,
    )


def test_tuple_ellipsis_str_decode_crazy_csv_scenario():
    assert StringCodec.get(tuple[str, ...]).decode('a,"b,c",d,"""e"""') == (
        "a",
        "b,c",
        "d",
        '"e"',
    )


def test_tuple_ellipsis_str_decode_int_error():
    with pytest.raises(DecodeError):
        StringCodec.get(tuple[int, ...]).decode("12,a,34,56")


# ----- list -----


def test_list_encodings():
    _test_encoding(list[int], [1, 2, 3])


def test_list_json_encode_item_type_error():
    with pytest.raises(EncodeError):
        JSONCodec.get(list[str]).encode([1, 2, 3])


def test_list_json_decode_success():
    value = [1.2, 3.4, 5.6]
    assert JSONCodec.get(list[float]).decode(value) == value


def test_list_json_decode_error():
    with pytest.raises(DecodeError):
        JSONCodec.get(list[str]).decode("not_a_list")


def test_list_str_encode_success():
    assert StringCodec.get(list[str]).encode(["a", "b", "c"]) == "a,b,c"


def test_list_str_decode_success():
    assert StringCodec.get(list[str]).decode("a,b,c") == ["a", "b", "c"]


def test_list_bytes_encode_success():
    assert BinaryCodec.get(list[str]).encode(["a", "b", "c"]) == b'["a", "b", "c"]'


def test_list_bytes_decode_success():
    assert BinaryCodec.get(list[str]).decode(b'["a", "b", "c"]') == [
        "a",
        "b",
        "c",
    ]


def test_list_str_decode_int_success():
    assert StringCodec.get(list[int]).decode("12,34,56") == [12, 34, 56]


def test_list_str_decode_float_success():
    assert StringCodec.get(list[float]).decode("12.34,56.78") == [
        12.34,
        56.78,
    ]


def test_list_str_decode_crazy_csv_scenario():
    assert StringCodec.get(list[str]).decode('a,"b,c",d,"""e"""') == [
        "a",
        "b,c",
        "d",
        '"e"',
    ]


def test_list_str_decode_int_error():
    with pytest.raises(DecodeError):
        StringCodec.get(list[int]).decode("12,a,34,56")


# ----- set -----


def test_set_encodings():
    _test_encoding(set[int], {1, 2, 3})


def test_set_json_encode_type_error():
    with pytest.raises(EncodeError):
        JSONCodec.get(set[str]).encode("i_am_not_a_set")


def test_set_json_encode_item_type_error():
    with pytest.raises(EncodeError):
        JSONCodec.get(set[str]).encode({1, 2, 3})


def test_set_json_decode_success():
    assert JSONCodec.get(set[float]).decode([1.2, 3.4, 5.6]) == {
        1.2,
        3.4,
        5.6,
    }


def test_set_json_decode_error():
    with pytest.raises(DecodeError):
        JSONCodec.get(set[str]).decode("not_a_set_either")


def test_set_str_decode_str_success():
    assert StringCodec.get(set[str]).decode("a,b,c") == {"a", "b", "c"}


def test_set_str_decode_str_encode():
    assert StringCodec.get(set[int]).encode({2, 3, 1}) == "1,2,3"  # sorts result


def test_set_str_decode_int_success():
    assert StringCodec.get(set[int]).decode("12,34,56") == {12, 34, 56}


def test_set_str_decode_float_success():
    assert StringCodec.get(set[float]).decode("12.34,56.78") == {
        12.34,
        56.78,
    }


def test_set_str_decode_crazy_csv_scenario():
    assert StringCodec.get(set[str]).decode('a,"b,c",d,"""e"""') == {
        "a",
        "b,c",
        "d",
        '"e"',
    }


def test_set_str_decode_int_error():
    with pytest.raises(DecodeError):
        StringCodec.get(set[int]).decode("12,a,34,56")


def test_set_bytes_encode_success():
    assert BinaryCodec.get(set[str]).encode({"a", "b", "c"}) == b'["a", "b", "c"]'


def test_set_bytes_decode_success():
    assert BinaryCodec.get(set[str]).decode(b'["a", "b", "c"]') == {
        "a",
        "b",
        "c",
    }


# ----- str -----


def test_str_encodings():
    _test_encoding(str, "foo")


def test_str_json_encode_success():
    value = "foo"
    assert JSONCodec.get(str).encode(value) == value


def test_str_json_encode_error():
    with pytest.raises(EncodeError):
        JSONCodec.get(str).encode(123)


def test_str_json_decode_success():
    value = "bar"
    assert JSONCodec.get(str).decode(value) == value


def test_str_json_decode_error():
    with pytest.raises(DecodeError):
        JSONCodec.get(str).decode(123)


def test_str_str_decode_success():
    value = "qux"
    assert StringCodec.get(str).decode(value) == value


# ----- int -----


def test_int_encodings():
    _test_encoding(int, 1)


def test_int_json_encode_success():
    value = 6
    assert JSONCodec.get(int).encode(value) == value


def test_int_json_encode_error():
    with pytest.raises(EncodeError):
        JSONCodec.get(int).encode(7.0)


def test_int_json_decode_success_int():
    value = 8
    assert JSONCodec.get(int).decode(value) == value


def test_int_json_decode_success_truncate_float():
    assert JSONCodec.get(int).decode(8.0) == 8


def test_int_json_decode_error_float():
    with pytest.raises(DecodeError):
        JSONCodec.get(int).decode(9.1)


def test_int_str_decode_success():
    assert StringCodec.get(int).decode("10") == 10


def test_int_str_decode_error():
    with pytest.raises(DecodeError):
        StringCodec.get(int).decode("11.2")


# ----- float -----


def test_float_encodings():
    _test_encoding(float, 1.2)


def test_float_json_encode_success():
    value = 6.1
    assert JSONCodec.get(float).encode(value) == value


def test_float_json_encode_error():
    with pytest.raises(EncodeError):
        JSONCodec.get(float).encode(7)


def test_float_json_decode_int():
    assert JSONCodec.get(float).decode(8) == 8.0


def test_float_json_decode_float():
    value = 9.1
    assert JSONCodec.get(float).decode(value) == value


def test_float_json_decode_error():
    with pytest.raises(DecodeError):
        JSONCodec.get(float).decode("10.2")


def test_float_str_decode_float():
    assert StringCodec.get(float).decode("11.3") == 11.3


def test_float_str_decode_int():
    assert StringCodec.get(float).decode("12") == 12.0


def test_float_str_decode_error():
    with pytest.raises(DecodeError):
        StringCodec.get(float).decode("1,2")


# ----- decimal -----


D = decimal.Decimal


def test_decimal_encodings():
    _test_encoding(D, D("1.234"))


def test_decimal_json_encode_success():
    assert JSONCodec.get(D).encode(D("6.1")) == "6.1"


def test_decimal_json_encode_error():
    with pytest.raises(EncodeError):
        JSONCodec.get(D).encode("7")


def test_decimal_json_decode_int():
    assert JSONCodec.get(D).decode("8") == D("8.0")


def test_decimal_json_decode_decimal():
    assert JSONCodec.get(D).decode("9.1") == D("9.1")


def test_decimal_json_decode_error():
    with pytest.raises(DecodeError):
        JSONCodec.get(D).decode("err")


def test_decimal_str_decode_decimal():
    assert StringCodec.get(D).decode("11.3") == D("11.3")


def test_decimal_str_decode_int():
    assert StringCodec.get(D).decode("12") == D("12.0")


def test_decimal_str_decode_error():
    with pytest.raises(DecodeError):
        StringCodec.get(D).decode("1,2")


# ----- bool -----


def test_bool_encodings():
    _test_encoding(bool, True)
    _test_encoding(bool, False)


def test_bool_json_encode_true():
    value = True
    assert JSONCodec.get(bool).encode(value) == value


def test_bool_json_encode_false():
    value = False
    assert JSONCodec.get(bool).encode(value) == value


def test_bool_json_encode_error():
    with pytest.raises(EncodeError):
        JSONCodec.get(bool).encode("bar")


def test_bool_json_decode_true():
    value = True
    assert JSONCodec.get(bool).decode(value) == value


def test_bool_json_decode_false():
    value = False
    assert JSONCodec.get(bool).decode(value) == value


def test_bool_json_decode_error():
    with pytest.raises(DecodeError):
        JSONCodec.get(bool).decode("baz")


def test_bool_str_encode_true():
    assert StringCodec.get(bool).encode(True) == "true"


def test_bool_str_encode_false():
    assert StringCodec.get(bool).encode(False) == "false"


def test_bool_str_decode_true():
    assert StringCodec.get(bool).decode("true") == True


def test_bool_str_decode_false():
    assert StringCodec.get(bool).decode("false") == False


def test_bool_str_decode_error():
    with pytest.raises(DecodeError):
        StringCodec.get(bool).decode("123")


# ----- date -----


def test_date_encodings():
    _test_encoding(datetime.date, datetime.date(2020, 12, 6))


def test_date_json_encode_success_naive():
    assert JSONCodec.get(datetime.date).encode(datetime.date(2016, 7, 8)) == "2016-07-08"


def test_date_json_encode_success_aware():
    assert JSONCodec.get(datetime.date).encode(datetime.date(2017, 6, 7)) == "2017-06-07"


def test_date_json_encode_error():
    with pytest.raises(EncodeError):
        JSONCodec.get(datetime.date).encode("definitely_not_a_date")


def test_date_json_decode_z():
    assert JSONCodec.get(datetime.date).decode("2018-08-09") == datetime.date(2018, 8, 9)


def test_date_json_decode_offset():
    assert JSONCodec.get(datetime.date).decode("2019-09-10") == datetime.date(2019, 9, 10)


def test_date_json_decode_missing_tz():
    assert JSONCodec.get(datetime.date).decode("2020-10-11") == datetime.date(2020, 10, 11)


def test_date_json_decode_error():
    with pytest.raises(DecodeError):
        JSONCodec.get(datetime.date).decode("14256910")


def test_date_str_decode_error():
    with pytest.raises(DecodeError):
        StringCodec.get(datetime.date).decode("14256910")


# ----- datetime -----


def test_datetime_encodings():
    _test_encoding(
        datetime.datetime,
        datetime.datetime(2020, 12, 6, 16, 10, 45, 0, datetime.timezone.utc),
    )


def test_datetime_json_encode_success_naive():
    assert (
        JSONCodec.get(datetime.datetime).encode(datetime.datetime(2016, 7, 8, 9, 10, 11))
        == "2016-07-08T09:10:11Z"
    )


def test_datetime_json_encode_success_aware():
    assert (
        JSONCodec.get(datetime.datetime).encode(
            datetime.datetime(2017, 6, 7, 8, 9, 10, 0, datetime.timezone.utc)
        )
        == "2017-06-07T08:09:10Z"
    )


def test_datetime_json_encode_error():
    with pytest.raises(EncodeError):
        JSONCodec.get(datetime.datetime).encode("definitely_not_a_datetime")


def test_datetime_json_decode_z():
    assert JSONCodec.get(datetime.datetime).decode("2018-08-09T10:11:12Z") == datetime.datetime(
        2018, 8, 9, 10, 11, 12, 0, datetime.timezone.utc
    )


def test_datetime_json_decode_offset():
    assert JSONCodec.get(datetime.datetime).decode(
        "2019-09-10T11:12:13+01:00"
    ) == datetime.datetime(2019, 9, 10, 10, 12, 13, 0, datetime.timezone.utc)


def test_datetime_json_decode_missing_tz():
    assert JSONCodec.get(datetime.datetime).decode("2020-10-11T12:13:14") == datetime.datetime(
        2020, 10, 11, 12, 13, 14, 0, datetime.timezone.utc
    )


def test_datetime_json_decode_error():
    with pytest.raises(DecodeError):
        JSONCodec.get(datetime.datetime).decode("1425691090159")


def test_datetime_str_decode_z():
    assert StringCodec.get(datetime.datetime).decode(
        "2021-11-12T13:14:15Z"
    ) == datetime.datetime(2021, 11, 12, 13, 14, 15, 0, datetime.timezone.utc)


def test_datetime_str_decode_offset():
    assert StringCodec.get(datetime.datetime).decode(
        "2022-12-13T14:15:16+01:00"
    ) == datetime.datetime(2022, 12, 13, 13, 15, 16, 0, datetime.timezone.utc)


def test_datetime_json_decode_missing_tz():
    assert StringCodec.get(datetime.datetime).decode(
        "2020-10-11T12:13:14"
    ) == datetime.datetime(2020, 10, 11, 12, 13, 14, 0, datetime.timezone.utc)


def test_datetime_str_decode_error():
    with pytest.raises(DecodeError):
        StringCodec.get(datetime.datetime).decode("1425691090160")


# ----- uuid -----


def test_uuid_encodings():
    _test_encoding(UUID, UUID("23954377-5c95-479f-b96f-94065922531d"))


def test_uuid_json_encode_success():
    value = "e9979b9c-c469-11e4-a0ad-37ff5ce3a7bf"
    assert JSONCodec.get(UUID).encode(UUID(value)) == value


def test_uuid_json_encode_error():
    with pytest.raises(EncodeError):
        JSONCodec.get(UUID).encode("definitely_not_a_uuid")


def test_uuid_json_decode_success():
    value = "15a64a3a-c46a-11e4-b790-cb538a10de85"
    assert JSONCodec.get(UUID).decode(value) == UUID(value)


def test_uuid_json_decode_error():
    with pytest.raises(DecodeError):
        JSONCodec.get(UUID).decode("this_is_not_a_uuid_either")


def test_uuid_str_decode_success():
    value = "3629cf84-c46a-11e4-9b09-43a2f172bb56"
    assert StringCodec.get(UUID).decode(value) == UUID(value)


def test_uuid_str_decode_error():
    with pytest.raises(DecodeError):
        StringCodec.get(UUID).decode("and_neither_is_this")


# ----- bytes -----


def test_bytes_encodings():
    _test_encoding(bytes, b"bytes_r_us")


def test_bytes_json_encode_success():
    value = bytes([4, 5, 6])
    assert JSONCodec.get(bytes).encode(value) == b64encode(value).decode()


def test_bytes_json_encode_error():
    with pytest.raises(EncodeError):
        JSONCodec.get(bytes).encode("definitely_not_a_bytes_object")


def test_bytes_json_decode_success():
    value = bytes([7, 8, 9])
    assert JSONCodec.get(bytes).decode(b64encode(value).decode()) == value


def test_bytes_json_decode_error():
    with pytest.raises(DecodeError):
        JSONCodec.get(bytes).decode("this_is_not_a_bytes_object_either")


def test_bytes_str_encode_success():
    value = bytes([0, 2, 4, 6, 8])
    assert StringCodec.get(bytes).encode(value) == b64encode(value).decode()


def test_bytes_str_decode_success():
    value = bytes([1, 3, 5, 7, 9])
    assert StringCodec.get(bytes).decode(b64encode(value).decode()) == value


def test_bytes_str_decode_error():
    with pytest.raises(DecodeError):
        StringCodec.get(bytes).decode(123)


# ----- union -----


def test_union_encodings():
    python_type = Union[int, UUID, bool]
    values = [123, UUID("06b959d0-65e0-11e7-866d-6be08781d5cb"), False]
    for value in values:
        _test_encoding(python_type, value)


def test_union_optional_value():
    assert JSONCodec.get(Optional[str]).decode("string") == "string"


def test_union_optional_none():
    assert JSONCodec.get(Optional[str]).decode(None) is None


# ----- union_type -----


def test_union_type_encodings():
    python_type = int | UUID | bool
    values = [123, UUID("06b959d0-65e0-11e7-866d-6be08781d5cb"), False]
    for value in values:
        _test_encoding(python_type, value)


def test_union_type_optional_value():
    assert JSONCodec.get(str | None).decode("string") == "string"


def test_union_type_optional_none():
    assert JSONCodec.get(str | None).decode(None) is None


# ----- literal -----


def test_literal_encodings():
    values = ("a", 1, True)
    for value in values:
        _test_encoding(Literal[values], value)


def test_literal_mixed_str_types():
    codec = StringCodec.get(Literal["a", 1])
    assert codec.decode("a") == "a"
    assert codec.decode("1") == 1


# ----- dataclass -----


def test_dataclass_json_encode_success():
    DC = make_dataclass("DC", [("eja", str), ("ejb", int)])
    assert JSONCodec.get(DC).encode(DC(eja="foo", ejb=123)) == {
        "eja": "foo",
        "ejb": 123,
    }


def test_dataclass_json_encode_error():
    DC = make_dataclass("DC", [("ejh", int)])
    with pytest.raises(EncodeError):
        JSONCodec.get(DC).encode(DC(ejh="not an int"))


def test_dataclass_json_decode_success():
    DC = make_dataclass("DC", [("dja", float), ("djb", bool)])
    assert JSONCodec.get(DC).decode({"dja": 802.11, "djb": True}) == DC(dja=802.11, djb=True)


def test_dataclass_json_decode_default_success():
    DC = make_dataclass("DC", [("djc", int), ("djd", str, field(default=None))])
    assert JSONCodec.get(DC).decode({"djc": 12345}) == DC(djc=12345, djd=None)


def test_dataclass_json_decode_optional_success():
    DC = make_dataclass("DC", (("x", int), ("y", str | None)))
    assert JSONCodec.get(DC).decode({"x": 1}) == DC(x=1, y=None)


def test_dataclass_json_decode_error():
    DC = make_dataclass("DC", [("djx", str)])
    with pytest.raises(DecodeError):
        JSONCodec.get(DC).decode({"djx": False})


def test_dataclass_json_encode_decode_keyword():
    DC = make_dataclass("DC", [("in_", str), ("inn", str)])
    codec = JSONCodec.get(DC)
    dc = DC(in_="a", inn="b")
    encoded = codec.encode(dc)
    assert encoded == {"in": "a", "inn": "b"}
    assert codec.decode(encoded) == dc


def test_dataclass_json_decode_invalid_type():
    DC = make_dataclass("DC", [("djx", str)])
    with pytest.raises(DecodeError):
        JSONCodec.get(DC).decode("not_a_dict")


def test_dataclass_json_decode_missing_field():
    DC = make_dataclass("DC", [("x", str)])
    with pytest.raises(DecodeError):
        JSONCodec.get(DC).decode({})


def test_datacls_json_decode_missing_field():
    DC = make_datacls("DC", [("x", str)])
    with pytest.raises(DecodeError):
        JSONCodec.get(DC).decode({})


# ----- any -----


def test_any_dataclass_json_codec_success():
    DC = make_dataclass("DC", [("i", int), ("s", str)])
    dc = DC(1, "a")
    encoded = JSONCodec.get(Any).encode(dc)
    decoded = JSONCodec.get(Any).decode(encoded)
    assert DC(**decoded) == dc


def test_any_dataclass_string_codec_success():
    DC = make_dataclass("DC", [("i", int), ("s", str)])
    dc = DC(1, "a")
    encoded = StringCodec.get(Any).encode(dc)
    decoded = JSONCodec.get(Any).decode(json.loads(encoded))
    assert DC(**decoded) == dc


def test_any_dataclass_binary_codec_success():
    DC = make_dataclass("DC", [("i", int), ("s", str)])
    dc = DC(1, "a")
    encoded = BinaryCodec.get(Any).encode(dc)
    decoded = JSONCodec.get(Any).decode(json.loads(encoded.decode()))
    assert DC(**decoded) == dc


# ----- Iterable -----


def test_iterable_json_decode():
    assert JSONCodec.get(Iterable[int]).decode([1, 2, 3]) == [1, 2, 3]


def test_iterable_string_decode():
    assert StringCodec.get(Iterable[int]).decode("1,2,3") == [1, 2, 3]


# ----- generics -----


def test_generic_dataclass_json():
    T = TypeVar("T")
    S = TypeVar("S")

    @dataclass
    class A(Generic[T]):
        a: list[T]

    @dataclass
    class B(Generic[S]):
        b: A[S]

    BB = B[bytes]
    bb = BB(b=A(a=[b"a", b"b"]))
    encoded = JSONCodec.get(BB).encode(bb)
    decoded = JSONCodec.get(BB).decode(encoded)
    assert decoded == bb


# ----- circular -----


def test_circular_dataclass():
    @dataclass
    class A:
        a: "A | None"

    affix_type_hints(A, localns=locals())
    JSONCodec.get(A)


def test_circular_typeddict():
    class A(TypedDict):
        a: "A | None"

    affix_type_hints(A, localns=locals())
    JSONCodec.get(A)
