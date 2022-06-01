import datetime
import decimal
import json
import pytest

from base64 import b64encode
from collections.abc import Iterable
from dataclasses import field, make_dataclass
from fondat.codec import JSON, Binary, DecodeError, EncodeError, String, get_codec
from fondat.data import make_datacls
from typing import Annotated, Any, Literal, Optional, TypedDict, Union
from uuid import UUID


def _equal(fn, val):
    assert val == fn(val)


def _test_encoding(python_type, value):
    for codec_type in (String, Binary, JSON):
        codec = get_codec(codec_type, python_type)
        assert codec.decode(codec.encode(value)) == value


# ----- dict -----


def test_dict_json_encode_success():
    T = dict[str, int]
    value = dict(a=1, b=2, c=3)
    assert get_codec(JSON, T).encode(value) == value


def test_dict_json_encode_error():
    T = dict[str, int]
    value = dict(a="not int")
    with pytest.raises(EncodeError):
        get_codec(JSON, T).encode(value)


def test_dict_json_decode_success():
    T = dict[str, bool]
    value = dict(a=True, b=False)
    assert get_codec(JSON, T).decode(value) == value


def test_dict_json_decode_error():
    T = dict[str, str]
    value = dict(a=False)
    with pytest.raises(DecodeError):
        get_codec(JSON, T).decode(value)


def test_raw_dict():
    value = {"a": 1}
    codec = get_codec(String, type(value))
    assert codec.decode(codec.encode(value)) == value


# ----- TypedDict -----


def test_typeddict_json_encode_success():
    TD = TypedDict("TD", dict(eja=str, ejb=int))
    value = dict(eja="foo", ejb=123)
    assert get_codec(JSON, TD).encode(value) == value


def test_typeddict_json_encode_optional_success():
    class TD(TypedDict, total=False):  # https://bugs.python.org/issue42059
        ejc: float
        ejd: bool

    value = dict(ejc=123.45)
    assert get_codec(JSON, TD).encode(value) == value


def test_typeddict_json_encode_optional_absent():
    class TD(TypedDict, total=False):  # https://bugs.python.org/issue42059
        eje: bool

    value = dict()
    assert get_codec(JSON, TD).encode(value) == value


def test_typeddict_json_encode_error():
    TD = TypedDict("TD", dict(ejh=int))
    with pytest.raises(EncodeError):
        get_codec(JSON, TD).encode(dict(ejh="not an int"))


def test_typeddict_json_decode_success():
    TD = TypedDict("TD", dict(dja=float, djb=bool))
    value = dict(dja=802.11, djb=True)
    assert get_codec(JSON, TD).decode(value) == value


def test_typeddict_json_decode_optional_success():
    class TD(TypedDict, total=False):  # https://bugs.python.org/issue42059
        djc: int
        djd: str

    value = dict(djc=12345)
    assert get_codec(JSON, TD).decode(value) == value


def test_typeddict_json_decode_error():
    TD = TypedDict("TD", dict(djx=str))
    value = dict(djx=False)
    with pytest.raises(DecodeError):
        get_codec(JSON, TD).decode(value)


# ----- tuple -----


def test_tuple_encodings():
    _test_encoding(tuple[int, str, float], (1, "foo", 2.3))


def test_tuple_json_encode_item_type_error():
    with pytest.raises(EncodeError):
        get_codec(JSON, tuple[str, str, str]).encode((1, 2, 3))


def test_tuple_json_decode_success():
    assert get_codec(JSON, tuple[float, float, float]).decode([1.2, 3.4, 5.6]) == (
        1.2,
        3.4,
        5.6,
    )


def test_tuple_json_decode_error():
    with pytest.raises(DecodeError):
        get_codec(JSON, tuple[str, str, str]).decode("not_a_tuple")


def test_tuple_str_encode_success():
    assert get_codec(String, tuple[str, str, str]).encode(("a", "b", "c")) == "a,b,c"


def test_tuple_str_decode_success():
    assert get_codec(String, tuple[str, str, str]).decode("a,b,c") == ("a", "b", "c")


def test_tuple_bytes_encode_success():
    assert get_codec(Binary, tuple[str, str, str]).encode(("a", "b", "c")) == b'["a", "b", "c"]'


def test_tuple_bytes_decode_success():
    assert get_codec(Binary, tuple[str, str, str]).decode(b'["a", "b", "c"]') == (
        "a",
        "b",
        "c",
    )


def test_tuple_str_decode_int_success():
    assert get_codec(String, tuple[int, str, float]).decode("12,foo,3.4") == (12, "foo", 3.4)


def test_tuple_str_decode_float_success():
    assert get_codec(String, tuple[float, float]).decode("12.34,56.78") == (
        12.34,
        56.78,
    )


def test_tuple_str_decode_crazy_csv_scenario():
    assert get_codec(String, tuple[str, str, str, str]).decode('a,"b,c",d,"""e"""') == (
        "a",
        "b,c",
        "d",
        '"e"',
    )


def test_tuple_str_decode_int_error():
    with pytest.raises(DecodeError):
        get_codec(String, tuple[int, int, int, int]).decode("12,a,34,56")


def test_tuple_ellipsis_encodings():
    _test_encoding(tuple[int, ...], (1, 2, 3))


def test_tuple_ellipsis_json_encode_item_type_error():
    with pytest.raises(EncodeError):
        get_codec(JSON, tuple[str, ...]).encode((1, 2, 3))


def test_tuple_ellipsis_json_decode_success():
    assert get_codec(JSON, tuple[float, ...]).decode([1.2, 3.4, 5.6]) == (1.2, 3.4, 5.6)


def test_tuple_ellipsis_json_decode_error():
    with pytest.raises(DecodeError):
        get_codec(JSON, tuple[str, ...]).decode("not_a_tuple")


def test_tuple_ellipsis_str_encode_success():
    assert get_codec(String, tuple[str, ...]).encode(("a", "b", "c")) == "a,b,c"


def test_tuple_ellipsis_str_decode_success():
    assert get_codec(String, tuple[str, ...]).decode("a,b,c") == ("a", "b", "c")


def test_tuple_ellipsis_bytes_encode_success():
    assert get_codec(Binary, tuple[str, ...]).encode(("a", "b", "c")) == b'["a", "b", "c"]'


def test_tuple_ellipsis_bytes_decode_success():
    assert get_codec(Binary, tuple[str, ...]).decode(b'["a", "b", "c"]') == (
        "a",
        "b",
        "c",
    )


def test_tuple_ellipsis_str_decode_int_success():
    assert get_codec(String, tuple[int, ...]).decode("12,34,56") == (12, 34, 56)


def test_tuple_ellipsis_str_decode_float_success():
    assert get_codec(String, tuple[float, ...]).decode("12.34,56.78") == (
        12.34,
        56.78,
    )


def test_tuple_ellipsis_str_decode_crazy_csv_scenario():
    assert get_codec(String, tuple[str, ...]).decode('a,"b,c",d,"""e"""') == (
        "a",
        "b,c",
        "d",
        '"e"',
    )


def test_tuple_ellipsis_str_decode_int_error():
    with pytest.raises(DecodeError):
        get_codec(String, tuple[int, ...]).decode("12,a,34,56")


# ----- list -----


def test_list_encodings():
    _test_encoding(list[int], [1, 2, 3])


def test_list_json_encode_item_type_error():
    with pytest.raises(EncodeError):
        get_codec(JSON, list[str]).encode([1, 2, 3])


def test_list_json_decode_success():
    _equal(get_codec(JSON, list[float]).decode, [1.2, 3.4, 5.6])


def test_list_json_decode_error():
    with pytest.raises(DecodeError):
        get_codec(JSON, list[str]).decode("not_a_list")


def test_list_str_encode_success():
    assert get_codec(String, list[str]).encode(["a", "b", "c"]) == "a,b,c"


def test_list_str_decode_success():
    assert get_codec(String, list[str]).decode("a,b,c") == ["a", "b", "c"]


def test_list_bytes_encode_success():
    assert get_codec(Binary, list[str]).encode(["a", "b", "c"]) == b'["a", "b", "c"]'


def test_list_bytes_decode_success():
    assert get_codec(Binary, list[str]).decode(b'["a", "b", "c"]') == [
        "a",
        "b",
        "c",
    ]


def test_list_str_decode_int_success():
    assert get_codec(String, list[int]).decode("12,34,56") == [12, 34, 56]


def test_list_str_decode_float_success():
    assert get_codec(String, list[float]).decode("12.34,56.78") == [
        12.34,
        56.78,
    ]


def test_list_str_decode_crazy_csv_scenario():
    assert get_codec(String, list[str]).decode('a,"b,c",d,"""e"""') == [
        "a",
        "b,c",
        "d",
        '"e"',
    ]


def test_list_str_decode_int_error():
    with pytest.raises(DecodeError):
        get_codec(String, list[int]).decode("12,a,34,56")


# ----- set -----


def test_set_encodings():
    _test_encoding(set[int], {1, 2, 3})


def test_set_json_encode_type_error():
    with pytest.raises(EncodeError):
        get_codec(JSON, set[str]).encode("i_am_not_a_set")


def test_set_json_encode_item_type_error():
    with pytest.raises(EncodeError):
        get_codec(JSON, set[str]).encode({1, 2, 3})


def test_set_json_decode_success():
    assert get_codec(JSON, set[float]).decode([1.2, 3.4, 5.6]) == {
        1.2,
        3.4,
        5.6,
    }


def test_set_json_decode_error():
    with pytest.raises(DecodeError):
        get_codec(JSON, set[str]).decode("not_a_set_either")


def test_set_str_decode_str_success():
    assert get_codec(String, set[str]).decode("a,b,c") == {"a", "b", "c"}


def test_set_str_decode_str_encode():
    assert get_codec(String, set[int]).encode({2, 3, 1}) == "1,2,3"  # sorts result


def test_set_str_decode_int_success():
    assert get_codec(String, set[int]).decode("12,34,56") == {12, 34, 56}


def test_set_str_decode_float_success():
    assert get_codec(String, set[float]).decode("12.34,56.78") == {
        12.34,
        56.78,
    }


def test_set_str_decode_crazy_csv_scenario():
    assert get_codec(String, set[str]).decode('a,"b,c",d,"""e"""') == {
        "a",
        "b,c",
        "d",
        '"e"',
    }


def test_set_str_decode_int_error():
    with pytest.raises(DecodeError):
        get_codec(String, set[int]).decode("12,a,34,56")


def test_set_bytes_encode_success():
    assert get_codec(Binary, set[str]).encode({"a", "b", "c"}) == b'["a", "b", "c"]'


def test_set_bytes_decode_success():
    assert get_codec(Binary, set[str]).decode(b'["a", "b", "c"]') == {
        "a",
        "b",
        "c",
    }


# ----- str -----


def test_str_encodings():
    _test_encoding(str, "foo")


def test_str_json_encode_success():
    _equal(get_codec(JSON, str).encode, "foo")


def test_str_json_encode_error():
    with pytest.raises(EncodeError):
        get_codec(JSON, str).encode(123)


def test_str_json_decode_success():
    _equal(get_codec(JSON, str).decode, "bar")


def test_str_json_decode_error():
    with pytest.raises(DecodeError):
        get_codec(JSON, str).decode(123)


def test_str_str_decode_success():
    _equal(get_codec(String, str).decode, "qux")


# ----- int -----


def test_int_encodings():
    _test_encoding(int, 1)


def test_int_json_encode_success():
    _equal(get_codec(JSON, int).encode, 6)


def test_int_json_encode_error():
    with pytest.raises(EncodeError):
        get_codec(JSON, int).encode(7.0)


def test_int_json_decode_success_int():
    _equal(get_codec(JSON, int).decode, 8)


def test_int_json_decode_success_round_float():
    _equal(get_codec(JSON, int).decode, 8.0)


def test_int_json_decode_error_float():
    with pytest.raises(DecodeError):
        get_codec(JSON, int).decode(9.1)


def test_int_str_decode_success():
    assert get_codec(String, int).decode("10") == 10


def test_int_str_decode_error():
    with pytest.raises(DecodeError):
        get_codec(String, int).decode("11.2")


# ----- float -----


def test_float_encodings():
    _test_encoding(float, 1.2)


def test_float_json_encode_success():
    _equal(get_codec(JSON, float).encode, 6.1)


def test_float_json_encode_error():
    with pytest.raises(EncodeError):
        get_codec(JSON, float).encode(7)


def test_float_json_decode_int():
    assert get_codec(JSON, float).decode(8) == 8.0


def test_float_json_decode_float():
    _equal(get_codec(JSON, float).decode, 9.1)


def test_float_json_decode_error():
    with pytest.raises(DecodeError):
        get_codec(JSON, float).decode("10.2")


def test_float_str_decode_float():
    assert get_codec(String, float).decode("11.3") == 11.3


def test_float_str_decode_int():
    assert get_codec(String, float).decode("12") == 12.0


def test_float_str_decode_error():
    with pytest.raises(DecodeError):
        get_codec(String, float).decode("1,2")


# ----- decimal -----


D = decimal.Decimal


def test_decimal_encodings():
    _test_encoding(D, D("1.234"))


def test_decimal_json_encode_success():
    assert get_codec(JSON, D).encode(D("6.1")) == "6.1"


def test_decimal_json_encode_error():
    with pytest.raises(EncodeError):
        get_codec(JSON, D).encode("7")


def test_decimal_json_decode_int():
    assert get_codec(JSON, D).decode("8") == D("8.0")


def test_decimal_json_decode_decimal():
    assert get_codec(JSON, D).decode("9.1") == D("9.1")


def test_decimal_json_decode_error():
    with pytest.raises(DecodeError):
        get_codec(JSON, D).decode("err")


def test_decimal_str_decode_decimal():
    assert get_codec(String, D).decode("11.3") == D("11.3")


def test_decimal_str_decode_int():
    assert get_codec(String, D).decode("12") == D("12.0")


def test_decimal_str_decode_error():
    with pytest.raises(DecodeError):
        get_codec(String, D).decode("1,2")


# ----- bool -----


def test_bool_encodings():
    _test_encoding(bool, True)
    _test_encoding(bool, False)


def test_bool_json_encode_true():
    _equal(get_codec(JSON, bool).encode, True)


def test_bool_json_encode_false():
    _equal(get_codec(JSON, bool).encode, False)


def test_bool_json_encode_error():
    with pytest.raises(EncodeError):
        get_codec(JSON, bool).encode("bar")


def test_bool_json_decode_true():
    _equal(get_codec(JSON, bool).decode, True)


def test_bool_json_decode_false():
    _equal(get_codec(JSON, bool).decode, False)


def test_bool_json_decode_error():
    with pytest.raises(DecodeError):
        get_codec(JSON, bool).decode("baz")


def test_bool_str_encode_true():
    assert get_codec(String, bool).encode(True) == "true"


def test_bool_str_encode_false():
    assert get_codec(String, bool).encode(False) == "false"


def test_bool_str_decode_true():
    assert get_codec(String, bool).decode("true") == True


def test_bool_str_decode_false():
    assert get_codec(String, bool).decode("false") == False


def test_bool_str_decode_error():
    with pytest.raises(DecodeError):
        get_codec(String, bool).decode("123")


# ----- date -----


def test_date_encodings():
    _test_encoding(datetime.date, datetime.date(2020, 12, 6))


def test_date_json_encode_success_naive():
    assert get_codec(JSON, datetime.date).encode(datetime.date(2016, 7, 8)) == "2016-07-08"


def test_date_json_encode_success_aware():
    assert get_codec(JSON, datetime.date).encode(datetime.date(2017, 6, 7)) == "2017-06-07"


def test_date_json_encode_error():
    with pytest.raises(EncodeError):
        get_codec(JSON, datetime.date).encode("definitely_not_a_date")


def test_date_json_decode_z():
    assert get_codec(JSON, datetime.date).decode("2018-08-09") == datetime.date(2018, 8, 9)


def test_date_json_decode_offset():
    assert get_codec(JSON, datetime.date).decode("2019-09-10") == datetime.date(2019, 9, 10)


def test_date_json_decode_missing_tz():
    assert get_codec(JSON, datetime.date).decode("2020-10-11") == datetime.date(2020, 10, 11)


def test_date_json_decode_error():
    with pytest.raises(DecodeError):
        get_codec(JSON, datetime.date).decode("14256910")


def test_date_str_decode_error():
    with pytest.raises(DecodeError):
        get_codec(String, datetime.date).decode("14256910")


# ----- datetime -----


def test_datetime_encodings():
    _test_encoding(
        datetime.datetime,
        datetime.datetime(2020, 12, 6, 16, 10, 45, 0, datetime.timezone.utc),
    )


def test_datetime_json_encode_success_naive():
    assert (
        get_codec(JSON, datetime.datetime).encode(datetime.datetime(2016, 7, 8, 9, 10, 11))
        == "2016-07-08T09:10:11Z"
    )


def test_datetime_json_encode_success_aware():
    assert (
        get_codec(JSON, datetime.datetime).encode(
            datetime.datetime(2017, 6, 7, 8, 9, 10, 0, datetime.timezone.utc)
        )
        == "2017-06-07T08:09:10Z"
    )


def test_datetime_json_encode_error():
    with pytest.raises(EncodeError):
        get_codec(JSON, datetime.datetime).encode("definitely_not_a_datetime")


def test_datetime_json_decode_z():
    assert get_codec(JSON, datetime.datetime).decode(
        "2018-08-09T10:11:12Z"
    ) == datetime.datetime(2018, 8, 9, 10, 11, 12, 0, datetime.timezone.utc)


def test_datetime_json_decode_offset():
    assert get_codec(JSON, datetime.datetime).decode(
        "2019-09-10T11:12:13+01:00"
    ) == datetime.datetime(2019, 9, 10, 10, 12, 13, 0, datetime.timezone.utc)


def test_datetime_json_decode_missing_tz():
    assert get_codec(JSON, datetime.datetime).decode(
        "2020-10-11T12:13:14"
    ) == datetime.datetime(2020, 10, 11, 12, 13, 14, 0, datetime.timezone.utc)


def test_datetime_json_decode_error():
    with pytest.raises(DecodeError):
        get_codec(JSON, datetime.datetime).decode("1425691090159")


def test_datetime_str_decode_z():
    assert get_codec(String, datetime.datetime).decode(
        "2021-11-12T13:14:15Z"
    ) == datetime.datetime(2021, 11, 12, 13, 14, 15, 0, datetime.timezone.utc)


def test_datetime_str_decode_offset():
    assert get_codec(String, datetime.datetime).decode(
        "2022-12-13T14:15:16+01:00"
    ) == datetime.datetime(2022, 12, 13, 13, 15, 16, 0, datetime.timezone.utc)


def test_datetime_json_decode_missing_tz():
    assert get_codec(String, datetime.datetime).decode(
        "2020-10-11T12:13:14"
    ) == datetime.datetime(2020, 10, 11, 12, 13, 14, 0, datetime.timezone.utc)


def test_datetime_str_decode_error():
    with pytest.raises(DecodeError):
        get_codec(String, datetime.datetime).decode("1425691090160")


# ----- uuid -----


def test_uuid_encodings():
    _test_encoding(UUID, UUID("23954377-5c95-479f-b96f-94065922531d"))


def test_uuid_json_encode_success():
    val = "e9979b9c-c469-11e4-a0ad-37ff5ce3a7bf"
    assert get_codec(JSON, UUID).encode(UUID(val)) == val


def test_uuid_json_encode_error():
    with pytest.raises(EncodeError):
        get_codec(JSON, UUID).encode("definitely_not_a_uuid")


def test_uuid_json_decode_success():
    val = "15a64a3a-c46a-11e4-b790-cb538a10de85"
    assert get_codec(JSON, UUID).decode(val) == UUID(val)


def test_uuid_json_decode_error():
    with pytest.raises(DecodeError):
        get_codec(JSON, UUID).decode("this_is_not_a_uuid_either")


def test_uuid_str_decode_success():
    val = "3629cf84-c46a-11e4-9b09-43a2f172bb56"
    assert get_codec(String, UUID).decode(val) == UUID(val)


def test_uuid_str_decode_error():
    with pytest.raises(DecodeError):
        get_codec(String, UUID).decode("and_neither_is_this")


# ----- bytes -----


def test_bytes_encodings():
    _test_encoding(bytes, b"bytes_r_us")


def test_bytes_json_encode_success():
    val = bytes([4, 5, 6])
    assert get_codec(JSON, bytes).encode(val) == b64encode(val).decode()


def test_bytes_json_encode_error():
    with pytest.raises(EncodeError):
        get_codec(JSON, bytes).encode("definitely_not_a_bytes_object")


def test_bytes_json_decode_success():
    val = bytes([7, 8, 9])
    assert get_codec(JSON, bytes).decode(b64encode(val).decode()) == val


def test_bytes_json_decode_error():
    with pytest.raises(DecodeError):
        get_codec(JSON, bytes).decode("this_is_not_a_bytes_object_either")


def test_bytes_str_encode_success():
    val = bytes([0, 2, 4, 6, 8])
    assert get_codec(String, bytes).encode(val) == b64encode(val).decode()


def test_bytes_str_decode_success():
    val = bytes([1, 3, 5, 7, 9])
    assert get_codec(String, bytes).decode(b64encode(val).decode()) == val


def test_bytes_str_decode_error():
    with pytest.raises(DecodeError):
        get_codec(String, bytes).decode(123)


# ----- union -----


def test_union_encodings():
    python_type = Union[int, UUID, bool]
    values = [123, UUID("06b959d0-65e0-11e7-866d-6be08781d5cb"), False]
    for value in values:
        _test_encoding(python_type, value)


def test_union_optional_value():
    assert get_codec(JSON, Optional[str]).decode("string") == "string"


def test_union_optional_none():
    assert get_codec(JSON, Optional[str]).decode(None) is None


# ----- union_type -----


def test_union_type_encodings():
    python_type = int | UUID | bool
    values = [123, UUID("06b959d0-65e0-11e7-866d-6be08781d5cb"), False]
    for value in values:
        _test_encoding(python_type, value)


def test_union_type_optional_value():
    assert get_codec(JSON, str | None).decode("string") == "string"


def test_union_type_optional_none():
    assert get_codec(JSON, str | None).decode(None) is None


# ----- literal -----


def test_literal_encodings():
    values = ("a", 1, True)
    for value in values:
        _test_encoding(Literal[values], value)


def test_literal_single_json_type():
    codec = get_codec(JSON, Literal["a", "b", "c"])
    assert codec.json_type == str


def test_literal_mixed_str_types():
    codec = get_codec(String, Literal["a", 1])
    assert codec.decode("a") == "a"
    assert codec.decode("1") == 1


# ----- dataclass -----


def test_dataclass_json_encode_success():
    DC = make_dataclass("DC", [("eja", str), ("ejb", int)])
    assert get_codec(JSON, DC).encode(DC(eja="foo", ejb=123)) == {
        "eja": "foo",
        "ejb": 123,
    }


def test_dataclass_json_encode_error():
    DC = make_dataclass("DC", [("ejh", int)])
    with pytest.raises(EncodeError):
        get_codec(JSON, DC).encode(DC(ejh="not an int"))


def test_dataclass_json_decode_success():
    DC = make_dataclass("DC", [("dja", float), ("djb", bool)])
    assert get_codec(JSON, DC).decode({"dja": 802.11, "djb": True}) == DC(dja=802.11, djb=True)


def test_dataclass_json_decode_default_success():
    DC = make_dataclass("DC", [("djc", int), ("djd", str, field(default=None))])
    assert get_codec(JSON, DC).decode({"djc": 12345}) == DC(djc=12345, djd=None)


def test_dataclass_json_decode_optional_success():
    DC = make_dataclass("DC", (("x", int), ("y", str | None)))
    assert get_codec(JSON, DC).decode({"x": 1}) == DC(x=1, y=None)


def test_dataclass_json_decode_error():
    DC = make_dataclass("DC", [("djx", str)])
    with pytest.raises(DecodeError):
        get_codec(JSON, DC).decode({"djx": False})


def test_dataclass_json_encode_decode_keyword():
    DC = make_dataclass("DC", [("in_", str), ("inn", str)])
    codec = get_codec(JSON, DC)
    dc = DC(in_="a", inn="b")
    encoded = codec.encode(dc)
    assert encoded == {"in": "a", "inn": "b"}
    assert codec.decode(encoded) == dc


def test_dataclass_json_decode_invalid_type():
    DC = make_dataclass("DC", [("djx", str)])
    with pytest.raises(DecodeError):
        get_codec(JSON, DC).decode("not_a_dict")


def test_dataclass_json_decode_missing_field():
    DC = make_dataclass("DC", [("x", str)])
    with pytest.raises(DecodeError):
        get_codec(JSON, DC).decode({})


def test_datacls_json_decode_missing_field():
    DC = make_datacls("DC", [("x", str)])
    with pytest.raises(DecodeError):
        get_codec(JSON, DC).decode({})


# ----- any -----


def test_any_dataclass_json_codec_success():
    DC = make_dataclass("DC", [("i", int), ("s", str)])
    dc = DC(1, "a")
    encoded = get_codec(JSON, Any).encode(dc)
    decoded = get_codec(JSON, Any).decode(encoded)
    assert DC(**decoded) == dc


def test_any_dataclass_string_codec_success():
    DC = make_dataclass("DC", [("i", int), ("s", str)])
    dc = DC(1, "a")
    encoded = get_codec(String, Any).encode(dc)
    decoded = get_codec(JSON, Any).decode(json.loads(encoded))
    assert DC(**decoded) == dc


def test_any_dataclass_binary_codec_success():
    DC = make_dataclass("DC", [("i", int), ("s", str)])
    dc = DC(1, "a")
    encoded = get_codec(Binary, Any).encode(dc)
    decoded = get_codec(JSON, Any).decode(json.loads(encoded.decode()))
    assert DC(**decoded) == dc


# ----- Iterable -----


def test_iterable_json_decode():
    assert get_codec(JSON, Iterable[int]).decode([1, 2, 3]) == [1, 2, 3]


def test_iterable_string_decode():
    assert get_codec(String, Iterable[int]).decode("1,2,3") == [1, 2, 3]


# ----- general -----


def test_get_codec_annotated_codec():
    class CustomCodec(JSON[int]):
        def encode(self, value: int) -> Any:
            return 123

        def encode(self, value: Any) -> int:
            return 123

    json_codec = CustomCodec()
    CustomType = Annotated[int, json_codec]
    assert get_codec(JSON, CustomType) is json_codec
    assert get_codec(String, CustomType) is not json_codec
