import dataclasses
import datetime
import decimal
import enum
import json
import pytest
import re

from base64 import b64encode
from fondat.codec import get_codec
from dataclasses import make_dataclass, field
from io import BytesIO
from typing import Optional, TypedDict, Union
from uuid import UUID


def _equal(fn, val):
    assert val == fn(val)


def _error(fn, val):
    with pytest.raises((TypeError, ValueError)):
        fn(val)


def _test_encodings(codec, value):
    assert codec.json_decode(codec.json_encode(value)) == value
    assert codec.str_decode(codec.str_encode(value)) == value
    assert codec.bytes_decode(codec.bytes_encode(value)) == value


# -- dict -----


def test_dict_json_encode_success():
    T = dict[str, int]
    value = dict(a=1, b=2, c=3)
    assert get_codec(T).json_encode(value) == value


def test_dict_json_encode_error():
    T = dict[str, int]
    value = dict(a="not int")
    with pytest.raises(TypeError):
        get_codec(T).json_encode(value)


def test_dict_json_decode_success():
    T = dict[str, bool]
    value = dict(a=True, b=False)
    assert get_codec(T).json_decode(value) == value


def test_dict_json_decode_error():
    T = dict[str, str]
    value = dict(a=False)
    with pytest.raises(TypeError):
        get_codec(T).json_decode(value)


# -- TypedDict -----


def test_typeddict_json_encode_success():
    TD = TypedDict("TD", dict(eja=str, ejb=int))
    value = dict(eja="foo", ejb=123)
    assert get_codec(TD).json_encode(value) == value


def test_typeddict_json_encode_optional_success():
    TD = TypedDict("TD", dict(ejc=float, ejd=bool), total=False)
    value = dict(ejc=123.45)
    assert get_codec(TD).json_encode(value) == value


def test_typeddict_json_encode_optional_absent():
    TD = TypedDict("TD", dict(eje=bool), total=False)
    value = dict()
    assert get_codec(TD).json_encode(value) == value


def test_typeddict_json_encode_error():
    TD = TypedDict("TD", dict(ejh=int))
    with pytest.raises(TypeError):
        get_codec(TD).json_encode(dict(ejh="not an int"))


def test_typeddict_json_decode_success():
    TD = TypedDict("TD", dict(dja=float, djb=bool))
    value = dict(dja=802.11, djb=True)
    assert get_codec(TD).json_decode(value) == value


def test_typeddict_json_decode_optional_success():
    TD = TypedDict("TD", dict(djc=int, djd=str), total=False)
    value = dict(djc=12345)
    assert get_codec(TD).json_decode(value) == value


def test_typeddict_json_decode_error():
    TD = TypedDict("TD", dict(djx=str))
    value = dict(djx=False)
    with pytest.raises(TypeError):
        get_codec(TD).json_decode(value)


# -- list -----


def test_list_encodings():
    _test_encodings(get_codec(list[int]), [1, 2, 3])


def test_list_json_encode_item_type_error():
    _error(get_codec(list[str]).json_encode, [1, 2, 3])


def test_list_json_decode_success():
    _equal(get_codec(list[float]).json_decode, [1.2, 3.4, 5.6])


def test_list_json_decode_error():
    _error(get_codec(list[str]).json_decode, "not_a_list")


def test_list_str_encode_success():
    assert get_codec(list[str]).str_encode(["a", "b", "c"]) == "a,b,c"


def test_list_str_decode_success():
    assert get_codec(list[str]).str_decode("a,b,c") == ["a", "b", "c"]


def test_list_bytes_encode_success():
    assert json.loads(
        get_codec(list[str]).bytes_encode(["a", "b", "c"]).decode()
    ) == json.loads('["a","b","c"]')


def test_list_bytes_decode_success():
    assert get_codec(list[str]).bytes_decode(b'["a","b","c"]') == [
        "a",
        "b",
        "c",
    ]


def test_list_str_decode_int_success():
    assert get_codec(list[int]).str_decode("12,34,56") == [12, 34, 56]


def test_list_str_decode_float_success():
    assert get_codec(list[float]).str_decode("12.34,56.78") == [
        12.34,
        56.78,
    ]


def test_list_str_decode_crazy_csv_scenario():
    assert get_codec(list[str]).str_decode('a,"b,c",d,"""e"""') == [
        "a",
        "b,c",
        "d",
        '"e"',
    ]


def test_list_str_decode_int_error():
    _error(get_codec(list[int]).str_decode, "12,a,34,56")


# -- set -----


def test_set_encodings():
    _test_encodings(get_codec(set[int]), {1, 2, 3})


def test_set_json_encode_type_error():
    _error(get_codec(set[str]).json_encode, "i_am_not_a_set")


def test_set_json_encode_item_type_error():
    _error(get_codec(set[str]).json_encode, {1, 2, 3})


def test_set_json_decode_success():
    assert get_codec(set[float]).json_decode([1.2, 3.4, 5.6]) == {
        1.2,
        3.4,
        5.6,
    }


def test_set_json_decode_error():
    _error(get_codec(set[str]).json_decode, "not_a_set_either")


def test_set_str_decode_str_success():
    assert get_codec(set[str]).str_decode("a,b,c") == {"a", "b", "c"}


def test_set_str_decode_str_encode():
    assert (
        get_codec(set[int]).str_encode({2, 3, 1}) == "1,2,3"
    )  # sorts result


def test_set_str_decode_int_success():
    assert get_codec(set[int]).str_decode("12,34,56") == {12, 34, 56}


def test_set_str_decode_float_success():
    assert get_codec(set[float]).str_decode("12.34,56.78") == {
        12.34,
        56.78,
    }


def test_set_str_decode_crazy_csv_scenario():
    assert get_codec(set[str]).str_decode('a,"b,c",d,"""e"""') == {
        "a",
        "b,c",
        "d",
        '"e"',
    }


def test_set_str_decode_int_error():
    _error(get_codec(set[int]).str_decode, "12,a,34,56")


def test_set_bytes_encode_success():
    assert json.loads(
        get_codec(set[str]).bytes_encode({"a", "b", "c"}).decode()
    ) == json.loads('["a","b","c"]')


def test_set_bytes_decode_success():
    assert get_codec(set[str]).bytes_decode(b'["a","b","c"]') == {
        "a",
        "b",
        "c",
    }


# -- str -----


str_codec = get_codec(str)


def test_str_encodings():
    _test_encodings(str_codec, "foo")


def test_str_json_encode_success():
    _equal(str_codec.json_encode, "foo")


def test_str_json_encode_error():
    with pytest.raises(TypeError):
        str_codec.json_encode(123)


def test_str_json_decode_success():
    _equal(str_codec.json_decode, "bar")


def test_str_json_decode_error():
    _error(str_codec.json_decode, 123)


def test_str_str_decode_success():
    _equal(str_codec.str_decode, "qux")


# -- int -----


int_codec = get_codec(int)


def test_int_encodings():
    _test_encodings(int_codec, 1)


def test_int_json_encode_success():
    _equal(int_codec.json_encode, 6)


def test_int_json_encode_error():
    _error(int_codec.json_encode, 7.0)


def test_int_json_decode_success_int():
    _equal(int_codec.json_decode, 8)


def test_int_json_decode_success_round_float():
    _equal(int_codec.json_decode, 8.0)


def test_int_json_decode_error_float():
    _error(int_codec.json_decode, 9.1)


def test_int_str_decode_success():
    assert int_codec.str_decode("10") == 10


def test_int_str_decode_error():
    _error(int_codec.str_decode, "11.2")


# -- float -----


float_codec = get_codec(float)


def test_float_encodings():
    _test_encodings(float_codec, 1.2)


def test_float_json_encode_success():
    _equal(float_codec.json_encode, 6.1)


def test_float_json_encode_error():
    _error(float_codec.json_encode, 7)


def test_float_json_decode_int():
    assert float_codec.json_decode(8) == 8.0


def test_float_json_decode_float():
    _equal(float_codec.json_decode, 9.1)


def test_float_json_decode_error():
    _error(float_codec.json_decode, "10.2")


def test_float_str_decode_float():
    assert float_codec.str_decode("11.3") == 11.3


def test_float_str_decode_int():
    assert float_codec.str_decode("12") == 12.0


def test_float_str_decode_error():
    _error(float_codec.str_decode, "1,2")


# -- decimal -----


D = decimal.Decimal
decimal_codec = get_codec(D)


def test_decimal_encodings():
    _test_encodings(decimal_codec, D("1.234"))


def test_decimal_json_encode_success():
    assert decimal_codec.json_encode(D("6.1")) == "6.1"


def test_decimal_json_encode_error():
    _error(decimal_codec.json_encode, "7")


def test_decimal_json_decode_int():
    assert decimal_codec.json_decode("8") == D("8.0")


def test_decimal_json_decode_decimal():
    assert decimal_codec.json_decode("9.1") == D("9.1")


def test_decimal_json_decode_error():
    _error(decimal_codec.json_decode, "err")


def test_decimal_str_decode_decimal():
    assert decimal_codec.str_decode("11.3") == D("11.3")


def test_decimal_str_decode_int():
    assert decimal_codec.str_decode("12") == D("12.0")


def test_decimal_str_decode_error():
    _error(decimal_codec.str_decode, "1,2")


# -- bool -----


bool_codec = get_codec(bool)


def test_bool_encodings():
    _test_encodings(bool_codec, True)


def test_bool_json_encode_true():
    _equal(bool_codec.json_encode, True)


def test_bool_json_encode_false():
    _equal(bool_codec.json_encode, False)


def test_bool_json_encode_error():
    _error(bool_codec.json_encode, "bar")


def test_bool_json_decode_true():
    _equal(bool_codec.json_decode, True)


def test_bool_json_decode_false():
    _equal(bool_codec.json_decode, False)


def test_bool_json_decode_error():
    _error(bool_codec.json_decode, "baz")


def test_bool_str_encode_true():
    assert bool_codec.str_encode(True) == "true"


def test_bool_str_encode_false():
    assert bool_codec.str_encode(False) == "false"


def test_bool_str_decode_true():
    assert bool_codec.str_decode("true") == True


def test_bool_str_decode_false():
    assert bool_codec.str_decode("false") == False


def test_bool_str_decode_error():
    _error(bool_codec.str_decode, "123")


# -- date -----


date_codec = get_codec(datetime.date)


def test_date_encodings():
    _test_encodings(date_codec, datetime.date(2020, 12, 6))


def test_date_json_encode_success_naive():
    assert date_codec.json_encode(datetime.date(2016, 7, 8)) == "2016-07-08"


def test_date_json_encode_success_aware():
    assert date_codec.json_encode(datetime.date(2017, 6, 7)) == "2017-06-07"


def test_date_json_encode_error():
    _error(date_codec.json_encode, "definitely_not_a_date")


def test_date_json_decode_z():
    assert date_codec.json_decode("2018-08-09") == datetime.date(2018, 8, 9)


def test_date_json_decode_offset():
    assert date_codec.json_decode("2019-09-10") == datetime.date(2019, 9, 10)


def test_date_json_decode_missing_tz():
    assert date_codec.json_decode("2020-10-11") == datetime.date(2020, 10, 11)


def test_date_json_decode_error():
    _error(date_codec.json_decode, "14256910")


def test_date_str_decode_error():
    _error(date_codec.str_decode, "14256910")


# -- datetime -----


datetime_codec = get_codec(datetime.datetime)


def test_datetime_encodings():
    _test_encodings(
        datetime_codec,
        datetime.datetime(2020, 12, 6, 16, 10, 45, 0, datetime.timezone.utc),
    )


def test_datetime_json_encode_success_naive():
    assert (
        datetime_codec.json_encode(datetime.datetime(2016, 7, 8, 9, 10, 11))
        == "2016-07-08T09:10:11Z"
    )


def test_datetime_json_encode_success_aware():
    assert (
        datetime_codec.json_encode(
            datetime.datetime(2017, 6, 7, 8, 9, 10, 0, datetime.timezone.utc)
        )
        == "2017-06-07T08:09:10Z"
    )


def test_datetime_json_encode_error():
    _error(datetime_codec.json_encode, "definitely_not_a_datetime")


def test_datetime_json_decode_z():
    assert datetime_codec.json_decode("2018-08-09T10:11:12Z") == datetime.datetime(
        2018, 8, 9, 10, 11, 12, 0, datetime.timezone.utc
    )


def test_datetime_json_decode_offset():
    assert datetime_codec.json_decode("2019-09-10T11:12:13+01:00") == datetime.datetime(
        2019, 9, 10, 10, 12, 13, 0, datetime.timezone.utc
    )


def test_datetime_json_decode_missing_tz():
    assert datetime_codec.json_decode("2020-10-11T12:13:14") == datetime.datetime(
        2020, 10, 11, 12, 13, 14, 0, datetime.timezone.utc
    )


def test_datetime_json_decode_error():
    _error(datetime_codec.json_decode, "1425691090159")


def test_datetime_str_decode_z():
    assert datetime_codec.str_decode("2021-11-12T13:14:15Z") == datetime.datetime(
        2021, 11, 12, 13, 14, 15, 0, datetime.timezone.utc
    )


def test_datetime_str_decode_offset():
    assert datetime_codec.str_decode("2022-12-13T14:15:16+01:00") == datetime.datetime(
        2022, 12, 13, 13, 15, 16, 0, datetime.timezone.utc
    )


def test_datetime_json_decode_missing_tz():
    assert datetime_codec.str_decode("2020-10-11T12:13:14") == datetime.datetime(
        2020, 10, 11, 12, 13, 14, 0, datetime.timezone.utc
    )


def test_datetime_str_decode_error():
    _error(datetime_codec.str_decode, "1425691090160")


# -- uuid -----


uuid_codec = get_codec(UUID)


def test_uuid_encodings():
    _test_encodings(uuid_codec, UUID("23954377-5c95-479f-b96f-94065922531d"))


def test_uuid_json_encode_success():
    val = "e9979b9c-c469-11e4-a0ad-37ff5ce3a7bf"
    assert uuid_codec.json_encode(UUID(val)) == val


def test_uuid_json_encode_error():
    _error(uuid_codec.json_encode, "definitely_not_a_uuid")


def test_uuid_json_decode_success():
    val = "15a64a3a-c46a-11e4-b790-cb538a10de85"
    assert uuid_codec.json_decode(val) == UUID(val)


def test_uuid_json_decode_error():
    _error(uuid_codec.json_decode, "this_is_not_a_uuid_either")


def test_uuid_str_decode_success():
    val = "3629cf84-c46a-11e4-9b09-43a2f172bb56"
    assert uuid_codec.str_decode(val) == UUID(val)


def test_uuid_str_decode_error():
    _error(uuid_codec.str_decode, "and_neither_is_this")


# -- bytes -----


bytes_codec = get_codec(bytes)


def test_bytes_encodings():
    _test_encodings(bytes_codec, b"bytes_r_us")


def test_bytes_json_encode_success():
    val = bytes([4, 5, 6])
    assert bytes_codec.json_encode(val) == b64encode(val).decode()


def test_bytes_json_encode_error():
    _error(bytes_codec.json_encode, "definitely_not_a_bytes_object")


def test_bytes_json_decode_success():
    val = bytes([7, 8, 9])
    assert bytes_codec.json_decode(b64encode(val).decode()) == val


def test_bytes_json_decode_error():
    _error(bytes_codec.json_decode, "this_is_not_a_bytes_object_either")


def test_bytes_str_encode_success():
    val = bytes([0, 2, 4, 6, 8])
    assert bytes_codec.str_encode(val) == b64encode(val).decode()


def test_bytes_str_decode_success():
    val = bytes([1, 3, 5, 7, 9])
    assert bytes_codec.str_decode(b64encode(val).decode()) == val


def test_bytes_str_decode_error():
    _error(bytes_codec.str_decode, 123)


# -- enum -----


class enum_type(enum.Enum):
    X_MEMBER = "x"
    Y_MEMBER = "y"


enum_codec = get_codec(enum_type)


def test_enum_encodings():
    _test_encodings(enum_codec, enum_type.X_MEMBER)


def test_enum_single_json_type():
    assert enum_codec.json_type == str


def test_enum_mixed_json_types():
    class mixed(enum.Enum):
        STR_MEMBER = "s"
        INT_MEMBER = 1

    codec = get_codec(mixed)
    assert codec.json_decode("s") == mixed.STR_MEMBER
    assert codec.json_decode(1) == mixed.INT_MEMBER


# -- union -----


def test_union_encodings():
    codec = get_codec(Union[int, UUID, bool])
    values = [123, UUID("06b959d0-65e0-11e7-866d-6be08781d5cb"), False]
    for value in values:
        _test_encodings(codec, value)


def test_union_optional_value():
    assert (
        get_codec(Optional[str]).json_decode("string") == "string"
    )


def test_union_optional_none():
    get_codec(Optional[str]).json_decode(None) is None


# -- dataclass -----


def test_dataclass_json_encode_success():
    DC = make_dataclass("DC", [("eja", str), ("ejb", int)])
    assert get_codec(DC).json_encode(DC(eja="foo", ejb=123)) == {
        "eja": "foo",
        "ejb": 123,
    }


def test_dataclass_json_encode_error():
    DC = make_dataclass("DC", [("ejh", int)])
    _error(get_codec(DC).json_encode, DC(ejh="not an int"))


def test_dataclass_json_decode_success():
    DC = make_dataclass("DC", [("dja", float), ("djb", bool)])
    assert get_codec(DC).json_decode({"dja": 802.11, "djb": True}) == DC(
        dja=802.11, djb=True
    )


def test_dataclass_json_decode_optional_success():
    DC = make_dataclass("DC", [("djc", int), ("djd", str, field(default=None))])
    assert get_codec(DC).json_decode({"djc": 12345}) == DC(
        djc=12345, djd=None
    )


def test_dataclass_json_decode_error():
    DC = make_dataclass("DC", [("djx", str)])
    _error(get_codec(DC).json_decode, {"djx": False})
