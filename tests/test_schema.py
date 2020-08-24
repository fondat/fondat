import asyncio
import dataclasses
import fondat.schema as s
import isodate
import json
import pytest
import re

from base64 import b64encode
from dataclasses import make_dataclass, field
from io import BytesIO
from datetime import date, datetime
from uuid import UUID


_UTC = isodate.tzinfo.Utc()


def _equal(fn, val):
    assert val == fn(val)


def _error(fn, val):
    with pytest.raises(s.SchemaError):
        fn(val)


# -- dict -----


def test_dict_validate_success():
    s.dict({"a": s.str()}, {"a"}).validate({"a": "b"})


def test_dict_validate_error():
    _error(s.dict({"c": s.int()}).validate, '{"this": "does not validate"}')


def test_dict_validate_required_success():
    s.dict({"e": s.float()}, {"e"}).validate({"e": 1.2})


def test_dict_validate_required_error():
    _error(s.dict({"f": s.str()}, {"f"}).validate, {})


def test_dict_validate_optional_success():
    s.dict({"k": s.str(), "l": s.str()}).validate({"k": "m"})


def test_dict_json_encode_success():
    _equal(
        s.dict({"eja": s.str(), "ejb": s.int()}, {"eja", "ejb"}).json_encode,
        {"eja": "foo", "ejb": 123},
    )


def test_dict_json_encode_optional_success():
    _equal(
        s.dict({"ejc": s.float(), "ejd": s.bool()}, {"ejc"}).json_encode,
        {"ejc": 123.45},
    )


def test_dict_json_encode_optional_absent():
    _equal(s.dict({"eje": s.bool()}).json_encode, {})


def test_dict_json_encode_error():
    _error(s.dict({"ejh": s.int()}, {"ejh"}).json_encode, {"ejh": "not an int"})


def test_dict_json_decode_success():
    _equal(
        s.dict({"dja": s.float(), "djb": s.bool()}, {"dja", "djb"}).json_decode,
        {"dja": 802.11, "djb": True},
    )


def test_dict_json_decode_optional_success():
    _equal(s.dict({"djc": s.int(), "djd": s.str()}).json_decode, {"djc": 12345})


def test_dict_json_decode_additional_property_success():
    value = {"djf": "baz", "djg": "additional_property"}
    assert (
        s.dict({"djf": s.str()}, {"djf"}, additional=True).json_decode(value) == value
    )


def test_dict_json_decode_error():
    _error(s.dict({"djx": s.str()}, {"djx"}).json_decode, {"djx": False})


def test_dict_unexpected_property_error():
    _error(s.dict({}).validate, {"foo": "bar"})


def test_dict_disallow_none():
    _error(s.dict({"foo": s.str()}).json_encode, None)


def test_dict_allow_none():
    assert s.dict({"foo": s.str()}, nullable=True).json_encode(None) == None


def test_dict_required_str():
    schema = s.dict({"fjx": s.str(), "fjy": s.str()}, "fjx fjy")
    _error(schema.validate, {})
    _error(schema.validate, {"fjx": "foo"})
    _error(schema.validate, {"fjy": "foo"})
    schema.validate({"fjx": "foo", "fjy": "foo"})


def test_dict_validate_additional_success():
    schema = s.dict({"foo": s.str()}, additional=s.int())
    value = {"foo": "bar", "a": 1, "b": 2, "c": 3}
    schema.validate(value)


def test_dict_validate_additional_success():
    schema = s.dict({"foo": s.str()}, additional=s.int())
    value = {"foo": "bar", "a": 1, "b": 2, "c": "three"}
    _error(schema.validate, value)


# -- list -----


def test_list_validate_type_str_success():
    s.list(items=s.str()).validate(["a", "b", "c"])


def test_list_validate_type_int_success():
    s.list(items=s.int()).validate([1, 2, 3])


def test_list_validate_type_str_error():
    _error(s.list(items=s.str()).validate, [4, 5, 6])


def test_list_validate_type_int_error():
    _error(s.list(items=s.int()).validate, ["d", "e", "f"])


def test_list_validate_type_error():
    _error(s.list(items=s.bool()).validate, "this_is_not_a_list")


def test_list_validate_min_success():
    s.list(items=s.int(), min_items=2).validate([1, 2, 3])


def test_list_validate_min_error():
    _error(s.list(items=s.int(), min_items=3).validate, [1, 2])


def test_list_validate_max_success():
    s.list(items=s.int(), max_items=5).validate([1, 2, 3, 4])


def test_list_validate_max_error():
    _error(s.list(items=s.int(), max_items=6).validate, [1, 2, 3, 4, 5, 6, 7])


def test_list_validate_unique_success():
    s.list(items=s.int(), unique=True).validate([1, 2, 3, 4, 5])


def test_list_validate_unique_error():
    _error(s.list(items=s.int(), unique=True).validate, [1, 2, 2, 3])


def test_list_json_encode_success():
    _equal(s.list(items=s.str()).json_encode, ["a", "b", "c"])


def test_list_json_encode_type_error():
    _error(s.list(items=s.str()).json_encode, "i_am_not_a_list")


def test_list_json_encode_item_type_error():
    _error(s.list(items=s.str()).json_encode, [1, 2, 3])


def test_list_json_decode_success():
    _equal(s.list(items=s.float()).json_decode, [1.2, 3.4, 5.6])


def test_list_json_decode_error():
    _error(s.list(items=s.str()).json_decode, "not_a_list_either")


def test_list_str_encode_success():
    assert s.list(items=s.str()).str_encode(["a", "b", "c"]) == "a,b,c"


def test_list_str_decode_success():
    assert s.list(items=s.str()).str_decode("a,b,c") == ["a", "b", "c"]


def test_list_bin_encode_success():
    assert json.loads(
        s.list(items=s.str()).bin_encode(["a", "b", "c"]).decode()
    ) == json.loads('["a","b","c"]')


def test_list_bin_decode_success():
    assert s.list(items=s.str()).bin_decode(b'["a","b","c"]') == ["a", "b", "c"]


def test_list_str_decode_int_success():
    assert s.list(items=s.int()).str_decode("12,34,56") == [12, 34, 56]


def test_list_str_decode_float_success():
    assert s.list(items=s.float()).str_decode("12.34,56.78") == [12.34, 56.78]


def test_list_str_decode_crazy_csv_scenario():
    assert s.list(items=s.str()).str_decode('a,"b,c",d,"""e"""') == [
        "a",
        "b,c",
        "d",
        '"e"',
    ]


def test_list_str_decode_int_error():
    _error(s.list(items=s.int()).str_decode, "12,a,34,56")


def test_list_disallow_none():
    _error(s.list(items=s.str()).json_encode, None)


def test_list_allow_none():
    assert s.list(items=s.str(), nullable=True).json_encode(None) == None


# -- set -----


def test_set_validate_type_str_success():
    s.set(items=s.str()).validate({"a", "b", "c"})


def test_set_validate_type_int_success():
    s.set(items=s.int()).validate({1, 2, 3})


def test_set_validate_type_str_error():
    _error(s.set(items=s.str()).validate, {4, 5, 6})


def test_set_validate_type_int_error():
    _error(s.set(items=s.int()).validate, {"d", "e", "f"})


def test_set_validate_type_error():
    _error(s.set(items=s.bool()).validate, "this_is_not_a_set")


def test_set_json_encode_success():
    schema = s.set(s.str())
    value = {"a", "b", "c"}
    encdec = schema.json_decode(schema.json_encode(value))
    assert encdec == value


def test_set_json_encode_type_error():
    _error(s.set(items=s.str()).json_encode, "i_am_not_a_list")


def test_set_json_encode_item_type_error():
    _error(s.set(items=s.str()).json_encode, {1, 2, 3})


def test_set_json_decode_success():
    _equal(s.set(items=s.float()).json_decode, {1.2, 3.4, 5.6})


def test_set_json_decode_error():
    _error(s.set(items=s.str()).json_decode, "not_a_set_either")


def test_set_str_decode_str_success():
    assert s.set(items=s.str()).str_decode("a,b,c") == {"a", "b", "c"}


def test_set_str_decode_str_encode():
    assert s.set(items=s.int()).str_encode({2, 3, 1}) == "1,2,3"  # sorts result


def test_set_str_decode_int_success():
    assert s.set(items=s.int()).str_decode("12,34,56") == {12, 34, 56}


def test_set_str_decode_float_success():
    assert s.set(items=s.float()).str_decode("12.34,56.78") == {12.34, 56.78}


def test_set_str_decode_crazy_csv_scenario():
    assert s.set(items=s.str()).str_decode('a,"b,c",d,"""e"""') == {
        "a",
        "b,c",
        "d",
        '"e"',
    }


def test_set_str_decode_int_error():
    _error(s.set(items=s.int()).str_decode, "12,a,34,56")


def test_set_bin_encode_success():
    assert json.loads(
        s.set(items=s.str()).bin_encode({"a", "b", "c"}).decode()
    ) == json.loads('["a","b","c"]')


def test_set_bin_decode_success():
    assert s.set(items=s.str()).bin_decode(b'["a","b","c"]') == {"a", "b", "c"}


def test_set_disallow_none():
    _error(s.set(items=s.str()).json_encode, None)


def test_set_allow_none():
    assert s.set(items=s.str(), nullable=True).json_encode(None) == None


# -- str -----


def test_str_validate_type_success():
    s.str().validate("foo")


def test_str_validate_type_error():
    _error(s.str().validate, 123)


def test_str_validate_min_length_success():
    s.str(min_length=3).validate("12345")


def test_str_validate_min_length_error():
    _error(s.str(min_length=4).validate, "123")


def test_str_validate_max_length_success():
    s.str(max_length=5).validate("12345")


def test_str_validate_max_length_error():
    _error(s.str(max_length=6).validate, "1234567")


def test_str_validate_pattern_success():
    s.str(pattern=re.compile(r"^abc$")).validate("abc")


def test_str_validate_pattern_error():
    _error(s.str(pattern=re.compile(r"^def$")).validate, "ghi")


def test_str_json_encode_success():
    _equal(s.str().json_encode, "foo")


def test_str_json_encode_error():
    _error(s.str().json_encode, 123)


def test_str_json_decode_success():
    _equal(s.str().json_decode, "bar")


def test_str_json_decode_error():
    _error(s.str().json_decode, [])


def test_str_str_decode_success():
    _equal(s.str().str_decode, "qux")


def test_str_validate_enum_success():
    s.str(enum=["a", "b", "c", "d", "e"]).validate("e")


def test_str_validate_enum_error():
    _error(s.str(enum=["f", "g", "h"]).validate, "i")


def test_str_disallow_none():
    _error(s.str().json_encode, None)


def test_str_allow_none():
    assert s.str(nullable=True).json_encode(None) == None


# -- int -----


def test_int_validate_type_success():
    s.int().validate(123)


def test_int_validate_type_error():
    _error(s.int().validate, 123.45)


def test_int_validate_min_success():
    s.int(min=1).validate(2)


def test_int_validate_minerror():
    _error(s.int(min=2).validate, 1)


def test_int_validate_max_success():
    s.int(max=3).validate(2)


def test_int_validate_max_error():
    _error(s.int(max=4).validate, 5)


def test_int_json_encode_success():
    _equal(s.int().json_encode, 6)


def test_int_json_encode_error():
    _error(s.int().json_encode, 7.0)


def test_int_json_decode_success_int():
    _equal(s.int().json_decode, 8)


def test_int_json_decode_success_round_float():
    _equal(s.int().json_decode, 8.0)


def test_int_json_decode_error_float():
    _error(s.int().json_decode, 9.1)


def test_int_str_decode_success():
    assert s.int().str_decode("10") == 10


def test_int_str_decode_error():
    _error(s.int().str_decode, "11.2")


def test_int_validate_enum_success():
    s.int(enum=[1, 2, 3, 4, 5]).validate(4)


def test_int_validate_enum_error():
    _error(s.int(enum=[6, 7, 8, 9]).validate, 3)


def test_int_disallow_none():
    _error(s.int().json_encode, None)


def test_int_allow_none():
    assert s.int(nullable=True).json_encode(None) == None


# -- float -----


def test_float_validate_type_success():
    s.float().validate(123.45)


def test_float_validate_type_error():
    _error(s.float().validate, "123.45")


def test_float_validate_min_success():
    s.float(min=1.0).validate(1.1)


def test_float_validate_min_error():
    _error(s.float(min=2.0).validate, 1.9)


def test_float_validate_max_success():
    s.float(max=3.0).validate(2.9)


def test_float_validate_max_error():
    _error(s.float(max=4.0).validate, 4.1)


def test_float_json_encode_success():
    _equal(s.float().json_encode, 6.1)


def test_float_json_encode_error():
    _error(s.float().json_encode, 7)


def test_float_json_decode_int():
    assert s.float().json_decode(8) == 8.0


def test_float_json_decode_float():
    _equal(s.float().json_decode, 9.1)


def test_float_json_decode_error():
    _error(s.float().json_decode, "10.2")


def test_float_str_decode_float():
    assert s.float().str_decode("11.3") == 11.3


def test_float_str_decode_int():
    assert s.float().str_decode("12") == 12.0


def test_float_str_decode_error():
    _error(s.float().str_decode, "1,2")


def test_float_validate_enum_success():
    s.float(enum=[1.2, 3.4, 5.6]).validate(3.4)


def test_float_validate_enum_error():
    _error(s.float(enum=[6.7, 8.9, 10.11]).validate, 12.13)


def test_float_disallow_none():
    _error(s.float().json_encode, None)


def test_float_allow_none():
    assert s.float(nullable=True).json_encode(None) == None


# -- decimal -----

import decimal

D = decimal.Decimal


def test_decimal_validate_type_success():
    s.decimal().validate(D(123.45))


def test_decimal_validate_type_error():
    _error(s.decimal().validate, "123.45")


def test_decimal_validate_min_success():
    s.decimal(min=D(1.0)).validate(D(1.1))


def test_decimal_validate_min_error():
    _error(s.decimal(min=D(2.0)).validate, D(1.9))


def test_decimal_validate_max_success():
    s.decimal(max=D(3.0)).validate(D(2.9))


def test_decimal_validate_max_error():
    _error(s.decimal(max=D(4.0)).validate, D(4.1))


def test_decimal_json_encode_success():
    assert s.decimal().json_encode(D("6.1")) == 6.1


def test_decimal_json_encode_error():
    _error(s.decimal().json_encode, "7")


def test_decimal_json_decode_int():
    assert s.decimal().json_decode(8) == D(8.0)


def test_decimal_json_decode_decimal():
    assert s.decimal().json_decode(9.1) == D("9.1")


def test_decimal_json_decode_error():
    _error(s.decimal().json_decode, "err")


def test_decimal_str_decode_decimal():
    assert s.decimal(prec=1).str_decode("11.3") == D("11.3")


def test_decimal_str_decode_int():
    assert s.decimal().str_decode("12") == D(12.0)


def test_decimal_str_decode_error():
    _error(s.decimal().str_decode, "1,2")


def test_decimal_validate_enum_success():
    s.decimal(enum=[D("1.2"), D("3.4"), D("5.6")]).validate(D("3.4"))


def test_decimal_validate_enum_error():
    _error(s.decimal(enum=[D("6.7"), D("8.9"), D("10.11")]).validate, D("12.13"))


def test_decimal_disallow_none():
    _error(s.decimal().json_encode, None)


def test_decimal_allow_none():
    assert s.decimal(nullable=True).json_encode(None) == None


def test_decimal_round_imprecise_float():
    assert s.decimal(prec=2).json_decode(1.23) == D("1.23")


def test_decimal_round_down():
    assert s.decimal(prec=2).str_encode(D("1.234")) == "1.23"


def test_decimal_round_up():
    assert s.decimal(prec=2).str_encode(D("1.235")) == "1.24"


# -- bool -----


def test_bool_validate_type_true():
    s.bool().validate(True)


def test_bool_validate_type_false():
    s.bool().validate(False)


def test_bool_validate_type_error():
    _error(s.bool().validate, "foo")


def test_bool_json_encode_true():
    _equal(s.bool().json_encode, True)


def test_bool_json_encode_false():
    _equal(s.bool().json_encode, False)


def test_bool_json_encode_error():
    _error(s.bool().json_encode, "bar")


def test_bool_json_decode_true():
    _equal(s.bool().json_decode, True)


def test_bool_json_decode_false():
    _equal(s.bool().json_decode, False)


def test_bool_json_decode_error():
    _error(s.bool().json_decode, "baz")


def test_bool_str_encode_true():
    assert s.bool().str_encode(True) == "true"


def test_bool_str_encode_false():
    assert s.bool().str_encode(False) == "false"


def test_bool_str_decode_true():
    assert s.bool().str_decode("true") == True


def test_bool_str_decode_false():
    assert s.bool().str_decode("false") == False


def test_bool_str_decode_error():
    _error(s.bool().str_decode, "123")


def test_bool_disallow_none():
    _error(s.bool().json_encode, None)


def test_bool_allow_none():
    assert s.bool(nullable=True).json_encode(None) == None


# -- date -----


def test_date_validate_type_success():
    s.date().validate(date(2015, 6, 7))


def test_date_validate_type_error():
    _error(s.date().validate, "this_is_not_a_date")


def test_date_json_encode_success_naive():
    assert s.date().json_encode(date(2016, 7, 8)) == "2016-07-08"


def test_date_json_encode_success_aware():
    assert s.date().json_encode(date(2017, 6, 7)) == "2017-06-07"


def test_date_json_encode_error():
    _error(s.date().json_encode, "definitely_not_a_date")


def test_date_json_decode_z():
    assert s.date().json_decode("2018-08-09") == date(2018, 8, 9)


def test_date_json_decode_offset():
    assert s.date().json_decode("2019-09-10") == date(2019, 9, 10)


def test_date_json_decode_missing_tz():
    assert s.date().json_decode("2020-10-11") == date(2020, 10, 11)


def test_date_json_decode_error():
    _error(s.date().json_decode, "14256910")


def test_date_str_decode_error():
    _error(s.date().str_decode, "14256910")


def test_date_disallow_none():
    _error(s.date().json_encode, None)


def test_date_allow_none():
    assert s.date(nullable=True).json_encode(None) == None


# -- datetime -----


def test_datetime_validate_type_success():
    s.datetime().validate(datetime(2015, 6, 7, 8, 9, 10, 0, _UTC))


def test_datetime_validate_type_error():
    _error(s.datetime().validate, "this_is_not_a_datetime")


def test_datetime_json_encode_success_naive():
    assert (
        s.datetime().json_encode(datetime(2016, 7, 8, 9, 10, 11))
        == "2016-07-08T09:10:11Z"
    )


def test_datetime_json_encode_success_aware():
    assert (
        s.datetime().json_encode(datetime(2017, 6, 7, 8, 9, 10, 0, _UTC))
        == "2017-06-07T08:09:10Z"
    )


def test_datetime_json_encode_error():
    _error(s.datetime().json_encode, "definitely_not_a_datetime")


def test_datetime_json_decode_z():
    assert s.datetime().json_decode("2018-08-09T10:11:12Z") == datetime(
        2018, 8, 9, 10, 11, 12, 0, _UTC
    )


def test_datetime_json_decode_offset():
    assert s.datetime().json_decode("2019-09-10T11:12:13+01:00") == datetime(
        2019, 9, 10, 10, 12, 13, 0, _UTC
    )


def test_datetime_json_decode_missing_tz():
    assert s.datetime().json_decode("2020-10-11T12:13:14") == datetime(
        2020, 10, 11, 12, 13, 14, 0, _UTC
    )


def test_datetime_json_decode_error():
    _error(s.datetime().json_decode, "1425691090159")


def test_datetime_str_decode_z():
    assert s.datetime().str_decode("2021-11-12T13:14:15Z") == datetime(
        2021, 11, 12, 13, 14, 15, 0, _UTC
    )


def test_datetime_str_decode_offset():
    assert s.datetime().str_decode("2022-12-13T14:15:16+01:00") == datetime(
        2022, 12, 13, 13, 15, 16, 0, _UTC
    )


def test_datetime_json_decode_missing_tz():
    assert s.datetime().str_decode("2020-10-11T12:13:14") == datetime(
        2020, 10, 11, 12, 13, 14, 0, _UTC
    )


def test_datetime_str_decode_error():
    _error(s.datetime().str_decode, "1425691090160")


def test_datetime_disallow_none():
    _error(s.datetime().json_encode, None)


def test_datetime_allow_none():
    assert s.datetime(nullable=True).json_encode(None) == None


def test_datetime_str_decode_retain_microsecond():
    assert s.datetime(fractional=True).str_decode(
        "2018-01-02T03:04:05.123Z"
    ) == datetime(2018, 1, 2, 3, 4, 5, 123000, _UTC)


def test_datetime_str_encode_retain_microsecond():
    s.datetime(fractional=True).str_encode(
        datetime(2018, 1, 2, 3, 4, 5, 123456, _UTC)
    ) == "2018-01-02T03:04:05.123456Z"


def test_datetime_str_decode_truncate_microsecond():
    assert s.datetime().str_decode("2018-01-02T03:04:05.123456Z") == datetime(
        2018, 1, 2, 3, 4, 5, 0, _UTC
    )


def test_datetime_str_encode_truncate_microsecond():
    assert (
        s.datetime().str_encode(datetime(2018, 1, 2, 3, 4, 5, 123456, _UTC))
        == "2018-01-02T03:04:05Z"
    )


# -- uuid -----


def test_uuid_validate_type_success():
    s.uuid().validate(UUID("af327a12-c469-11e4-8e4f-af4f7c44473b"))


def test_uuid_validate_type_error():
    _error(s.uuid().validate, "this_is_not_a_uuid")


def test_uuid_json_encode_success():
    val = "e9979b9c-c469-11e4-a0ad-37ff5ce3a7bf"
    assert s.uuid().json_encode(UUID(val)) == val


def test_uuid_json_encode_error():
    _error(s.uuid().json_encode, "definitely_not_a_uuid")


def test_uuid_json_decode_success():
    val = "15a64a3a-c46a-11e4-b790-cb538a10de85"
    assert s.uuid().json_decode(val) == UUID(val)


def test_uuid_json_decode_error():
    _error(s.uuid().json_decode, "this_is_not_a_uuid_either")


def test_uuid_str_decode_success():
    val = "3629cf84-c46a-11e4-9b09-43a2f172bb56"
    assert s.uuid().str_decode(val) == UUID(val)


def test_uuid_str_decode_error():
    _error(s.uuid().str_decode, "and_neither_is_this")


def test_uuid_disallow_none():
    _error(s.uuid().json_encode, None)


def test_uuid_allow_none():
    assert s.uuid(nullable=True).json_encode(None) == None


# -- bytes -----


def test_bytes_validate_type_success():
    s.bytes().validate(bytes([1, 2, 3]))


def test_bytes_validate_type_error():
    _error(s.bytes().validate, "this_is_not_a_bytes_object")


def test_bytes_json_encode_success():
    val = bytes([4, 5, 6])
    assert s.bytes().json_encode(val) == b64encode(val).decode()


def test_bytes_json_encode_error():
    _error(s.bytes().json_encode, "definitely_not_a_bytes_object")


def test_bytes_json_decode_success():
    val = bytes([7, 8, 9])
    assert s.bytes().json_decode(b64encode(val).decode()) == val


def test_bytes_json_decode_error():
    _error(s.bytes().json_decode, "this_is_not_a_bytes_object_either")


def test_bytes_str_encode_success():
    val = bytes([0, 2, 4, 6, 8])
    assert s.bytes().str_encode(val) == b64encode(val).decode()


def test_bytes_str_decode_success():
    val = bytes([1, 3, 5, 7, 9])
    assert s.bytes().str_decode(b64encode(val).decode()) == val


def test_bytes_str_decode_error():
    _error(s.uuid().str_decode, "and_neither_is_this_a_bytes")


def test_bytes_disallow_none():
    _error(s.bytes().json_encode, None)


def test_bytes_allow_none():
    assert s.bytes(nullable=True).json_encode(None) == None


def test_bytes_invalid_format():
    with pytest.raises(ValueError):
        s.bytes(format="foo")


def test_bytes_hex_str_encode():
    assert s.bytes(format="hex").str_encode(b"\xde\xad\xbe\xef") == "deadbeef"


def test_bytes_hex_str_decode():
    assert s.bytes(format="hex").str_decode("DEADBEEF") == b"\xde\xad\xbe\xef"


# -- all_of -----


_all_of_schemas = s.all_of(
    [
        s.dict({"a": s.str()}, {"a"}, additional=True),
        s.dict({"b": s.int()}, {"b"}, additional=True),
    ]
)


def test_all_of_none_match():
    _error(_all_of_schemas.validate, {"c": "nope"})


def test_all_of_one_match():
    _error(_all_of_schemas.validate, {"a": "foo"})


def test_all_of_validation_all_match():
    _all_of_schemas.validate({"a": "foo", "b": 1})


def test_all_of_json_code():
    value = {"a": "foo", "b": 1, "c": [1, 2, 3]}
    schema = _all_of_schemas
    assert schema.json_decode(schema.json_encode(value)) == value


def test_all_of_nullable_outer():
    s.all_of(
        (
            s.dict({"a": s.str()}, additional=True),
            s.dict({"b": s.int()}, additional=True),
        ),
        nullable=True,
    ).validate(None)


def test_all_of_nullable_inner():
    s.all_of(
        (
            s.dict({"a": s.str()}, nullable=True, additional=True),
            s.dict({"b": s.int()}, nullable=True, additional=True),
        )
    ).validate(None)


# -- any_of -----


def test_any_of_none_match():
    _error(s.any_of([s.str(), s.int()]).validate, 123.45)


def test_any_of_either_match():
    s.any_of([s.str(), s.int()]).validate("one")
    s.any_of([s.str(), s.int()]).validate(1)


def test_any_of_json_codec():
    for value in [123.45, False]:
        schema = s.any_of([s.float(), s.bool()])
        assert schema.json_decode(schema.json_encode(value)) == value


def test_any_of_nullable_outer():
    s.any_of((s.int(), s.str()), nullable=True).validate(None)


def test_any_of_nullable_inner():
    s.any_of((s.int(nullable=True), s.str())).validate(None)


def test_any_of_match_int():
    assert isinstance(s.any_of((s.int(), s.str())).match(1), s.int)


def test_any_of_match_str():
    assert isinstance(s.any_of((s.int(), s.str())).match("one"), s.str)


def test_any_of_match_none():
    assert s.one_of((s.int(), s.str())).match(False) == None


# -- one_of -----


def test_one_of_none_match():
    _error(s.one_of([s.str(), s.int()]).validate, 123.45)


def test_one_of_either_match():
    s.one_of([s.str(), s.int()]).validate("one")
    s.one_of([s.str(), s.int()]).validate(1)


def test_one_of_validation_all_match_str():
    _error(s.one_of([s.str(), s.str()]).validate, "string")


def test_one_of_validation_all_match_nullable():
    _error(s.one_of([s.str(nullable=True), s.int(nullable=True)]).validate, None)


def test_one_of_json_codec():
    for value in [123, UUID("06b959d0-65e0-11e7-866d-6be08781d5cb"), False]:
        schema = s.one_of([s.int(), s.uuid(), s.bool()])
        assert schema.json_decode(schema.json_encode(value)) == value


def test_one_of_nullable_outer():
    s.one_of((s.int(), s.str()), nullable=True).validate(None)


def test_one_of_nullable_inner():
    s.one_of((s.int(nullable=True), s.str())).validate(None)


def test_one_of_match_int():
    assert isinstance(s.one_of((s.int(), s.str())).match(1), s.int)


def test_one_of_match_str():
    assert isinstance(s.one_of((s.int(), s.str())).match("one"), s.str)


def test_one_of_match_none():
    assert s.one_of((s.int(), s.str())).match(False) == None


def test_one_of_match_all():
    assert s.one_of([s.str(), s.str()]).match("string") == None


def test_one_of_match_inner_none_single():
    assert isinstance(s.one_of((s.str(nullable=True), s.int())).match(None), s.str)


def test_one_of_match_inner_none_ambiguous():
    assert s.one_of((s.str(nullable=True), s.int(nullable=True))).match(None) == None


# -- stream -----


def test_stream_validate_type_success():
    s.stream().validate(object())


# -- dataclass -----


def test_dataclass_validate_success():
    DC = make_dataclass("DC", [("a", s.str())])
    s.dataclass(DC).validate(DC(a="b"))


def test_dataclass_validate_error():
    DC = make_dataclass("DC", [("c", s.int())])
    _error(s.dataclass(DC).validate, DC(c="does not validate"))


def test_dataclass_validate_required_success():
    DC = make_dataclass("DC", [("e", s.float())])
    s.dataclass(DC).validate(DC(e=1.2))


def test_dataclass_validate_required_error():
    DC = make_dataclass("DC", [("f", s.str())])
    _error(s.dataclass(DC).validate, DC(f=None))


def test_dataclass_validate_optional_success():
    DC = make_dataclass(
        "DC", [("k", s.str()), ("l", s.str(nullable=True), field(default=None))]
    )
    s.dataclass(DC).validate(DC(k="m"))


def test_dataclass_json_encode_success():
    DC = make_dataclass("DC", [("eja", s.str()), ("ejb", s.int())])
    assert s.dataclass(DC).json_encode(DC(eja="foo", ejb=123)) == {
        "eja": "foo",
        "ejb": 123,
    }


def test_dataclass_json_encode_error():
    DC = make_dataclass("DC", [("ejh", s.int())])
    _error(s.dataclass(DC).json_encode, DC(ejh="not an int"))


def test_dataclass_json_decode_success():
    DC = make_dataclass("DC", [("dja", s.float()), ("djb", s.bool())])
    assert s.dataclass(DC).json_decode({"dja": 802.11, "djb": True}) == DC(
        dja=802.11, djb=True
    )


def test_dataclass_json_decode_optional_success():
    DC = make_dataclass("DC", [("djc", s.int()), ("djd", s.str(), field(default=None))])
    assert s.dataclass(DC).json_decode({"djc": 12345}) == DC(djc=12345, djd=None)


def test_dataclass_json_decode_error():
    DC = make_dataclass("DC", [("djx", s.str())])
    _error(s.dataclass(DC).json_decode, {"djx": False})


def test_dataclass_disallow_none():
    DC = make_dataclass("DC", [("foo", s.str())])
    _error(s.dataclass(DC).json_encode, None)


def test_dataclass_allow_none():
    DC = make_dataclass("DC", [("foo", s.str())])
    assert s.dataclass(DC, nullable=True).json_encode(None) == None


def test_dataclass_required():
    @dataclasses.dataclass
    class DC:
        fjx: s.str()
        fjy: s.str()
        _required = "fjx fjy"

    schema = s.dataclass(DC)
    _error(schema.validate, DC(fjx=None, fjy=None))
    _error(schema.validate, DC(fjx="foo", fjy=None))
    _error(schema.validate, DC(fjx=None, fjy="foo"))
    schema.validate(DC(fjx="foo", fjy="foo"))


# -- resource -----


class Resource:
    pass


def test_resource_class():
    assert s.resource(Resource).cls == Resource


def test_resource_class_tuple():
    assert s.resource(("tests.test_schema", "Resource")).cls == Resource


# -- SchemaError -----


def test_SchemaError_str():
    assert str(s.SchemaError("text", "/a/b")) == "/a/b: text"


# -- decorators -----


def test_sync_decorator_success():
    @s.validate
    def fn() -> s.str():
        return "str_ftw"

    fn()


def test_sync_decorator_error():
    @s.validate
    def fn() -> s.str():
        return 1

    with pytest.raises(ValueError):
        fn()


def test_async_decorator_success():
    @s.validate
    async def coro() -> s.str():
        return "str_ftw"

    assert asyncio.run(coro()) == "str_ftw"


def test_async_decorator_error():
    @s.validate
    def coro() -> s.str():
        return 1

    with pytest.raises(ValueError):
        asyncio.run(coro())
