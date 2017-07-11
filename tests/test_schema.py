
import isodate
import re
import roax.schema as s
import unittest

from base64 import b64encode
from datetime import datetime
from uuid import UUID

_UTC = isodate.tzinfo.Utc()

class TestSchema(unittest.TestCase):

    def _equal(self, fn, val):
        self.assertEqual(val, fn(val))

    def _error(self, fn, val):
        with self.assertRaises(s.SchemaError):
            fn(val)

    # ----- dict ---------------

    def test_dict_validate_success(self):
        s.dict({"a": s.str()}).validate({"a": "b"})

    def test_dict_validate_error(self):
        self._error(s.dict({"c": s.int()}).validate, '{"this": "is_not_a_dict"}')

    def test_dict_validate_required_success(self):
        s.dict({"e": s.float()}).validate({"e": 1.2})

    def test_dict_validate_required_error(self):
        self._error(s.dict({"f": s.str()}).validate, {})

    def test_dict_validate_optional_success(self):
        s.dict({"k": s.str(), "l": s.str(required=False)}).validate({"k": "m"})

    def test_dict_validate_default(self):
        s.dict({"n": s.str(required=False, default="o")}).validate({})

    def test_dict_json_encode_success(self):
        self._equal(s.dict({"eja": s.str(), "ejb": s.int()}).json_encode, {"eja": "foo", "ejb": 123})

    def test_dict_json_encode_optional_success(self):
        self._equal(s.dict({"ejc": s.float(), "ejd": s.bool(required=False)}).json_encode, {"ejc": 123.45})

    def test_dict_json_encode_default_success(self):
        self.assertEqual(s.dict({"eje": s.bool(required=False, default=False)}).json_encode({}), {"eje": False}) 

    def test_dict_json_encode_error(self):
        self._error(s.dict({"ejh": s.int()}).json_encode, {"ejh": "not an int"})

    def test_dict_json_decode_success(self):
        self._equal(s.dict({"dja": s.float(), "djb": s.bool()}).json_decode, {"dja": 802.11, "djb": True})

    def test_dict_json_decode_optional_success(self):
        self._equal(s.dict({"djc": s.int(), "djd": s.str(required=False)}).json_decode, {"djc": 12345})

    def test_dict_json_decode_default_success(self):
        self.assertEqual(s.dict({"dje": s.str(required=False, default="defaulty")}).json_decode({}), {"dje": "defaulty"}) 

    def test_dict_json_decode_ignore_success(self):
        self.assertEqual(s.dict({"djf": s.str()}).json_decode({"djf": "baz", "djg": "ignoreme"}), {"djf": "baz"})

    def test_dict_json_decode_error(self):
        self._error(s.dict({"djx": s.str()}).json_decode, {"djx": False})

    def test_dict_unexpected_property_error(self):
        self._error(s.dict({}).validate, {"foo": "bar"})

    # ----- list ---------------

    def test_list_validate_type_str_success(self):
        s.list(items=s.str()).validate(["a", "b", "c"])

    def test_list_validate_type_int_success(self):
        s.list(items=s.int()).validate([1, 2, 3])

    def test_list_validate_type_str_error(self):
        self._error(s.list(items=s.str()).validate, [4, 5, 6])

    def test_list_validate_type_int_error(self):
        self._error(s.list(items=s.int()).validate, ["d", "e", "f"])

    def test_list_validate_type_error(self):
        self._error(s.list(items=s.bool()).validate, "this_is_not_a_list")

    def test_list_validate_min_items_success(self):
        s.list(items=s.int(), min_items=2).validate([1, 2, 3])

    def test_list_validate_min_items_error(self):
        self._error(s.list(items=s.int(), min_items=3).validate, [1, 2])

    def test_list_validate_max_items_success(self):
        s.list(items=s.int(), max_items=5).validate([1, 2, 3, 4])

    def test_list_validate_max_items_error(self):
        self._error(s.list(items=s.int(), max_items=6).validate, [1, 2, 3, 4, 5, 6, 7])

    def test_list_validate_unique_success(self):
        s.list(items=s.int(), unique_items=True).validate([1, 2, 3, 4, 5])

    def test_list_validate_unique_error(self):
        self._error(s.list(items=s.int(), unique_items=True).validate, [1, 2, 2, 3])

    def test_list_json_encode_success(self):
        self._equal(s.list(items=s.str()).json_encode, ["a", "b", "c"])
    
    def test_list_json_encode_type_error(self):
        self._error(s.list(items=s.str()).json_encode, "i_am_not_a_list")

    def test_list_json_encode_item_type_error(self):
        self._error(s.list(items=s.str()).json_encode, [1, 2, 3])

    def test_list_json_decode_success(self):
        self._equal(s.list(items=s.float()).json_decode, [1.2, 3.4, 5.6])

    def test_list_json_decode_error(self):
        self._error(s.list(items=s.str()).json_decode, "not_a_list_either")

    def test_list_str_decode_str_success(self):
        self.assertEqual(s.list(items=s.str()).str_decode("a,b,c"), ["a", "b", "c"])

    def test_list_str_decode_int_success(self):
        self.assertEqual(s.list(items=s.int()).str_decode("12,34,56"), [12, 34, 56])

    def test_list_str_decode_float_success(self):
        self.assertEqual(s.list(items=s.float()).str_decode("12.34,56.78"), [12.34, 56.78])

    def test_list_str_decode_crazy_csv_scenario(self):
        self.assertEqual(s.list(items=s.str()).str_decode('a,"b,c",d,"""e"""'), ["a","b,c","d",'"e"'])

    def test_list_str_decode_int_error(self):
        self._error(s.list(items=s.int()).str_decode, "12,a,34,56")

    # ----- str ---------------

    def test_str_validate_type_success(self):
        s.str().validate("foo")

    def test_str_validate_type_error(self):
        self._error(s.str().validate, 123)

    def test_str_validate_min_len_success(self):
        s.str(min_len=3).validate("12345")

    def test_str_validate_min_len_error(self):
        self._error(s.str(min_len=4).validate, "123")

    def test_str_validate_max_len_success(self):
        s.str(max_len=5).validate("12345")

    def test_str_validate_max_len_error(self):
        self._error(s.str(max_len=6).validate, "1234567")

    def test_str_validate_pattern_success(self):
        s.str(pattern=re.compile(r"^abc$")).validate("abc")

    def test_str_validate_pattern_error(self):
        self._error(s.str(pattern=re.compile(r"^def$")).validate, "ghi")

    def test_str_json_encode_success(self):
        self._equal(s.str().json_encode, "foo")

    def test_str_json_encode_error(self):
        self._error(s.str().json_encode, 123)

    def test_str_json_decode_success(self):
        self._equal(s.str().json_decode, "bar")

    def test_str_json_decode_error(self):
        self._error(s.str().json_decode, [])

    def test_str_str_decode_success(self):
        self._equal(s.str().str_decode, "qux")

    def test_str_validate_enum_success(self):
        s.str(enum=["a", "b", "c", "d", "e"]).validate("e")

    def test_str_validate_enum_error(self):
        self._error(s.str(enum=["f", "g", "h"]).validate, "i")

    # ----- int ---------------

    def test_int_validate_type_success(self):
        s.int().validate(123)

    def test_int_validate_type_error(self):
        self._error(s.int().validate, 123.45)

    def test_int_validate_minimum_success(self):
        s.int(minimum=1).validate(2)

    def test_int_validate_minimum_error(self):
        self._error(s.int(minimum=2).validate, 1)

    def test_int_validate_maximum_success(self):
        s.int(maximum=3).validate(2)

    def test_int_validate_maximum_error(self):
        self._error(s.int(maximum=4).validate, 5)

    def test_int_json_encode_success(self):
        self._equal(s.int().json_encode, 6)

    def test_int_json_encode_error(self):
        self._error(s.int().json_encode, 7.0)

    def test_int_json_decode_success_int(self):
        self._equal(s.int().json_decode, 8)

    def test_int_json_decode_success_round_float(self):
        self._equal(s.int().json_decode, 8.0)

    def test_int_json_decode_error_float(self):
        self._error(s.int().json_decode, 9.1)

    def test_int_str_decode_success(self):
        self.assertEqual(s.int().str_decode("10"), 10)

    def test_int_str_decode_error(self):
        self._error(s.int().str_decode, "11.2")

    def test_int_validate_enum_success(self):
        s.int(enum=[1, 2, 3, 4, 5]).validate(4)

    def test_int_validate_enum_error(self):
        self._error(s.int(enum=[6, 7, 8, 9]).validate, 3)

    # ----- float ---------------

    def test_float_validate_type_success(self):
        s.float().validate(123.45)

    def test_float_validate_type_error(self):
        self._error(s.float().validate, "123.45")

    def test_float_validate_minimum_success(self):
        s.float(minimum=1.0).validate(1.1)

    def test_float_validate_minimum_error(self):
        self._error(s.float(minimum=2.0).validate, 1.9)

    def test_float_validate_maximum_success(self):
        s.float(maximum=3.0).validate(2.9)

    def test_float_validate_maximum_error(self):
        self._error(s.float(maximum=4.0).validate, 4.1)

    def test_float_json_encode_success(self):
        self._equal(s.float().json_encode, 6.1)

    def test_float_json_encode_error(self):
        self._error(s.float().json_encode, 7)

    def test_float_json_decode_int(self):
        self.assertEqual(s.float().json_decode(8), 8.0)

    def test_float_json_decode_float(self):
        self._equal(s.float().json_decode, 9.1)

    def test_float_json_decode_error(self):
        self._error(s.float().json_decode, "10.2")

    def test_float_str_decode_float(self):
        self.assertEqual(s.float().str_decode("11.3"), 11.3)

    def test_float_str_decode_int(self):
        self.assertEqual(s.float().str_decode("12"), 12.0)

    def test_float_str_decode_error(self):
        self._error(s.float().str_decode, "1,2")

    def test_float_validate_enum_success(self):
        s.float(enum=[1.2, 3.4, 5.6]).validate(3.4)

    def test_float_validate_enum_error(self):
        self._error(s.float(enum=[6.7, 8.9, 10.11]).validate, 12.13)

    # ----- bool ---------------

    def test_bool_validate_type_true(self):
        s.bool().validate(True)

    def test_bool_validate_type_false(self):
        s.bool().validate(False)

    def test_bool_validate_type_error(self):
        self._error(s.bool().validate, "foo")

    def test_bool_json_encode_true(self):
        self._equal(s.bool().json_encode, True)

    def test_bool_json_encode_false(self):
        self._equal(s.bool().json_encode, False)

    def test_bool_json_encode_error(self):
        self._error(s.bool().json_encode, "bar")

    def test_bool_json_decode_true(self):
        self._equal(s.bool().json_decode, True)

    def test_bool_json_decode_false(self):
        self._equal(s.bool().json_decode, False)

    def test_bool_json_decode_error(self):
        self._error(s.bool().json_decode, "baz")

    def test_bool_str_decode_true(self):
        self.assertEqual(s.bool().str_decode("true"), True)

    def test_bool_str_decode_false(self):
        self.assertEqual(s.bool().str_decode("false"), False)

    def test_bool_str_decode_error(self):
        self._error(s.bool().str_decode, "123")

    # ----- datetime ---------------

    def test_datetime_validate_type_success(self):
        s.datetime().validate(datetime(2015, 6, 7, 8, 9, 10, 0, _UTC))

    def test_datetime_validate_type_error(self):
        self._error(s.datetime().validate, "this_is_not_a_datetime")

    def test_datetime_json_encode_success_naive(self):
        self.assertEqual(s.datetime().json_encode(datetime(2016, 7, 8, 9, 10, 11)), "2016-07-08T09:10:11Z")

    def test_datetime_json_encode_success_aware(self):
        self.assertEqual(s.datetime().json_encode(datetime(2017, 6, 7, 8, 9, 10, 0, _UTC)), "2017-06-07T08:09:10Z")

    def test_datetime_json_encode_error(self):
        self._error(s.datetime().json_encode, "definitely_not_a_datetime")

    def test_datetime_json_decode_z(self):
        self.assertEqual(s.datetime().json_decode("2018-08-09T10:11:12Z"), datetime(2018, 8, 9, 10, 11, 12, 0, _UTC))

    def test_datetime_json_decode_offset(self):
        self.assertEqual(s.datetime().json_decode("2019-09-10T11:12:13+01:00"), datetime(2019, 9, 10, 10, 12, 13, 0, _UTC))

    def test_datetime_json_decode_missing_tz(self):
        self.assertEqual(s.datetime().json_decode("2020-10-11T12:13:14"), datetime(2020, 10, 11, 12, 13, 14, 0, _UTC))

    def test_datetime_json_decode_error(self):
        self._error(s.datetime().json_decode, "1425691090159")

    def test_datetime_str_decode_z(self):
        self.assertEqual(s.datetime().str_decode("2021-11-12T13:14:15Z"), datetime(2021, 11, 12, 13, 14, 15, 0, _UTC))

    def test_datetime_str_decode_offset(self):
        self.assertEqual(s.datetime().str_decode("2022-12-13T14:15:16+01:00"), datetime(2022, 12, 13, 13, 15, 16, 0, _UTC))

    def test_datetime_json_decode_missing_tz(self):
        self.assertEqual(s.datetime().str_decode("2020-10-11T12:13:14"), datetime(2020, 10, 11, 12, 13, 14, 0, _UTC))

    def test_datetime_str_decode_error(self):
        self._error(s.datetime().str_decode, "1425691090160")

    # ----- uuid ---------------

    def test_uuid_validate_type_success(self):
        s.uuid().validate(UUID("af327a12-c469-11e4-8e4f-af4f7c44473b"))

    def test_uuid_validate_type_error(self):
        self._error(s.uuid().validate, "this_is_not_a_uuid")

    def test_uuid_json_encode_success(self):
        val = "e9979b9c-c469-11e4-a0ad-37ff5ce3a7bf"
        self.assertEqual(s.uuid().json_encode(UUID(val)), val)

    def test_uuid_json_encode_error(self):
        self._error(s.uuid().json_encode, "definitely_not_a_uuid")

    def test_uuid_json_decode_success(self):
        val = "15a64a3a-c46a-11e4-b790-cb538a10de85"
        self.assertEqual(s.uuid().json_decode(val), UUID(val))

    def test_uuid_json_decode_error(self):
        self._error(s.uuid().json_decode, "this_is_not_a_uuid_either")

    def test_uuid_str_decode_success(self):
        val = "3629cf84-c46a-11e4-9b09-43a2f172bb56"
        self.assertEqual(s.uuid().str_decode(val), UUID(val))

    def test_uuid_str_decode_error(self):
        self._error(s.uuid().str_decode, "and_neither_is_this")

    # ----- bytes ---------------

    def test_bytes_validate_type_success(self):
        s.bytes().validate(bytes([1,2,3]))

    def test_bytes_validate_type_error(self):
        self._error(s.bytes().validate, "this_is_not_a_bytes_object")

    def test_bytes_json_encode_success(self):
        val = bytes([4,5,6])
        self.assertEqual(s.bytes().json_encode(val), b64encode(val).decode())

    def test_bytes_json_encode_error(self):
        self._error(s.bytes().json_encode, "definitely_not_a_bytes_object")

    def test_bytes_json_decode_success(self):
        val = bytes([7,8,9])
        self.assertEqual(s.bytes().json_decode(b64encode(val).decode()), val)

    def test_bytes_json_decode_error(self):
        self._error(s.bytes().json_decode, "this_is_not_a_bytes_object_either")

    def test_bytes_str_encode_success(self):
        val = bytes([0,2,4,6,8])
        self.assertEqual(s.bytes().str_encode(val), b64encode(val).decode())

    def test_bytes_str_decode_success(self):
        val = bytes([1,3,5,7,9])
        self.assertEqual(s.bytes().str_decode(b64encode(val).decode()), val)

    def test_bytes_str_decode_error(self):
        self._error(s.uuid().str_decode, "and_neither_is_this_a_bytes")

    # ----- decorators ---------------

    def test_params_decorator_mismatch_a(self):
        with self.assertRaises(TypeError):
            @s.validate(params=s.dict({"a": s.str()}))
            def fn(b):
                pass

    def test_params_decorator_mismatch_b(self):
        with self.assertRaises(TypeError):
            @s.validate(params=s.dict({}))
            def fn(b):
                pass

    def test_returns_error(self):
        @s.validate(returns=s.str())
        def fn():
            return 1
        with self.assertRaises(ValueError):
            fn()

    def test_returns_success(self):
        @s.validate(returns=s.str())
        def fn():
            return "str_ftw"
        fn()

    # ----- allof ---------------

    def test_allof_none_match(self):
        self._error(s.allof([s.str(), s.bool()]).validate, 123)

    def test_allof_one_match(self):
        self._error(s.allof([s.str(), s.bool()]).validate, "hello")

    def test_allof_validation_all_match(self):
        s.allof([s.str(min_len=2), s.str(max_len=8)]).validate("hello")

    def test_allof_json_code(self):
        for value in [ "12", "123", "1234" ]:
            schema = s.allof([s.str(min_len=2), s.str(max_len=4)])
            self.assertEqual(schema.json_decode(schema.json_encode(value)), value)

    # ----- anyof ---------------

    def test_anyof_none_match(self):
        self._error(s.anyof([s.str(), s.int()]).validate, 123.45)

    def test_anyof_either_match(self):
        s.anyof([s.str(), s.int()]).validate("one")
        s.anyof([s.str(), s.int()]).validate(1)

    def test_anyof_json_codec(self):
        for value in [ 123.45, None, False ]:
            schema = s.anyof([s.none(), s.float(), s.bool()])
            self.assertEqual(schema.json_decode(schema.json_encode(value)), value)

    # ----- oneof ---------------

    def test_oneof_none_match(self):
        self._error(s.oneof([s.str(), s.int()]).validate, 123.45)

    def test_oneof_either_match(self):
        s.oneof([s.str(), s.int()]).validate("one")
        s.oneof([s.str(), s.int()]).validate(1)

    def test_oneof_validation_all_match(self):
        self._error(s.oneof([s.str(), s.str()]).validate, "string")

    def test_oneof_json_codec(self):
        for value in [ 123, UUID("06b959d0-65e0-11e7-866d-6be08781d5cb"), None, False ]:
            schema = s.oneof([s.none(), s.int(), s.uuid(), s.bool()])
            self.assertEqual(schema.json_decode(schema.json_encode(value)), value)

if __name__ == "__main__":
    unittest.main()
