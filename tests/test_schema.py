import unittest
import re
import isodate
from datetime import datetime
from uuid import UUID
import roax.schema as s

_UTC = isodate.tzinfo.Utc()

class TestSchema(unittest.TestCase):

    def _equal(self, fn, val):
        self.assertEqual(val, fn(val))

    def _error(self, fn, val):
        with self.assertRaises(s.SchemaError):
            fn(val)

    # ----- dict ---------------

    def test_dict_validate_success(self):
        s.dict(fields={"a": s.str()}).validate({"a": "b"})

    def test_dict_validate_error(self):
        self._error(s.dict(fields={"c": s.int()}).validate, '{"this": "is_not_a_dict"}')

    def test_dict_validate_required_success(self):
        s.dict(fields={"e": s.float()}).validate({"e": 1.2})

    def test_dict_validate_required_error(self):
        self._error(s.dict(fields={"f": s.str()}).validate, {})

    def test_dict_validate_ignore_extra_fields(self):
        s.dict(fields={"g": s.str()}).validate({"g": "h", "i": "j"})

    def test_dict_validate_optional_success(self):
        s.dict(fields={"k": s.str(), "l": s.str(required=False)}).validate({"k": "m"})

    def test_dict_validate_default(self):
        s.dict(fields={"n": s.str(required=False, default="o")}).validate({})

    def test_dict_encode_json_success(self):
        self._equal(s.dict(fields={"eja": s.str(), "ejb": s.int()}).encode_json, {"eja": "foo", "ejb": 123})

    def test_dict_encode_json_optional_success(self):
        self._equal(s.dict(fields={"ejc": s.float(), "ejd": s.bool(required=False)}).encode_json, {"ejc": 123.45})

    def test_dict_encode_json_default_success(self):
        self.assertEqual(s.dict(fields={"eje": s.bool(required=False, default=False)}).encode_json({}), {"eje": False}) 

    def test_dict_encode_json_ignore_success(self):
        self.assertEqual(s.dict(fields={"ejf": s.int()}).encode_json({"ejf": 456, "ejg": "bar"}), {"ejf": 456})

    def test_dict_encode_json_error(self):
        self._error(s.dict(fields={"ejh": s.int()}).encode_json, {"ejh": "not an int"})

    def test_dict_decode_json_success(self):
        self._equal(s.dict(fields={"dja": s.float(), "djb": s.bool()}).decode_json, {"dja": 802.11, "djb": True})

    def test_dict_decode_json_optional_success(self):
        self._equal(s.dict(fields={"djc": s.int(), "djd": s.str(required=False)}).decode_json, {"djc": 12345})

    def test_dict_decode_json_default_success(self):
        self.assertEqual(s.dict(fields={"dje": s.str(required=False, default="defaulty")}).decode_json({}), {"dje": "defaulty"}) 

    def test_dict_decode_json_ignore_success(self):
        self.assertEqual(s.dict(fields={"djf": s.str()}).decode_json({"djf": "baz", "djg": "ignoreme"}), {"djf": "baz"})

    def test_dict_decode_json_error(self):
        self._error(s.dict(fields={"djx": s.str()}).decode_json, {"djx": False})

    def test_dict_decode_param_success(self):
        self.assertEqual(s.dict(fields={"dpa": s.str(), "dpb": s.int()}).decode_param({"dpa": "foo", "dpb": "123"}), {"dpa": "foo", "dpb": 123})

    def test_dict_decode_param_optional_success(self):
        self._equal(s.dict(fields={"dpc": s.str(required=False), "dpd": s.str()}).decode_param, {"dpd": "qux"})

    def test_dict_decode_param_default_int_success(self):
        self.assertEqual(s.dict(fields={"p1": s.int(required=False, default=90210)}).decode_param({}), {"p1": 90210}) 

    def test_dict_decode_param_default_float_success(self):
        self.assertEqual(s.dict(fields={"p2": s.float(required=False, default=999.123)}).decode_param({}), {"p2": 999.123}) 

    def test_dict_decode_param_default_true_success(self):
        self.assertEqual(s.dict(fields={"p3": s.bool(required=False, default=True)}).decode_param({}), {"p3": True}) 

    def test_dict_decode_param_default_false_success(self):
        self.assertEqual(s.dict(fields={"p4": s.bool(required=False, default=False)}).decode_param({}), {"p4": False}) 

    def test_dict_decode_param_ignore_success(self):
        self.assertEqual(s.dict(fields={"dpf": s.bool()}).decode_param({"dpf": "true", "djg": "ignoreme"}), {"dpf": True})

    def test_dict_decode_param_error(self):
        self._error(s.dict(fields={"dpx": s.int()}).decode_param, {"dpx": "not_an_int"})

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

    def test_list_encode_json_success(self):
        self._equal(s.list(items=s.str()).encode_json, ["a", "b", "c"])
    
    def test_list_encode_json_type_error(self):
        self._error(s.list(items=s.str()).encode_json, "i_am_not_a_list")

    def test_list_encode_json_item_type_error(self):
        self._error(s.list(items=s.str()).encode_json, [1, 2, 3])

    def test_list_decode_json_success(self):
        self._equal(s.list(items=s.float()).decode_json, [1.2, 3.4, 5.6])

    def test_list_decode_json_error(self):
        self._error(s.list(items=s.str()).decode_json, "not_a_list_either")

    def test_list_decode_param_str_success(self):
        self.assertEqual(s.list(items=s.str()).decode_param("a,b,c"), ["a", "b", "c"])

    def test_list_decode_param_int_success(self):
        self.assertEqual(s.list(items=s.int()).decode_param("12,34,56"), [12, 34, 56])

    def test_list_decode_param_float_success(self):
        self.assertEqual(s.list(items=s.float()).decode_param("12.34,56.78"), [12.34, 56.78])

    def test_list_decode_param_crazy_csv_scenario(self):
        self.assertEqual(s.list(items=s.str()).decode_param('a,"b,c",d,"""e"""'), ["a","b,c","d",'"e"'])

    def test_list_decode_param_int_error(self):
        self._error(s.list(items=s.int()).decode_param, "12,a,34,56")

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

    def test_str_encode_json_success(self):
        self._equal(s.str().encode_json, "foo")

    def test_str_encode_json_error(self):
        self._error(s.str().encode_json, 123)

    def test_str_decode_json_success(self):
        self._equal(s.str().decode_json, "bar")

    def test_str_decode_json_error(self):
        self._error(s.str().decode_json, [])

    def test_str_decode_param_success(self):
        self._equal(s.str().decode_param, "qux")

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

    def test_int_encode_json_success(self):
        self._equal(s.int().encode_json, 6)

    def test_int_encode_json_error(self):
        self._error(s.int().encode_json, 7.0)

    def test_int_decode_json_success(self):
        self._equal(s.int().decode_json, 8)

    def test_int_decode_json_error(self):
        self._error(s.int().decode_json, 9.0)

    def test_int_decode_param_success(self):
        self.assertEqual(s.int().decode_param("10"), 10)

    def test_int_decode_param_error(self):
        self._error(s.int().decode_param, "11.2")

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

    def test_float_encode_json_success(self):
        self._equal(s.float().encode_json, 6.1)

    def test_float_encode_json_error(self):
        self._error(s.float().encode_json, 7)

    def test_float_decode_json_int(self):
        self.assertEqual(s.float().decode_json(8), 8.0)

    def test_float_decode_json_float(self):
        self._equal(s.float().decode_json, 9.1)

    def test_float_decode_json_error(self):
        self._error(s.float().decode_json, "10.2")

    def test_float_decode_param_float(self):
        self.assertEqual(s.float().decode_param("11.3"), 11.3)

    def test_float_decode_param_int(self):
        self.assertEqual(s.float().decode_param("12"), 12.0)

    def test_float_decode_param_error(self):
        self._error(s.float().decode_param, "1,2")

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

    def test_bool_encode_json_true(self):
        self._equal(s.bool().encode_json, True)

    def test_bool_encode_json_false(self):
        self._equal(s.bool().encode_json, False)

    def test_bool_encode_json_error(self):
        self._error(s.bool().encode_json, "bar")

    def test_bool_decode_json_true(self):
        self._equal(s.bool().decode_json, True)

    def test_bool_decode_json_false(self):
        self._equal(s.bool().decode_json, False)

    def test_bool_decode_json_error(self):
        self._error(s.bool().decode_json, "baz")

    def test_bool_decode_param_true(self):
        self.assertEqual(s.bool().decode_param("true"), True)

    def test_bool_decode_param_false(self):
        self.assertEqual(s.bool().decode_param("false"), False)

    def test_bool_decode_param_error(self):
        self._error(s.bool().decode_param, "123")

    # ----- datetime ---------------

    def test_datetime_validate_type_success(self):
        s.datetime().validate(datetime(2015, 6, 7, 8, 9, 10, 0, _UTC))

    def test_datetime_validate_type_error(self):
        self._error(s.datetime().validate, "this_is_not_a_datetime")

    def test_datetime_encode_json_success_naive(self):
        self.assertEqual(s.datetime().encode_json(datetime(2016, 7, 8, 9, 10, 11)), "2016-07-08T09:10:11Z")

    def test_datetime_encode_json_success_aware(self):
        self.assertEqual(s.datetime().encode_json(datetime(2017, 6, 7, 8, 9, 10, 0, _UTC)), "2017-06-07T08:09:10Z")

    def test_datetime_encode_json_error(self):
        self._error(s.datetime().encode_json, "definitely_not_a_datetime")

    def test_datetime_decode_json_z(self):
        self.assertEqual(s.datetime().decode_json("2018-08-09T10:11:12Z"), datetime(2018, 8, 9, 10, 11, 12, 0, _UTC))

    def test_datetime_decode_json_offset(self):
        self.assertEqual(s.datetime().decode_json("2019-09-10T11:12:13+01:00"), datetime(2019, 9, 10, 10, 12, 13, 0, _UTC))

    def test_datetime_decode_json_missing_tz(self):
        self.assertEqual(s.datetime().decode_json("2020-10-11T12:13:14"), datetime(2020, 10, 11, 12, 13, 14, 0, _UTC))

    def test_datetime_decode_json_error(self):
        self._error(s.datetime().decode_json, "1425691090159")

    def test_datetime_decode_param_z(self):
        self.assertEqual(s.datetime().decode_param("2021-11-12T13:14:15Z"), datetime(2021, 11, 12, 13, 14, 15, 0, _UTC))

    def test_datetime_decode_param_offset(self):
        self.assertEqual(s.datetime().decode_param("2022-12-13T14:15:16+01:00"), datetime(2022, 12, 13, 13, 15, 16, 0, _UTC))

    def test_datetime_decode_json_missing_tz(self):
        self.assertEqual(s.datetime().decode_param("2020-10-11T12:13:14"), datetime(2020, 10, 11, 12, 13, 14, 0, _UTC))

    def test_datetime_decode_param_error(self):
        self._error(s.datetime().decode_param, "1425691090160")

    # ----- uuid ---------------

    def test_uuid_validate_type_success(self):
        s.uuid().validate(UUID("af327a12-c469-11e4-8e4f-af4f7c44473b"))

    def test_uuid_validate_type_error(self):
        self._error(s.uuid().validate, "this_is_not_a_uuid")

    def test_uuid_encode_json_success(self):
        val = "e9979b9c-c469-11e4-a0ad-37ff5ce3a7bf"
        self.assertEqual(s.uuid().encode_json(UUID(val)), val)

    def test_uuid_encode_json_error(self):
        self._error(s.uuid().encode_json, "definitely_not_a_uuid")

    def test_uuid_decode_json_success(self):
        val = "15a64a3a-c46a-11e4-b790-cb538a10de85"
        self.assertEqual(s.uuid().decode_json(val), UUID(val))

    def test_uuid_decode_json_error(self):
        self._error(s.uuid().decode_json, "this_is_not_a_uuid_either")

    def test_uuid_decode_param_success(self):
        val = "3629cf84-c46a-11e4-9b09-43a2f172bb56"
        self.assertEqual(s.uuid().decode_param(val), UUID(val))

    def test_uuid_decode_param_error(self):
        self._error(s.uuid().decode_param, "and_neither_is_this")


if __name__ == "__main__":
    unittest.main()
