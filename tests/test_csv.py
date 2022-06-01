import pytest

from dataclasses import make_dataclass
from datetime import date
from decimal import Decimal
from fondat.csv import (
    currency_codec,
    dataclass_codec,
    fixed_codec,
    percent_codec,
    typeddict_codec,
)
from typing import Optional, TypedDict


def _test_codec(codec, python_value, string_value):
    assert codec.encode(python_value) == string_value
    assert codec.decode(string_value) == python_value


def test_int_currency_floating_prefix():
    codec = currency_codec(int, prefix="$")
    _test_codec(codec, 123, "$123")


def test_float_currency_floating_prefix():
    codec = currency_codec(float, prefix="$")
    _test_codec(codec, 123.45, "$123.45")
    _test_codec(codec, 678.9, "$678.9")


def test_decimal_currency_floating_prefix():
    codec = currency_codec(Decimal, prefix="$")
    _test_codec(codec, Decimal("678.90"), "$678.9")


def test_int_currency_suffix():
    codec = currency_codec(int, suffix=" лв")
    _test_codec(codec, 123, "123 лв")


def test_float_currency_floating_suffix():
    codec = currency_codec(float, suffix=" лв")
    _test_codec(codec, 123.45, "123.45 лв")
    _test_codec(codec, 678.9, "678.9 лв")


def test_decimal_currency_floating_suffix():
    codec = currency_codec(Decimal, suffix=" лв")
    _test_codec(codec, Decimal("678.90"), "678.9 лв")


def test_float_currency_fixed2_prefix():
    codec = currency_codec(float, prefix="$", precision=2)
    _test_codec(codec, 123.45, "$123.45")
    _test_codec(codec, 678.9, "$678.90")
    assert codec.encode(678.9012345) == "$678.90"


def test_decimal_currency_prec2_prefix():
    codec = currency_codec(Decimal, prefix="$", precision=2)
    _test_codec(codec, Decimal("678.9"), "$678.90")
    assert codec.encode(Decimal("678.9012345")) == "$678.90"


def test_float_currency_prec2_suffix():
    codec = currency_codec(float, suffix=" лв", precision=2)
    _test_codec(codec, 123.45, "123.45 лв")
    assert codec.encode(678.9012345) == "678.90 лв"


def test_decimal_currency_prec2_suffix():
    codec = currency_codec(Decimal, suffix=" лв", precision=2)
    _test_codec(codec, Decimal("678.9"), "678.90 лв")
    assert codec.encode(Decimal("678.9012345")) == "678.90 лв"


def test_int_currency_prec0_prefix():
    codec = currency_codec(int, prefix="$", precision=0)
    _test_codec(codec, 123, "$123")


def test_float_currency_prec0_prefix():
    codec = currency_codec(float, prefix="$", precision=0)
    _test_codec(codec, 123.0, "$123")
    assert codec.encode(123.45) == "$123"
    assert codec.encode(678.90) == "$679"


def test_decimal_currency_prec0_prefix():
    codec = currency_codec(Decimal, prefix="$", precision=0)
    _test_codec(codec, Decimal("123.0"), "$123")
    assert codec.encode(Decimal("678.90")) == "$679"


def test_int_currency_prec0_suffix():
    codec = currency_codec(int, suffix=" лв", precision=0)
    _test_codec(codec, 123, "123 лв")


def test_float_currency_prec0_suffix():
    codec = currency_codec(float, suffix=" лв", precision=0)
    _test_codec(codec, 123.0, "123 лв")
    assert codec.encode(123.45) == "123 лв"
    assert codec.encode(678.90) == "679 лв"


def test_decimal_currency_prec0_suffix():
    codec = currency_codec(Decimal, suffix=" лв", precision=0)
    _test_codec(codec, Decimal("123.0"), "123 лв")
    assert codec.encode(Decimal("678.90")) == "679 лв"


def test_percentage_prec2():
    codec = percent_codec(float, 2)
    _test_codec(codec, 0.123, "12.30%")
    _test_codec(codec, 1.234, "123.40%")
    assert codec.encode(0.001234) == "0.12%"


def test_percentage_prec0():
    codec = percent_codec(float, 0)
    _test_codec(codec, 0.12, "12%")
    assert codec.encode(0.123) == "12%"
    assert codec.encode(0.001234) == "0%"
    assert codec.encode(1.235) == "124%"


def test_float_fixed_prec2():
    codec = fixed_codec(float, 2)
    _test_codec(codec, 123.45, "123.45")
    _test_codec(codec, 678.9, "678.90")


def test_decimal_fixed_prec2():
    codec = fixed_codec(Decimal, 2)
    _test_codec(codec, Decimal("678.90"), "678.90")


def test_float_fixed_prec0():
    codec = fixed_codec(Decimal, 0)
    _test_codec(codec, 123.00, "123")
    assert codec.encode(123.45) == "123"
    assert codec.encode(678.90) == "679"


def test_decimal_fixed_prec0():
    codec = fixed_codec(Decimal, 0)
    _test_codec(codec, Decimal("123.00"), "123")
    assert codec.encode(Decimal("678.90")) == "679"


def test_dc_encode():
    DC = make_dataclass("DC", (("x", int), ("y", float), ("z", date)))
    codecs = {"y": percent_codec(int, 2)}
    dcc = dataclass_codec(dataclass=DC, codecs=codecs)
    assert dcc.encode(DC(x=1, y=0.123, z=date(2021, 3, 2))) == ["1", "12.30%", "2021-03-02"]


def test_dc_encode_none():
    DC = make_dataclass("DC", (("x", Optional[str]), ("y", str)))
    dcc = dataclass_codec(dataclass=DC)
    assert dcc.encode(DC(x=None, y="")) == ["", ""]


def test_dc_decode_simple():
    DC = make_dataclass("DC", (("x", int), ("y", float), ("z", date)))
    dcc = dataclass_codec(dataclass=DC)
    assert dcc.decode(["1", "12.35", "2021-03-02"]) == DC(x=1, y=12.35, z=date(2021, 3, 2))


def test_dc_decode_reordered():
    DC = make_dataclass("DC", (("x", int), ("y", float), ("z", date)))
    dcc = dataclass_codec(dataclass=DC, columns=["z", "x", "y"])
    assert dcc.decode(["2021-03-02", "1", "12.35"]) == DC(x=1, y=12.35, z=date(2021, 3, 2))


def test_dc_decode_codec():
    DC = make_dataclass("DC", (("x", str), ("y", float)))
    codecs = {"y": currency_codec(float, "$")}
    dcc = dataclass_codec(dataclass=DC, codecs=codecs)
    assert dcc.decode(["foo", "$123.45"]) == DC(x="foo", y=123.45)


def test_dc_decode_ignore():
    DC = make_dataclass("DC", (("a", str), ("b", str)))
    dcc = dataclass_codec(dataclass=DC, columns=["a", "c", "b", "d"])
    assert dcc.decode(["a", "c", "b", "d"]) == DC(a="a", b="b")


def test_dc_decode_empty_columns():
    DC = make_dataclass("DC", (("a", Optional[str]), ("b", str)))
    dcc = dataclass_codec(dataclass=DC, columns=["a", "b", "c"])
    assert dcc.decode(["", ""]) == DC(a=None, b="")


def test_dc_columns():
    DC = make_dataclass("DC", (("a", str), ("b", str)))
    dcc = dataclass_codec(dataclass=DC)
    assert dcc.columns == ("a", "b")


def test_td_encode():
    TD = TypedDict("TD", {"x": int, "y": float, "z": date})
    codecs = {"y": percent_codec(int, 2)}
    tdc = typeddict_codec(typeddict=TD, codecs=codecs)
    assert tdc.encode({"x": 1, "y": 0.123, "z": date(2021, 3, 2)}) == [
        "1",
        "12.30%",
        "2021-03-02",
    ]


def test_td_encode():
    TD = TypedDict("TD", {"x": int, "y": float, "z": date})
    codecs = {"y": percent_codec(int, 2)}
    tdc = typeddict_codec(typeddict=TD, codecs=codecs)
    assert tdc.encode({"x": 1, "y": 0.123, "z": date(2021, 3, 2)}) == [
        "1",
        "12.30%",
        "2021-03-02",
    ]


def test_td_encode_missing_none():
    TD = TypedDict("TD", {"x": Optional[str], "y": str}, total=False)
    tdc = typeddict_codec(typeddict=TD)
    assert tdc.encode({"y": ""}) == ["", ""]


def test_td_columns():
    TD = TypedDict("TD", {"x": str, "y": str})
    tdc = typeddict_codec(typeddict=TD)
    assert tdc.columns == ("x", "y")


def test_dc_decode_invalid_empty_column():
    DC = make_dataclass("DC", (("a", int), ("b", float)))
    dcc = dataclass_codec(dataclass=DC, columns=["a", "b"])
    with pytest.raises(ValueError):
        dcc.decode(["a", ""])
