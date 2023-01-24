import fondat.csv
import fondat.stream
import pytest

from dataclasses import make_dataclass
from datetime import date
from decimal import Decimal
from fondat.csv import CurrencyCodec, DataclassCodec, FixedCodec, PercentCodec, TypedDictCodec
from typing import Optional, TypedDict


def _test_codec(codec, python_value, string_value):
    assert codec.encode(python_value) == string_value
    assert codec.decode(string_value) == python_value


def test_int_currency_floating_prefix():
    codec = CurrencyCodec(int, prefix="$")
    _test_codec(codec, 123, "$123")


def test_float_currency_floating_prefix():
    codec = CurrencyCodec(float, prefix="$")
    _test_codec(codec, 123.45, "$123.45")
    _test_codec(codec, 678.9, "$678.9")


def test_decimal_currency_floating_prefix():
    codec = CurrencyCodec(Decimal, prefix="$")
    _test_codec(codec, Decimal("678.90"), "$678.9")


def test_int_currency_suffix():
    codec = CurrencyCodec(int, suffix=" лв")
    _test_codec(codec, 123, "123 лв")


def test_float_currency_floating_suffix():
    codec = CurrencyCodec(float, suffix=" лв")
    _test_codec(codec, 123.45, "123.45 лв")
    _test_codec(codec, 678.9, "678.9 лв")


def test_decimal_currency_floating_suffix():
    codec = CurrencyCodec(Decimal, suffix=" лв")
    _test_codec(codec, Decimal("678.90"), "678.9 лв")


def test_float_currency_fixed2_prefix():
    codec = CurrencyCodec(float, prefix="$", precision=2)
    _test_codec(codec, 123.45, "$123.45")
    _test_codec(codec, 678.9, "$678.90")
    assert codec.encode(678.9012345) == "$678.90"


def test_decimal_currency_prec2_prefix():
    codec = CurrencyCodec(Decimal, prefix="$", precision=2)
    _test_codec(codec, Decimal("678.9"), "$678.90")
    assert codec.encode(Decimal("678.9012345")) == "$678.90"


def test_float_currency_prec2_suffix():
    codec = CurrencyCodec(float, suffix=" лв", precision=2)
    _test_codec(codec, 123.45, "123.45 лв")
    assert codec.encode(678.9012345) == "678.90 лв"


def test_decimal_currency_prec2_suffix():
    codec = CurrencyCodec(Decimal, suffix=" лв", precision=2)
    _test_codec(codec, Decimal("678.9"), "678.90 лв")
    assert codec.encode(Decimal("678.9012345")) == "678.90 лв"


def test_int_currency_prec0_prefix():
    codec = CurrencyCodec(int, prefix="$", precision=0)
    _test_codec(codec, 123, "$123")


def test_float_currency_prec0_prefix():
    codec = CurrencyCodec(float, prefix="$", precision=0)
    _test_codec(codec, 123.0, "$123")
    assert codec.encode(123.45) == "$123"
    assert codec.encode(678.90) == "$679"


def test_decimal_currency_prec0_prefix():
    codec = CurrencyCodec(Decimal, prefix="$", precision=0)
    _test_codec(codec, Decimal("123.0"), "$123")
    assert codec.encode(Decimal("678.90")) == "$679"


def test_int_currency_prec0_suffix():
    codec = CurrencyCodec(int, suffix=" лв", precision=0)
    _test_codec(codec, 123, "123 лв")


def test_float_currency_prec0_suffix():
    codec = CurrencyCodec(float, suffix=" лв", precision=0)
    _test_codec(codec, 123.0, "123 лв")
    assert codec.encode(123.45) == "123 лв"
    assert codec.encode(678.90) == "679 лв"


def test_decimal_currency_prec0_suffix():
    codec = CurrencyCodec(Decimal, suffix=" лв", precision=0)
    _test_codec(codec, Decimal("123.0"), "123 лв")
    assert codec.encode(Decimal("678.90")) == "679 лв"


def test_encode_int_currency_none():
    codec = CurrencyCodec(int)
    assert codec.encode(None) == ""


def test_percentage_prec2():
    codec = PercentCodec(float, 2)
    _test_codec(codec, 0.123, "12.30%")
    _test_codec(codec, 1.234, "123.40%")
    assert codec.encode(0.001234) == "0.12%"


def test_percentage_prec0():
    codec = PercentCodec(float, 0)
    _test_codec(codec, 0.12, "12%")
    assert codec.encode(0.123) == "12%"
    assert codec.encode(0.001234) == "0%"
    assert codec.encode(1.235) == "124%"


def test_encode_float_percentage_none():
    codec = PercentCodec(float, 2)
    assert codec.encode(None) == ""


def test_float_fixed_prec2():
    codec = FixedCodec(float, 2)
    _test_codec(codec, 123.45, "123.45")
    _test_codec(codec, 678.9, "678.90")


def test_decimal_fixed_prec2():
    codec = FixedCodec(Decimal, 2)
    _test_codec(codec, Decimal("678.90"), "678.90")


def test_float_fixed_prec0():
    codec = FixedCodec(Decimal, 0)
    _test_codec(codec, 123.00, "123")
    assert codec.encode(123.45) == "123"
    assert codec.encode(678.90) == "679"


def test_decimal_fixed_prec0():
    codec = FixedCodec(Decimal, 0)
    _test_codec(codec, Decimal("123.00"), "123")
    assert codec.encode(Decimal("678.90")) == "679"


def test_dc_encode():
    DC = make_dataclass("DC", (("x", int), ("y", float), ("z", date)))
    codecs = {"y": PercentCodec(int, 2)}
    dcc = DataclassCodec(dataclass=DC, codecs=codecs)
    assert dcc.encode(DC(x=1, y=0.123, z=date(2021, 3, 2))) == ["1", "12.30%", "2021-03-02"]


def test_dc_encode_none():
    DC = make_dataclass("DC", (("x", Optional[str]), ("y", str)))
    dcc = DataclassCodec(dataclass=DC)
    assert dcc.encode(DC(x=None, y="")) == ["", ""]


def test_dc_decode_simple():
    DC = make_dataclass("DC", (("x", int), ("y", float), ("z", date)))
    dcc = DataclassCodec(dataclass=DC)
    assert dcc.decode(["1", "12.35", "2021-03-02"]) == DC(x=1, y=12.35, z=date(2021, 3, 2))


def test_dc_decode_reordered():
    DC = make_dataclass("DC", (("x", int), ("y", float), ("z", date)))
    dcc = DataclassCodec(dataclass=DC, columns=["z", "x", "y"])
    assert dcc.decode(["2021-03-02", "1", "12.35"]) == DC(x=1, y=12.35, z=date(2021, 3, 2))


def test_dc_decode_codec():
    DC = make_dataclass("DC", (("x", str), ("y", float)))
    codecs = {"y": CurrencyCodec(float, "$")}
    dcc = DataclassCodec(dataclass=DC, codecs=codecs)
    assert dcc.decode(["foo", "$123.45"]) == DC(x="foo", y=123.45)


def test_dc_decode_ignore():
    DC = make_dataclass("DC", (("a", str), ("b", str)))
    dcc = DataclassCodec(dataclass=DC, columns=["a", "c", "b", "d"])
    assert dcc.decode(["a", "c", "b", "d"]) == DC(a="a", b="b")


def test_dc_decode_empty_columns():
    DC = make_dataclass("DC", (("a", Optional[str]), ("b", str)))
    dcc = DataclassCodec(dataclass=DC, columns=["a", "b", "c"])
    assert dcc.decode(["", ""]) == DC(a=None, b="")


def test_td_encode():
    TD = TypedDict("TD", {"x": int, "y": float, "z": date})
    codecs = {"y": PercentCodec(int, 2)}
    tdc = TypedDictCodec(typeddict=TD, codecs=codecs)
    assert tdc.encode({"x": 1, "y": 0.123, "z": date(2021, 3, 2)}) == [
        "1",
        "12.30%",
        "2021-03-02",
    ]


def test_td_encode():
    TD = TypedDict("TD", {"x": int, "y": float, "z": date})
    codecs = {"y": PercentCodec(int, 2)}
    tdc = TypedDictCodec(typeddict=TD, codecs=codecs)
    assert tdc.encode({"x": 1, "y": 0.123, "z": date(2021, 3, 2)}) == [
        "1",
        "12.30%",
        "2021-03-02",
    ]


def test_td_encode_missing_none():
    TD = TypedDict("TD", {"x": Optional[str], "y": str}, total=False)
    tdc = TypedDictCodec(typeddict=TD)
    assert tdc.encode({"y": ""}) == ["", ""]


def test_td_columns():
    TD = TypedDict("TD", {"x": str, "y": str})
    tdc = TypedDictCodec(typeddict=TD)
    assert tdc.columns == ("x", "y")


def test_dc_decode_invalid_empty_column():
    DC = make_dataclass("DC", (("a", int), ("b", float)))
    dcc = DataclassCodec(dataclass=DC, columns=["a", "b"])
    with pytest.raises(ValueError):
        dcc.decode(["a", ""])


async def test_stream():
    class AIter:
        def __init__(self, rows):
            self.rows = iter(rows)

        def __aiter__(self):
            return self

        async def __anext__(self):
            try:
                return self.rows.__next__()
            except StopIteration:
                raise StopAsyncIteration

    csv = [["id", "name"], ["1", "Joe"], ["2", "Jane"], ["3", "Marshall"], ["4", "Hudson"]]

    aiter = AIter(csv)
    data = await fondat.stream.Reader(fondat.csv.CSVStream(aiter)).read()
    read = [row async for row in fondat.csv.CSVReader(fondat.stream.BytesStream(data))]
    assert read == csv
