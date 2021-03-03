import pytest

import fondat.csv
import io

from dataclasses import make_dataclass
from datetime import date
from decimal import Decimal


def test_currency_floating_prefix():
    encode = fondat.csv.currency_encoder(prefix="$")
    assert encode(123) == "$123"
    assert encode(123.45) == "$123.45"
    assert encode(678.90) == "$678.9"
    assert encode(Decimal("678.90")) == "$678.9"


def test_currency_floating_suffix():
    encode = fondat.csv.currency_encoder(suffix=" лв")
    assert encode(123) == "123 лв"
    assert encode(123.45) == "123.45 лв"
    assert encode(678.90) == "678.9 лв"
    assert encode(Decimal("678.90")) == "678.9 лв"


def test_currency_fixed2_prefix():
    encode = fondat.csv.currency_encoder(prefix="$", precision=2)
    assert encode(123) == "$123.00"
    assert encode(123.45) == "$123.45"
    assert encode(678.9012345) == "$678.90"
    assert encode(Decimal("678.9012345")) == "$678.90"


def test_currency_fixed2_suffix():
    encode = fondat.csv.currency_encoder(suffix=" лв", precision=2)
    assert encode(123) == "123.00 лв"
    assert encode(123.45) == "123.45 лв"
    assert encode(678.9012345) == "678.90 лв"
    assert encode(Decimal("678.9012345")) == "678.90 лв"


def test_currency_fixed0_prefix():
    encode = fondat.csv.currency_encoder(prefix="$", precision=0)
    assert encode(123) == "$123"
    assert encode(123.45) == "$123"
    assert encode(678.90) == "$679"
    assert encode(Decimal("678.90")) == "$679"


def test_currency_fixed0_suffix():
    encode = fondat.csv.currency_encoder(suffix=" лв", precision=0)
    assert encode(123) == "123 лв"
    assert encode(123.45) == "123 лв"
    assert encode(678.90) == "679 лв"
    assert encode(Decimal("678.90")) == "679 лв"


def test_percentage_float():
    encode = fondat.csv.percent_encoder()
    assert encode(0.123) == "12.3%"
    assert encode(0.001234) == "0.1234%"
    assert encode(1.234) == "123.4%"


def test_percentage_fixed2():
    encode = fondat.csv.percent_encoder(precision=2)
    assert encode(0.123) == "12.30%"
    assert encode(0.001234) == "0.12%"
    assert encode(1.234) == "123.40%"


def test_percentage_fixed0():
    encode = fondat.csv.percent_encoder(precision=0)
    assert encode(0.123) == "12%"
    assert encode(0.001234) == "0%"
    assert encode(1.235) == "124%"


def test_number_float():
    encode = fondat.csv.number_encoder()
    assert encode(123) == "123"
    assert encode(123.45) == "123.45"
    assert encode(678.90) == "678.9"
    assert encode(Decimal("678.90")) == "678.9"


def test_number_fixed2():
    encode = fondat.csv.number_encoder(precision=2)
    assert encode(123) == "123.00"
    assert encode(123.45) == "123.45"
    assert encode(678.90) == "678.90"
    assert encode(Decimal("678.90")) == "678.90"


def test_number_fixed2():
    encode = fondat.csv.number_encoder(precision=0)
    assert encode(123) == "123"
    assert encode(123.45) == "123"
    assert encode(678.90) == "679"
    assert encode(Decimal("678.90")) == "679"


def test_dataclass():
    DC = make_dataclass("DC", (("x", int), ("y", float), ("z", date)))
    with io.StringIO() as s:
        dcw = fondat.csv.DataclassWriter(s, DC, {"x": fondat.csv.number_encoder(precision=2)})
        dcw.writeheader()
        assert s.getvalue() == "x,y,z\r\n"
        s.seek(0)
        s.truncate()
        dcw.writerow(DC(1, 2.34, date(2021, 3, 2)))
        assert s.getvalue() == "1.00,2.34,2021-03-02\r\n"
