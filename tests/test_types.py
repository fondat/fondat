import pytest

from fondat.types import str_enum


def test_str_enum_iterable():
    E = str_enum("E", ("a", "b", "c"))
    assert E.A == "a"
    assert E.B == "b"
    assert E.C == "c"


def test_str_enum_str_spaces():
    E = str_enum("E", "a b c")
    assert E.A == "a"
    assert E.B == "b"
    assert E.C == "c"


def test_str_enum_str_commas():
    E = str_enum("E", "a,b,c")
    assert E.A == "a"
    assert E.B == "b"
    assert E.C == "c"


def test_str_enum_str_mixed():
    E = str_enum("E", "a,,  b,c")
    assert E.A == "a"
    assert E.B == "b"
    assert E.C == "c"
