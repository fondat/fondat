import pytest

import fondat.enum


def test_str_enum_iterable():
    E = fondat.enum.str_enum("E", ("a", "b", "c"))
    assert E.A == "a"
    assert E.B == "b"
    assert E.C == "c"


def test_str_enum_str_spaces():
    E = fondat.enum.str_enum("E", "a b c")
    assert E.A == "a"
    assert E.B == "b"
    assert E.C == "c"


def test_str_enum_str_commas():
    E = fondat.enum.str_enum("E", "a,b,c")
    assert E.A == "a"
    assert E.B == "b"
    assert E.C == "c"


def test_str_enum_str_mixed():
    E = fondat.enum.str_enum("E", "a,,  b,c")
    assert E.A == "a"
    assert E.B == "b"
    assert E.C == "c"
