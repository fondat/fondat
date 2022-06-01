import pytest

from fondat.lazy import LazyMap, lazy, lazy_import, lazy_import_attr


@lazy
def lz1():
    return "lz1_value"


@lazy
def lz2():
    return "lz2_value"


def not_decorated():
    return "bacon"


def test_lazymap():
    map = LazyMap(
        {
            "a": "a_value",
            "z1": lz1,
        }
    )
    assert len(map) == 2
    assert "a" in map
    assert "z1" in map
    assert "z2" not in map
    assert map["a"] == "a_value"
    assert map["z1"] == "lz1_value"
    map["a"] = "a1"
    assert map["a"] == "a1"
    map["z1"] = "lz1v"
    assert map["z1"] == "lz1v"
    map["z1"] = lz1
    assert map["z1"] == "lz1_value"
    map["z1"] = not_decorated
    assert map["z1"] == not_decorated
    map["z2"] = lz2
    assert map["z2"] == "lz2_value"
    assert len(map) == 3
    del map["z1"]
    assert len(map) == 2
    del map["z2"]
    assert len(map) == 1
    with pytest.raises(KeyError):
        del map["nobody_home"]
    del map["a"]
    assert len(map) == 0


def test_lazy_import():
    m = lazy_import("csv")
    m = m()
    import csv

    assert m is csv


def test_lazy_import_attr():
    r = lazy_import_attr("csv", "reader")
    r = r()
    import csv

    assert r is csv.reader
