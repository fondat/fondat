import pytest


from dataclasses import make_dataclass, field
from fondat.patch import json_merge_patch
from typing import Optional


def test_merge_patch_rfc_7386_test_cases():
    assert json_merge_patch(value={"a": "b"}, patch={"a": "c"}) == {"a": "c"}
    assert json_merge_patch(value={"a": "b"}, patch={"b": "c"}) == {"a": "b", "b": "c"}
    assert json_merge_patch(value={"a": "b"}, patch={"a": None}) == {}
    assert json_merge_patch(value={"a": "b", "b": "c"}, patch={"a": None}) == {"b": "c"}
    assert json_merge_patch(value={"a": ["b"]}, patch={"a": "c"}) == {"a": "c"}
    assert json_merge_patch(value={"a": "c"}, patch={"a": ["b"]}) == {"a": ["b"]}
    assert json_merge_patch(value={"a": {"b": "c"}}, patch={"a": {"b": "d", "c": None}}) == {
        "a": {"b": "d"}
    }
    assert json_merge_patch(value={"a": [{"b": "c"}]}, patch={"a": [1]}) == {"a": [1]}
    assert json_merge_patch(value=["a", "b"], patch=["c", "d"]) == ["c", "d"]
    assert json_merge_patch(value={"a": "b"}, patch=["c"]) == ["c"]
    assert json_merge_patch(value={"a": "foo"}, patch=None) == None
    assert json_merge_patch(value={"a": "foo"}, patch="bar") == "bar"
    assert json_merge_patch(value={"e": None}, patch={"a": 1}) == {"e": None, "a": 1}
    assert json_merge_patch(value=[1, 2], patch={"a": "b", "c": None}) == {"a": "b"}
    assert json_merge_patch(value={}, patch={"a": {"bb": {"ccc": None}}}) == {"a": {"bb": {}}}


def test_merge_patch_dc_flat_success():
    DC = make_dataclass("DC", (("a", str),))
    assert json_merge_patch(type=DC, value=DC(a="b"), patch={"a": "c"}) == DC(a="c")


def test_merge_patch_dc_flat_error():
    DC = make_dataclass("DC", (("a", str),))
    with pytest.raises(TypeError):
        json_merge_patch(type=DC, value=DC(a="b"), patch={"a": 1})


def test_merge_patch_dc_flat_optional():
    DC = make_dataclass("DC", (("a", str), ("b", Optional[str], field(default=None))))
    assert json_merge_patch(type=DC, value=DC(a="b"), patch={"b": "c"}) == DC(a="b", b="c")


def test_merge_patch_dc_flat_delete():
    DC = make_dataclass("DC", (("a", str), ("b", Optional[str], field(default=None))))
    assert json_merge_patch(type=DC, value=DC(a="b", b="c"), patch={"b": None}) == DC(
        a="b", b=None
    )


def test_merge_patch_dc_nested_add():
    DC2 = make_dataclass("DC2", (("b", str),))
    DC1 = make_dataclass("DC1", (("a", DC2),))
    assert json_merge_patch(
        type=DC1, value=DC1(a=DC2(b="c")), patch={"a": {"b": "d", "c": None}}
    ) == DC1(a=DC2(b="d"))


def test_merge_patch_dc_nested_delete():
    DC2 = make_dataclass("DC2", (("b", Optional[str]),))
    DC1 = make_dataclass("DC1", (("a", DC2),))
    assert json_merge_patch(type=DC1, value=DC1(a=DC2(b="c")), patch={"a": {"b": None}}) == DC1(
        a=DC2(b=None)
    )
