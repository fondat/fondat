from dataclasses import field, make_dataclass
from fondat.patch import json_merge_diff, json_merge_patch
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


def test_merge_patch_dc_flat():
    DC = make_dataclass("DC", (("a", str),))
    assert json_merge_patch(type=DC, value=DC(a="b"), patch={"a": "c"}) == DC(a="c")


def test_merge_patch_dc_flat_optional_union():
    DC = make_dataclass("DC", (("a", str), ("b", Optional[str], field(default=None))))
    assert json_merge_patch(type=DC, value=DC(a="b"), patch={"b": "c"}) == DC(a="b", b="c")


def test_merge_patch_dc_flat_optional_union_type():
    DC = make_dataclass("DC", (("a", str), ("b", str | None, field(default=None))))
    assert json_merge_patch(type=DC, value=DC(a="b"), patch={"b": "c"}) == DC(a="b", b="c")


def test_merge_patch_dc_flat_delete():
    DC = make_dataclass("DC", (("a", str), ("b", str | None, field(default=None))))
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
    DC2 = make_dataclass("DC2", (("b", str | None),))
    DC1 = make_dataclass("DC1", (("a", DC2),))
    assert json_merge_patch(type=DC1, value=DC1(a=DC2(b="c")), patch={"a": {"b": None}}) == DC1(
        a=DC2(b=None)
    )


def test_merge_diff_rfc_7386_test_cases():
    assert json_merge_diff(old={"a": "b"}, new={"a": "c"}) == {"a": "c"}
    assert json_merge_diff(old={"a": "b"}, new={"a": "b", "b": "c"}) == {"b": "c"}
    assert json_merge_diff(old={"a": "b"}, new={}) == {"a": None}
    assert json_merge_diff(old={"a": "b", "b": "c"}, new={"b": "c"}) == {"a": None}
    assert json_merge_diff(old={"a": ["b"]}, new={"a": "c"}) == {"a": "c"}
    assert json_merge_diff(old={"a": "c"}, new={"a": ["b"]}) == {"a": ["b"]}
    assert json_merge_diff(old={"a": {"b": "c"}}, new={"a": {"b": "d"}}) == {"a": {"b": "d"}}
    assert json_merge_diff(old={"a": [{"b": "c"}]}, new={"a": [1]}) == {"a": [1]}
    assert json_merge_diff(old=["a", "b"], new=["c", "d"]) == ["c", "d"]
    assert json_merge_diff(old={"a": "b"}, new=["c"]) == ["c"]
    assert json_merge_diff(old={"a": "foo"}, new=None) == None
    assert json_merge_diff(old={"a": "foo"}, new="bar") == "bar"
    assert json_merge_diff(old={"e": None}, new={"e": None, "a": 1}) == {"a": 1}
    assert json_merge_diff(old={}, new={"a": {"bb": {}}}) == {"a": {"bb": {}}}
