import pytest

from fondat.patch import merge_patch


def _test_patch(original, patch, result):
    assert merge_patch(original, patch) == result


def test_merge_patch_01():
    original = {"a": "b"}
    patch = {"a": "c"}
    result = {"a": "c"}
    _test_patch(original, patch, result)


def test_merge_patch_02():
    original = {"a": "b"}
    patch = {"b": "c"}
    result = {"a": "b", "b": "c"}
    _test_patch(original, patch, result)


def test_merge_patch_03():
    original = {"a": "b"}
    patch = {"a": None}
    result = {}
    _test_patch(original, patch, result)


def test_merge_patch_04():
    original = {"a": "b", "b": "c"}
    patch = {"a": None}
    result = {"b": "c"}
    _test_patch(original, patch, result)


def test_merge_patch_05():
    original = {"a": ["b"]}
    patch = {"a": "c"}
    result = {"a": "c"}
    _test_patch(original, patch, result)


def test_merge_patch_06():
    original = {"a": "c"}
    patch = {"a": ["b"]}
    result = {"a": ["b"]}
    _test_patch(original, patch, result)


def test_merge_patch_07():
    original = {"a": {"b": "c"}}
    patch = {"a": {"b": "d", "c": None}}
    result = {"a": {"b": "d"}}
    _test_patch(original, patch, result)


def test_merge_patch_08():
    original = {"a": [{"b": "c"}]}
    patch = {"a": [1]}
    result = {"a": [1]}
    _test_patch(original, patch, result)


def test_merge_patch_09():
    original = ["a", "b"]
    patch = ["c", "d"]
    result = ["c", "d"]
    _test_patch(original, patch, result)


def test_merge_patch_10():
    original = {"a": "b"}
    patch = ["c"]
    result = ["c"]
    _test_patch(original, patch, result)


def test_merge_patch_11():
    original = {"a": "foo"}
    patch = None
    result = None
    _test_patch(original, patch, result)


def test_merge_patch_12():
    original = {"a": "foo"}
    patch = "bar"
    result = "bar"
    _test_patch(original, patch, result)


def test_merge_patch_13():
    original = {"e": None}
    patch = {"a": 1}
    result = {"e": None, "a": 1}
    _test_patch(original, patch, result)


def test_merge_patch_14():
    original = [1, 2]
    patch = {"a": "b", "c": None}
    result = {"a": "b"}
    _test_patch(original, patch, result)


def test_merge_patch_15():
    original = {}
    patch = {"a": {"bb": {"ccc": None}}}
    result = {"a": {"bb": {}}}
    _test_patch(original, patch, result)
