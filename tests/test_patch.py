
import unittest

from roax.patch import merge_patch


class TestPatch(unittest.TestCase):

    def _test_patch(self, original, patch, result):
        self.assertEqual(merge_patch(original, patch), result)

    def test_merge_patch_01(self):
        original = {"a": "b"}
        patch = {"a": "c"}
        result = {"a": "c"}
        self._test_patch(original, patch, result)

    def test_merge_patch_02(self):
        original = {"a": "b"}
        patch = {"b": "c"}
        result = {"a": "b", "b": "c"}
        self._test_patch(original, patch, result)

    def test_merge_patch_03(self):
        original = {"a": "b"}
        patch = {"a": None}
        result = {}
        self._test_patch(original, patch, result)

    def test_merge_patch_04(self):
        original = {"a": "b", "b": "c"}
        patch = {"a": None}
        result = {"b": "c"}
        self._test_patch(original, patch, result)

    def test_merge_patch_05(self):
        original = {"a": ["b"]}
        patch = {"a": "c"}
        result = {"a": "c"}
        self._test_patch(original, patch, result)

    def test_merge_patch_06(self):
        original = {"a": "c"}
        patch = {"a": ["b"]}
        result = {"a": ["b"]}
        self._test_patch(original, patch, result)

    def test_merge_patch_07(self):
        original = {"a": {"b": "c"}}
        patch = {"a": {"b": "d", "c": None}}
        result = {"a": {"b": "d"}}
        self._test_patch(original, patch, result)

    def test_merge_patch_08(self):
        original = {"a": [{"b": "c"}]} 
        patch = {"a": [1]}
        result = {"a": [1]}
        self._test_patch(original, patch, result)

    def test_merge_patch_09(self):
        original = ["a", "b"]
        patch = ["c", "d"]
        result = ["c", "d"]
        self._test_patch(original, patch, result)

    def test_merge_patch_10(self):
        original = {"a": "b"}
        patch = ["c"]
        result = ["c"]
        self._test_patch(original, patch, result)

    def test_merge_patch_11(self):
        original = {"a": "foo"}
        patch = None
        result = None
        self._test_patch(original, patch, result)

    def test_merge_patch_12(self):
        original = {"a":"foo"}
        patch = "bar"
        result = "bar"
        self._test_patch(original, patch, result)

    def test_merge_patch_13(self):
        original = {"e": None} 
        patch = {"a": 1}
        result = {"e": None, "a": 1}
        self._test_patch(original, patch, result)

    def test_merge_patch_14(self):
        original = [1, 2]
        patch = {"a": "b", "c": None}
        result = {"a": "b"}
        self._test_patch(original, patch, result)

    def test_merge_patch_15(self):
        original = {}
        patch = {"a": {"bb": {"ccc": None}}}
        result = {"a": {"bb": {}}}
        self._test_patch(original, patch, result)
