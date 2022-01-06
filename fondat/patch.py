"""Document partial modification (patch) module."""

import collections.abc

from copy import deepcopy
from fondat.codec import get_codec, JSON
from typing import Any


def _json_merge_patch(target, patch):
    if isinstance(patch, collections.abc.Mapping):
        if not isinstance(target, collections.abc.Mapping):
            target = {}
        for key, value in patch.items():
            if value is None:
                target.pop(key, None)
            else:  # recursive
                target[key] = _json_merge_patch(target.get(key), value)
        return target
    else:
        return deepcopy(patch)


def json_merge_patch(*, value: Any, type: type = Any, patch: Any) -> Any:
    """
    Return the result of applying a JSON Merge Patch document to the JSON representation of
    a specified value, per RFC 7386.

    Parameters:
    • value: value to be patched
    • type: type of value to be patched
    • patch: JSON Merge Patch document to apply to value
    """
    codec = get_codec(JSON, type)
    result = codec.decode(_json_merge_patch(codec.encode(value), patch))
    return result


def _json_merge_diff(old: Any, new: Any) -> Any:
    if isinstance(old, collections.abc.Mapping) and isinstance(new, collections.abc.Mapping):
        diff = {}
        for key in new:
            if key in old:
                old_value = old[key]
                new_value = new[key]
                if new_value != old_value:  # recursive
                    if (d := _json_merge_diff(old_value, new_value)) != {}:
                        diff[key] = d
            else:
                diff[key] = new[key]
        for key in old:
            if key not in new:
                diff[key] = None
        return diff
    else:
        return new


def json_merge_diff(*, old: Any, new: Any, type: type = Any) -> Any:
    """
    Return a JSON Merge Patch document per RFC 7386, the result of comparing the JSON
    representations of specified old and new values.
    """
    codec = get_codec(JSON, type)
    return _json_merge_diff(codec.encode(old), codec.encode(new))
