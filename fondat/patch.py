"""Document partial modification (patch) module."""

import collections.abc
import copy

from fondat.codec import get_codec, JSON
from typing import Any


def _json_merge_patch(target, patch):
    if isinstance(patch, collections.abc.Mapping):
        if not isinstance(target, collections.abc.Mapping):
            target = {}
        for key, value in patch.items():
            if value is None:
                target.pop(key, None)
            else:
                target[key] = _json_merge_patch(target.get(key), value)
        return target
    else:
        return copy.deepcopy(patch)


def json_merge_patch(*, value: Any, type: type = Any, patch: Any) -> Any:
    """
    Return a new patched value, the result of applying a JSON Merge Patch document to the JSON
    representation of the specified value, per RFC 7386.

    Parameters:
    • value: value to be patched
    • type: type of value to be patched
    • patch: JSON Merge Patch document to apply to value
    """
    codec = get_codec(JSON, type)
    return codec.decode(_json_merge_patch(codec.encode(value), patch))
