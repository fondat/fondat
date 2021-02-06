"""Document partial modification (patch) module."""

import collections.abc
import copy


def merge_patch(target, patch):
    """
    Apply JSON Merge Patch document, per RFC 7386.

    Parameters:
    • target: JSON value to mutate
    • patch: patch document to apply
    """
    if isinstance(patch, collections.abc.Mapping):
        if not isinstance(target, collections.abc.Mapping):
            target = {}
        for key, value in patch.items():
            if value is None:
                target.pop(key, None)
            else:
                target[key] = merge_patch(target.get(key), value)
        return target
    else:
        return copy.deepcopy(patch)
