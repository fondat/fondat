"""Roax document patch module."""

import collections.abc
import copy


def merge_patch(target, patch):
    """Apply JSON merge patch document, per RFC 7386."""
    if isinstance(patch, collections.abc.Mapping):
        if not isinstance(target, collections.abc.Mapping):
            target = {}
        for name, value in patch.items():
            if value is None:
                target.pop(name, None)
            else:
                target[name] = merge_patch(target.get(name), value)
        return target
    else:
        return copy.deepcopy(patch)
