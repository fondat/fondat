"""Roax document patch module."""

from collections.abc import Mapping
from copy import deepcopy


def merge_patch(target, patch):
    """Apply JSON merge patch document, per RFC 7386."""
    if isinstance(patch, Mapping):
        if not isinstance(target, Mapping):
            target = {}
        for name, value in patch.items():
            if value is None:
                target.pop(name, None)
            else:
                target[name] = merge_patch(target.get(name), value)
        return target
    else:
        return deepcopy(patch)
