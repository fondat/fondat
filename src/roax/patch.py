"""Roax document patch module."""

# Copyright © 2018 Paul Bryan.
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from collections import Mapping
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
