"""Module to manage a stack of context values."""

# Copyright © 2017 Paul Bryan.
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import threading

from collections import Mapping
from contextlib import contextmanager

_local = threading.local()

_local.stack = []

def stack():
    """Return the current context stack."""
    return _local.stack

@contextmanager
def context(value):
    """Context manager that pushes a value onto the context stack."""
    s = stack()
    s.append(value)
    pos = len(s) - 1
    yield value
    if s[pos] != value:
        raise RuntimeError("context value on stack was modified")
    while len(s) > pos:
        s.pop()

def get(type):
    """Return the last context value with specified type, or None if not found."""
    for value in reverse(stack()):
        if isinstance(value, Mapping):
            if value.get("type") == type:
                return value
