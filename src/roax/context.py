"""Module to manage a stack of context values."""

# Copyright © 2017–2018 Paul Bryan.
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import threading

from collections import Mapping
from contextlib import contextmanager


_local = threading.local()

def stack():
    """Return the current context stack."""
    try:
        return _local.stack
    except AttributeError:  # newly seen thread
        _local.stack = []
        return _local.stack

@contextmanager
def context(*args, **varargs):
    """
    Context manager that pushes a value onto the context stack.

    This function accepts context values in two ways:
    context(mapping): Context is initialized from a mapping object's key-value pairs.
    context(**kwargs): Context is initialized with name-value pairs in keyword arguments. 
    """
    s = stack()
    value = dict(*args, **varargs)
    s.append(value)
    pos = len(s) - 1
    yield value
    if s[pos] != value:
        raise RuntimeError("context value on stack was modified")
    del s[pos + 1:]

def get(context_type):
    """Return the last context value with the specified type, or None if not found."""
    for value in reverse(stack()):
        if isinstance(value, Mapping):
            if value.get("context_type") == context_type:
                return value
