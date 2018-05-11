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


def get_stack():
    """Return the current context stack."""
    try:
        return _local.stack
    except AttributeError:  # newly seen thread
        _local.stack = []
        return _local.stack


class push():
    """
    Push a value onto the context stack. Returns a value that is passed to the
    pop() function to pop it from the stack.
    """

    def __init__(self, *args, **varargs):
        """
        Initialize context manager.

        Accepts context values as follows:
        - push(mapping): Context is initialized from a mapping object's key-value pairs.
        - push(**kwargs): Context is initialized with name-value pairs in keyword arguments. 
        """
        stack = get_stack()
        self.value = dict(*args, **varargs)
        stack.append(self.value)
        self.pos = len(stack) - 1

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pop(self)


def pop(pushed):
    """
    Pop a value that was pushed onto the context stack.

    :param pushed: The value returned from push().
    """
    pos = pushed.pos
    value = pushed.value
    stack = get_stack()
    if stack[pos] != value:
        raise RuntimeError("context value on stack was modified")
    del stack[pos:]


def last(*args, **varargs):
    """
    Return the last context value pushed on the stack with with the specified keys
    and values, or None if not found.

    The value to search for can be expressed as follows:
    - last(mapping): Value is expressed as a mapping object's key-value pairs.
    - last(**kwargs): Value is expressed with name-value pairs in keyword arguments. 
    """
    values = dict(*args, **varargs)
    for value in reversed(get_stack()):
        if isinstance(value, Mapping):
            if {k: value.get(k) for k in values} == values:
                return value


def find(*args, **varargs):
    """
    Return a list of all context values pushed onto the stack with the
    specified keys and values. Values are returned in stack order; the last pushed
    value is the last value in the return value.

    The value to search for can be expressed as follows:
    - find(mapping): Value is expressed as a mapping object's key-value pairs.
    - find(**kwargs): Value is expressed with name-value pairs in keyword arguments. 
    """
    result = []
    values = dict(*args, **varargs)
    for value in get_stack():
        if isinstance(value, Mapping):
            if {k: value.get(k) for k in values} == values:
                result.append(value)
    return result
