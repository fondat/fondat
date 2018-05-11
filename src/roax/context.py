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


class context():
    """
    Context manager that pushes a value onto the context stack. Pops the value upon
    completion.
    """

    def __init__(self, *args, **varargs):
        """
        Initialize context manager.

        Accepts context values as follows:
        - context(None): Nothing is pushed onto the stack.
        - context(mapping): Context is initialized from a mapping object's key-value pairs.
        - context(**kwargs): Context is initialized with name-value pairs in keyword arguments. 
        """
        self.value = dict(*args, **varargs)

    def __enter__(self):
        self.pushed = push(self.value)

    def __exit__(self, *args):
        pop(self.pushed)


def _values(*args, **varargs):
    """Conveniently parse values as single mapping or as kwargs."""
    if len(args) == 1 and args[0] is None:
        return None
    return dict(*args, **varargs)


def get_stack():
    """Return the current context stack."""
    try:
        return _local.stack
    except AttributeError:  # newly seen thread
        _local.stack = []
        return _local.stack

def root(*args, **varargs):
    """
    Context manager that pushes the value onto the context stack, clearing the
    stack first to ensure that no values from previous activity is present.

    Accepts context values as follows:
    - root(None): Nothing is pushed onto the stack.
    - root(mapping): Context is initialized from a mapping object's key-value pairs.
    - root(**kwargs): Context is initialized with name-value pairs in keyword arguments. 
    """
    clear()
    return context(*args, **varargs)

def push(*args, **varargs):
    """
    Push a value onto the context stack. Returns a value that is passed into
    pop() function to pop it from the stack.

    This function accepts context values as follows:
    - push(None): Nothing is pushed onto the stack.
    - push(mapping): Context is initialized from a mapping object's key-value pairs.
    - push(**kwargs): Context is initialized with name-value pairs in keyword arguments. 
    """
    stack = get_stack()
    pos = None
    value = _values(*args, **varargs)
    if value is not None:
        stack.append(value)
        pos = len(stack) - 1
    return (pos, value)

def pop(pushed):
    """
    Pop a value that was pushed onto the context stack.

    :param pushed: The value returned from the push() function.
    """
    pos, value = pushed
    stack = get_stack()
    if pos is not None:
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
    values = _values(*args, **varargs)
    if not values:
        return None
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
    values = _values(*args, **varargs)
    if not values:
        return None
    for value in get_stack():
        if isinstance(value, Mapping):
            if {k: value.get(k) for k in values} == values:
                result.append(value)
    return result

def clear():
    """Clear the context stack."""
    get_stack().clear()
