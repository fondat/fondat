"""
Module to manage execution context stacks.

The execution context stack allows values to be "built up" as a request is handled and
operations are performed. It can provide information about the origin of a request, identify
principal(s) making a request, and include access tokens or any other information that can be
used when making authorization decisions.

The execution context stack is stored in context-local state; therefore sepeate stacks are
maintained for different threads and asynchrous tasks.
"""

import contextvars
import datetime
import uuid

from typing import Any, Generator


_stack = contextvars.ContextVar("_fondat_stack")


class _Element:
    """
    A context stack element.

    The context stack is a linked list of elements; each stack element contains a value and a
    reference to the previously pushed element below it on the stack. Because stacks are linked
    lists, they can be safely forked when asynchronous tasks are performed.

    Each element is iterable; it will iterate over the values of the entire stack, beginning
    with its value, then the previously pushed element's value below it, and so on. In this
    manner, the element at the top of a stack represents the entire stack.

    Parameters:
    • value: the value this stack element contains
    • prev: the element below this elemment on the stack, or None if this is the first element
    """

    __slots__ = {"_value", "_prev", "_len"}

    def __init__(self, value, prev=None):
        self._value = value
        self._prev = prev
        self._len = prev._len + 1 if prev else 1

    def __iter__(self):
        class _iter:
            __slots__ = {"_ptr"}

            def __init__(self, ptr):
                self._ptr = ptr

            def __iter__(self):
                return self

            def __next__(self):
                if self._ptr is None:
                    raise StopIteration
                result = self._ptr._value
                self._ptr = self._ptr._prev
                return result

        return _iter(self)

    def __len__(self):
        return self._len


class StackContextManager:
    """
    A context manager returned from pushing a value on the context stack, to automatically pop
    a value from the stack upon exit.
    """

    __slots__ = {"_token"}

    def __init__(self, token):
        self._token = token

    def pop(self):
        """
        Manually pop the pushed value from the stack. Use with caution. The preferred use of
        this context manager is to use "with" keyword to pop from the stack upon exit.
        """
        if self._token:
            _stack.reset(self._token)
        self._token = None

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.pop()


def push(*args, **kwargs) -> StackContextManager:
    """
    Push a new value onto the execution context stack, and return a context manager that will
    pop the value from the stack upon exit.

    A context value is a mapping of key-value pairs. It is expressed as:
    • push(mapping): element is initialized from a mapping object's key-value pairs
    • push(**kwargs): element is initialized with name-value pairs in keyword arguments

    A pushed value must contain a "context" value to express the type of context being pushed
    onto the stack; context keys and "context" value beginning with "fondat." are reserved.

    If no context-local stack exists, then pushing a value causes a new stack to be created
    with an initial "fondat.root" element, which will contain a unique identifier "id" and
    timestamp "time".
    """
    value = dict(*args, **kwargs)
    if "context" not in value:
        raise ValueError('pushed context must have a "context" item')
    stack = _stack.get(None)
    if not stack:
        stack = _Element(
            dict(
                context="fondat.root",
                id=uuid.uuid4(),
                time=datetime.datetime.now(tz=datetime.timezone.utc),
            )
        )
    token = _stack.set(_Element(value, stack))
    return StackContextManager(token)


def find(*args, **kwargs) -> Generator[Any, None, None]:
    """
    Return a generator that yields elements on the context stack that match the specified keys
    and values. Elements are returned in the order of most recently pushed to least recently
    pushed.

    The elements to match can be expressed as follows:
    • find(mapping): match is expressed as a mapping object's key-value pairs
    • find(**kwargs): match is expressed with name-value pairs in keyword arguments

    Supplying no parameters will yield all elements on the stack.
    """
    test = dict(*args, **kwargs).items() or None
    return (value for value in _stack.get(()) if test is None or test <= value.items())


def first(*args, **kwargs) -> Any:
    """
    Return the least recently pushed element on the contact stack that matches the specified
    keys and values, or None if no such element exists.

    The element to match for can be expressed as follows:
    • first(mapping): match is expressed as a mapping object's key-value pairs
    • first(**kwargs): match is expressed with name-value pairs in keyword arguments
    """
    result = None
    for result in find(*args, **kwargs):
        pass
    return result


def last(*args, **kwargs) -> Any:
    """
    Return the most recently pushed element on the contact stack that matches the specified
    keys and values, or None if no such element exists.

    The element to match for can be expressed as follows:
    • last(mapping): match is expressed as a mapping object's key-value pairs
    • last(**kwargs): match is expressed with name-value pairs in keyword arguments
    """
    return next(iter(find(*args, **kwargs)), None)
