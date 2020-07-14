"""
Module to manage the context stack.

The context stack allows context to be built up as operations are performed.
It can provide information about the origin of a request, identify principal(s)
making the request, include access tokens or any other information that can be
used when making authorization decisions.

The context stack is stored in context-local state; therefore sepeate stacks
are maintained for different threads and asynchrous tasks.
"""

import contextvars
import collections.abc
import datetime
import uuid


_stack = contextvars.ContextVar("_roax_stack")


class Element:
    """
    A context stack element.
 
    The context stack is a linked list; each stack element contains a value
    and a reference to the next element below it on the stack. Because stacks
    are linked lists, they can be safely forked when asynchronous tasks are
    performed.

    Each element is iterable; it will iterate over the values of the entire
    stack, beginning with this element's value, then the next element's
    value below it, and so on. In this sense the element at the top represents
    the entire stack.

    Parameters:
    • value: the value to place at the top of the stack.
    • next: next element below this elemment, or None if this is the first element.
    """

    def __init__(self, value, next=None):
        self._value = value
        self._next = next
        pass

    def __iter__(self):
        class _iter:
            def __init__(self, next):
                self.next = next

            def __iter__(self):
                return self

            def __next__(self):
                if self.next is None:
                    raise StopIteration
                result = self.next._value
                self.next = self.next._next
                return result

        return _iter(self)

    def __len__(self):
        return sum(1 for _ in self)


def stack():
    """Return the context-local stack or None if no stack currently exists."""
    return _stack.get(None)


class push:
    """
    Return a context manager that:
    • upon entry, pushes a context value onto the context stack
    • upon exit, resets the content stack to its original state

    A pushed value must be a mapping, and should contain a "context" value
    expressing the type of context being pushed onto the stack.

    If no context-local stack exists, then pushing a value causes a new stack
    to be created. The stack will be initialized with a "root" value, which
    will contain unique identifier "id" and timestamp "time" values.

    A context value is a mapping of key-value pairs. It is expressed as:
    • push(mapping): Value is initialized from a mapping object's key-value pairs.
    • push(**kwargs): Value is initialized with name-value pairs in keyword arguments. 
    """

    def __init__(self, *args, **kwargs):
        self._value = dict(*args, **kwargs)

    def __enter__(self):
        stack_ = stack()
        if not stack_:
            stack_ = Element(
                dict(
                    context="root",
                    id=uuid.uuid4(),
                    time=datetime.datetime.now(tz=datetime.timezone.utc),
                )
            )
        self._token = _stack.set(Element(self._value, stack_))
        return self

    def __exit__(self, *args):
        _stack.reset(self._token)


def find(*args, **kwargs):
    """
    Return a list of all values on the contact stack that have the specified
    keys and values. Values are returned in the order of most recent to least
    recent (top of stack to bottom).

    The value to search for can be expressed as follows:
    • find(mapping): Value is expressed as a mapping object's key-value pairs.
    • find(**kwargs): Value is expressed with name-value pairs in keyword arguments. 
    """
    result = []
    test = dict(*args, **kwargs).items()
    for value in stack() or ():
        if isinstance(value, collections.abc.Mapping):
            if test <= value.items():  # test for subset
                result.append(value)
    return result


def first(*args, **kwargs):
    """
    Return the first (least recent) value pushed on to the contact stack that
    has the specified keys and values, or None if no such value is found.

    The value to search for can be expressed as follows:
    • first(mapping): Value is expressed as a mapping object's key-value pairs.
    • first(**kwargs): Value is expressed with name-value pairs in keyword arguments. 
    """
    found = find(*args, **kwargs)
    if found:
        return found[-1]


def last(*args, **kwargs):
    """
    Return the last (most recent) value pushed on to the contact stack that
    has the specified keys and values, or None if no such value is found.

    The value to search for can be expressed as follows:
    • last(mapping): Value is expressed as a mapping object's key-value pairs.
    • last(**kwargs): Value is expressed with name-value pairs in keyword arguments. 
    """
    test = dict(*args, **kwargs).items()
    for value in stack() or ():
        if isinstance(value, collections.abc.Mapping):
            if test <= value.items():  # test for subset
                return value  # return fast
