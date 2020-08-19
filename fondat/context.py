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


_stack = contextvars.ContextVar("_fondat_stack")


class _Element:
    """
    A context stack element.
 
    The context stack is a linked list of elements; each stack element
    contains a value and a reference to the next element below it on the
    stack. Because stacks are linked lists, they can be safely forked when
    asynchronous tasks are performed.

    Each element is iterable; it will iterate over the values of the entire
    stack, beginning with the referenced element's value, then the next
    element's value below it, and so on. In this sense the element at the top
    represents the entire stack.

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


class push:
    """
    Return a context manager that:
    • upon entry, pushes an element onto the context stack
    • upon exit, resets the content stack to its original state

    A pushed element must be a mapping, and should contain a "context" value
    expressing the type of context being pushed onto the stack; context
    values beginning with "fondat." are reserved.

    If no context-local stack exists, then pushing a value causes a new stack
    to be created with an initial "root" element, which will contain a unique
    identifier "id" and timestamp "time".

    A context element is a mapping of key-value pairs. It is expressed as:
    • push(mapping): Element is initialized from a mapping object's key-value pairs.
    • push(**kwargs): Element is initialized with name-value pairs in keyword arguments. 
    """

    def __init__(self, *args, **kwargs):
        self._value = dict(*args, **kwargs)

    def __enter__(self):
        stack = _stack.get(None)
        if not stack:
            stack = _Element(
                dict(
                    context="root",
                    id=uuid.uuid4(),
                    time=datetime.datetime.now(tz=datetime.timezone.utc),
                )
            )
        self._token = _stack.set(_Element(self._value, stack))

    def __exit__(self, *args):
        _stack.reset(self._token)


def find(*args, **kwargs):
    """
    Return a generator that yields elements on the context stack that match
    the specified keys and values. Elements are returned in the order of most
    recent to least recent (top of stack to bottom).

    The elements to match can be expressed as follows:
    • find(mapping): Match is expressed as a mapping object's key-value pairs.
    • find(**kwargs): Match is expressed with name-value pairs in keyword arguments.

    Supplying no parameters will yield all elements on the stack. 
    """
    test = dict(*args, **kwargs).items()
    return (value for value in _stack.get(()) if test <= value.items())


def first(*args, **kwargs):
    """
    Return the first (least recent) element pushed on to the contact stack
    that matches the specified keys and values, or None if no such element is
    found.

    The element to match for can be expressed as follows:
    • first(mapping): Match is expressed as a mapping object's key-value pairs.
    • first(**kwargs): Match is expressed with name-value pairs in keyword arguments. 
    """
    result = None
    for result in find(*args, **kwargs):
        pass
    return result


def last(*args, **kwargs):
    """
    Return the last (most recent) element pushed on to the contact stack that
    matches the specified keys and values, or None if no such element is found.

    The element to match for can be expressed as follows:
    • last(mapping): Match is expressed as a mapping object's key-value pairs.
    • last(**kwargs): Match is expressed with name-value pairs in keyword arguments. 
    """
    return next(iter(find(*args, **kwargs)), None)
