"""Module to provide runtime support for type hints."""

from typing import get_type_hints


def affix_type_hints(obj, globalns=None, localns=None, attrs=True):
    """
    Affixes an object's type hints to the object.

    Parameters:
    • obj: Function, method, module or class object.
    • globalns: Global namespace to evaluate type hints.
    • localns: Local namespace to evaluate type hints.
    • attrs: Affix all of object's attribute type hints.

    Type hints are affixed by first being resolved through
    typing.get_type_hints, then by storing the result in the object's
    __annotations__ attribute.

    If the object is a class, this function will affix annotations from all
    superclasses into the object annotations.

    Affixation provides the following benefits:
    • time and scope of annotation evaluation is under the control of the caller
    • annotations are not re-evaluated for every call to typing.get_type_hints
    """

    if getattr(obj, "__annotations__", None):
        obj.__annotations__ = get_type_hints(
            obj, globalns, localns, include_extras=True
        )
    if attrs:
        for name in dir(obj):
            affix_type_hints(getattr(obj, name), globalns, localns, False)
