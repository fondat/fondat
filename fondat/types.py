"""Module to manage types and type hints."""

import functools
import typing


NoneType = type(None)


def affix_type_hints(obj=None, *, globalns=None, localns=None, attrs: bool = True):
    """
    Affixes an object's type hints to the object by materializing evaluated string type hints
    into the type's __annotations__ attribute.

    This function exists due to PEP 563, in which annotations are stored as strings, are only
    evaluated when typing.get_type_hints is called; this will be the expected behavior of
    annotations in Python 3.11. The work in PEP 649, if accepted, will likely eliminate the
    need to affix type hints.

    This function can be applied as a decorator to a class or function.

    Parameters:
    • obj: function, method, module or class object
    • globalns: global namespace to evaluate type hints
    • localns: local namespace to evaluate type hints
    • attrs: affix all of object's attribute type hints

    Type hints are affixed by first resolving through typing.get_type_hints, then by storing
    the result in the object's __annotations__ attribute.

    If the object is a class, this function will affix annotations from all superclasses into
    the object annotations.

    Affixation provides the following benefits (under PEP 563):
    • time and scope of annotation evaluation is under the control of the caller
    • annotations are not re-evaluated for every call to typing.get_type_hints
    """

    if obj is None:
        return functools.partial(
            affix_type_hints, globalns=globalns, localns=localns, attrs=attrs
        )

    if getattr(obj, "__annotations__", None):
        obj.__annotations__ = typing.get_type_hints(obj, globalns, localns, include_extras=True)
    if attrs:
        for name in dir(obj):
            if not name.startswith("__") and not name.endswith("__"):
                affix_type_hints(
                    getattr(obj, name), globalns=globalns, localns=localns, attrs=False
                )

    return obj


def split_annotated(hint):
    """Return a tuple containing the python type and annotations."""
    if not typing.get_origin(hint) is typing.Annotated:
        return hint, ()
    args = typing.get_args(hint)
    return args[0], args[1:]


def is_optional(hint):
    """Return if the specified type is optional (contains Union[..., None])."""
    python_type, _ = split_annotated(hint)
    if not typing.get_origin(python_type) is typing.Union:
        return python_type is NoneType
    for arg in typing.get_args(python_type):
        if is_optional(arg):
            return True
    return False


def strip_optional(hint):
    """Strip optionality from the type."""
    python_type, annotations = split_annotated(hint)
    if not typing.get_origin(python_type) is typing.Union:
        return hint
    python_type = typing.Union[
        tuple(
            strip_optional(arg) for arg in typing.get_args(python_type) if arg is not NoneType
        )
    ]
    if not annotations:
        return python_type
    return typing.Annotated[tuple([python_type, *annotations])]


def is_subclass(cls, cls_or_tuple):
    """A more forgiving issubclass."""
    try:
        return issubclass(cls, cls_or_tuple)
    except:
        return False


def is_instance(obj, class_or_tuple):
    """A more forgiving isinstance."""
    try:
        return isinstance(obj, class_or_tuple)
    except:
        return False


def is_typeddict(type):
    """Return if a type is a TypedDict."""
    return is_subclass(type, dict) and getattr(type, "__annotations__", None) is not None


def literal_values(type):
    """Return a set of all values in a Literal type."""
    return set(typing.get_args(type))
