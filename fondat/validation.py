"""Module that support validation of data."""

from __future__ import annotations

import asyncio
import dataclasses
import fondat.error
import inspect
import re
import typing
import wrapt

from collections.abc import Callable, Iterable, Mapping
from fondat.types import NoneType, is_instance, is_subclass, split_annotated
from typing import Any, Literal, Union


class Validator:
    """Base class for type annotation that performs validation."""

    def validate(self, value: Any) -> None:
        raise NotImplementedError


class MinLen(Validator):
    """Type annotation that validates a value has a minimum length."""

    __slots__ = ("value",)

    def __init__(self, value: int):
        self.value = value

    def validate(self, value: Any) -> None:
        if len(value) < self.value:
            raise ValueError(f"minimum length: {self.value}")

    def __repr__(self):
        return f"MinLen({self.value})"


class MaxLen(Validator):
    """Type annotation that validates a value has a maximum length."""

    __slots__ = ("value",)

    def __init__(self, value: int):
        self.value = value

    def validate(self, value: Any) -> None:
        if len(value) > self.value:
            raise ValueError(f"maximum length: {self.value}")

    def __repr__(self):
        return f"MaxLen({self.value})"


class MinValue(Validator):
    """Type annotation that validates a value has a minimum value."""

    __slots__ = ("value",)

    def __init__(self, value: Any):
        self.value = value

    def validate(self, value: Any) -> None:
        if value < self.value:
            raise ValueError(f"minimum value: {self.value}")

    def __repr__(self):
        return f"MinValue({self.value})"


class MaxValue(Validator):
    """Type annotation that validates a value has a maximum value."""

    __slots__ = ("value",)

    def __init__(self, value: Any):
        self.value = value

    def validate(self, value: Any) -> None:
        if value > self.value:
            raise ValueError(f"maximum value: {self.value}")

    def __repr__(self):
        return f"MaxValue({self.value})"


class Pattern(Validator):
    """
    Type annotation that validates a value matches a pattern.

    Parameters:
    • pattern: pattern object or string to match
    • message: message to include in raised value error
    """

    __slots__ = ("pattern", "message")

    def __init__(self, pattern: Union[str, re.Pattern], message: str = None):
        self.pattern = re.compile(pattern) if isinstance(pattern, str) else pattern
        self.message = message

    def validate(self, value: Any) -> None:
        if not self.pattern.match(value):
            raise ValueError(
                self.message
                if self.message is not None
                else f"value does not match required pattern"
            )

    def __repr__(self):
        return f"Pattern({self.pattern})"


def _validate_union(value, args):
    if value is None and NoneType in args:
        return
    for arg in args:
        try:
            return validate(value, arg)
        except (TypeError, ValueError) as e:
            continue
    raise TypeError(f"expecting type of: Union[{args}]; received: {type(value)} ({value})")


def _validate_literal(value, args):
    for arg in args:
        if arg == value and type(arg) is type(value):
            return
    raise ValueError(f"expecting one of: {args}; received: {value}")


def _validate_typeddict(value, python_type):
    for item_key, item_type in typing.get_type_hints(python_type, include_extras=True).items():
        try:
            validate(value[item_key], item_type, f"item {item_key}")
        except KeyError:
            if item_key in python_type.__required_keys__:
                raise ValueError(f"missing required item: {item_key}")


def _validate_mapping(value, python_type, args):
    key_type, value_type = args
    for key, value in value.items():
        validate(key, key_type, f"mapping key {key}")
        validate(value, value_type, f"value in {key}")


def _validate_tuple(value, python_type, args):
    if len(args) == 2 and args[1] is Ellipsis:
        item_type = args[0]
        for item_value in value:
            validate(item_value, item_type)
    elif len(value) != len(args):
        raise ValueError(
            f"expecting tuple[{', '.join(str(arg) for arg in args)}]; received: {value}"
        )
    else:
        for n in range(len(args)):
            with fondat.error.prepend((TypeError, ValueError), "[", n, "]: "):
                validate(value[n], args[n])


def _validate_iterable(value, python_type, args):
    item_type = args[0]
    index = 0
    for item_value in value:
        with fondat.error.prepend((TypeError, ValueError), "[", index, "]: "):
            validate(item_value, item_type)
        index += 1


def _validate_dataclass(value, python_type):
    for attr_name, attr_type in typing.get_type_hints(python_type, include_extras=True).items():
        validate(getattr(value, attr_name), attr_type, f"attribute {attr_name}")


def _validate(value, type_hint):

    python_type, annotations = split_annotated(type_hint)
    origin = typing.get_origin(python_type)
    args = typing.get_args(python_type)

    # validate using specified validator type annotations
    for annotation in annotations:
        if isinstance(annotation, Validator):
            annotation.validate(value)

    # aggregate type validation
    if python_type is Any:
        return
    elif origin is Union:
        return _validate_union(value, args)
    elif origin is Literal:
        return _validate_literal(value, args)

    # TypedDict
    if is_subclass(python_type, dict) and hasattr(python_type, "__annotations__"):
        origin = dict

    # basic type validation
    if origin and not is_instance(value, origin):
        raise TypeError(f"expecting {origin.__name__}; received {type(value)}")
    elif not origin and not is_instance(value, python_type):
        raise TypeError(f"expecting {python_type}; received {type(value)}")
    elif python_type is int and is_instance(value, bool):  # bool is subclass of int
        raise TypeError("expecting int; received bool")
    elif is_subclass(origin, Iterable) and is_instance(value, (str, bytes, bytearray)):
        raise TypeError(f"expecting Iterable; received {type(value)}")

    # structured type validation
    if is_subclass(python_type, dict) and hasattr(python_type, "__annotations__"):
        return _validate_typeddict(value, python_type)
    elif is_subclass(origin, Mapping):
        return _validate_mapping(value, python_type, args)
    elif is_subclass(origin, tuple):
        return _validate_tuple(value, python_type, args)
    elif is_subclass(origin, Iterable):
        return _validate_iterable(value, python_type, args)
    elif dataclasses.is_dataclass(python_type):
        return _validate_dataclass(value, python_type)


def validate(value: Any, type_hint: Any, in_: str = None) -> NoneType:
    """Validate a value."""

    try:
        _validate(value, type_hint)

    except (TypeError, ValueError) as e:
        if in_:
            if not e.args:
                e.args = (in_,)
            else:
                e.args = (f"{in_}: {e.args[0]}", *e.args[1:])
        raise


def validate_arguments(callable: Callable):
    """Decorate a function or coroutine to validate its arguments using type annotations."""

    sig = inspect.signature(callable)

    positional_params = [
        p.name
        for p in sig.parameters.values()
        if p.kind in {p.POSITIONAL_ONLY, p.POSITIONAL_OR_KEYWORD}
    ]

    def _validate(instance, args, kwargs):
        hints = typing.get_type_hints(callable, include_extras=True)
        if instance:
            args = (instance, *args)
        params = {
            **{p: v for p, v in zip(positional_params, args)},
            **kwargs,
        }
        for param in (p for p in sig.parameters.values() if p.name in params):
            if hint := hints.get(param.name):
                validate(params[param.name], hint, f"parameter {param.name}")

    if asyncio.iscoroutinefunction(callable):

        @wrapt.decorator
        async def decorator(wrapped, instance, args, kwargs):
            _validate(instance, args, kwargs)
            return await wrapped(*args, **kwargs)

    else:

        @wrapt.decorator
        def decorator(wrapped, instance, args, kwargs):
            _validate(instance, args, kwargs)
            return wrapped(*args, **kwargs)

    return decorator(callable)


def validate_return_value(callable: Callable):
    """Decorate a function or coroutine to validate its return value using type annotations."""

    type_ = typing.get_type_hints(callable, include_extras=True).get("return")

    def _validate(result):
        if type_ is not None:
            validate(result, type_, "return value")

    if asyncio.iscoroutinefunction(callable):

        @wrapt.decorator
        async def decorator(wrapped, instance, args, kwargs):
            result = await wrapped(*args, **kwargs)
            _validate(result)
            return result

    else:

        @wrapt.decorator
        def decorator(wrapped, instance, args, kwargs):
            result = wrapped(*args, **kwargs)
            _validate(result)
            return result

    return decorator(callable)


def is_valid(value: Any, type_hint: Any) -> bool:
    """Return if a value is valid for specified type."""

    try:
        validate(value, type_hint)
    except (TypeError, ValueError):
        return False
    return True
