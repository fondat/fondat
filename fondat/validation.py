"""Module that support validation of data."""

from __future__ import annotations

import asyncio
import collections.abc
import dataclasses
import enum
import inspect
import re
import typing
import wrapt

from collections.abc import Callable, Iterable, Mapping
from fondat.types import is_instance, is_subclass, split_annotated
from typing import Annotated, Any, Literal, Union


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


class MaxLen(Validator):
    """Type annotation that validates a value has a maximum length."""

    __slots__ = ("value",)

    def __init__(self, value: int):
        self.value = value

    def validate(self, value: Any) -> None:
        if len(value) > self.value:
            raise ValueError(f"maximum length: {self.value}")


class MinValue(Validator):
    """Type annotation that validates a value has a minimum value."""

    __slots__ = ("value",)

    def __init__(self, value: Any):
        self.value = value

    def validate(self, value: Any) -> None:
        if value < self.value:
            raise ValueError(f"minimum value: {self.value}")


class MaxValue(Validator):
    """Type annotation that validates a value has a maximum value."""

    __slots__ = ("value",)

    def __init__(self, value: Any):
        self.value = value

    def validate(self, value: Any) -> None:
        if value > self.value:
            raise ValueError(f"maximum value: {self.value}")


class Pattern(Validator):
    """Type annotation that validates a value matches a pattern."""

    __slots__ = ("value",)

    def __init__(self, value: Union[str, re.Pattern]):
        self.value = re.compile(value) if isinstance(value, str) else value

    def validate(self, value: Any) -> None:
        if not self.value.match(value):
            raise ValueError(f"does not match pattern: '{self.value.pattern}'")


def _decorate_exception(e, addition):
    if not e.args:
        e.args = (addition,)
    else:
        e.args = (f"{e.args[0]} {addition}", *e.args[1:])


def _validate_union(value, args):
    for arg in args:
        try:
            return validate(value, arg)
        except (TypeError, ValueError) as e:
            continue
    raise TypeError(f"Union[{args}]: {value}")


def _validate_literal(value, args):
    for arg in args:
        if arg == value and type(arg) is type(value):
            return
    raise ValueError(f"expecting one of: {args}; got: {value}")


def _validate_typeddict(value, python_type):
    for item_key, item_type in typing.get_type_hints(python_type, include_extras=True).items():
        try:
            validate(value[item_key], item_type)
        except KeyError:
            if item_key in python_type.__required_keys__:
                raise ValueError(f"missing required item: {item_key}")
        except (TypeError, ValueError) as e:
            _decorate_exception(e, f"in item: {item_key}")
            raise


def _validate_mapping(value, python_type, args):
    key_type, value_type = args
    for key, value in value.items():
        try:
            validate(key, key_type)
        except (TypeError, ValueError) as e:
            _decorate_exception(e, f"for key: {key}")
            raise
        try:
            validate(value, value_type)
        except (TypeError, ValueError) as e:
            _decorate_exception(e, f"in: {key}")
            raise


def _validate_iterable(value, python_type, args):
    item_type = args[0]
    for item_value in value:
        validate(item_value, item_type)


def _validate_dataclass(value, python_type):
    for attr_name, attr_type in typing.get_type_hints(python_type, include_extras=True).items():
        try:
            validate(getattr(value, attr_name), attr_type)
        except (TypeError, ValueError) as e:
            _decorate_exception(e, f"in attribute: {attr_name}")
            raise


def validate(value: Any, type_hint: Any) -> NoneType:
    """Validate a value."""

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

    if is_subclass(python_type, dict) and hasattr(python_type, "__annotations__"):
        origin = dict

    # basic type validation
    if origin and not is_instance(value, origin):
        raise TypeError(f"expecting {origin.__name__}; got {value}")
    elif not origin and not is_instance(value, python_type):
        raise TypeError(f"expecting {python_type}; got {value}")
    elif python_type is int and is_instance(value, bool):  # bool is subclass of int
        raise TypeError("expecting int; got bool")
    elif is_subclass(origin, Iterable) and is_instance(value, (str, bytes, bytearray)):
        raise TypeError(f"expecting Iterable; got {value}")

    # structured type validation
    if is_subclass(python_type, dict) and hasattr(python_type, "__annotations__"):
        return _validate_typeddict(value, python_type)
    elif is_subclass(origin, Mapping):
        return _validate_mapping(value, python_type, args)
    elif is_subclass(origin, Iterable):
        return _validate_iterable(value, python_type, args)
    elif dataclasses.is_dataclass(python_type):
        return _validate_dataclass(value, python_type)


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
                try:
                    validate(params[param.name], hint)
                except (TypeError, ValueError) as e:
                    _decorate_exception(e, f"in parameter: {param.name}")
                    raise

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
            try:
                validate(result, type_)
            except (TypeError, ValueError) as e:
                _decorate_exception(e, "in return value")
                raise

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
