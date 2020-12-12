"""Module that provides type annotations."""

from __future__ import annotations

import asyncio
import collections.abc
import dataclasses
import enum
import inspect
import re
import typing
import wrapt


class Description:
    """Type annotation to provide a textual description."""

    def __init__(self, value):
        self.value = value


class Example:
    """Type annotation to provide an example value."""

    def __init__(self, value):
        self.value = value


class Validator:
    """Base class for type annotations that perform validation."""

    def validate(self, value: typing.Any) -> None:
        raise NotImplementedError


class MinLen(Validator):
    """Type annotation that validates a value has a minimum length."""

    def __init__(self, value: int):
        self.value = value

    def validate(self, value: typing.Any) -> None:
        if len(value) < self.value:
            raise ValueError(f"minimum length: {self.value}")


class MaxLen(Validator):
    """Type annotation that validates a value has a maximum length."""

    def __init__(self, value: int):
        self.value = value

    def validate(self, value: typing.Any) -> None:
        if len(value) > self.value:
            raise ValueError(f"maximum length: {self.value}")


class MinValue(Validator):
    def __init__(self, value: typing.Any):
        self.value = value

    def validate(self, value: typing.Any) -> None:
        if value < self.value:
            raise ValueError(f"minimum value: {self.value}")


class MaxValue(Validator):
    def __init__(self, value: typing.Any):
        self.value = value

    def validate(self, value: typing.Any) -> None:
        if value > self.value:
            raise ValueError(f"maximum value: {self.value}")


class Pattern(Validator):
    """Type annotation that validates a value matches a pattern."""

    def __init__(self, value: typing.Union[str, re.Pattern]):
        self.value = re.compile(value) if isinstance(value, str) else value

    def validate(self, value: typing.Any) -> None:
        if not self.value.match(value):
            raise ValueError(f"does not match pattern: '{self.value.pattern}'")


def _decorate_exception(e, addition):
    if not e.args:
        e.args = (addition,)
    else:
        e.args = (f"{e.args[0]} {addition}", *e.args[1:])


def _validate_union(type_, value):
    for arg in typing.get_args(type_):
        try:
            return validate(value, arg)
        except (TypeError, ValueError):
            continue
    raise TypeError


def _validate_typeddict(type_, value):
    for item_key, item_type in typing.get_type_hints(
        type_, include_extras=True
    ).items():
        try:
            validate(value[item_key], item_type)
        except KeyError:
            if item_key in type_.__required_keys__:
                raise ValueError(f"missing required item: {item_key}")
        except (TypeError, ValueError) as e:
            _decorate_exception(e, f"in item: {item_key}")
            raise


def _validate_mapping(type_, value):
    key_type, value_type = typing.get_args(type_)
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


def _validate_iterable(type_, value):
    if isinstance(value, str) or isinstance(value, collections.abc.ByteString):
        return  # these are not the iterables we are looking for
    item_type = typing.get_args(type_)[0]
    for item_value in value:
        validate(item_value, item_type)


def _validate_dataclass(type_, value):

    for attr_name, attr_type in typing.get_type_hints(
        type_, include_extras=True
    ).items():
        try:
            validate(getattr(value, attr_name), attr_type)
        except (TypeError, ValueError) as e:
            _decorate_exception(e, f"in attribute: {attr_name}")
            raise


def _issubclass(cls, cls_or_tuple):
    """A more forgiving issubclass."""
    try:
        return issubclass(cls, cls_or_tuple)
    except:
        return False


def _isinstance(obj, class_or_tuple):
    """A more forgiving isinstance."""
    try:
        return isinstance(obj, class_or_tuple)
    except:
        return False


def validate(value, type_):
    """Validate a value."""

    origin = typing.get_origin(type_)
    args = typing.get_args(type_)

    annotations = ()

    if origin is typing.Annotated:
        type_ = args[0]
        annotations = args[1:]
        origin = typing.get_origin(type_)
        args = typing.get_args(type_)

    if origin:
        type_ = origin[args]
    else:
        origin = type_

    # basic type validation
    if origin is typing.Any:
        pass
    elif origin is typing.Union:
        _validate_union(type_, value)
    elif not _isinstance(value, dict) and not _isinstance(value, origin):
        raise TypeError(
            f"expecting {origin.__name__}, received {value.__class__.__name__}"
        )

    # detailed type validation
    if _issubclass(type_, dict) and getattr(type_, "__annotations__", None):
        _validate_typeddict(type_, value)
    elif _issubclass(origin, collections.abc.Mapping):
        _validate_mapping(type_, value)
    elif origin is int and _isinstance(value, bool):  # bool is subclass of int
        raise TypeError(f"expecting int")
    elif _issubclass(origin, collections.abc.Iterable):
        _validate_iterable(type_, value)
    elif dataclasses.is_dataclass(type_):
        _validate_dataclass(type_, value)

    # validate using validator type annotations
    for annotation in annotations:
        if isinstance(annotation, Validator):
            annotation.validate(value)


def validate_arguments(callable):
    """
    Decorate a function or coroutine to validate its arguments using type
    annotations.
    """

    sig = inspect.signature(callable)

    positional_params = [
        p.name
        for p in sig.parameters.values()
        if p.kind in {p.POSITIONAL_ONLY, p.POSITIONAL_OR_KEYWORD}
    ]

    def _validate(instance, args, kwargs):
        hints = typing.get_type_hints(callable)
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


def validate_return_value(callable):
    """
    Decorate a function or coroutine to validate its return value using type
    annotations.
    """

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
