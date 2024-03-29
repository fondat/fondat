"""Module that support validation of data."""

import asyncio
import dataclasses
import fondat.types
import inspect
import re
import types
import typing
import wrapt

from collections.abc import Callable, Iterable, Mapping
from contextlib import contextmanager
from fondat.types import MISSING, is_instance, is_subclass, split_annotations
from types import NoneType
from typing import Any, Self, TypeVar


class ValidationError(ValueError):
    """
    Error raised when a validation fails.

    Parameters:
    • message: human-readable error message
    • path: path to the attribute that is the subject of the validation error
    • code: machine-readable error code
    • value: value that was the cause of the validation error
    """

    __slots__ = {"message", "path", "code"}

    Path = list[str | int]

    def __init__(
        self,
        message: str | None = None,
        path: Path | None = None,
        code: str | None = None,
        value: Any = MISSING,
    ):
        self.message = message
        self.path = path
        self.code = code
        self.value = value

    def __repr__(self):
        s = []
        if self.message is not None:
            s.append(f"message={self.message!r}")
        if self.path is not None:
            s.append(f"path={self.path!r}")
        if self.code is not None:
            s.append(f"code={self.code!r}")
        if self.value is not MISSING:
            s.append(f"value={self.value!r}")
        if not s:
            return self.__class__.__name__
        return f"{self.__class__.__name__}({', '.join(s)})"

    def __str__(self):
        s = []
        if self.message:
            s.append(self.message)
        if self.code:
            if not self.message:
                s.append(self.code)
            else:
                s.append(f"({self.code})")
        if self.path:
            s.append(f"in {'/'.join(self.path)}")
        if self.value is not MISSING:
            s.append(f"(value: {self.value!r})")
        return " ".join(s)

    @staticmethod
    @contextmanager
    def path_on_error(path: Path | str | int):
        """
        Execute within context that inserts a base error path if a ValidationError is raised.
        Base error path can be a full path or a single path segment.
        """
        try:
            yield
        except ValidationError as ve:
            if ve.path is None:
                ve.path = []
            match path:
                case str() | int():
                    ve.path.insert(0, path)
                case list():
                    ve.path = path + ve.path
            raise


class ValidationErrors(ValueError):
    """
    Error that collects multiple validation errors, to be raised together.

    Parameters and attributes:
    • errors: collected validation errors
    """

    __slots__ = {"errors"}

    def __init__(self, *errors):
        self.errors = list(errors)

    def __repr__(self):
        return f"{self.__class__.__name__}{', '.join(self.errors)!r})"

    def __str__(self):
        return ", ".join(str(e) for e in self.errors)

    def __len__(self):
        return len(self.errors)

    def __bool__(self):
        return bool(self.errors)

    def __iter__(self):
        return iter(self.errors)

    @classmethod
    @contextmanager
    def collect(cls) -> Iterable[Self]:
        """
        Establish a context around a new ValidationErrors exception object. Upon exit of the
        context, if the ValidationErrors exception object contains any ValidationError
        exceptions, the new ValidationErrors exception object will be raised.
        """
        validation_errors = cls()
        yield validation_errors
        if validation_errors:
            raise validation_errors

    @contextmanager
    def catch(self) -> Iterable[None]:
        """
        Establish a context to catch a ValidationError exception and add it to the
        ValidationErrors exception object. If an error is caught, it will be suppressed, and
        execution will continue outside of the context.
        """
        try:
            yield
        except ValidationError as ve:
            self.add(ve)

    def add(self, error: ValidationError) -> None:
        """Add a ValidationError exception to the ValidationErrors exception."""
        self.errors.append(error)


class Validator:
    """Base class for type annotation that performs validation."""

    def validate(self, value: Any) -> None:
        raise NotImplementedError


class MinLen(Validator):
    """Type annotation that validates a value has a minimum length."""

    __slots__ = {"value"}

    def __init__(self, value: int):
        self.value = value

    def validate(self, value: Any) -> None:
        if len(value) < self.value:
            raise ValidationError(f"mininum length: {self.value}", value=value)

    def __repr__(self):
        return f"MinLen({self.value})"


class MaxLen(Validator):
    """Type annotation that validates a value has a maximum length."""

    __slots__ = {"value"}

    def __init__(self, value: int):
        self.value = value

    def validate(self, value: Any) -> None:
        if len(value) > self.value:
            raise ValidationError(f"maximum length: {self.value}", value=value)

    def __repr__(self):
        return f"MaxLen({self.value})"


class MinValue(Validator):
    """Type annotation that validates a value has a minimum value."""

    __slots__ = {"value"}

    def __init__(self, value: Any):
        self.value = value

    def validate(self, value: Any) -> None:
        if value < self.value:
            raise ValidationError(f"minimum value: {self.value}", value=value)

    def __repr__(self):
        return f"MinValue({self.value})"


class MaxValue(Validator):
    """Type annotation that validates a value has a maximum value."""

    __slots__ = {"value"}

    def __init__(self, value: Any):
        self.value = value

    def validate(self, value: Any) -> None:
        if value > self.value:
            raise ValidationError(f"maximum value: {self.value}", value=value)

    def __repr__(self):
        return f"MaxValue({self.value})"


class Pattern(Validator):
    """
    Type annotation that validates a value matches a pattern.

    Parameters:
    • pattern: pattern object or string to match
    """

    __slots__ = {"pattern"}

    def __init__(self, pattern: str | re.Pattern):
        self.pattern = re.compile(pattern) if isinstance(pattern, str) else pattern

    def validate(self, value: Any) -> None:
        if not self.pattern.match(value):
            raise ValidationError(f"expecting pattern: {self.pattern.pattern}", value=value)

    def __repr__(self):
        return f"Pattern({self.pattern})"


def _validate_union(value, args):
    if value is None and NoneType in args:
        return
    for arg in args:
        try:
            return validate(value, arg)
        except ValidationError as e:
            continue
    union_repr = " | ".join("None" if a is NoneType else a.__name__ for a in args)
    raise ValidationError(f"expecting union: {union_repr}", value=value)


def _validate_literal(value, args):
    for arg in args:
        if arg == value and type(arg) is type(value):
            return
    literal_repr = ", ".join(repr(a) for a in args)
    raise ValidationError(f"expecting one of: {literal_repr}", value=value)


def validation_error_path(segment: str | int):
    """Deprecated. Use ValidationError.path_on_error."""
    return ValidationError.path_on_error(segment)


def _validate_typeddict(value, python_type):
    for item_key, item_type in typing.get_type_hints(python_type, include_extras=True).items():
        try:
            with ValidationError.path_on_error(item_key):
                validate(value[item_key], item_type)
        except KeyError:
            if item_key in python_type.__required_keys__:
                raise ValidationError("required", path=[item_key], value=value)


def _validate_mapping(value, python_type, args):
    key_type, value_type = args
    for key, value in value.items():
        validate(key, key_type)
        with ValidationError.path_on_error(key):
            validate(value, value_type)


def _validate_tuple(value, python_type, args):
    if len(args) == 2 and args[1] is Ellipsis:
        item_type = args[0]
        index = 0
        for n in range(len(value)):
            with ValidationError.path_on_error(n):
                validate(value[n], item_type)
        index += 1
    elif len(value) != len(args):
        raise ValidationError(
            f"expecting tuple[{', '.join(str(arg) for arg in args)}]", value=value
        )
    else:
        for n in range(len(args)):
            with ValidationError.path_on_error(n):
                validate(value[n], args[n])


def _validate_iterable(value, python_type, args):
    item_type = args[0]
    index = 0
    for item_value in value:
        with ValidationError.path_on_error(index):
            validate(item_value, item_type)
        index += 1


def _validate_typevar(value, python_type):
    validate(value, fondat.types.resolve_typevar(python_type))


def _validate_dataclass(value, python_type, origin):
    dc_type = python_type if not origin else origin
    with fondat.types.capture_typevars(python_type):
        for attr_name, attr_type in typing.get_type_hints(dc_type, include_extras=True).items():
            with ValidationError.path_on_error(attr_name):
                validate(getattr(value, attr_name), attr_type)


def validate_value(value: Any, type_hint: Any) -> None:
    """Validate a value."""

    python_type, annotations = split_annotations(type_hint)
    origin = typing.get_origin(python_type)
    args = typing.get_args(python_type)

    # validate using specified validator annotations
    for annotation in annotations:
        if isinstance(annotation, Validator):
            annotation.validate(value)

    if python_type is Any:
        return
    elif isinstance(python_type, TypeVar):
        return _validate_typevar(value, python_type)

    match origin:
        case types.UnionType | typing.Union:
            return _validate_union(value, args)
        case typing.Literal:
            return _validate_literal(value, args)

    # TypedDict
    if is_subclass(python_type, dict) and hasattr(python_type, "__annotations__"):
        origin = dict

    # basic type validation
    if origin and not is_instance(value, origin):
        raise ValidationError(f"expecting type: {origin.__name__}", value=value)
    elif not origin and not is_instance(value, python_type):
        raise ValidationError(f"expecting type: {python_type.__name__}", value=value)
    elif python_type is int and is_instance(value, bool):  # bool is subclass of int
        raise ValidationError("expecting type: int", value=value)
    elif is_subclass(origin, Iterable) and is_instance(value, (str, bytes, bytearray)):
        raise ValidationError(f"expecting type: Iterable", value=value)

    # structured type validation
    if is_subclass(python_type, dict) and hasattr(python_type, "__annotations__"):
        return _validate_typeddict(value, python_type)
    elif is_subclass(origin, Mapping):
        return _validate_mapping(value, python_type, args)
    elif is_subclass(origin, tuple):
        return _validate_tuple(value, python_type, args)
    elif is_subclass(origin, Iterable):
        return _validate_iterable(value, python_type, args)
    elif dataclasses.is_dataclass(python_type) or dataclasses.is_dataclass(origin):
        return _validate_dataclass(value, python_type, origin)


def validate(value: Any, type_hint: Any) -> None:
    """Deprecated. Use validate_value."""
    validate_value(value, type_hint)


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
                with ValidationError.path_on_error(param.name):
                    validate(params[param.name], hint)

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
    """Decorate a function or coroutine to validate its return value using type hints."""

    return_type = typing.get_type_hints(callable, include_extras=True).get("return")

    def _validate(result):
        if return_type is not None:
            with ValidationError.path_on_error("return"):
                validate(result, return_type)

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
    except ValidationError:
        return False
    return True


def validate_condition(
    condition: bool,
    *,
    errors: ValidationErrors | None = None,
    message: str | None = None,
    path: ValidationError.Path | None = None,
    code: str | None = None,
) -> None:
    """
    Validate a condition. If the condition is false then a ValidationError is created. If an
    errors object is specified, then the error is added to it; otherwise the ValidationError
    is raised.

    Parameters:
    • errors: object to add error to
    • message: human-readable error message
    • path: path to the attribute that is the subject of the validation error
    • code: machine-readable error code
    """
    if not condition:
        error = ValidationError(message=message, path=path, code=code)
        if errors is None:
            raise error
        errors.add(error)
