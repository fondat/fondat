"""
Module to implement resources composed of operations.

A resource is an addressible object that exposes operations through a uniform interface. A
resource class contains operation methods, each decorated with the @operation decorator.

A resource can expose subordinate resources through an attribute, method or property that is
also decorated as a resource.

For information on how security policies are evaluated, see the `authorize` function.
"""

import asyncio
import fondat.context as context
import fondat.monitor as monitor
import functools
import inspect
import logging
import types
import wrapt

from collections.abc import Callable, Iterable, Mapping
from contextlib import contextmanager
from copy import deepcopy
from fondat.cache import CacheResource, hash_json
from fondat.codec import JSONCodec
from fondat.error import BadRequestError, ForbiddenError, UnauthorizedError
from fondat.security import Policy
from fondat.types import literal_values
from fondat.validation import ValidationError, validate_arguments
from typing import Any, Literal, TypeVar


T = TypeVar("T")


_logger = logging.getLogger(__name__)


def _summary(function: Callable[..., Any]) -> str:
    """
    Derive summary information from a function's docstring or name. The summary is the first
    sentence of the docstring, ending in a period, or if no dostring is present, the
    function's name capitalized.
    """
    if not function.__doc__:
        return f"{function.__name__.capitalize()}."
    result = []
    for word in function.__doc__.split():
        result.append(word)
        if word.endswith("."):
            break
    return " ".join(result)


async def authorize(policies: Iterable[Policy]):
    """
    Evaluate the specified security policies.

    Parameters:
    • policies: security policies to evaluate

    Security exceptions are: UnauthorizedError and ForbiddenError.

    This function evaluates the security policies in the order specified. If a non-security
    exception occurs, it shall be immediately raised, ceasing further evaluaton. Otherwise,
    if one of the security policies does not raise a security exception, then authorization
    shall be immediately granted, ceasing further evaluation.

    If security policies all raise security exceptions, then the first ForbiddenError exception
    is raised if encountered, otherwise the first UnauthorizedError exception is raised.
    """
    exception = None
    for policy in policies or ():
        try:
            await policy.apply()
            return  # security policy authorized the operation
        except ForbiddenError as fe:
            if not isinstance(exception, ForbiddenError):
                exception = fe
        except UnauthorizedError as ue:
            if not exception:
                exception = ue
        except:
            raise
    if exception:
        raise exception


def resource(wrapped: type[T] | None = None, *, tag: str | None = None) -> type[T]:
    """
    Decorate a class as a resource containing operations and/or subordinate resources.

    Parameters:
    • tag: tag to group resources  [resource class name]
    """

    if wrapped is None:
        return functools.partial(resource, tag=tag)
    wrapped._fondat_resource = types.SimpleNamespace(tag=tag or wrapped.__name__)
    return wrapped


Method = Literal["get", "put", "post", "delete", "patch"]

_methods = literal_values(Method)


@contextmanager
def _log_and_suppress():
    try:
        yield
    except Exception as exception:
        _logger.debug(exception, exc_info=True)


@validate_arguments
def operation(
    wrapped: T = None,
    *,
    method: Method | None = None,
    type: Literal["query", "mutation"] | None = None,
    policies: Iterable[Policy] | None = None,
    publish: bool = True,
    deprecated: bool = False,
    cache: CacheResource | None = None,
) -> T:
    """
    Decorate a resource coroutine as an operation.

    Parameters:
    • method: method of operation  [inferred from wrapped coroutine name]
    • type: type of operation  [inferred from method]
    • policies: security policies for the operation
    • publish: publish the operation in documentation
    • deprecated: flag the operation as deprecated
    • cache: resource to cache operation results

    When an operation is called:
    • its arguments are copied to prevent side effects
    • its arguments are validated against their type hints

    Exceptions that an operation should raise:
    • fondat.error.Error (e.g. BadRequestError)
    • fondat.validation.ValidationError (automatically reraised as a BadRequestError)

    The cache is a resource that exposes cache entry resources. A MemoryResource can serve as
    an operation cache resource out of the box. It is safe for a single cache resource to be
    shared by multiple operations.
    """

    if wrapped is None:
        return functools.partial(
            operation,
            method=method,
            type=type,
            policies=policies,
            publish=publish,
            deprecated=deprecated,
            cache=cache,
        )

    if not asyncio.iscoroutinefunction(wrapped):
        raise TypeError("operation must be a coroutine")

    if method is None:
        method = wrapped.__name__
        if method not in _methods:
            raise TypeError(f"method must be one of: {_methods}")

    if not type:
        type = "query" if method == "get" else "mutation"

    description = wrapped.__doc__ or wrapped.__name__
    summary = _summary(wrapped)

    sig = inspect.signature(wrapped)
    params = [param for param in sig.parameters.values()]
    returns = sig.return_annotation if sig.return_annotation is not sig.empty else None

    if not params or params[0].name != "self":
        raise TypeError("operation first parameter must be self")
    for param in params[1:]:
        if param.kind is param.VAR_POSITIONAL:
            raise TypeError("operation with *args is not supported")
        if param.kind is param.VAR_KEYWORD:
            raise TypeError("operation with **kwargs is not supported")
        if param.annotation is param.empty:
            raise TypeError(f"operation parameter must have type hint: {param.name}")

    if cache and not is_resource(cache):
        raise TypeError("cache must be a resource")

    @wrapt.decorator
    async def wrapper(wrapped, instance, args, kwargs):
        args, kwargs = deepcopy(args), deepcopy(kwargs)  # avoid side effects
        resource_name = f"{instance.__class__.__module__}.{instance.__class__.__qualname__}"
        operation = getattr(wrapped, "_fondat_operation")
        arguments = dict(zip((p.name for p in params[1:]), args)) | kwargs
        operation_name = wrapped.__name__
        tags = {"resource": resource_name, "operation": operation_name}
        _logger.debug("operation: %s.%s(%s)", resource_name, operation_name, arguments)
        with context.push(tags | {"context": "fondat.operation", "arguments": arguments}):
            async with monitor.counter(
                name="operation_invocations", tags=tags, status="status"
            ):
                async with monitor.timer(name="operation_duration", tags=tags):
                    await authorize(operation.policies)
                    if cache:
                        with _log_and_suppress():
                            cache_entry = cache[
                                hash_json(
                                    tags | {"arguments": JSONCodec.get(Any).encode(arguments)}
                                )
                            ]
                            return JSONCodec.get(returns).decode(await cache_entry.get())
                    try:
                        result = await wrapped(*args, **kwargs)
                    except ValidationError as ve:
                        raise BadRequestError from ve
                    if cache:
                        with _log_and_suppress():
                            await cache_entry.put(JSONCodec.get(returns).encode(result))
                    return result

    wrapped._fondat_operation = types.SimpleNamespace(
        method=method,
        type=type,
        policies=policies,
        publish=publish,
        deprecated=deprecated,
        summary=summary,
        description=description,
    )

    wrapped = validate_arguments(wrapped)
    return wrapper(wrapped)


def query(wrapped: T | None = None, *, method: str = "get", **kwargs) -> T:
    """Decorator to define a query operation."""

    if wrapped is None:
        return functools.partial(
            query,
            method=method,
            **kwargs,
        )

    return operation(wrapped, type="query", method=method, **kwargs)


def mutation(wrapped: T | None = None, *, method: str = "post", **kwargs) -> T:
    """Decorator to define an mutation operation."""

    if wrapped is None:
        return functools.partial(
            mutation,
            method=method,
            **kwargs,
        )

    return operation(wrapped, type="mutation", method=method, **kwargs)


@resource
class ContainerResource:
    """
    Resource to contain subordinate resources.

    Parameter:
    • subordinates: mapping of names to subordinate resource objects
    """

    def __init__(self, subordinates: Mapping[str, type]):
        self.__subordinates = subordinates

    def __getattr__(self, name):
        try:
            return self.__subordinates[name]
        except KeyError:
            raise AttributeError(f"no such resource: {name}")

    def __dir__(self):
        return [*super().__dir__(), *self.__subordinates.keys()]


def container_resource(resources: Mapping[str, Any], tag: str | None = None):
    """Deprecated. Use ContainerResource."""

    @resource(tag=tag)
    class DeprecatedContainerResource(ContainerResource):
        def __init__(self):
            super().__init__(subordinates=resources)

    return DeprecatedContainerResource()


def is_resource(obj_or_type: Any) -> bool:
    """Return if object or type is a resource."""
    return hasattr(obj_or_type, "_fondat_resource")


def is_operation(obj_or_type: Any) -> bool:
    """Return if object is a resource operation method."""
    return hasattr(obj_or_type, "_fondat_operation")
