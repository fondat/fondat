"""
Module to implement resources composed of operations.

A resource is an addressible object that exposes operations through a uniform inteerface.

A resource class contains operation methods, each decorated with the @operation decorator.

A resource can expose subordinate resources through an attribute, method or property that is
also decorated as a resource.

For information on how security policies are evaluated, see the authorize function.
"""

import asyncio
import functools
import inspect
import fondat.context as context
import fondat.monitoring as monitoring
import logging
import types
import wrapt

from collections.abc import Iterable, Mapping
from copy import deepcopy
from fondat.error import ForbiddenError, UnauthorizedError
from fondat.security import Policy
from fondat.validation import validate_arguments
from typing import Any, Literal


_logger = logging.getLogger(__name__)


def _summary(function):
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


def resource(wrapped: type = None, *, tag: str = None):
    """
    Decorate a class to be a resource containing operations.

    Parameters:
    • tag: tag to group resources  [resource class name]
    """

    if wrapped is None:
        return functools.partial(resource, tag=tag)
    wrapped._fondat_resource = types.SimpleNamespace(tag=tag or wrapped.__name__)
    return wrapped


def is_resource(obj_or_type: Any) -> bool:
    """Return if object or type represents a resource."""
    return getattr(obj_or_type, "_fondat_resource", None) is not None


def is_operation(obj_or_type: Any) -> bool:
    """Return if object represents a resource operation."""
    return getattr(obj_or_type, "_fondat_operation", None) is not None


_methods = {"get", "put", "post", "delete", "patch"}


@validate_arguments
def operation(
    wrapped=None,
    *,
    method: Literal[tuple(_methods)] = None,
    type: Literal["query", "mutation"] = None,
    policies: Iterable[Policy] = None,
    publish: bool = True,
    deprecated: bool = False,
):
    """
    Decorate a resource coroutine that performs an operation.

    Parameters:
    • method: method of operation  [inferred from wrapped function name]
    • type: type of operation  [inferred from method]
    • policies: security policies for the operation
    • publish: publish the operation in documentation
    • deprecated: declare the operation as deprecated

    When an operation is called:
    • its arguments are copied to prevent side effects
    • its arguments are validated against their type hints
    """

    if wrapped is None:
        return functools.partial(
            operation,
            publish=publish,
            policies=policies,
            deprecated=deprecated,
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

    parameters = inspect.signature(wrapped).parameters
    if not parameters or iter(parameters).__next__() != "self":
        raise TypeError("operation first parameter must be self")
    for p in parameters.values():
        if p.kind is p.VAR_POSITIONAL:
            raise TypeError("operation with *args is not supported")
        elif p.kind is p.VAR_KEYWORD:
            raise TypeError("operation with **kwargs is not supported")

    @wrapt.decorator
    async def wrapper(wrapped, instance, args, kwargs):
        args, kwargs = deepcopy(args), deepcopy(kwargs)  # avoid side effects
        operation = getattr(wrapped, "_fondat_operation")
        resource_name = f"{instance.__class__.__module__}.{instance.__class__.__qualname__}"
        operation_name = wrapped.__name__
        tags = {"resource": resource_name, "operation": operation_name}
        _logger.debug(
            "operation: %s.%s(args=%s, kwargs=%s)", resource_name, operation_name, args, kwargs
        )
        with context.push({"context": "fondat.operation", **tags}):
            async with monitoring.timer({"name": "operation_duration_seconds", **tags}):
                async with monitoring.counter({"name": "operation_calls_total", **tags}):
                    await authorize(operation.policies)
                    return await wrapped(*args, **kwargs)

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


def query(wrapped=None, *, method: str = "get", **kwargs):
    """Decorator to define a query operation."""

    if wrapped is None:
        return functools.partial(
            query,
            method=method,
            **kwargs,
        )

    return operation(wrapped, type="query", method=method, **kwargs)


def mutation(wrapped=None, *, method: str = "post", **kwargs):
    """Decorator to define an mutation operation."""

    if wrapped is None:
        return functools.partial(
            mutation,
            method=method,
            **kwargs,
        )

    return operation(wrapped, type="mutation", method=method, **kwargs)


def container_resource(resources: Mapping[str, type], tag: str = None):
    """
    Create a resource to contain subordinate resources.

    Parameters:
    • resources: mapping of resource names to resource objects
    • tag: tag to group the resource

    Suborindates are accessed as attributes by name.
    """

    @resource(tag=tag)
    class Container:
        def __getattr__(self, name):
            try:
                return resources[name]
            except KeyError:
                raise AttributeError(f"no such resource: {name}")

        def __dir__(self):
            return [*super().__dir__(), *resources.keys()]

    return Container()
