"""Module to implement resources."""

import datetime
import importlib
import roax.context as context
import roax.monitor
import roax.schema as s
import threading
import wrapt


_now = lambda: datetime.datetime.now(tz=datetime.timezone.utc)


class _Operation:
    """A resource operation."""

    def __init__(self, attrs):
        for k in attrs:
            self.__setattr__(k, attrs[k])

    def call(self, **kwargs):
        """Call the resource operation with keyword arguments."""
        return getattr(self.resource, self.function)(**kwargs)


class ResourceError(Exception):
    """
    Base class for all resource errors.

    Parameters:
    • detail: textual description of the error.
    • code: the HTTP status most closely associated with the error.
    """

    def __init__(self, detail=None, code=None):
        super().__init__(self, detail)
        self.detail = detail or getattr(self, "detail", "Internal Server Error")
        self.code = code or getattr(self, "code", 500)

    def __str__(self):
        return self.detail


class BadRequest(ResourceError):
    """Raised if the request is malformed."""

    code, detail = 400, "Bad Request"


class Unauthorized(ResourceError):
    """Raised if the request lacks valid authentication credentials."""

    code, detail = 401, "Unauthorized"

    def __init__(self, detail=None, challenge=None):
        """
        Initialize resource error.

        • detail: Human-readable description of the error.
        • challenge: Applicable authentication scheme and parameters.
        """
        super().__init__()
        self.challenge = challenge


class Forbidden(ResourceError):
    """Raised if authorization to the resource is refused."""

    code, detail = 403, "Forbidden"


class NotFound(ResourceError):
    """Raised if the resource could not be found."""

    code, detail = 404, "Not Found"


class OperationNotAllowed(ResourceError):
    """Raised if the resource does not allow the requested operation."""

    code, detail = 405, "Operation Not Allowed"


class Conflict(ResourceError):
    """Raised if there is a conflict with the current state of the resource."""

    code, detail = 409, "Conflict"


class PreconditionFailed(ResourceError):
    """Raised if the revision provided does not match the current resource."""

    code, detail = 412, "Precondition Failed"


class InternalServerError(ResourceError):
    """Raised if the server encountered an unexpected condition."""

    code, detail = 500, "Internal Server Error"


class Resource:
    """
    Base class for a resource.
    
    Parameters and instance variables:
    • name: Short name of the resource.  [class name in lower case]
    • description: Short description of the resource.  [resource docstring]
    """

    def __init__(self, name=None, description=None):
        super().__init__()
        self.name = name or getattr(self, "name", type(self).__name__.lower())
        self.description = (
            description
            or getattr(self, "description", None)
            or self.__doc__
            or self.__class__.__name__
        )
        self.operations = {}
        for function in (
            attr for attr in (getattr(self, nom) for nom in dir(self)) if callable(attr)
        ):
            try:
                operation = function._roax_operation_
            except:
                continue  # ignore undecorated functions
            self._register_operation(**operation)

    def _register_operation(self, **operation):
        """Register a resource operation."""
        name = operation["name"]
        if name in self.operations:
            raise ValueError(f"operation name already registered: {name}")
        self.operations[name] = _Operation({**operation, "resource": self})


class Resources:
    """
    Provides a single object to hold shared application resources. Resource
    classes expressed as string values are lazily imported and instantiated at the
    time of first access; this solves potential circular dependencies between
    resources.

    Initialize resources with a mapping of resource names to resources. Resources
    are expressed as either string or reference to resource instance. If string,
    class name is expressed as module.class; for example:
    `myapp.resources.v1.FooResource`. Resources classes expressed as strings must
    have `__init__` methods that take no arguments other than `self`.

    Resources are exposed as object attributes. Example initialization:

    ```
    resources = Resources({
        'foo': 'myapp.resources.v1:FooResource',
        'bar': 'myapp.resources.v1:BarResource',
        'qux': qux_instance,
    })
    ```

    Resources can be accessed as attributes or subscript, like:
    `resources.foo` and `resources['bar']`.
    """

    def __init__(self, resources={}):
        super().__init__()
        self._lock = threading.Lock()
        self._resources = {}
        for key, resource in resources.items():
            self.register(key, resource)

    def register(self, key, resource=None):
        """
        Register (or deregister) a resource.

        The resource is expressed as either string or reference to resource
        instance. If string, class name is expressed as `module:class`; for
        example: `myapp.resources.v1:FooResource`. A resource class expressed
        in a string must have an `__init__` method that takes no arguments
        (other than `self`).

        Parameters:
        • key: The key to register or deregister.
        • resource: Resource class name, resource instance, or None to deregister.
        """
        if (
            resource is not None
            and not isinstance(resource, str)
            and not isinstance(resource, Resource)
        ):
            raise ValueError("resource must be str, Resource type or None")
        if isinstance(resource, str) and "." not in resource:
            raise ValueError("resource class name must be fully qualified")
        if resource is None:
            self._resources.pop(key, None)
        else:
            self._resources[key] = resource

    def _resolve(self, key):
        _resources = super().__getattribute__("_resources")
        if isinstance(_resources[key], str):
            with super().__getattribute__("_lock"):
                if isinstance(_resources[key], str):
                    mod, cls = _resources[key].split(":")
                    _resources[key] = getattr(importlib.import_module(mod), cls)()
        return _resources[key]

    def __getattribute__(self, name):
        try:
            return super().__getattribute__("_resolve")(name)
        except KeyError as ke:
            return super().__getattribute__(name)

    def __getitem__(self, key):
        return self._resolve(key)

    def __dir__(self):
        return list(super().__dir__() + self.map.keys())


def _summary(function):
    """
    Derive summary information from a function's docstring or name. The summary is
    the first sentence of the docstring, ending in a period, or if no dostring is
    present, the function's name capitalized.
    """
    if not function.__doc__:
        return f"{function.__name__.capitalize()}."
    result = []
    for word in function.__doc__.split():
        result.append(word)
        if word.endswith("."):
            break
    return " ".join(result)


def authorize(security):
    """
    Peform authorization of the operation. If one security requirement does not
    raise an exception, then authorization is granted. If all security requirements
    raise exceptions, then authorization is denied, and the exception raised by the
    first security requirement is raised.
    """
    exception = None
    for requirement in security or []:
        try:
            requirement.authorize()
            return  # security requirement authorized the operation
        except Exception as e:
            if not exception:
                exception = e  # first exception encountered
    if exception:
        raise exception


def operation(
    *,
    name=None,
    type=None,
    summary=None,
    description=None,
    params=None,
    returns=None,
    security=None,
    publish=True,
    deprecated=False,
):
    """
    Decorate a resource method to register it as a resource operation.

    Parameters:
    • name: Operation name. Required if the operation type is 'query' or 'action'.
    • type: Type of operation being registered.  {'create', 'read', 'update', 'delete', 'action', 'query', 'patch'}
    • summary: Short summary of what the operation does.
    • description: Verbose description of the operation.  [function docstring]
    • params: Mapping of operation parameter names to their schemas.
    • returns: Schema of operation return value.
    • security: Security schemes, one of which must be satisfied to perform the operation.
    • publish: Publish the operation in documentation.
    • deprecated: Declare the operation as deprecated.
    """

    def decorator(function):
        _name = name
        _type = type
        _description = description or function.__doc__ or function.__name__
        __summary = summary or _summary(function)
        if _name is None:
            _name = function.__name__
        if _type is None and _name in {"create", "read", "update", "delete", "patch"}:
            _type = _name
        valid_types = {"create", "read", "update", "delete", "query", "action", "patch"}
        if _type not in valid_types:
            raise ValueError(f"operation type must be one of: {valid_types}")

        def wrapper(wrapped, instance, args, kwargs):
            tags = {"resource": wrapped.__self__.name, "operation": _name}
            with context.push({**tags, "context": "operation"}):
                with roax.monitor.timer({**tags, "name": "operation_duration_seconds"}):
                    with roax.monitor.counter(
                        {**tags, "name": "operation_calls_total"}
                    ):
                        authorize(security)
                        return wrapped(*args, **kwargs)

        decorated = s.validate(params, returns)(wrapt.decorator(wrapper)(function))
        operation = dict(
            function=function.__name__,
            name=_name,
            type=_type,
            summary=__summary,
            description=_description,
            params=s.function_params(function, params),
            returns=returns,
            security=security,
            publish=publish,
            deprecated=deprecated,
        )
        try:
            getattr(function, "__self__")._register_operation(**operation)
        except AttributeError:  # not yet bound to an instance
            function._roax_operation_ = operation  # __init__ will register it
        return decorated

    return decorator
