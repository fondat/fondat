"""Module to implement resources."""

import datetime
import importlib
import inspect
import roax.context as context
import roax.monitor
import roax.schema
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
    • detail: Textual description of the error.
    • code: HTTP status most closely associated with the error.
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
    
    Parameters and attributes:
    • name: Short name of the resource.  [class name in lower case]
    • description: Short description of the resource.  [resource docstring]
    • security: Security requirements for resource operations.

    Description be defined in a class variable.

    Security requirements can be defined as a class variable. All resource
    operations inherit the resource's security requirements unless overrriden
    in the operation itself. 
    """

    def __init__(self, name=None, description=None, security=None):
        super().__init__()
        self.name = name or getattr(self, "name", type(self).__name__.lower())
        self.description = (
            description
            or getattr(self, "description", None)
            or self.__doc__
            or self.__class__.__name__
        )
        self.security = (
            security if security is not None else getattr(self, "security", None)
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
            raise ValueError(f"{name} operation name already registered")
        security = (
            operation["security"]
            if operation["security"] is not None
            else self.security
        )
        self.operations[name] = _Operation(
            {**operation, "resource": self, "security": security}
        )


class Resources:
    """
    Provides a single object to hold shared application resources. 

    Resources are expressed as either string or resource instance. If string,
    class name is expressed as "module:class"; for example:
    "myapp.resources.v1:FooResource". Resources classes expressed as strings
    must have __init__ methods that take no arguments other than "self".
    Resource classes expressed as string values are lazily imported and
    instantiated at the time of first access; this solves potential circular
    dependencies between resources.

    Resources are exposed as object attributes. Example initialization:

    resources = Resources({
        "foo": "myapp.resources.v1:FooResource",
        "bar": "myapp.resources.v1:BarResource",
        "qux": qux_instance,
    })

    Resources can be accessed as either attributes or items, like:
    resources.foo and resources["foo"].
    """

    def __init__(self, resources={}):
        super().__init__()
        self._roax_lock = threading.Lock()
        self._roax_resources = {}
        for name, value in resources.items():
            setattr(self, name, value)

    def __getattr__(self, name):
        try:
            return self[name]
        except KeyError:
            raise AttributeError(
                f"{self.__class__.__name__} object has no attribute '{name}'"
            )

    def __setattr__(self, name, value):
        if name.startswith("_roax_") or name in super().__dir__():
            super().__setattr__(name, value)
        else:
            self[name] = value

    def __delattr__(self, name):
        if name.startswith("_roax_") or name in super().__dir__():
            super().__delattr__(name)
        else:
            try:
                del self[name]
            except KeyError:
                raise AttributeError(name)

    def __dir__(self):
        return [*super().__dir__(), *iter(self._roax_resources)]

    def __len__(self):
        return len(self._roax_resources)

    def __getitem__(self, key):
        return self._roax_resolve(key)

    def __setitem__(self, key, value):
        if not isinstance(value, str) and not isinstance(value, Resource):
            raise TypeError("resource must be str or Resource type")
        if isinstance(value, str) and ("." not in value or ":" not in value):
            raise ValueError("resource class name must be fully qualified")
        self._roax_resources[key] = value

    def __delitem__(self, key):
        del self._roax_resources[key]

    def __iter__(self):
        return iter(self._roax_resources)

    def __contains__(self, item):
        return item in self._roax_resources

    def _roax_resolve(self, key):
        resources = self._roax_resources
        if isinstance(resources[key], str):
            with self._roax_lock:
                if isinstance(resources[key], str):
                    mod, cls = resources[key].split(":")
                    resources[key] = getattr(importlib.import_module(mod), cls)()
        return resources[key]


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
    Peform authorization of the operation.
    
    This function executes all of the security requirements associated with
    the operation. If a single security requirement does not raise a
    security exception {Unauthorized, Forbidden}, then this function passes
    and authorization is granted.

    If all security requirements raised a security exception: if one security
    requirement raised Forbidden, then Forbidden is raised; otherwise
    Unauthorized is raised. If a non-security exception is raised, then
    it is re-raised by this function.
    """
    exception = None
    for requirement in security or []:
        try:
            requirement.authorize()
            return  # security requirement authorized the operation
        except Forbidden:
            exception = Forbidden
        except Unauthorized:
            if not exception:
                exception = Unauthorized
        except:
            raise
    if exception:
        raise exception()


def _params(function):
    sig = inspect.signature(function)
    return roax.schema.dict(
        props={k: v for k, v in function.__annotations__.items() if k != "return"},
        required={p.name for p in sig.parameters.values() if p.default is p.empty},
    )


def operation(
    _fn=None,
    *,
    name=None,
    type=None,
    summary=None,
    description=None,
    security=None,
    publish=True,
    deprecated=False,
):
    """
    Decorate a resource method to register it as a resource operation.

    Parameters:
    • name: Operation name. Required if the operation type is "query" or "action".
    • type: Type of operation being registered.  {"create", "read", "update", "delete", "action", "query", "patch"}
    • summary: Short summary of what the operation does.
    • description: Verbose description of the operation.  [function docstring]
    • security: Security requirements for the operation.
    • publish: Publish the operation in documentation.
    • deprecated: Declare the operation as deprecated.
    """

    _valid_types = {"create", "read", "update", "delete", "query", "action", "patch"}

    def decorator(function):
        _name = name
        _type = type
        _description = description or function.__doc__ or function.__name__
        __summary = summary or _summary(function)
        if _name is None:
            _name = function.__name__
        if _type is None and _name in {"create", "read", "update", "delete", "patch"}:
            _type = _name
        if _type not in _valid_types:
            raise ValueError(f"operation type must be one of: {_valid_types}")

        def wrapper(wrapped, instance, args, kwargs):
            operation = wrapped.__self__.operations[wrapped.__name__]
            tags = {"operation": operation.name, "resource": operation.resource.name}
            with context.push({"context": "roax.operation", **tags}):
                with roax.monitor.timer({"name": "operation_duration_seconds", **tags}):
                    with roax.monitor.counter(
                        {"name": "operation_calls_total", **tags}
                    ):
                        authorize(operation.security)
                        return wrapped(*args, **kwargs)

        decorated = roax.schema.validate(wrapt.decorator(wrapper)(function))
        operation = dict(
            function=function.__name__,
            name=_name,
            type=_type,
            summary=__summary,
            description=_description,
            params=_params(function),
            returns=function.__annotations__.get("return"),
            security=security,
            publish=publish,
            deprecated=deprecated,
        )
        try:
            getattr(function, "__self__")._register_operation(**operation)
        except AttributeError:  # not yet bound to an instance
            function._roax_operation_ = operation  # __init__ will register it
        return decorated

    if callable(_fn):
        return decorator(_fn)
    else:
        return decorator
