"""Module to implement resources."""

# Copyright © 2015–2018 Paul Bryan.
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import roax.context as context
import roax.schema as s
import wrapt


class _Operation:
    """A resource operation.""" 
    def __init__(self, **kwargs):
        for k in kwargs:
            self.__setattr__(k, kwargs[k])


class Resource:
    """Base class for a resource."""

    def _register_operation(self, **operation):
        """Register a resource operation."""
        name = operation["name"]
        if name in self.operations:
            raise ValueError("operation already registered: {}".format(name))
        self.operations[name] = _Operation(**operation)

    def __init__(self, name=None, description=None):
        """
        Initialize resource. Arguments can be alternatively declared as class
        or instance variables.

        :param name: Short name of the resource.  (Default: the class name in lower case.)
        :param description: Short description of the resource.  (Default: the resource docstring.)
        """
        super().__init__()
        self.name = name or getattr(self, "name", type(self).__name__.lower())
        self.description = description or getattr(self, "description", None) or self.__doc__ or self.__class__.__name__
        self.operations = {}
        for function in (attr for attr in (getattr(self, nom) for nom in dir(self)) if callable(attr)):
            try:
                operation = function._roax_operation
            except:
                continue  # ignore undecorated functions
            self._register_operation(**{**operation, "resource": self, "function": function})

    def call(self, name, params={}):
        """Call a resource operation."""
        try:
            return self.operations[name].function(**params)
        except KeyError as e:
            raise BadRequest("no such operation: {}".format(name))


def _summary(function):
    """
    Derive summary information from a function's docstring or name. The summary is
    the first sentence of the docstring, ending in a period, or if no dostring is
    present, the function's name capitalized.
    """
    if not function.__doc__:
        return "{}.".format(function.__name__.capitalize())
    result = []
    for word in function.__doc__.split():
        result.append(word)
        if word.endswith("."):
            break
    return " ".join(result)


def authorize(security):
    """
    Peforms authorization of the operation. If one security requirement does not
    raise an exception, then authorization is granted. If all security requirements
    raise exceptions, then authorization is denied, and the exception raised by the
    first security requirement is raised.
    """
    exception = None
    for requirement in security or []:
        try:
            requirement.authorize()
        except Exception as e:
            if not exception:
                exception = e
        else:  # one security requirement authorizes the operation
            return
    if exception:
        raise exception


def operation(
        *, name=None, type=None, summary=None, description=None, params=None,
        returns=None, security=None, publish=True, deprecated=False):
    """
    Decorate a resource function to register it as a resource operation.

    :param name: Operation name. Required if the operation type is "query" or "action".
    :param type: Type of operation being registered {create,read,update,delete,action,query,patch}.
    :param summary: Short summary of what the operation does.
    :param description: Verbose description of the operation (default: function docstring).
    :param params: Mapping of operation's parameter names to their schemas.
    :param returns: Schema of operation's return value.
    :param security: Security schemes, one of which must be satisfied to perform the operation.
    :param publish: Publish the operation in documentation.
    :param deprecated: Declare the operation as deprecated.
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
            raise ValueError("operation type must be one of: {}".format(valid_types))
        def wrapper(wrapped, instance, args, kwargs):
            with context.push(context_type="operation", operation_resource=wrapped.__self__, operation_name=_name):
                authorize(security)
                return wrapped(*args, **kwargs)
        _params = s.function_params(function, params)
        decorated = s.validate(_params, returns)(wrapt.decorator(wrapper)(function))
        operation = dict(function=decorated, name=_name, type=_type,
            summary=__summary, description=_description, params=_params,
            returns=returns, security=security, publish=publish, deprecated=deprecated)
        try:
            resource = getattr(function, "__self__")
            resource._register_operation(**{**operation, "resource": resource})
        except AttributeError:  # not yet bound to an instance
            function._roax_operation = operation  # __init__ will register it
        return decorated
    return decorator


class ResourceError(Exception):
    """Base class for all resource errors."""

    def __init__(self, detail=None, code=None):
        """
        Initialize resource error.

        :param detail: textual description of the error.
        :param code: the HTTP status most closely associated with the error.
        """
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

        :param detail: Human-readable description of the error.
        :param challenge: Applicable authentication scheme and parameters.
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
