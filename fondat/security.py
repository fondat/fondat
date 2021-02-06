"""Module for authentication and authorization of resource operations."""

import fondat.context

from fondat.error import UnauthorizedError


class SecurityScheme:
    """
    Base class for security schemes.

    Parameters and attributes:
    • name: name of security scheme
    • type: type of security scheme
    • description: a short description for the security scheme

    A security scheme is required if security requirements and security schemes should be
    published in OpenAPI documents.
    """

    def __init__(self, name: str, type: str, *, description: str = None):
        super().__init__()
        self.name = name
        self.type = type
        self.description = description

    # TODO: move to OpenAPI
    @property
    def json(self):
        """JSON representation of the security scheme."""
        result = {}
        result["type"] = self.type
        if self.description is not None:
            result["description"] = self.description
        return result


class SecurityRequirement:
    """
    Base class to perform authorization of resource operations.

    Parameters:
    • scheme: security scheme to associate with the security requirement
    • scope: scheme-specific scope names required for authorization
    """

    def __init__(self, scheme: SecurityScheme = None, scopes=[]):
        super().__init__()
        self.scheme = scheme
        self.scopes = scopes

    async def authorize(self):
        """
        Raise an exception if no authorization to perform the operation is granted.

        If no valid context or credentials are established, then UnauthorizedError should be
        raised. If a valid context or credentials are established, but are insufficient to
        provide authorization for the operation, then ForbiddenError should be raised.
        """
        raise NotImplementedError

    # TODO: move to OpenAPI
    # If the requirement is associated with a security scheme, both the security requirement and
    # the security scheme will be included in any generated OpenAPI document.
    @property
    def json(self):
        if self.scheme:
            return {self.scheme.name: self.scopes}


class ContextSecurityRequirement(SecurityRequirement):
    """
    Authorizes an operation if a context with the specified properies exists on the context
    stack.
    """

    def __init__(self, *args, **varargs):
        """
        Initialize context security requirement.

        The context value to search for can be expressed as either of the following:
        • ContextSecurityRequirement(mapping): mapping object's key-value pairs
        • ContextSecurityRequirement(**kwargs): name-value pairs in keyword arguments

        """
        super().__init__()
        self.context = dict(*args, **varargs)

    async def authorize(self):
        if not fondat.context.last(self.context):
            raise UnauthorizedError


class CallerSecurityRequirement(SecurityRequirement):
    """
    Authorizes an operation if it's called by another operation.

    Parameters:
    • resource: String containing module and class name of operation resource.
    • operation: String containing name of operation.
    """

    def __init__(self, resource, operation):
        self.resource = resource
        self.operation = operation

    async def authorize(self):
        ctx = fondat.context.last(context="fondat.operation")
        if ctx["resource"] != self.resource or ctx["operation"] != self.operation:
            raise UnauthorizedError
