"""Module for authentication and authorization to resource operations."""

import roax.context
import roax.resource


class SecurityRequirement:
    """
    Base class to perform authorization of resource operations.

    Parameters:
    • scheme: Security scheme to associate with the security requirement.
    • scope: Scheme-specific scope names required for authorization.

    If the requirement is associated with a security scheme, both the security
    requirement and the security scheme will be included in any generated
    OpenAPI document.
    """

    def __init__(self, scheme=None, scopes=[]):
        super().__init__()
        self.scheme = scheme
        self.scopes = scopes

    def authorize(self):
        """
        Raise an exception if no authorization to perform the operation is
        granted.

        If no valid context or credentials are established, then the
        roax.resource.Unauthorized exception should be raised.

        If a valid context or credentials are established, but are
        insufficient to provide authorization for the operation, then
        the roax.resource.Forbidden exception should be raised. 
        """
        raise NotImplementedError

    @property
    def json(self):
        if self.scheme:
            return {self.scheme.name: self.scopes}


class SecurityScheme:
    """
    Base class for security schemes.

    Parameters:    
    • name: Name of security scheme.
    • type: Type of security scheme.
    • description: A short description for the security scheme.

    A security scheme is required if security requirements and security schemes
    should be published in OpenAPI documents.
    """

    def __init__(self, name, type, *, description=None, **kwargs):
        super().__init__()
        self.name = name
        self.type = type
        self.description = description

    @property
    def json(self):
        """JSON representation of the security scheme."""
        result = {}
        result["type"] = self.type
        if self.description is not None:
            result["description"] = self.description
        return result


class ContextSecurityRequirement(SecurityRequirement):
    """
    Authorizes an operation if a context with the specified properies exists on the
    context stack.
    """

    def __init__(self, *args, **varargs):
        """
        Initialize context security requirement.

        The context value to search for can be expressed as follows:
        • ContextSecurityRequirement(mapping): Mapping object's key-value pairs.
        • ContextSecurityRequirement(**kwargs): Name-value pairs in keyword arguments. 

        """
        super().__init__()
        self.context = dict(*args, **varargs)

    def authorize(self):
        if not roax.context.last(self.context):
            raise roax.resource.Unauthorized


class CLISecurityRequirement(ContextSecurityRequirement):
    """
    Security requirement that authorizes an operation if it was initiated (directly
    or indirectly) from the command line interface.
    """

    def __init__(self):
        super().__init__(context="cli")


class NestedOperationSecurityRequirement(SecurityRequirement):
    """
    Authorizes an operation if it's called (directly or indirectly) from another
    operation.
    """

    def authorize(self):
        if len(roax.context.find(context="operation")) < 2:
            raise roax.resource.Unauthorized


cli = CLISecurityRequirement()

nested = NestedOperationSecurityRequirement()
