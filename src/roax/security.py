"""Module to perform resource authentication and authorization."""

# Copyright © 2017–2018 Paul Bryan.
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import binascii

from base64 import b64decode
from roax.context import context, get_context
from roax.resource import Unauthorized


class SecurityRequirement:
    """Performs authorization of resource operations."""

    def __init__(self, scheme=None):
        """
        Initialize security requirement.

        :param scheme: Associated security scheme, if applicable.
        """
        super().__init__()
        self.scheme = scheme
        self.json = {}

    def authorize(self):
        """
        Determine authorization for the operation. Raises an exception if authorization
        is not granted. The exception raised should be a ResourceError (like
        Unauthorized), or be meaningful relative to its associated security scheme.        
        """
        raise NotImplementedError()


class SecurityScheme:
    """Base class for security schemes."""

    def __init__(self, type, *, description=None, **kwargs):
        super().__init__()
        self.type = type
        self.description = description

    @property
    def context(self):
        """Context that the scheme pushes onto the context stack."""
        result = {}
        result["context_type"] = "security"
        result["security_type"] = self.type
        return result

    @property
    def json(self):
        """JSON representation of the security scheme."""
        result = {}
        result["type"] = self.type
        if self.description is not None:
            result["description"] = self.description
        return result


class HTTPSecurityScheme(SecurityScheme):
    """Base class for HTTP authentication security scheme."""

    def __init__(self, scheme, **kwargs):
        super().__init__("http", **kwargs)
        self.scheme = scheme

    @property
    def context(self):
        """Context that the scheme pushes onto the context stack."""
        result = super().context
        result["security_scheme"] = self.scheme
        return result

    @property
    def json(self):
        """JSON representation of the security scheme."""
        result = super().json
        result["http_scheme"] = scheme
        return result


class HTTPBasicSecurityScheme(HTTPSecurityScheme):
    """Base class for HTTP basic authentication security scheme."""

    def Unauthorized(detail=None):
        """Return an Unauthorized exception populated with scheme and realm."""
        return Unauthorized(detail, "Basic realm={}".format(self.realm))

    def __init__(self, realm, **kwargs):
        """TODO: Description."""
        super().__init__("basic", **kwargs)
        self.realm = realm

    def filter(self, request, chain):
        """
        Filters the incoming HTTP request. If the request contains credentials in the
        HTTP Basic authentication scheme, they are passed to the authenticate method.
        If authentication is successful, a context is added to the context stack.  
        """
        auth = None
        if request.authorization and request.authorization[0].lower() == "basic":
            try:
                user_id, password = b64decode(request.authorization[1]).decode().split(":", 1)
            except (binascii.Error, UnicodeDecodeError):
                pass
            auth = self.authenticate(user_id, password)
            if auth:
                with context({**auth, **self.context}):
                    return chain.next(request)
        return chain.next(request)

    def get_context(self):
        """Return the context that this scheme pushes on the stack."""
        return get_context(self.context)

    @property
    def context(self):
        """Context that the scheme pushes onto the context stack."""
        result = super().context
        result["http_realm"] = self.realm
        return result

    def authenticate(user_id, password):
        """
        Perform authentication of credentials supplied in the HTTP request. If
        authentication is successful, a dict is returned, which is added
        to the context that is pushed on the context stack. If authentication
        fails, None is returned. This method should not raise an exception
        unless an unrecoverable error occurs.
        """
        raise NotImplementedError()


class CLISecurityRequirement(SecurityRequirement):
    """
    Security requirement that authorizes an operation if it was initiated (directly
    or indirectly) from the command line interface.
    """

    def authorize(self):
        """Perform authorization of the operation."""
        if not get_context({"context_type": "cli"}):
            raise Unauthorized()


cli = CLISecurityRequirement()
