"""Module to provide read access to static content."""

# Copyright © 2018 Paul Bryan.
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from roax.resource import Resource, operation


class StaticResource(Resource):
    """A resource that serves a static file."""

    def __init__(self, content, schema, name=None, description=None, security=None):
        """
        Initialize the static resource.

        content: the static content to return in a read operation.
        schema: the schema of the static content.
        name: The short name of the resource.
        description: A short description of the resource.
        security: The security requirements to read the resource.
        """
        super().__init__(name, description)
        self.content = content
        self.schema = schema
        self.read.__doc__ = "Read the {} resource.".format(name)
        self.read = operation(params={}, returns=self.schema, security=security)(self.read)

    def read(self):
        """Read the static resource."""
        return self.content
