"""Module to provide read access to static content."""

# Copyright © 2018 Paul Bryan.
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from roax.resource import Resource, operation


class StaticResource(Resource):
    """A resource that serves static content."""

    def __init__(self, content, schema, name=None, description=None, security=None):
        """
        Initialize the static resource.

        :param content: Static content to return in a read operation.
        :param schema: Schema of the static content.
        :param name: Short name of the resource.
        :param description: Short description of the resource.
        :param security: Security requirements to read the resource.
        """
        super().__init__(name, description)
        self.content = content
        self.schema = schema
        description = "Read the {} resource.".format(self.name)
        self.read = operation(params={}, returns=self.schema, description=description, security=security)(self.read)

    def read(self):
        """Read the static resource."""
        return self.content
