"""Module to provide read access to static content."""

# Copyright © 2018 Paul Bryan.
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from roax.resource import Resource, operation


class StaticResource(Resource):
    """
    TODO: Description.
    """

    def __init__(self, content, schema, name=None, description=None):
        """
        TODO: Description.

        content: the static content to return in a read operation.
        schema: the schema of the static content.
        """
        super().__init__(name, description)
        self.content = content
        self.schema = schema
        self.read = operation(params={}, returns=self.schema)(self.read)

    def read(self):
        """Read the static resource."""
        return self.content
