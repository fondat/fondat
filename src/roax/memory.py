"""Module to store resource items in files."""

# Copyright © 2018 Paul Bryan.
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from copy import copy
from roax.resource import Resource, Conflict, InternalServerError, NotFound
from threading import Lock


class MemoryResource(Resource):
    """
    Base class for an in-memory resource; all items are stored in a mapping.
    Resource item identifiers must be hashable values.
    """

    def __init__(self, name=None, description=None, mapping=None):
        """
        Initialize in-memory resource.

        :param name: Short name of the resource. (default: class name in lower case)
        :param description: Short description of the resource. (default: resource docstring)
        :param mapping: Mapping to store items in. (default: new dictionary)
        """
        super().__init__(name, description)
        self._lock = Lock()
        self.mapping = mapping or getattr(self, "mapping", {})

    def create(self, id, _body):
        """Create a resource item."""
        with self._lock:
            if id in self.mapping:
                raise Conflict("{} item already exists".format(self.name))
            self.mapping[id] = copy(_body)
        return {"id": id}

    def read(self, id):
        """Read a resource item."""
        try:
            return copy(self.mapping[id])
        except KeyError:
            raise NotFound("{} item not found".format(self.name))

    def update(self, id, _body):
        """Update a resource item."""
        if id not in self.mapping:
            raise NotFound("{} item not found".format(self.name))
        self.mapping[id] = copy(_body)

    def delete(self, id):
        """Delete a resource item."""
        try:
            del self.mapping[id]
        except KeyError:
            raise NotFound("{} item not found".format(self.name))

    def list(self):
        """Return a list of all resource item identifiers."""
        return [k for k in self.mapping.keys()]
