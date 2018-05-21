"""Module to store resource items in files."""

# Copyright © 2017–2018 Paul Bryan.
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import json
import os
import roax.schema as s

from copy import copy
from os.path import expanduser
from roax.patch import merge_patch
from roax.resource import Resource, Conflict, InternalServerError, NotFound

try:
    import fcntl  # supported in *nix
except ImportError:
    fcntl = None


_map = [(c, "%{:02X}".format(ord(c))) for c in "%/\\:*?\"<>|"]

def _quote(s):
    """_quote('abc/def') -> 'abc%2Fdef'"""
    for m in _map:
        s = s.replace(m[0], m[1])
    return s

def _unquote(s):
    """_unquote('abc%2Fdef') -> 'abc/def'"""
    if "%" in s:
        for m in _map:
            s = s.replace(m[1], m[0])
    return s


class FileResource(Resource):
    """
    Base class for a file-based resource; each item is a stored as a separate file
    in a directory.

    This class is appropriate for up to hundreds or thousands of items; it is
    probably not appropriate for tens of thousands or more.
    """

    def __init__(self, dir=None, name=None, description=None, schema=None, id_schema=None, extension=None):
        """
        Initialize file resource. All arguments can be alternatively declared as class
        or instance variables.

        :param name: Short name of the resource. (default: the class name in lower case)
        :param description: Short description of the resource. (default: the resource docstring)
        :param dir: Directory to store resource items in.
        :param schema: Schema for resource items.
        ;param id_schema: Schema for resource item identifiers. (default: str)
        :param extenson: Filename extension to use for each file (including dot).
        """
        super().__init__(name, description)
        self.dir = expanduser((dir or self.dir).rstrip("/"))
        self.schema = schema or self.schema
        self.id_schema = id_schema or getattr(self, "id_schema", s.str())
        self.extension = extension or getattr(self, "extension", "")
        os.makedirs(self.dir, exist_ok=True)
        self.__doc__ = self.schema.description

    def create(self, id, _body):
        """Create a resource item."""
        try:
            self._write("xb", id, _body)
        except NotFound:
            raise InternalServerError("file resource directory not found")
        return {"id": id}

    def read(self, id):
        """Read a resource item."""
        return self._read("rb", id)

    def update(self, id, _body):
        """Update a resource item."""
        return self._write("wb", id, _body)

    def delete(self, id):
        """Delete a resource item."""
        try:
            os.remove(self._filename(id))
        except FileNotFoundError:
            raise NotFound("{} item not found: {}".format(self.name, id))

    def list(self):
        """Return a list of all resource item identifiers."""
        result = []
        for name in os.listdir(self.dir):
            if name.endswith(self.extension):
                name = name[:len(name) - len(self.extension)]
                str_id = _unquote(name)
                if name != _quote(str_id):  # ignore improperly encoded names
                    continue
                try:
                    result.append(self.id_schema.str_decode(str_id))
                except s.SchemaError:
                    pass  # ignore filenames that can't be parsed
        return result

    def _filename(self, id):
        """Return the full filename for the specified resource item identifier."""
        return "{}/{}{}".format(self.dir, _quote(self.id_schema.str_encode(id)), self.extension)

    def _open(self, id, mode):
        try:
            file = open(self._filename(id), mode)
            if fcntl:
                fcntl.flock(file.fileno(), fcntl.LOCK_EX)
            return file
        except FileNotFoundError:
            raise NotFound("{} item not found: {}".format(self.name, id))
        except FileExistsError:
            raise Conflict("{} item already exists: {}".format(self.name, id))

    def _read(self, mode, id):
        with self._open(id, mode) as file:
            try:
                result = self.schema.bin_decode(file.read())
                self.schema.validate(result)
            except s.SchemaError as se:
                raise InternalServerError("content read from file failed schema validation") from se
        return result

    def _write(self, mode, id, body):
        with self._open(id, mode) as file:
            file.write(self.schema.bin_encode(body))
