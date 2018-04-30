"""Module to store resource items in the files."""

# Copyright © 2017–2018 Paul Bryan.
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import inspect
import json
import os
import roax.schema as s

from collections import ChainMap
from copy import copy
from os.path import expanduser
from roax.resource import Resource, Conflict, InternalServerError, NotFound, operation

try:
    import fcntl
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
    A file resource; each item is a stored as a separate file in a directory. This
    class is appropriate for up to hundreds of items; it is probably not
    appropriate for thousands or more.

    The way the item file is encoded depends on the document schema:
    - dict: each resource is stored as a text file containing a JSON object.
    - str: each resource stored as a UTF-8 text file.
    - bytes: each resource is a file containing binary data. 
    """

    def _filename(self, id):
        return "{}/{}{}".format(self.dir, _quote(self.id_schema.str_encode(id)), self.extension)

    def _open(self, id, mode):
        try:
            encoding = None if isinstance(self.schema, s.bytes) else "utf-8"
            mode = (mode + "b") if isinstance(self.schema, s.bytes) else mode
            file = open(self._filename(id), mode, encoding=encoding)
            if fcntl:
                fcntl.flock(file.fileno(), fcntl.LOCK_EX)
            return file
        except FileNotFoundError:
            raise NotFound("resource not found")
        except FileExistsError:
            raise Conflict("resource already exists")

    def _read(self, file):
        file.seek(0)
        try:
            if isinstance(self.schema, s.dict):
                return self.schema.json_decode(json.load(file))
            else:
                result = file.read()
                self.schema.validate(result)
                return result
        except ValueError:
            raise InternalServerError("malformed document")
        except s.SchemaError:
            raise InternalServerError("document failed schema validation")

    def _write(self, file, body, id):
        file.seek(0)
        file.truncate()
        if isinstance(self.schema, s.dict) and self.id_property in self.schema.properties:
            if self.id_property and self.id_property in self.schema.properties:
                body = ChainMap({self.id_property: id}, body)
            json.dump(self.schema.json_encode(body), file, separators=(",",":"), ensure_ascii=False)
        else:
            file.write(body)    

    def __init__(self, dir, name=None, description=None, *, schema=None, extension=None, id_property=None):
        """
        Initialize file resource.

        name: The short name of the resource. Default: the class name in lower case.
        description: A short description of the resource. Default: the resource docstring.
        dir: The directory to store resource documents in.
        schema: Document schema, or declare as class or instance variable.
        extenson: The filename extension to use for each file (including dot).
        id_property: Name of resource identifier property in document schema. Default: "id".
        """
        super().__init__()
        self.schema = schema or self.schema
        self.id_property = id_property or getattr(self, "id_property", "id")
        if not isinstance(self.schema, s.dict) or self.id_property not in self.schema.properties:
            self.id_property = None
        self.id_schema = self.schema.properties[self.id_property] if self.id_property else s.str()
        self.dir = expanduser(dir.rstrip("/"))
        os.makedirs(self.dir, exist_ok=True)
        self.extension = extension or getattr(self, "extension", "")
        self.__doc__ = self.schema.description

        def _p(function):
            result = {}
            for param in inspect.signature(function).parameters.values():
                if param.name == "id":
                    schema = self.id_schema
                elif param.name == "_body":
                    schema = self.schema
                else:
                    raise s.SchemaError("operation doesn't support {} parameter".format(param.name))
                schema = copy(schema)
                schema.required = param.default is inspect._empty
                if param.default is not inspect._empty:
                    schema.default = param.default
                result[param.name] = schema
            return result

        self.create = operation(params=_p(self.create), returns=s.dict({"id": self.id_schema}))(self.create)
        self.read = operation(params=_p(self.read), returns=self.schema)(self.read)
        self.update = operation(params=_p(self.update), returns=None)(self.update)
        self.delete = operation(params=_p(self.delete), returns=None)(self.delete)
        self.query_ids = operation(type="query", params={}, returns=s.list(self.id_schema))(self.query_ids)

    def create(self, _body, id):
        """Create a resource item."""
        try:
            with self._open(id, "x") as file:
                self._write(file, _body, id)
        except NotFound:
            raise InternalServerError("file resource directory not found")
        return { "id": id }

    def read(self, id):
        """Read a resource item."""
        with self._open(id, "r") as file:
            body = self._read(file)
        if isinstance(self.schema, s.dict) and self.id_property in self.schema.properties:
            body[self.id_property] = id  # filename is canonical identifier
        return body

    def update(self, id, _body):
        """Update a resource item."""
        with self._open(id, "r+") as file:
            self._write(file, _body, id)

    def delete(self, id):
        """Delete a resource item."""
        try:
            os.remove(self._filename(id))
        except FileNotFoundError:
            raise NotFound("resource not found")

    def query_ids(self):
        """Return all resource item identifiers."""
        result = []
        for name in os.listdir(self.dir):
            if name.endswith(self.extension):
                name = name[0:0-len(self.extension)]
                str_id = _unquote(name)
                if name != _quote(str_id):  # ignore improperly encoded names
                    continue
                try:
                    result.append(self.id_schema.str_decode(str_id))
                except s.SchemaError:
                    pass  # ignore filenames that can't be parsed
        return result
