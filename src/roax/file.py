"""Module to store resources in the filesystem."""

# Copyright © 2017 Paul Bryan.
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import inspect
import json
import os
import roax.schema as s
import roax.resource as r

from collections import ChainMap
from copy import copy
from os.path import expanduser

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

class FileResourceSet(r.ResourceSet):
    """
    A set of resources, stored in the filesystem; each resource is a separate
    file in a directory. This class is appropriate for up to hundreds of
    resources; it is probably not appropriate for thousands or more.

    The way the file is encoded depends on the document schema:
    dict: each resource is stored as a text file containing a JSON object.
    str: each resource stored as a UTF-8 text file.
    bytes: each resource is a file containing binary data. 
    """

    def _filename(self, _id):
        return "{}/{}{}".format(self.dir, _quote(self.id_schema.str_encode(_id)), self.extension)

    def _open(self, _id, mode):
        try:
            encoding = None if isinstance(self.schema, s.bytes) else "utf-8"
            mode = (mode + "b") if isinstance(self.schema, s.bytes) else mode
            file = open(self._filename(_id), mode, encoding=encoding)
            if fcntl:
                fcntl.flock(file.fileno(), fcntl.LOCK_EX)
            return file
        except FileNotFoundError:
            raise r.NotFound("resource not found")
        except FileExistsError:
            raise r.PreconditionFailed("resource already exists")

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
            raise r.InternalServerError("malformed document")
        except s.SchemaError:
            raise r.InternalServerError("document failed schema validation")

    def _write(self, file, _doc, _id):
        file.seek(0)
        file.truncate()
        if isinstance(self.schema, s.dict):
            if "_id" in self.schema.properties:
                _doc = ChainMap({"_id": _id}, _doc)
            json.dump(self.schema.json_encode(_doc), file, separators=(",",":"), ensure_ascii=False)
        else:
            file.write(_doc)    

    @property
    def id_schema(self):
        if isinstance(self.schema, s.dict) and "_id" in self.schema.properties:
            return self.schema.properties["_id"]
        else:
            return s.str()

    def __init__(self, dir, *, schema=None, extension=None, gen_id=None):
        """
        Initialize file resource set.

        dir: the directory to store resource documents in.
        schema: document schema, or declare as class or instance variable.
        extenson: the filename extension to use for each file (including dot).
        gen_id: function to generate _id, or define as method in subclass.
        """
        self.dir = expanduser(dir.rstrip("/"))
        os.makedirs(self.dir, exist_ok=True)
        self.schema = schema or self.schema
        super().__init__()
        self.extension = extension if extension else ""
        if gen_id:
            self.gen_id = gen_id
        self.__doc__ = self.schema.description

        def params(function):
            result = {}
            for param in inspect.signature(function).parameters.values():
                if param.name == "_id":
                    schema = self.id_schema
                elif param.name == "_doc":
                    schema = self.schema
                else:
                    raise s.SchemaError("Method doesn't support {} parameter".format(param.name))
                schema = copy(schema)
                schema.required = param.default is inspect._empty
                if param.default is not inspect._empty:
                    schema.default = param.default
                result[param.name] = schema
            return s.dict(result)

        self.create = r.method(params=params(self.create), returns=s.dict({"_id": self.id_schema}))(self.create)
        self.read = r.method(params=params(self.read), returns=self.schema)(self.read)
        self.update = r.method(params=params(self.update), returns=s.none())(self.update)
        self.delete = r.method(params=params(self.delete), returns=s.none())(self.delete)
        self.query_ids = r.method(params=s.dict({}), returns=s.list(self.id_schema))(self.query_ids)

    def gen_id(self, _doc):
        """
        Generate a new document identifier.
        
        _doc: the document being created.
        """
        raise NotImplementedError()

    def create(self, _doc, _id=None):
        """Create a new resource."""
        if _id is None:
            _id = self.gen_id(_doc)
        try:
            with self._open(_id, "x") as file:
                self._write(file, _doc, _id)
        except r.NotFound:
            raise r.InternalServerError("file resource set directory not found")
        return { "_id": _id }

    def read(self, _id):
        """Read a resource."""
        with self._open(_id, "r") as file:
            _doc = self._read(file)
        if isinstance(self.schema, s.dict) and "_id" in self.schema.properties:
            _doc["_id"] = _id # filename is canonical identifier
        return _doc

    def update(self, _id, _doc):
        """Update a resource."""
        with self._open(_id, "r+") as file:
            self._write(file, _doc, _id)

    def delete(self, _id):
        """Update a resource."""
        try:
            os.remove(self._filename(_id))
        except FileNotFoundError:
            raise r.NotFound("resource not found")

    def query_ids(self):
        """Return all resource identifiers."""
        result = []
        for name in os.listdir(self.dir):
            if name.endswith(self.extension):
                name = name[0:0-len(self.extension)]
                str_id = _unquote(name)
                if name != _quote(str_id): # ignore improperly encoded names
                    continue
                try:
                    result.append(self.id_schema.str_decode(str_id))
                except s.SchemaError:
                    pass # ignore filenames that can't be parsed
        return result
