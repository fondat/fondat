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
    A set of resources, backed by files in the filesystem.
    Appropriate for hundreds of resources; probably inappropriate
    for thousands or more.
    """

    @property
    def _id_schema(self):
        return self.schema.properties["_id"]

    @property
    def _rev_schema(self):
        return self.schema.properties.get("_rev")

    def _filename(self, _id):
        return "{}/{}.json".format(self.dir, _quote(self._id_schema.str_encode(_id)))

    def _open(self, _id, mode):
        try:
            file = open(self._filename(_id), mode, encoding="utf-8")
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
            return self.schema.json_decode(json.load(file))
        except ValueError:
            raise r.InternalServerError("malformed JSON document")
        except s.SchemaError:
            raise r.InternalServerError("document failed schema validation")

    def _write(self, file, _doc, _id, _rev):
        file.seek(0)
        file.truncate()
        _doc = ChainMap({"_id": _id}, _doc)
        if self._rev_schema and _rev is not None:
            _doc["_rev"] = _rev
        json.dump(self.schema.json_encode(_doc), file, separators=(",",":"), ensure_ascii=False)

    def __init__(self, dir, mkdir=True, rev=False, schema=None, gen_id=None, gen_rev=None):
        """
        Initialize file resource set.

        dir -- the directory to store JSON resource documents in.
        mkdir -- automatically make the directory if it does not exist.
        rev -- require preconditions for update operations.
        schema -- document schema; or declare as class or instance variable.
        gen_id -- function to generate _id; or define as method in subclass.
        gen_rev -- function to generate _rev value; or define as method in subclass.
        """
        if schema:
            self.schema = schema
        super().__init__()
        self.dir = dir.rstrip("/")
        if mkdir:
            os.makedirs(self.dir, exist_ok=True)
        self.rev = rev
        if gen_id:
            self.gen_id = gen_id
        if gen_rev:
            self.gen_rev = gen_rev

        def params(function):
            result = {}
            sig = inspect.signature(function)
            for name in ["_id", "_rev", "_doc"]:
                param = sig.parameters.get(name)
                schema = self.schema if name == "_doc" else self.schema.properties.get(name)
                if schema and param:
                    schema = copy(schema)
                    schema.required = param.default is inspect._empty
                    if param.default is not inspect._empty:
                        schema.default = param.default
                    result[name] = schema
            return s.dict(result)

        def returns(names):
            result={}
            for name in names:
                schema = self.schema.properties.get(name)
                if schema:
                    result[name] = schema
            return s.dict(result) if len(result) > 0 else None

        self.create = r.method(params=params(self.create), returns=returns(["_id","_rev"]))(self.create)
        self.read = r.method(params=params(self.read), returns=self.schema)(self.read)
        self.update = r.method(params=params(self.update), returns=returns(["_rev"]))(self.update)
        self.delete = r.method(params=params(self.delete))(self.delete)
        self.query_ids = r.method(params=s.dict({}), returns=s.list(self.schema.properties["_id"]))(self.query_ids)
        self.query_docs = r.method(params=s.dict({}), returns=s.list(self.schema))(self.query_docs)

    def gen_id(self, _doc):
        """
        Generate a new document identifier.
        
        _doc -- the document being created.
        """
        raise NotImplementedError()

    def gen_rev(self, old, new):
        """
        Generate a revision value for a document being written.

        old -- the old document value; None if new document.
        new -- the new document value being written.
        """
        raise NotImplementedError()

    def create(self, _doc, _id=None):
        """Create a new resource."""
        if _id is None:
            _id = self.gen_id(_doc)
        _rev = self.gen_rev(None, _doc) if self._rev_schema else None
        try:
            with self._open(_id, "x") as file:
                self._write(file, _doc, _id, _rev)
        except r.NotFound:
            raise r.InternalServerError("file resource set directory not found")
        result = { "_id": _id }
        if _rev:
            result["_rev"] = _rev
        return result

    def read(self, _id):
        """Read a resource."""
        with self._open(_id, "r") as file:
            _doc = self._read(file)
        _doc["_id"] = _id # filename is canonical identifier
        return _doc

    def update(self, _id, _doc, _rev=None):
        """Update a resource."""
        if self.rev and _rev is None:
            raise r.BadRequest("_rev is required")
        with self._open(_id, "r+") as file:
            if self._rev_schema:
                old = self._read(file)
                if _rev is not None and old["_rev"] != _rev:
                    raise r.PreconditionFailed("_rev does not match")
                _rev = self.gen_rev(old, _doc)
            self._write(file, _doc, _id, _rev)
        return { "_rev": _rev } if _rev else None

    def delete(self, _id, _rev=None):
        """Update a resource."""
        if self.rev and _rev is None:
            raise r.BadRequest("_rev is required")
        if _rev:
            with self._open(_id, "r") as file:
                old = self._read(file)
                if old["_rev"] != _rev:
                    raise r.PreconditionFailed("_rev does not match")            
        try:
            os.remove(self._filename(_id))
        except FileNotFoundError:
            raise r.NotFound("resource not found")

    def query_ids(self):
        """Return all resource identifiers."""
        result = []
        for name in os.listdir(self.dir):
            if name.endswith(".json"):
                name = name[0:-5]
                str_id = _unquote(name)
                if name == _quote(str_id): # ignore improperly encoded names
                    try:
                        result.append(self._id_schema.str_decode(str_id))
                    except s.SchemaError:
                        pass # ignore filenames that can't be parsed
        return result

    def query_docs(self):
        """Return all resource documents."""
        result = []
        for _id in self.query_ids():
            try:
                result.append(self.read(_id))
            except r.NotFound:
                pass # ignore resources deleted since query_ids
        return result
