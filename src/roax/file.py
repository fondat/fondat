
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

class FileResourceSet(r.ResourceSet):
    """TODO: Description."""

    def _filename(self, _id):
        _id = self._id_schema.str_encode(_id)
        for c in '/\\:*?"<>|':
            if c in _id:
                raise r.ResourceException("_id contains invalid character")
        return "{}/{}.json".format(self.dir, _id)

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

    def __init__(self, dir, strict_rev=False):
        super().__init__()
        self.dir = dir.rstrip("/")
        self.strict_rev = strict_rev
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
        self.query_ids = r.method(returns=s.list(self.schema.properties["_id"]))(self.query_ids)
        self.query_all = r.method(returns=s.list(self.schema))(self.query_all)

    @property
    def _id_schema(self):
        return self.schema.properties["_id"]

    @property
    def _rev_schema(self):
        return self.schema.properties.get("_rev")

    def gen_id(self):
        """Generate a new identifier. Base class implementation returns None."""
        return None

    def gen_rev(self, _doc):
        """Generate a revision value for the document. Base class implementation returns None."""
        return None

    def create(self, _doc, _id=None):
        """Create a new resource."""
        if _id is None:
            _id = self.gen_id()
            if _id is None:
                raise r.InternalServerError("cannot generate _id")
        _rev = self.gen_rev(_doc) if self._rev_schema else None
        if self._rev_schema and _rev is None:
            raise r.InternalServerError("cannot generate _rev")
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
        _doc["_id"] = _id
        return _doc

    def update(self, _id, _doc, _rev=None):
        """Update a resource."""
        if self.strict_rev and _rev is None:
            raise r.BadRequest("_rev is required")
        with self._open(_id, "r+") as file:
            if self._rev_schema:
                old = self._read(file)
                if _rev is not None and old["_rev"] != _rev:
                    raise r.PreconditionFailed("_rev does not match")
                _rev = self.gen_rev(old)
            self._write(file, _doc, _id, _rev)
        return { "_rev": _rev } if _rev else None

    def delete(self, _id, _rev=None):
        """Update a resource."""
        if self.strict_rev and _rev is None:
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
                try:
                    result.append(self._id_schema.str_decode(name[0:-5]))
                except s.SchemaError:
                    pass # ignore filenames that can't be parsed
        return result

    def query_all(self):
        """Return all resources."""
        result = []
        for _id in self.query_ids():
            try:
                result.append(self.read(_id))
            except r.NotFound:
                pass # ignore resources deleted since query_ids
        return result
