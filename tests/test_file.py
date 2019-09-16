import pytest
import roax.resource as r
import roax.schema as s

from roax.file import FileResource
from roax.resource import Conflict, InternalServerError, NotFound, operation
from tempfile import TemporaryDirectory
from uuid import uuid4


def test_crud_dict():

    _schema = s.dict({"id": s.uuid(), "foo": s.str(), "bar": s.int()}, "foo bar")

    class FooResource(FileResource):

        schema = _schema
        id_schema = _schema.props["id"]

        @operation()
        def create(self, _body: _schema) -> s.dict({"id": _schema.props["id"]}):
            return super().create(uuid4(), _body)

        @operation()
        def read(self, id: _schema.props["id"]) -> _schema:
            return {**super().read(id), "id": id}

        @operation()
        def update(self, id: _schema.props["id"], _body: _schema):
            return super().update(id, _body)

        @operation()
        def delete(self, id: _schema.props["id"]):
            return super().delete(id)

        @operation(type="query")
        def list(self) -> s.list(_schema.props["id"]):
            return super().list()

    with TemporaryDirectory() as dir:
        rs = FooResource(dir)
        r1 = {"foo": "hello", "bar": 1}
        id = rs.create(r1)["id"]
        r1["id"] = id
        r2 = rs.read(id)
        assert r1 == r2
        r1["bar"] = 2
        rs.update(id, r1)
        r2 = rs.read(id)
        assert r1 == r2
        rs.delete(id)
        assert rs.list() == []


def test_crud_str():

    _schema = s.str()

    class FRS(FileResource):

        schema = _schema

        @operation()
        def create(self, id: s.str(), _body: _schema) -> s.dict({"id": s.str()}):
            return super().create(id, _body)

        @operation()
        def read(self, id: s.str()) -> _schema:
            return super().read(id)

        @operation()
        def update(self, id: s.str(), _body: _schema):
            return super().update(id, _body)

        @operation()
        def delete(self, id: s.str()):
            return super().delete(id)

        @operation(type="query")
        def list(self) -> s.list(s.str()):
            return super().list()

    with TemporaryDirectory() as dir:
        frs = FRS(dir, extension=".txt")
        body = "你好，世界!"
        id = "hello_world"
        assert frs.create(id, body)["id"] == id
        assert frs.list() == [id]
        assert frs.read(id) == body
        body = "Goodbye world!"
        frs.update(id, body)
        assert frs.read(id) == body
        frs.delete(id)
        assert frs.list() == []


def test_crud_bytes():
    with TemporaryDirectory() as dir:
        frs = FileResource(dir, schema=s.bytes(), extension=".bin")
        body = b"\x00\x0e\0x01\0x01\0x00"
        id = "binary"
        assert frs.create(id, body)["id"] == id
        assert frs.list() == [id]
        assert frs.read(id) == body
        body = bytes((1, 2, 3, 4, 5))
        frs.update(id, body)
        assert frs.read(id) == body
        frs.delete(id)
        assert frs.list() == []


def test_quote_unquote():
    with TemporaryDirectory() as dir:
        fr = FileResource(dir, schema=s.bytes(), extension=".bin")
        body = b"body"
        id = "resource%identifier"
        assert fr.create(id, body)["id"] == id
        fr.delete(id)


def test_invalid_directory():
    with TemporaryDirectory() as dir:
        fr = FileResource(dir, schema=s.bytes(), extension=".bin")
    # directory should now be deleted underneath the resource
    body = b"body"
    id = "resource%identifier"
    with pytest.raises(InternalServerError):
        fr.create(id, body)
    with pytest.raises(NotFound):
        fr.read(id)
    with pytest.raises(NotFound):
        fr.update(id, body)
    with pytest.raises(NotFound):
        fr.delete(id)
    with pytest.raises(InternalServerError):
        fr.list()


def test_create_conflict():
    with TemporaryDirectory() as dir:
        fr = FileResource(dir, schema=s.str(), extension=".bin")
        fr.create("1", "foo")
        with pytest.raises(Conflict):
            fr.create("1", "foo")


def test_read_schemaerror():
    with TemporaryDirectory() as dir:
        fr = FileResource(dir, schema=s.int(), extension=".int")
        fr.create("1", 1)
        with open("{}/1.int".format(dir), "w") as f:
            f.write("a")
        with pytest.raises(InternalServerError):
            fr.read("1")


def test_quotable():
    with TemporaryDirectory() as dir:
        fr = FileResource(dir, schema=s.str())
        id = "1%2F2"
        value = "Value"
        fr.create(id, value)
        assert fr.read(id) == value
