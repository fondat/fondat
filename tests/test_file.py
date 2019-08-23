import pytest
import roax.resource as r
import roax.schema as s

from roax.file import FileResource
from roax.resource import Conflict, InternalServerError, NotFound, operation
from tempfile import TemporaryDirectory
from uuid import uuid4


def test_crud_dict():

    _schema = s.dict(
        properties={"id": s.uuid(), "foo": s.str(), "bar": s.int()},
        required={"foo", "bar"},
    )

    class FooResource(FileResource):

        schema = _schema
        id_schema = _schema.properties["id"]

        @operation(
            params={"_body": _schema}, returns=s.dict({"id": _schema.properties["id"]})
        )
        def create(self, _body):
            return super().create(uuid4(), _body)

        @operation(params={"id": _schema.properties["id"]}, returns=_schema)
        def read(self, id):
            return {**super().read(id), "id": id}

        @operation(params={"id": _schema.properties["id"], "_body": _schema})
        def update(self, id, _body):
            return super().update(id, _body)

        @operation(params={"id": _schema.properties["id"]})
        def delete(self, id):
            return super().delete(id)

        @operation(type="query", returns=s.list(_schema.properties["id"]))
        def list(self):
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

        @operation(
            params={"id": s.str(), "_body": _schema}, returns=s.dict({"id": s.str()})
        )
        def create(self, id, _body):
            return super().create(id, _body)

        @operation(params={"id": s.str()}, returns=_schema)
        def read(self, id):
            return super().read(id)

        @operation(params={"id": s.str(), "_body": _schema})
        def update(self, id, _body):
            return super().update(id, _body)

        @operation(params={"id": s.str()})
        def delete(self, id):
            return super().delete(id)

        @operation(type="query", returns=s.list(s.str()))
        def list(self):
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
