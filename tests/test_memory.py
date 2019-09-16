import pytest
import roax.resource as r
import roax.schema as s

from roax.memory import MemoryResource
from roax.resource import BadRequest, Conflict, NotFound, operation
from time import sleep
from uuid import uuid4


def test_crud_dict():

    _schema = s.dict({"id": s.uuid(), "foo": s.str(), "bar": s.int()}, "foo bar")

    class FooResource(MemoryResource):
        @operation()
        def create(self, _body: _schema) -> s.dict({"id": _schema.props["id"]}):
            return super().create(uuid4(), _body)

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

    rs = FooResource()
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

    class StrResource(MemoryResource):
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

    mr = StrResource()
    body = "你好，世界!"
    id = "hello_world"
    assert mr.create(id, body)["id"] == id
    assert mr.list() == [id]
    assert mr.read(id) == body
    body = "Goodbye world!"
    mr.update(id, body)
    assert mr.read(id) == body
    mr.delete(id)
    assert mr.list() == []


def test_crud_bytes():

    mr = MemoryResource()
    body = b"\x00\x0e\0x01\0x01\0x00"
    id = "binary"
    assert mr.create(id, body)["id"] == id
    assert mr.list() == [id]
    assert mr.read(id) == body
    body = bytes((1, 2, 3, 4, 5))
    mr.update(id, body)
    assert mr.read(id) == body
    mr.delete(id)
    assert mr.list() == []


def test_create_conflict():
    mr = MemoryResource()
    mr.create("1", "foo")
    with pytest.raises(Conflict):
        mr.create("1", "foo")


def test_read_notfound():
    mr = MemoryResource()
    with pytest.raises(NotFound):
        mr.read("1")


def test_delete_notfound():
    mr = MemoryResource()
    with pytest.raises(NotFound):
        mr.delete("1")


def test_clear():
    mr = MemoryResource()
    mr.create("1", "foo")
    mr.create("2", "bar")
    assert len(mr.list()) == 2
    mr.clear()
    assert len(mr.list()) == 0


def test_size_limit():
    mr = MemoryResource(size=1)
    mr.create("1", "foo")
    with pytest.raises(BadRequest):
        mr.create("2", "bar")


def test_size_evict():
    mr = MemoryResource(size=2, evict=True)
    mr.create("1", "foo")
    mr.create("2", "bar")
    mr.create("3", "qux")
    assert set(mr.list()) == {"2", "3"}


def test_ttl():
    mr = MemoryResource(ttl=0.1)
    mr.create("1", "foo")
    mr.read("1")
    sleep(0.2)
    with pytest.raises(NotFound):
        read = mr.read("1")
