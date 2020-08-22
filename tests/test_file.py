import fondat.resource as r
import fondat.schema as s
import pytest

import bz2
import gzip
import lzma
import zlib


from fondat.file import file_resource
from fondat.resource import Conflict, InternalServerError, NotFound, operation
from tempfile import TemporaryDirectory


pytestmark = pytest.mark.asyncio


async def test_compression():
    schema = s.dict({"id": s.str(), "foo": s.str(), "bar": s.int()}, "foo bar")
    for algorithm in (None, bz2, gzip, lzma, zlib):
        with TemporaryDirectory() as dir:
            rs = file_resource(dir, schema, compress=algorithm)()
            r1 = {"id": "id1", "foo": "hello", "bar": 1}
            await rs.put(r1["id"], r1)
            r2 = await rs.get(r1["id"])
            assert r2 == r1


async def test_gpdl_dict():
    schema = s.dict({"id": s.str(), "foo": s.str(), "bar": s.int()}, "foo bar")
    with TemporaryDirectory() as dir:
        fr = file_resource(dir, schema)()
        r1 = {"id": "id1", "foo": "hello", "bar": 1}
        id = r1["id"]
        await fr.put(id, r1)
        assert await fr.list() == [id]
        r2 = await fr.get(id)
        assert r1 == r2
        r1["bar"] = 2
        await fr.put(id, r1)
        r2 = await fr.get(id)
        assert r1 == r2
        await fr.delete(id)
        assert await fr.list() == []


async def test_gpdl_str():
    with TemporaryDirectory() as dir:
        fr = file_resource(dir, s.str())()
        data = "你好，世界!"
        id = "hello_world"
        await fr.put(id, data)
        assert await fr.list() == [id]
        assert await fr.get(id) == data
        data = "Goodbye world!"
        await fr.put(id, data)
        assert await fr.get(id) == data
        await fr.delete(id)
        assert await fr.list() == []


async def test_gpdl_bytes():
    with TemporaryDirectory() as dir:
        fr = file_resource(dir, schema=s.bytes(), extension=".bin")()
        data = b"\x00\x0e\x01\x01\x00"
        id = "binary"
        await fr.put(id, data)
        assert await fr.list() == [id]
        assert await fr.get(id) == data
        data = bytes((1, 2, 3, 4, 5))
        await fr.put(id, data)
        assert await fr.get(id) == data
        await fr.delete(id)
        assert await fr.list() == []


async def test_quote_unquote():
    with TemporaryDirectory() as dir:
        fr = file_resource(dir, schema=s.bytes(), extension=".bin")()
        data = b"body"
        id = "resource%identifier"
        await fr.put(id, data)
        await fr.get(id) == data
        await fr.delete(id)


async def test_invalid_directory():
    with TemporaryDirectory() as dir:
        fr = file_resource(dir, schema=s.bytes(), extension=".bin")()
    # directory should now be deleted underneath the resource
    data = b"body"
    id = "resource%identifier"
    with pytest.raises(InternalServerError):
        await fr.put(id, data)
    with pytest.raises(NotFound):
        await fr.get(id)
    with pytest.raises(NotFound):
        await fr.delete(id)
    with pytest.raises(InternalServerError):
        await fr.list()


async def test_schemaerror():
    with TemporaryDirectory() as dir:
        fr = file_resource(dir, schema=s.int(), extension=".int")()
        await fr.put("1", 1)
        with open("{}/1.int".format(dir), "w") as f:
            f.write("a")
        with pytest.raises(InternalServerError):
            await fr.get("1")


async def test_quotable():
    with TemporaryDirectory() as dir:
        fr = file_resource(dir, s.str())()
        id = "1%2F2"
        value = "Value"
        await fr.put(id, value)
        assert await fr.get(id) == value
