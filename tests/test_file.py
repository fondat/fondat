import bz2
import gzip
import lzma
import pytest
import uuid
import zlib

from dataclasses import make_dataclass
from fondat.error import InternalServerError, NotFoundError
from fondat.file import directory_resource, file_resource
from fondat.paging import paginate
from fondat.resource import operation
from tempfile import TemporaryDirectory


pytestmark = pytest.mark.asyncio


async def test_compression():
    DC = make_dataclass("DC", (("key", str), ("foo", str), ("bar", int)))
    for algorithm in (None, bz2, gzip, lzma, zlib):
        with TemporaryDirectory() as dir:
            dr = directory_resource(dir, str, DC, compress=algorithm)
            r1 = DC(key="id1", foo="hello", bar=1)
            await dr["id1"].put(r1)
            r2 = await dr["id1"].get()
            assert r2 == r1


async def test_crud_dict():
    DC = make_dataclass("DC", (("key", str), ("foo", str), ("bar", int)))
    with TemporaryDirectory() as dir:
        dr = directory_resource(dir, str, DC)
        key = "id1"
        r1 = DC(key=key, foo="hello", bar=1)
        await dr[key].put(r1)
        assert (await dr.get()).items == [r1.key]
        r2 = await dr[key].get()
        assert r1 == r2
        r1.bar = 2
        await dr[key].put(r1)
        r2 = await dr[key].get()
        assert r1 == r2
        await dr[key].delete()
        assert (await dr.get()).items == []


async def test_crud_str():
    with TemporaryDirectory() as dir:
        dr = directory_resource(dir, str, str)
        key = "hello_world"
        value = "你好，世界!"
        await dr[key].put(value)
        assert (await dr.get()).items == [key]
        assert await dr[key].get() == value
        value = "さようなら世界！"
        await dr[key].put(value)
        assert await dr[key].get() == value
        await dr[key].delete()
        assert (await dr.get()).items == []


async def test_crud_bytes():
    with TemporaryDirectory() as dir:
        dr = directory_resource(dir, str, bytes, extension=".bin")
        key = "binary"
        value = b"\x00\x0e\x01\x01\x00"
        await dr[key].put(value)
        assert (await dr.get()).items == [key]
        assert await dr[key].get() == value
        value = bytes((1, 2, 3, 4, 5))
        await dr[key].put(value)
        assert await dr[key].get() == value
        await dr[key].delete()
        assert (await dr.get()).items == []


async def test_crud_uuid_key():
    with TemporaryDirectory() as dir:
        dr = directory_resource(dir, uuid.UUID, bytes, extension=".bin")
        key = uuid.UUID("74e47a84-183c-43d3-b934-3568504a7459")
        value = b"\x00\x0e\x01\x01\x00"
        await dr[key].put(value)
        with open(f"{dir}/{str(key)}.bin", "rb") as file:
            assert file.read() == value
        assert (await dr.get()).items == [key]
        assert await dr[key].get() == value
        value = bytes((1, 2, 3, 4, 5))
        await dr[key].put(value)
        assert await dr[key].get() == value
        await dr[key].delete()
        assert (await dr.get()).items == []


async def test_quote_unquote():
    with TemporaryDirectory() as dir:
        dr = directory_resource(dir, str, bytes, extension=".bin")
        key = "resource%identifier"
        value = b"body"
        await dr[key].put(value)
        await dr[key].get() == value
        await dr[key].delete()


async def test_invalid_directory():
    with TemporaryDirectory() as dir:
        dr = directory_resource(dir, str, bytes, extension=".bin")
    # directory should now be deleted underneath the resource
    key = "resource%identifier"
    value = b"body"
    with pytest.raises(InternalServerError):
        await dr[key].put(value)
    with pytest.raises(NotFoundError):
        await dr[key].get()
    with pytest.raises(NotFoundError):
        await dr[key].delete()
    with pytest.raises(InternalServerError):
        await dr.get()


async def test_decode_error():
    with TemporaryDirectory() as dir:
        dr = directory_resource(dir, str, int, extension=".int")
        await dr["1"].put(1)
        with open(f"{dir}/1.int", "w") as f:
            f.write("a")
        with pytest.raises(InternalServerError):
            await dr["1"].get()


async def test_quotable():
    with TemporaryDirectory() as dir:
        dr = directory_resource(dir, str, str)
        key = "1%2F2"
        value = "Value"
        await dr[key].put(value)
        assert await dr[key].get() == value


async def test_read_only():
    with TemporaryDirectory() as dir:
        path = f"{dir}/test.txt"
        content = b"content"
        with open(path, "wb") as file:
            file.write(content)
        fr = file_resource(path, value_type=bytes, read_only=True)
        assert await fr.get() == content
        with pytest.raises(AttributeError):
            await fr.put(b"nope")
        with pytest.raises(AttributeError):
            await fr.delete()


async def test_pagination():
    with TemporaryDirectory() as dir:
        count = 1000
        for n in range(0, count):
            with open(f"{dir}/{n:04d}.txt", "w") as file:
                file.write(f"{n:04d}")
        dr = directory_resource(path=dir, key_type=str, value_type=str, extension=".txt")
        page = await dr.get(limit=100)
        assert len(page.items) == 100
        assert page.remaining == count - 100
        page = await dr.get(limit=100, cursor=page.cursor)
        assert len(page.items) == 100
        assert page.remaining == count - 200
        assert len([v async for v in paginate(dr.get)]) == count
