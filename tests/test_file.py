import bz2
import gzip
import importlib.resources
import lzma
import pytest
import uuid
import zlib

from dataclasses import make_dataclass
from fondat.error import InternalServerError, NotFoundError
from fondat.file import directory_resource, file_resource
from fondat.pagination import paginate
from fondat.stream import BytesStream
from tempfile import TemporaryDirectory


pytestmark = pytest.mark.asyncio


async def test_compression(tmp_path):
    DC = make_dataclass("DC", (("key", str), ("foo", str), ("bar", int)))
    for algorithm in (None, bz2, gzip, lzma, zlib):
        path = tmp_path / (algorithm.__name__ if algorithm else "None")
        path.mkdir()
        dr = directory_resource(path=path, value_type=DC, compress=algorithm, writeable=True)
        r1 = DC(key="id1", foo="hello", bar=1)
        await dr["id1"].put(r1)
        r2 = await dr["id1"].get()
        assert r2 == r1


async def test_crud_dict(tmp_path):
    DC = make_dataclass("DC", (("key", str), ("foo", str), ("bar", int)))
    dr = directory_resource(path=tmp_path, value_type=DC, writeable=True)
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


async def test_crud_str(tmp_path):
    dr = directory_resource(path=tmp_path, value_type=str, writeable=True)
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


async def test_crud_missing_dir(tmp_path):
    with pytest.raises(FileNotFoundError):
        dr = directory_resource(tmp_path / "missing")


async def test_crud_bytes(tmp_path):
    dr = directory_resource(path=tmp_path, value_type=bytes, extension=".bin", writeable=True)
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


async def test_crud_uuid_key(tmp_path):
    dir = str(tmp_path)
    dr = directory_resource(
        path=dir, key_type=uuid.UUID, value_type=bytes, extension=".bin", writeable=True
    )
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


async def test_quote_unquote(tmp_path):
    dr = directory_resource(path=tmp_path, value_type=bytes, extension=".bin", writeable=True)
    key = "resource%identifier"
    value = b"body"
    await dr[key].put(value)
    await dr[key].get() == value
    await dr[key].delete()


async def test_invalid_directory():
    with TemporaryDirectory() as dir:
        dr = directory_resource(path=dir, value_type=bytes, extension=".bin", writeable=True)
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


async def test_decode_error(tmp_path):
    dir = str(tmp_path)
    dr = directory_resource(path=dir, value_type=int, extension=".int", writeable=True)
    await dr["1"].put(1)
    with open(f"{dir}/1.int", "w") as f:
        f.write("a")
    with pytest.raises(InternalServerError):
        await dr["1"].get()


async def test_quotable(tmp_path):
    dr = directory_resource(path=tmp_path, value_type=str, writeable=True)
    key = "1%2F2"
    value = "Value"
    await dr[key].put(value)
    assert await dr[key].get() == value


async def test_read_only(tmp_path):
    dir = str(tmp_path)
    path = f"{dir}/test.txt"
    content = b"content"
    with open(path, "wb") as file:
        file.write(content)
    fr = file_resource(path=path, value_type=bytes)
    assert await fr.get() == content
    with pytest.raises(AttributeError):
        await fr.put(b"nope")
    with pytest.raises(AttributeError):
        await fr.delete()


async def test_pagination(tmp_path):
    dir = str(tmp_path)
    count = 1000
    for n in range(0, count):
        with open(f"{dir}/{n:04d}.txt", "w") as file:
            file.write(f"{n:04d}")
    dr = directory_resource(path=dir, value_type=str, extension=".txt", writeable=True)
    page = await dr.get(limit=100)
    assert len(page.items) == 100
    assert page.remaining == count - 100
    page = await dr.get(limit=100, cursor=page.cursor)
    assert len(page.items) == 100
    assert page.remaining == count - 200
    assert len([v async for v in paginate(dr.get)]) == count


async def test_read_package_dir():
    import tests

    path = importlib.resources.files(tests) / "test_file"
    dr = directory_resource(path=path, value_type=str, extension=".txt")
    files = [f async for f in paginate(dr.get)]
    assert files == ["f1", "f2"]
    assert await dr["f1"].get() == "file1"
    assert await dr["f2"].get() == "file2"


async def test_crud_stream(tmp_path):
    dr = directory_resource(path=tmp_path, writeable=True)
    key = "hello_world.txt"
    value = b"\x01\x02\x03"
    await dr[key].put(BytesStream(value))
    result = await dr[key].get()
    assert result.content_type == "text/plain"  # from .txt extension
    assert b"".join([b async for b in result]) == value
    value = b"\x04\x05\x06"
    await dr[key].put(BytesStream(value))
    assert b"".join([b async for b in await dr[key].get()]) == value
    await dr[key].delete()
    assert (await dr.get()).items == []
    key = "foo.json"
    value = b'{"a": 123}'
    await dr[key].put(BytesStream(value))
    result = await dr[key].get()
    assert result.content_type == "application/json"  # from .json extension
    assert b"".join([b async for b in result]) == value


async def test_traversal_attack(tmp_path):
    main_path = tmp_path / "main"
    main_path.mkdir()
    subdir_path = main_path / "subdir"
    subdir_path.mkdir()
    with (tmp_path / "forbidden.txt").open("w") as file:
        file.write("forbidden")
    with (subdir_path / "forbidden.txt").open("w") as file:
        file.write("forbidden")
    with (main_path / "permitted.txt").open("w") as file:
        file.write("permitted")
    dr = directory_resource(path=main_path, value_type=str)
    with pytest.raises(NotFoundError):
        await dr["subdir/forbidden.txt"].get()
    with pytest.raises(NotFoundError):
        await dr["../forbidden.txt"].get()
    assert await dr["permitted.txt"].get() == "permitted"


async def test_content_length(tmp_path):
    file_size = 4096
    path = tmp_path / "block.bin"
    with path.open("wb") as file:
        file.write(b"x" * file_size)
    resource = file_resource(path)
    stream = await resource.get()
    assert stream.content_length == file_size
