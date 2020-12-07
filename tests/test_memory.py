import fondat.resource as r
import fondat.schema as s
import pytest

from fondat.memory import memory_resource
from fondat.resource import BadRequest, Conflict, NotFound, operation, link
from time import sleep
from uuid import uuid4


pytestmark = pytest.mark.asyncio


async def test_gpdl_dict():
    schema = s.dict({"foo": s.str(), "bar": s.int()}, "foo bar")
    mr = memory_resource(schema)()
    id = "id1"
    r1 = {"foo": "hello", "bar": 1}
    await mr[id].put(r1)
    r2 = await mr[id].get()
    assert r1 == r2
    r1["bar"] = 2
    await mr[id].put(r1)
    r2 = await mr[id].get()
    assert r1 == r2
    await mr[id].delete()
    assert await mr.get() == []


async def test_gpdl_str():
    mr = memory_resource(s.str())()
    data = "你好，世界!"
    id = "hello_world"
    await mr[id].put(data)
    assert await mr.get() == [id]
    assert await mr[id].get() == data
    data = "Goodbye world!"
    await mr[id].put(data)
    assert await mr[id].get() == data
    await mr[id].delete()
    assert await mr.get() == []


async def test_gpdl_bytes():
    mr = memory_resource(s.bytes())()
    data = b"\x00\x0e\0x01\0x01\0x00"
    id = "binary"
    await mr[id].put(data)
    assert await mr.get() == [id]
    assert await mr[id].get() == data
    data = bytes((1, 2, 3, 4, 5))
    await mr[id].put(data)
    assert await mr[id].get() == data
    await mr[id].delete()
    assert await mr.get() == []


async def test_get_notfound():
    mr = memory_resource(s.str())()
    with pytest.raises(NotFound):
        await mr["1"].get()


async def test_delete_notfound():
    mr = memory_resource(s.str())()
    with pytest.raises(NotFound):
        await mr["1"].delete()


async def test_clear():
    mr = memory_resource(s.str())()
    await mr["1"].put("foo")
    await mr["2"].put("bar")
    assert len(await mr.get()) == 2
    await mr.clear()
    assert len(await mr.get()) == 0


async def test_size_limit():
    mr = memory_resource(s.str(), size=1)()
    await mr["1"].put("foo")
    with pytest.raises(BadRequest):
        await mr["2"].put("bar")


async def test_size_evict():
    mr = memory_resource(s.str(), size=2, evict=True)()
    await mr["1"].put("foo")
    await mr["2"].put("bar")
    await mr["3"].put("qux")
    assert set(await mr.get()) == {"2", "3"}


async def test_ttl():
    mr = memory_resource(s.str(), ttl=0.1)()
    await mr["1"].put("foo")
    await mr["1"].get()
    sleep(0.2)
    with pytest.raises(NotFound):
        await mr["1"].get()
