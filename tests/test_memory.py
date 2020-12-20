import pytest

from dataclasses import make_dataclass
from fondat.error import BadRequestError, NotFoundError
from fondat.memory import memory_resource
from fondat.resource import operation
from time import sleep
from uuid import uuid4


pytestmark = pytest.mark.asyncio


async def test_gpdl_dict():
    DC = make_dataclass("DC", [("foo", str), ("bar", int)])
    resource = memory_resource(key_type=str, value_type=DC)
    id = "id1"
    r1 = DC("hello", 1)
    await resource[id].put(r1)
    r2 = await resource[id].get()
    assert r1 == r2
    r1.bar = 2
    await resource[id].put(r1)
    r2 = await resource[id].get()
    assert r1 == r2
    await resource[id].delete()
    assert await resource.get() == []


async def test_gpdl_str():
    resource = memory_resource(key_type=str, value_type=str)
    data = "你好，世界!"
    id = "hello_world"
    await resource[id].put(data)
    assert await resource.get() == [id]
    assert await resource[id].get() == data
    data = "Goodbye world!"
    await resource[id].put(data)
    assert await resource[id].get() == data
    await resource[id].delete()
    assert await resource.get() == []


async def test_gpdl_bytes():
    resource = memory_resource(key_type=str, value_type=bytes)
    data = b"\x00\x0e\0x01\0x01\0x00"
    id = "binary"
    await resource[id].put(data)
    assert await resource.get() == [id]
    assert await resource[id].get() == data
    data = bytes((1, 2, 3, 4, 5))
    await resource[id].put(data)
    assert await resource[id].get() == data
    await resource[id].delete()
    assert await resource.get() == []


async def test_get_notfound():
    resource = memory_resource(key_type=str, value_type=str)
    with pytest.raises(NotFoundError):
        await resource["1"].get()


async def test_delete_notfound():
    resource = memory_resource(key_type=str, value_type=str)
    with pytest.raises(NotFoundError):
        await resource["1"].delete()


async def test_clear():
    resource = memory_resource(key_type=str, value_type=str)
    await resource["1"].put("foo")
    await resource["2"].put("bar")
    assert len(await resource.get()) == 2
    await resource.clear()
    assert len(await resource.get()) == 0


async def test_size_limit():
    resource = memory_resource(key_type=str, value_type=str, size=1)
    await resource["1"].put("foo")
    with pytest.raises(BadRequestError):
        await resource["2"].put("bar")


async def test_size_evict():
    resource = memory_resource(key_type=str, value_type=str, size=2, evict=True)
    await resource["1"].put("foo")
    await resource["2"].put("bar")
    await resource["3"].put("qux")
    assert set(await resource.get()) == {"2", "3"}


async def test_ttl():
    resource = memory_resource(key_type=str, value_type=str, ttl=0.1)
    await resource["1"].put("foo")
    await resource["1"].get()
    sleep(0.2)
    with pytest.raises(NotFoundError):
        await resource["1"].get()
