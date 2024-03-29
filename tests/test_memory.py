import fondat.error
import pytest

from dataclasses import make_dataclass
from fondat.error import NotFoundError
from fondat.memory import MemoryResource
from time import sleep


async def test_gpdl_dict():
    DC = make_dataclass("DC", [("foo", str), ("bar", int)])
    resource = MemoryResource(key_type=str, value_type=DC)
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
    resource = MemoryResource(key_type=str, value_type=str)
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
    resource = MemoryResource(key_type=str, value_type=bytes)
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
    resource = MemoryResource(key_type=str, value_type=str)
    with pytest.raises(NotFoundError):
        await resource["1"].get()


async def test_delete_notfound():
    resource = MemoryResource(key_type=str, value_type=str)
    with pytest.raises(NotFoundError):
        await resource["1"].delete()


async def test_clear():
    resource = MemoryResource(key_type=str, value_type=str)
    await resource["1"].put("foo")
    await resource["2"].put("bar")
    assert len(await resource.get()) == 2
    await resource.clear()
    assert len(await resource.get()) == 0


async def test_size_limit():
    resource = MemoryResource(key_type=str, value_type=str, size=1)
    await resource["1"].put("foo")
    with pytest.raises(fondat.error.errors.InsufficientStorageError):
        await resource["2"].put("bar")


async def test_size_evict():
    resource = MemoryResource(key_type=str, value_type=str, size=2, evict=True)
    await resource["1"].put("foo")
    await resource["2"].put("bar")
    await resource["3"].put("qux")
    assert await resource.get() == ["2", "3"]


async def test_expire_get():
    resource = MemoryResource(key_type=str, value_type=str, expire=0.01)
    await resource["1"].put("foo")
    await resource["1"].get()
    sleep(0.01)
    with pytest.raises(NotFoundError):
        await resource["1"].get()


async def test_expire_put():
    resource = MemoryResource(key_type=str, value_type=str, expire=0.01)
    await resource["1"].put("foo")
    await resource["1"].get()
    await resource["2"].put("bar")
    sleep(0.01)
    await resource["3"].put("baz")
    with pytest.raises(KeyError):
        resource._storage["1"]
    with pytest.raises(KeyError):
        resource._storage["2"]
    assert await resource["3"].get() == "baz"


async def test_gpdl_non_hashable_keys():
    KeyType = dict[str, str]
    resource = MemoryResource(key_type=KeyType, value_type=str)
    assert len(await resource.get()) == 0
    key = {"id": "id1"}
    v1 = "foo"
    await resource[key].put(v1)
    assert len(await resource.get()) == 1
    v2 = await resource[key].get()
    assert v1 == v2
    v1 = "bar"
    await resource[key].put(v1)
    v2 = await resource[key].get()
    assert v1 == v2
    await resource[key].delete()
    assert await resource.get() == []
