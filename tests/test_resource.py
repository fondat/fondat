import asyncio
import fondat.resource
import pytest

from dataclasses import dataclass
from fondat.annotation import Description
from fondat.error import BadRequestError
from fondat.memory import MemoryResource
from fondat.resource import mutation, operation, query, resource
from typing import Annotated, Any
from uuid import UUID


@dataclass
class Body:
    key: UUID | None = None
    foo: str | None = None


@resource
class R1:

    i_am_r1 = "i_am_r1"

    @operation  # no params
    async def post(self, body: Body) -> UUID:
        return UUID("705d9048-97d6-4071-8359-3dbf0531fee9")

    @operation()  # empty params
    async def delete(self, id: UUID):
        pass

    @query
    async def q1(self) -> str:
        return self.i_am_r1

    @query
    async def q2(self, p1: Annotated[str, "p1"], p2: Annotated[str, Description("p2")]) -> str:
        return self.i_am_r1

    @mutation
    async def baz(self, x: str, y: int) -> None:
        pass


async def test_call():
    await R1().post(Body(foo="bar"))


async def test_call_invalid_body_type():
    with pytest.raises(BadRequestError):
        await R1().post(body=1)


async def test_query():
    assert await R1().q1() == R1.i_am_r1


async def test_valid_args():
    await R1().baz(x="hello", y=1)


async def test_invalid_args():
    with pytest.raises(BadRequestError):
        await R1().baz(x=1, y="hello")


@resource
class R2:
    @operation
    async def get(self) -> str:
        return "str"


def test_container():
    root = fondat.resource.ContainerResource(r1=R1(), r2=R2())
    assert root.r1.__class__ is R1
    assert root.r2.__class__ is R2


def test_nested_containers():
    c1 = fondat.resource.ContainerResource(r2=R2())
    c2 = fondat.resource.ContainerResource(c1=c1)
    assert fondat.resource.is_resource(c2)
    assert fondat.resource.is_resource(c2.c1)
    assert fondat.resource.is_resource(c2.c1.r2)
    assert fondat.resource.is_operation(c2.c1.r2.get)


def test_invalid_method():
    with pytest.raises(TypeError):

        class R:
            @operation
            async def invalid_method_name(self) -> None:
                pass


async def test_operation_cache():
    cache = MemoryResource(key_type=bytes, value_type=Any, expire=0.1)

    @resource
    class Resource:
        def __init__(self):
            self.counter = 0

        @operation(cache=cache)
        async def post(self) -> int:
            self.counter += 1
            return self.counter

    r = Resource()
    assert (await r.post()) == 1
    assert (await r.post()) == 1
    await asyncio.sleep(0.1)
    assert (await r.post()) == 2


async def test_operation_cache_defaults():
    cache = MemoryResource(key_type=bytes, value_type=Any)

    @resource
    class Resource:
        def __init__(self):
            self.counter = 0

        @operation(cache=cache)
        async def post(self, s: str = "foo") -> int:
            self.counter += 1
            return self.counter

    r = Resource()
    assert (await r.post("foo")) == 1
    assert (await r.post()) == 1
    assert (await r.post("bar")) == 2
    await cache.clear()
    assert (await r.post()) == 3
    assert (await r.post("foo")) == 3
    assert (await r.post("bar")) == 4
