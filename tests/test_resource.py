import pytest

import fondat.resource

from dataclasses import dataclass
from fondat.resource import resource, operation, query, mutation
from typing import Optional
from uuid import UUID


pytestmark = pytest.mark.asyncio


@dataclass
class Body:
    key: Optional[UUID] = None
    foo: str = None


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
    async def qux(self) -> str:
        return self.i_am_r1

    @mutation
    async def baz(self, x: str, y: int) -> None:
        pass


async def test_call():
    await R1().post(Body(foo="bar"))


async def test_call_invalid_type():
    with pytest.raises(TypeError):
        await R1().post(1)


async def test_outer_scope():
    assert await R1().qux.get() == R1.i_am_r1


async def test_inner_call():
    r1 = R1()
    assert await r1.qux.get() == await r1.qux()


async def test_valid_args():
    await R1().baz("hello", 1)


async def test_invalid_args():
    with pytest.raises(TypeError):
        await R1().baz(1, "hello")


@resource
class R2:
    @operation
    async def get(self) -> str:
        return "str"


def test_container():
    root = fondat.resource.Container(
        {
            "r1": R1(),
            "r2": R2(),
        }
    )
    assert root.r1.__class__ is R1
    assert root.r2.__class__ is R2
