import pytest

import fondat.resource

from dataclasses import dataclass
from fondat.annotation import Description
from fondat.error import BadRequestError
from fondat.resource import resource, operation, query, mutation
from typing import Annotated, Optional
from uuid import UUID


pytestmark = pytest.mark.asyncio


@dataclass
class Body:
    key: Optional[UUID] = None
    foo: Optional[str] = None


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
    root = fondat.resource.container_resource({"r1": R1(), "r2": R2()})
    assert root.r1.__class__ is R1
    assert root.r2.__class__ is R2


def test_nested_containers():
    c1 = fondat.resource.container_resource({"r2": R2()})
    c2 = fondat.resource.container_resource({"c1": c1})
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
