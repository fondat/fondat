import pytest

import fondat.openapi
import json

from datetime import date
from fondat.codec import get_codec, JSON, String
from fondat.http import InCookie, InHeader, InQuery
from fondat.openapi import generate_openapi, openapi_resource
from fondat.resource import resource, operation, query, mutation, container_resource
from fondat.types import Description, Example, NoneType, dataclass
from fondat.validation import validate
from typing import Annotated, Optional, Union
from uuid import UUID


@dataclass
class DB:
    foo: UUID


@dataclass
class DC:
    a: Annotated[str, "this is a description"]
    b: Annotated[int, Description("this is also a description")]
    c: DB
    d: list[str]
    e: dict[str, str]
    f: Optional[str]
    g: str = None
    h: Optional[str] = None
    i: Annotated[str, Example("aaa")]
    j: Annotated[date, Example(date(2021, 1, 1))]
    #    k: Annotated[list[date], Example([date(2021, 1, 1), date(2021, 12, 31)])]
    #    k: Annotated[list[str], Example(["a", "b"])]
    k: Annotated[int, Example(1)]


@resource
class Root:
    def __init__(self):
        self.a = ResourceA()
        self.b = ResourceB()


@resource(tag="a")
class Sub2:
    def __init__(self, key: str):
        self.key = key

    @operation
    async def get(self) -> str:
        """Get the A subordinate, subordinate resource."""
        return key


@resource(tag="a")
class Sub1:
    def __init__(self, key: str):
        self.key = key

    @operation
    async def get(self) -> str:
        """Get the A subordinate resource."""
        return key

    def __getitem__(self, key: str) -> Sub2:
        return Sub2(key)


@resource(tag="a")
class ResourceA:
    @operation
    async def get(self) -> str:
        """Get the A resource."""
        return "Hello from Resource A"

    @mutation
    async def return_none(self) -> NoneType:
        """Method explicitly does not return a value."""
        pass

    @mutation
    async def return_optional(self) -> Optional[int]:
        """Method optionally returns an int."""
        return None

    @mutation
    async def return_union(self) -> Union[int, float, None]:
        """Method returns int, float or None."""
        return None

    def __getitem__(self, key: str) -> Sub1:
        return Sub1(key)


@resource(tag="b")
class ResourceB:
    @operation
    async def get(self, param1: str) -> DC:
        """Get the B resource."""
        return DC("text", 1)

    @query
    async def query1(self, param2: str = "this is a default value") -> list[str]:
        """Perform some query."""
        return ["a", "b", "c"]

    @query
    async def query2(
        self,
        p1: Annotated[str, "the p1 parameter"],
        p2: Annotated[str, Description("the p2 parameter")],
    ) -> str:
        return f"{p1}+{p2}"

    @query
    async def query3(
        self,
        p1: Annotated[str, InHeader("X-Param1")],
        p2: Annotated[str, InCookie("P2-Cookie")],
    ) -> str:
        return "query3"

    @mutation
    async def mutate(self, DC):
        """Perform some mutation."""
        pass


def test_generate():
    doc = generate_openapi(
        resource=Root(), info=fondat.openapi.Info(title="title", version="version")
    )
    validate(doc, fondat.openapi.OpenAPI)
    js = get_codec(JSON, fondat.openapi.OpenAPI).encode(doc)
    print(json.dumps(js))


@pytest.mark.asyncio
async def test_resource():
    info = fondat.openapi.Info(title="title", version="version")
    root = Root()
    resource = openapi_resource(resource=root, info=info)
    result = await resource.get()
    validate(result, fondat.openapi.OpenAPI)
    assert generate_openapi(resource=root, info=info) == result


@pytest.mark.asyncio
async def test_nested_containers():
    @resource
    class R1:
        @operation
        async def get(self) -> str:
            return "str"

    c1 = container_resource({"r1": R1()})
    c2 = container_resource({"c1": c1})
    info = fondat.openapi.Info(title="title", version="version")
    doc = generate_openapi(resource=c2, info=info)
    validate(doc, fondat.openapi.OpenAPI)
    c1_r1 = doc.paths.get("/c1/r1")
    assert c1_r1 is not None
    assert c1_r1.get is not None
    js = get_codec(JSON, fondat.openapi.OpenAPI).encode(doc)


def test_openapi_generate_openapi_specification():
    info = fondat.openapi.Info(title="title", version="version")
    root = openapi_resource(resource=None, info=info, publish=True)
    result = generate_openapi(resource=root, info=info)
    validate(result, fondat.openapi.OpenAPI)
    js = get_codec(JSON, fondat.openapi.OpenAPI).encode(result)
    # print(json.dumps(js))
