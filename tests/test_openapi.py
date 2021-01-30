import pytest

import fondat.openapi
import json

from dataclasses import dataclass
from fondat.codec import get_codec, JSON, String
from fondat.resource import resource, operation, query, mutation
from fondat.validate import validate
from typing import Annotated as A, Optional
from uuid import UUID


@dataclass
class DB:
    foo: UUID


@dataclass
class DC:
    a: A[str, "this is a description"]
    b: int
    c: DB
    d: list[str]
    e: dict[str, str]
    f: Optional[str]
    g: str = None
    h: Optional[str] = None


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

    def __getitem__(self, key: str) -> Sub1:
        return Sub1(key)


@resource(tag="b")
class ResourceB:
    @operation
    async def get(self, param1: str) -> DC:
        """Get the B resource."""
        return DC("text", 1)

    @query
    async def query(self) -> list[str]:
        """Perform some query."""
        return ["a", "b", "c"]

    @mutation
    async def mutate(self, DC):
        """Perform some mutation."""
        pass


def test_generate():
    doc = fondat.openapi.generate_openapi(
        info=fondat.openapi.Info(title="title", version="version"), root=Root()
    )
    validate(doc, fondat.openapi.OpenAPI)
    print(json.dumps(get_codec(JSON, fondat.openapi.OpenAPI).encode(doc)))


@pytest.mark.asyncio
async def test_resource():
    resource = fondat.openapi.openapi_resource(
        info=fondat.openapi.Info(title="title", version="version"), root=Root()
    )
    validate(await resource.get(), fondat.openapi.OpenAPI)
