import pytest

import fondat.openapi
import json

from dataclasses import dataclass
from fondat.codec import get_codec, JSON, String
from fondat.resource import resource, operation, query, mutation
from fondat.validate import validate
from typing import Optional
from uuid import UUID


@dataclass
class DB:
    foo: UUID


@dataclass
class DC:
    a: str
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

@resource
class ResourceA:
    @operation
    async def get(self) -> str:
        """Get the A resource."""
        return "Hello from Resource A"

@resource
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
        info=fondat.openapi.Info(title="title", version="version"),
        root=Root()
    )
    validate(doc, fondat.openapi.OpenAPI)
    print(json.dumps(get_codec(JSON, fondat.openapi.OpenAPI).encode(doc)))


@pytest.mark.asyncio
async def test_resource():
    resource = fondat.openapi.openapi_resource(
        info=fondat.openapi.Info(title="title", version="version"),
        root=Root()
    )
    validate(await resource.get(), fondat.openapi.OpenAPI)

