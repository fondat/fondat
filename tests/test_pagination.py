from base64 import b64decode, b64encode
from dataclasses import dataclass
from fondat.pagination import Page, paginate
from fondat.resource import operation, resource


@dataclass
class Item:
    value: int


@resource
class Resource:
    def __init__(self, count: int, limit: int):
        self.values = [Item(n) for n in range(0, count)]
        self.limit = limit

    @operation
    async def get(self, limit: int | None = None, cursor: bytes | None = None) -> Page[Item]:
        start = int(b64decode(cursor).decode()) if cursor else 0
        stop = min(start + self.limit, len(self.values))
        return Page[Item](
            items=self.values[start:stop],
            cursor=(b64encode(str(stop).encode()) if stop < len(self.values) else None),
        )


resource = Resource(100, 10)


async def test_page():
    page = await resource.get()
    last = page.items[-1].value
    page = await resource.get(cursor=page.cursor)
    assert page.items[0].value == last + 1


async def test_paginate():
    items = [item async for item in paginate(resource.get)]
    assert len(items) == len(resource.values)
    assert items[0].value == 0
    assert items[len(items) - 1].value == len(items) - 1
