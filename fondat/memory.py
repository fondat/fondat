"""Module to store resource items in memory."""

from __future__ import annotations

import copy
import threading

from collections import namedtuple
from collections.abc import Iterable
from datetime import datetime, timedelta, timezone
from fondat.error import BadRequestError, NotFoundError
from fondat.http import AsBody
from fondat.resource import resource, operation, mutation
from fondat.security import SecurityRequirement
from fondat.types import affix_type_hints
from typing import Annotated, Union


_now = lambda: datetime.now(tz=timezone.utc)

_delta = lambda s: timedelta(seconds=s)

_Item = namedtuple("Item", "value,time")

_Oldest = namedtuple("Oldest", "key,time")


def memory_resource(
    key_type: type,
    value_type: type,
    size: int = None,
    evict: bool = False,
    expire: Union[int, float] = None,
    publish: bool = True,
    security: Iterable[SecurityRequirement] = None,
):
    """
    Return a new resource that stores items in memory.

    Parameters:
    • key_type: type for the key for each item
    • value_type: type for value stored in each item
    • size: maximum number of items to store  [unlimited]
    • evict: should oldest item be evicted to make room for a new item
    • expire: expire time for each value in seconds  [unlimited]
    • publish: publish the operation in documentation
    • security: security requirements to apply to all operations
    """

    @resource
    class MemoryResource:
        def __init__(self):
            self.storage = {}
            self.lock = threading.Lock()

        def __getitem__(self, key: key_type) -> Item:
            return Item(self, key)

        @operation(publish=publish, security=security)
        async def get(self) -> list[key_type]:
            """Return list of item keys."""
            now = _now()
            with self.lock:
                return [
                    key
                    for key, item in self.storage.items()
                    if not self.expire or item.time + _delta(self.expire) <= now
                ]

        @mutation(publish=publish, security=security)
        async def clear(self) -> None:
            """Remove all items."""
            with self.lock:
                self.storage.clear()

    MemoryResource.key_type = key_type
    MemoryResource.value_type = value_type
    MemoryResource.size = size
    MemoryResource.evict = evict
    MemoryResource.expire = expire

    @resource
    class Item:
        def __init__(self, container: MemoryResource, key: key_type):
            self.container = container
            self.key = key

        @operation(publish=publish, security=security)
        async def get(self) -> value_type:
            """Read item."""
            item = self.container.storage.get(self.key)
            if item is None or (
                self.container.expire and _now() > item.time + _delta(self.container.expire)
            ):
                raise NotFoundError
            return item.value

        @operation(publish=publish, security=security)
        async def put(self, value: Annotated[value_type, AsBody]) -> None:
            """Write item."""
            with self.container.lock:
                now = _now()
                if self.container.expire:  # purge expired entries; pay the price on puts
                    for key in {
                        k
                        for k, v in self.container.storage.items()
                        if v.time + _delta(container.expire) <= now
                    }:
                        del self.container.storage[self.key]
                while (
                    self.container.evict
                    and self.container.size
                    and len(self.container.storage) >= self.container.size
                ):  # evict oldest entry
                    oldest = None
                    for _key, _item in self.container.storage.items():
                        if not oldest or _item.time < oldest.time:
                            oldest = _Oldest(_key, _item.time)
                    if oldest:
                        del self.container.storage[oldest.key]
                if self.container.size and len(self.container.storage) >= self.container.size:
                    raise BadRequestError("item size limit reached")
                self.container.storage[self.key] = _Item(copy.deepcopy(value), now)

        @operation(publish=publish, security=security)
        async def delete(self):
            """Delete item."""
            await self.get()  # ensure item exists and has not expired
            with self.container.lock:
                self.container.storage.pop(self.key, None)

    affix_type_hints(MemoryResource, localns=locals())
    affix_type_hints(Item, localns=locals())

    MemoryResource.__qualname__ = "MemoryResource"

    return MemoryResource()
