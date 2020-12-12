"""Module to store resource items in memory."""

from __future__ import annotations

import collections
import collections.abc
import copy
import datetime
import fondat.security
import threading
import typing

from fondat.resource import resource, operation, NotFound, BadRequest
from fondat.typing import affix_type_hints


_now = lambda: datetime.datetime.now(tz=datetime.timezone.utc)

_delta = lambda s: datetime.timedelta(seconds=s)

_Item = collections.namedtuple("Item", "data,time")

_Oldest = collections.namedtuple("Oldest", "key,time")


def memory_resource(
    key_type: type,
    value_type: type,
    size: int = None,
    evict: bool = False,
    ttl: int = None,
    security: collections.abc.Iterable[fondat.security.SecurityRequirement] = None,
):
    """
    Return a new resource class that stores items in a dictionary.

    Parameters:
    • key_type: Type for the key for each item.
    • value_type: Type for value stored in each item.
    • size: Maximum number of items to store.  [unlimited]
    • evict: Should oldest item be evicted to make room for a new item.
    • ttl: Maximum item time to live, in seconds.  [unlimited]
    • security: Security requirements to apply to all operations.
    """

    @resource
    class Container:
        def __init__(self):
            self.storage = {}
            self.lock = threading.Lock()

        def __getitem__(self, key: key_type) -> Item:
            return Item(self, key)

        @operation(security=security)
        async def get(self) -> list[key_type]:
            """Return list of item keys."""
            now = _now()
            with self.lock:
                return [
                    key
                    for key, item in self.storage.items()
                    if not self.ttl or item.time + _delta(self.ttl) <= now
                ]

        @operation(type="mutation", security=security)
        async def clear(self) -> None:
            """Remove all items."""
            with self.lock:
                self.storage.clear()

    Container.key_type = key_type
    Container.value_type = value_type
    Container.size = size
    Container.evict = evict
    Container.ttl = ttl

    @resource
    class Item:
        def __init__(self, container: Container, key: key_type):
            self.container = container
            self.key = key

        @operation(security=security)
        async def get(self) -> value_type:
            """Read item."""
            item = self.container.storage.get(self.key)
            if item is None or (
                self.container.ttl and _now() > item.time + _delta(self.container.ttl)
            ):
                raise NotFound
            return item.data

        @operation(security=security)
        async def put(self, data: value_type) -> None:
            """Write item."""
            with self.container.lock:
                now = _now()
                if self.container.ttl:  # purge expired entries; pay the price on puts
                    for key in {
                        k
                        for k, v in self.container.storage.items()
                        if v.time + _delta(container.ttl) <= now
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
                if (
                    self.container.size
                    and len(self.container.storage) >= self.container.size
                ):
                    raise BadRequest("item size limit reached")
                self.container.storage[self.key] = _Item(copy.deepcopy(data), now)

        @operation(security=security)
        async def delete(self):
            """Delete item."""
            await self.get()  # ensure item exists and has not expired
            with self.container.lock:
                self.container.storage.pop(self.key, None)

    affix_type_hints(Container, localns=locals())
    affix_type_hints(Item, localns=locals())

    Container.__name__ = Container.__qualname__ = "MemoryResource"

    return Container
