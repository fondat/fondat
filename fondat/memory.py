"""Module to store resource items in memory."""

from __future__ import annotations

import fondat.error

from collections import namedtuple
from copy import deepcopy
from fondat.codec import StringCodec
from fondat.error import NotFoundError
from fondat.http import AsBody
from fondat.resource import mutation, operation, resource
from fondat.stream import Stream
from time import time
from typing import Annotated, Generic, TypeVar


K = TypeVar("K")
V = TypeVar("V")


@resource
class MemoryResource(Generic[K, V]):
    """
    Represents a collection of items in memory.

    • key_type: type of item key
    • value_type: type of each item
    • size: maximum number of items to store  [unlimited]
    • evict: evict oldest item to make room for new item
    • expire: time to expire items in seconds  [unlimited]
    """

    _Item = namedtuple("_Item", "value,time")

    @resource
    class ItemResource(Generic[K, V]):
        """
        Represents an item in memory.

        Parameters:
        • memory: resource where item resides
        • key: item key in collection
        """

        def __init__(self, memory: MemoryResource, key: str):
            self.memory = memory
            self.key = key

        @operation
        async def get(self) -> V:
            """Get item."""
            item = self.memory._storage.get(self.key)
            if not item or (self.memory.expire and time() > item.time + self.memory.expire):
                raise NotFoundError
            return deepcopy(item.value)

        @operation
        async def put(self, value: Annotated[V, AsBody]) -> None:
            """Store item."""
            now = time()
            if self.memory.expire:  # purge expired entries
                for key in {
                    key
                    for key, item in self.memory._storage.items()
                    if item.time + self.memory.expire <= now
                }:
                    del self.memory._storage[key]
            if self.memory.size and self.memory.evict:  # evict oldest entry
                Oldest = namedtuple("Oldest", "key,time")
                self.memory._storage.pop(self.key, None)
                while len(self.memory._storage) >= self.memory.size:
                    oldest = None
                    for key, item in self.memory._storage.items():
                        if not oldest or item.time < oldest.time:
                            oldest = Oldest(key, item.time)
                    if oldest:
                        self.memory._storage.pop(oldest.key, None)
            if self.memory.size and len(self.memory._storage) >= self.memory.size:
                raise fondat.error.errors.InsufficientStorageError
            self.memory._storage[self.key] = MemoryResource._Item(value, now)

        @operation
        async def delete(self) -> None:
            """Delete item."""
            await self.get()  # ensure item exists and has not expired
            self.memory._storage.pop(self.key, None)

    def __init__(
        self,
        key_type: type[K],
        value_type: type[V],
        size: int | None = None,
        evict: bool = False,
        expire: int | float | None = None,
    ):
        self.key_type = key_type
        self._key_codec = StringCodec.get(key_type)
        if value_type is Stream:
            raise TypeError("value type not supported: {value_type}")
        self.value_type = value_type
        self.size = size
        self.evict = evict
        self.expire = expire
        self._storage: dict[str, "MemoryResource._Item"] = {}

    @operation
    async def get(self) -> list[K]:
        """Return collection item keys."""
        now = time()
        return [
            self._key_codec.decode(key)
            for key, item in self._storage.items()
            if not self.expire or item.time + self.expire <= now
        ]

    @mutation
    async def clear(self) -> None:
        """Remove all items from collection."""
        self._storage.clear()

    def __getitem__(self, key: K) -> "MemoryResource.ItemResource[K, V]":
        return self.ItemResource(self, self._key_codec.encode(key))
