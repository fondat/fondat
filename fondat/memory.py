"""Module to store resource items in memory."""

import collections
import copy
import datetime
import fondat.schema as s
import threading
import wrapt

from fondat.resource import resource, operation, link, NotFound, BadRequest


_now = lambda: datetime.datetime.now(tz=datetime.timezone.utc)

_Item = collections.namedtuple("Item", "data,time")

_Oldest = collections.namedtuple("Oldest", "id,time")


def memory_resource(schema, size=None, evict=False, ttl=None, security=None):
    """
    Return a new resource class that stores items in a dictionary.

    Parameters:
    • schema: Schema of items.
    • size: Maximum number of items to store.  [unlimited]
    • evict: Should oldest item be evicted to make room for a new item. 
    • ttl: Maximum item time to live, in seconds.  [unlimited]
    • security: Security requirements to apply to all operations.
    """

    @resource
    class MemoryResource:

        @resource
        class Item:

            def __init__(self, mr, id):
                self._mr = mr
                self.id = id

            @operation(security=security)
            async def get(self) -> schema:
                """Read item."""
                item = self._mr._items.get(self.id)
                if item is None or (self._mr._ttl and _now() > item.time + self._mr._ttl):
                    raise NotFound
                return item.data

            @operation(security=security)
            async def put(self, data: schema):
                """Write item."""
                with self._mr._lock:
                    now = _now()
                    if self._mr._ttl:  # purge expired entries; pay the price on puts
                        for id in {
                            k for k, v in self._mr._items.items() if v.time + self._mr._ttl <= now
                        }:
                            del self._mr._items[self.id]
                    while evict and size and len(self._mr._items) >= size:  # evict oldest entry
                        oldest = None
                        for _id, _item in self._mr._items.items():
                            if not oldest or _item.time < oldest.time:
                                oldest = _Oldest(_id, _item.time)
                        if oldest:
                            del self._mr._items[oldest.id]
                    if size and len(self._mr._items) >= size:
                        raise BadRequest("item size limit reached")
                    self._mr._items[self.id] = _Item(copy.deepcopy(data), now)

            @operation(security=security)
            async def delete(self):
                """Delete item."""
                await self.get()  # ensure item exists and has not expired
                with self._mr._lock:
                    self._mr._items.pop(self.id, None)

        def __init__(self):
            self._items = {}
            self._ttl = datetime.timedelta(seconds=ttl) if ttl else None
            self._lock = threading.Lock()

        @operation(security=security)
        async def get(self) -> s.list(s.str()):
            """Return list of item identifiers."""
            now = _now()
            with self._lock:
                return [
                    id
                    for id, item in self._items.items()
                    if not self._ttl or item.time + self._ttl <= now
                ]

        @operation(type="mutation", security=security)
        async def clear(self):
            """Remove all items."""
            with self._lock:
                self._items.clear()

        @link
        def __getitem__(self, id: s.str()) -> Item:
            return MemoryResource.Item(self, id)

    return MemoryResource
