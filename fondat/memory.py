"""Module to store resource items in memory."""

import collections
import copy
import datetime
import fondat.schema as s
import threading
import wrapt

from fondat.resource import resource, operation, NotFound, BadRequest


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
        def __init__(self):
            self._items = {}
            self._ttl = datetime.timedelta(seconds=ttl) if ttl else None
            self._lock = threading.Lock()

        @operation(security=security)
        async def get(self, id: s.str()) -> schema:
            """Read item."""
            item = self._items.get(id)
            if item is None or (self._ttl and _now() > item.time + self._ttl):
                raise NotFound("item not found")
            return item.data

        @operation(security=security)
        async def put(self, id: s.str(), data: schema):
            """Write item."""
            with self._lock:
                now = _now()
                if self._ttl:  # purge expired entries
                    for id in {
                        k for k, v in self._items.items() if v.time + self._ttl <= now
                    }:
                        del self._items[id]
                while evict and size and len(self._items) >= size:  # evict oldest entry
                    oldest = None
                    for _id, _item in self._items.items():
                        if not oldest or _item.time < oldest.time:
                            oldest = _Oldest(_id, _item.time)
                    if oldest:
                        del self._items[oldest.id]
                if size and len(self._items) >= size:
                    raise BadRequest("item size limit reached")
                self._items[id] = _Item(copy.deepcopy(data), now)

        @operation(security=security)
        async def delete(self, id: s.str()):
            """Delete item."""
            await self.get(id)  # ensure item exists and not expired
            with self._lock:
                self._items.pop(id, None)

        @operation(type="query", security=security)
        async def list(self) -> s.list(s.str()):
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

    return MemoryResource
