"""Module to store resource items in memory."""

import wrapt

from .resource import BadRequest, Resource, Conflict, NotFound
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from threading import Lock


@wrapt.decorator
def _with_lock(wrapped, instance, args, kwargs):
    with instance._lock:
        return wrapped(*args, **kwargs)


_now = lambda: datetime.now(tz=timezone.utc)


class MemoryResource(Resource):
    """
    Base class for an in-memory resource; all items are stored in a dictionary.
    Resource item identifiers must be hashable values.
    """

    def __init__(self, size=None, evict=False, ttl=None, name=None, description=None):
        """
        Initialize in-memory resource.

        :param size: Maximum number of items to store.  [unlimited]
        :param evict: Should oldest item be evicted to make room for a new item.  [False]
        :param ttl: Maximum item time to live, in seconds.  [unlimited]
        :param name: Short name of the resource.  [class name in lower case]
        :param description: Short description of the resource.  [resource docstring]
        """
        super().__init__(name, description)
        self.size = size
        self.evict = evict
        self._ttl = timedelta(seconds=ttl) if ttl else None
        self._lock = Lock()
        self._entries = {}

    @_with_lock  # iterates over and modifies entries
    def create(self, id, _body):
        """Create a resource item."""
        if self._ttl:  # purge expired entries
            now = _now()
            self._entries = {k: v for k, v in self._entries if v[0] + self._ttl <= now }
        if id in self._entries:
            raise Conflict("{} item already exists".format(self.name))
        if self.size and len(self._entries) >= self.size:
            if self.evict:  # evict oldest entry
                oldest = None
                for key, entry in self._entries.items():
                    if not oldest or entry[0] < oldest[0]:
                        oldest = (entry[0], key)
                if oldest:
                    del self._entries[oldest[1]]
        if self.size and len(self._entries) >= self.size:
            raise BadRequest("{} item size limit reached".format(self.name))
        self._entries[id] = (_now(), deepcopy(_body))
        return {"id": id}

    def read(self, id):
        """Read a resource item."""
        return deepcopy(self.__get(id)[1])

    @_with_lock  # modifies entries
    def update(self, id, _body):
        """Update a resource item."""
        old = self.__get(id)
        self._entries[id] = (old[0], deepcopy(_body))

    @_with_lock  # modifies entries
    def delete(self, id):
        """Delete a resource item."""
        self.__get(id)  # ensure item exists and not expired
        self._entries.pop(id, None)

    @_with_lock  # iterates over entries
    def list(self):
        """Query that returns a list of all resource item identifiers."""
        return [k for k, v in self._entries.items() if not self._ttl or v[0] + self._ttl <= now]

    @_with_lock  # modifies entries
    def clear(self):
        """Action that removes all resource items."""
        self._entries.clear()

    def __get(self, id):
        result = self._entries.get(id)
        if not result or (self._ttl and _now() > result[0] + self._ttl):
            raise NotFound("{} item not found".format(self.name))
        return result
