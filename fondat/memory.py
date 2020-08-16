"""Module to store resource items in memory."""

import copy
import datetime
import roax.resource
import threading
import wrapt


@wrapt.decorator
def _with_lock(wrapped, instance, args, kwargs):
    with instance._lock:
        return wrapped(*args, **kwargs)


_now = lambda: datetime.datetime.now(tz=datetime.timezone.utc)


class MemoryResource(roax.resource.Resource):
    """
    In-memory resource; all items are stored in a dictionary. Resource item
    identifiers must be hashable values.

    Parameters:
    • size: Maximum number of items to store.  [unlimited]
    • evict: Should oldest item be evicted to make room for a new item.  [False]
    • ttl: Maximum item time to live, in seconds.  [unlimited]
    • name: Short name of the resource.  [class name in lower case]
    • description: Short description of the resource.  [resource docstring]
    """

    def __init__(self, size=None, evict=False, ttl=None, name=None, description=None):
        super().__init__(name, description)
        self.size = size
        self.evict = evict
        self._ttl = datetime.timedelta(seconds=ttl) if ttl else None
        self._lock = threading.Lock()
        self._entries = {}

    @_with_lock  # iterates over and modifies entries
    def create(self, id, _body):
        """Create a resource item."""
        if self._ttl:  # purge expired entries
            now = _now()
            self._entries = {k: v for k, v in self._entries if v[0] + self._ttl <= now}
        if id in self._entries:
            raise roax.resource.Conflict(f"{self.name} item already exists")
        if self.size and len(self._entries) >= self.size:
            if self.evict:  # evict oldest entry
                oldest = None
                for key, entry in self._entries.items():
                    if not oldest or entry[0] < oldest[0]:
                        oldest = (entry[0], key)
                if oldest:
                    del self._entries[oldest[1]]
        if self.size and len(self._entries) >= self.size:
            raise roax.resource.BadRequest(f"{self.name} item size limit reached")
        self._entries[id] = (_now(), copy.deepcopy(_body))
        return {"id": id}

    def read(self, id):
        """Read a resource item."""
        return copy.deepcopy(self.__get(id)[1])

    @_with_lock  # modifies entries
    def update(self, id, _body):
        """Update a resource item."""
        old = self.__get(id)
        self._entries[id] = (old[0], copy.deepcopy(_body))

    @_with_lock  # modifies entries
    def delete(self, id):
        """Delete a resource item."""
        self.__get(id)  # ensure item exists and not expired
        self._entries.pop(id, None)

    @_with_lock  # iterates over entries
    def list(self):
        """Query that returns a list of all resource item identifiers."""
        return [
            k
            for k, v in self._entries.items()
            if not self._ttl or v[0] + self._ttl <= now
        ]

    @_with_lock  # modifies entries
    def clear(self):
        """Action that removes all resource items."""
        self._entries.clear()

    def __get(self, id):
        result = self._entries.get(id)
        if not result or (self._ttl and _now() > result[0] + self._ttl):
            raise roax.resource.NotFound(f"{self.name} item not found")
        return result
