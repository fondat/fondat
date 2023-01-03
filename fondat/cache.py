"""Fondat cache module."""

import hashlib
import json

from typing import Any, Protocol, runtime_checkable


JSON = Any


@runtime_checkable
class CacheResource(Protocol):
    """Prototype cache resource."""

    def __getitem__(self, key: JSON) -> "EntryResource":  # subordinate
        ...


@runtime_checkable
class EntryResource(Protocol):
    """Prototype cache entry resource."""

    async def get(self) -> JSON:
        ...

    async def put(self, value: JSON) -> None:
        ...

    async def delete(self) -> None:
        ...


def hash_json(value: JSON) -> bytes:
    """
    Return a deterministic, unique hash value for a given JSON object model value.
    This can be useful for generating a cache entry key.
    """
    return hashlib.sha256(
        json.dumps(value, separators=(",", ":"), sort_keys=True).encode()
    ).digest()
